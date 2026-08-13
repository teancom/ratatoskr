// pattern: Imperative Shell
//
// End-to-end tests for the namespaced-resource surface. Exercises
// `resources/list`, `resources/templates/list`, and `resources/read`
// across a full gateway setup with two fake upstreams running in-process.

mod common;
//
// The pinned properties:
//   (a) Two upstreams with the same bare URI produce distinct namespaced
//       URIs downstream — namespacing actually disambiguates.
//   (b) `resources/read` dispatches to the correct upstream based on
//       the URI's authority (lua_name). The wrong upstream never sees
//       the call.
//   (c) An unknown prefix returns an error, not a silent pass-through
//       or an empty result.
//
// Plus supplementary properties:
//   - Resources and templates aggregate across all registered upstreams.
//   - A non-`mcp-proxy://` URI handed to `resources/read` is rejected
//     with a decode error, not a lookup miss.
//   - One upstream's error doesn't tank the whole catalog
//     (bootstrap-best-effort posture extended to list-time failures).
//
// Fake upstreams are in-process via `tokio::io::duplex` + `AsyncRwTransport`,
// same pattern as `tool_list_cache.rs`. Keeps tests subprocess-free and fast.

use std::sync::Arc;

use mcp_client::{
    ResourceListCache, SamplingBroker, SharedSamplingBroker, ToolListCache, UpstreamEventBus,
    UpstreamHandle, UpstreamManager,
};
use rmcp::{
    ClientHandler, ErrorData, RoleServer, ServerHandler, ServiceExt,
    model::{
        ClientCapabilities, ClientInfo, Implementation, ListResourceTemplatesResult,
        ListResourcesResult, PaginatedRequestParams, RawResource, RawResourceTemplate,
        ReadResourceRequestParams, ReadResourceResult, Resource, ResourceContents,
        ResourceTemplate, ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

// -------- Fake upstream with static resource/template lists --------

/// Fixture description for one upstream: what resources it advertises,
/// what templates, and what `read_resource` should return for each URI.
#[derive(Clone, Default)]
struct Fixture {
    resources: Vec<Resource>,
    templates: Vec<ResourceTemplate>,
    /// Map of upstream URI -> response text. Any URI not in the map
    /// returns an rmcp error from `read_resource`.
    read_responses: std::collections::HashMap<String, String>,
}

impl Fixture {
    fn with_resources(mut self, resources: Vec<Resource>) -> Self {
        self.resources = resources;
        self
    }

    fn with_templates(mut self, templates: Vec<ResourceTemplate>) -> Self {
        self.templates = templates;
        self
    }

    fn with_read(mut self, upstream_uri: &str, response: &str) -> Self {
        self.read_responses
            .insert(upstream_uri.to_string(), response.to_string());
        self
    }
}

#[derive(Clone)]
struct FakeResourceServer {
    name: String,
    fixture: Fixture,
}

impl ServerHandler for FakeResourceServer {
    fn get_info(&self) -> ServerInfo {
        // Advertise resources capability so rmcp's list_all_* pagination
        // wrapper is willing to call us.
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_resources()
                .enable_resources_list_changed()
                .build(),
        )
        .with_server_info(Implementation::new(self.name.clone(), "0.0.1"))
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        Ok(ListResourcesResult {
            resources: self.fixture.resources.clone(),
            next_cursor: None,
            meta: None,
        })
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        Ok(ListResourceTemplatesResult {
            resource_templates: self.fixture.templates.clone(),
            next_cursor: None,
            meta: None,
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        match self.fixture.read_responses.get(request.uri.as_str()) {
            Some(text) => Ok(ReadResourceResult::new(vec![ResourceContents::text(
                text.clone(),
                request.uri.clone(),
            )])),
            None => Err(ErrorData::invalid_params(
                format!("unknown resource: {}", request.uri),
                None,
            )),
        }
    }
}

// -------- Fixture helpers --------

fn resource(uri: &str, name: &str, description: Option<&str>) -> Resource {
    let mut raw = RawResource::new(uri, name);
    if let Some(d) = description {
        raw = raw.with_description(d);
    }
    Resource::new(raw, None)
}

fn template(uri_template: &str, name: &str, description: Option<&str>) -> ResourceTemplate {
    let mut raw = RawResourceTemplate::new(uri_template, name);
    if let Some(d) = description {
        raw = raw.with_description(d);
    }
    ResourceTemplate::new(raw, None)
}

// -------- Downstream client --------

#[derive(Clone)]
struct PlainClient;

impl ClientHandler for PlainClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("resources-test-client", "0.0.1"),
        )
    }
}

// -------- Harness --------

struct Harness {
    upstream_handles: Vec<Arc<UpstreamHandle>>,
    fake_upstream_tasks: Vec<tokio::task::JoinHandle<()>>,
    downstream_url: String,
    ct: CancellationToken,
    server_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn shutdown(mut self) {
        match common::cancel_and_join_server(&self.ct, self.server_handle).await {
            common::ServerShutdown::Clean => {}
            common::ServerShutdown::ServeErr(e) => panic!("gateway server returned error: {e:#}"),
            common::ServerShutdown::Panicked(join_err) => {
                panic!("gateway server task panicked: {join_err}")
            }
            common::ServerShutdown::TimedOut => {
                eprintln!("warning: gateway server did not shut down within 3 s")
            }
        }
        // Drop all gateway-side upstream handles BEFORE awaiting the
        // fake tasks — each fake's service.waiting() returns only
        // once its duplex peer (a gateway-side half held by an
        // UpstreamHandle) disconnects.
        self.upstream_handles.clear();
        for task in self.fake_upstream_tasks {
            if let Err(e) = task.await {
                panic!("fake upstream task panicked: {e}");
            }
        }
    }
}

/// Low-level harness starter: takes already-spawned upstream handles plus
/// their fake-task JoinHandles and wires them into a gateway. The handles
/// are registered with the manager here so the caller doesn't have to.
///
/// This is what makes heterogeneous-upstream tests (e.g. one working + one
/// `BrokenFixture`) possible without duplicating the gateway setup; the
/// ordinary `start_harness` is a thin wrapper that builds homogeneous
/// `FakeResourceServer`s first and then delegates here.
async fn start_harness_with_handles(
    upstream_handles: Vec<Arc<UpstreamHandle>>,
    fake_upstream_tasks: Vec<tokio::task::JoinHandle<()>>,
    bus: Arc<UpstreamEventBus>,
    broker: SharedSamplingBroker,
) -> Harness {
    let manager = Arc::new(UpstreamManager::new());
    for handle in &upstream_handles {
        manager.register(handle.clone()).await;
    }

    let tool_cache = Arc::new(ToolListCache::new());
    let resource_cache = Arc::new(ResourceListCache::new());

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().expect("local addr");
    let downstream_url = format!("http://{addr}/mcp");

    let ct = CancellationToken::new();
    let shutdown_ct = ct.clone();
    let ct_for_serve = ct.clone();
    let supervisor_ct = ct.child_token();

    let server_handle = tokio::spawn(async move {
        gateway::serve_on(
            listener,
            gateway::AppContext {
                tool_cache,
                resource_cache,
                ..common::test_app_context(manager, bus, broker).await
            },
            Vec::new(),
            ct_for_serve,
            supervisor_ct,
            async move {
                shutdown_ct.cancelled().await;
            },
        )
        .await
    });

    Harness {
        upstream_handles,
        fake_upstream_tasks,
        downstream_url,
        ct,
        server_handle,
    }
}

/// Spin up a gateway with one or more fake `FakeResourceServer` upstreams.
///
/// Each entry in `upstreams` is `(display_name, lua_name, Fixture)`. For
/// tests that need heterogeneous upstreams (e.g. a `BrokenFixture`
/// alongside a working server), call `spawn_fake_upstream` per-upstream
/// and then `start_harness_with_handles` directly.
async fn start_harness(upstreams: Vec<(String, String, Fixture)>) -> Harness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());

    let mut fake_tasks = Vec::new();
    let mut upstream_handles = Vec::new();

    for (name, lua_name, fixture) in upstreams {
        let server = FakeResourceServer {
            name: name.clone(),
            fixture,
        };
        let (handle, task) =
            common::spawn_fake_upstream(&name, &lua_name, server, &bus, &broker).await;
        upstream_handles.push(handle);
        fake_tasks.push(task);
    }

    start_harness_with_handles(upstream_handles, fake_tasks, bus, broker).await
}

async fn connect_client(url: &str) -> rmcp::service::RunningService<rmcp::RoleClient, PlainClient> {
    let transport = rmcp::transport::StreamableHttpClientTransport::from_uri(url.to_string());
    PlainClient
        .serve(transport)
        .await
        .expect("downstream client connect")
}

// -------- Tests --------

#[tokio::test]
async fn list_resources_aggregates_across_upstreams_with_namespacing() {
    // Property (a): two upstreams shipping resources with the SAME bare
    // URI produce DISTINCT namespaced URIs downstream. The namespacing
    // is what makes cross-upstream aggregation unambiguous.
    let datadog_eu = Fixture::default().with_resources(vec![
        resource("datadog://mcp/toolsets", "toolsets", Some("EU region")),
        resource("datadog://mcp/whoami", "whoami", Some("EU region")),
    ]);
    let datadog_us = Fixture::default().with_resources(vec![
        resource("datadog://mcp/toolsets", "toolsets", Some("US region")),
        resource("datadog://mcp/whoami", "whoami", Some("US region")),
    ]);

    let harness = start_harness(vec![
        (
            "datadog-eu".to_string(),
            "datadog_eu".to_string(),
            datadog_eu,
        ),
        (
            "datadog-us".to_string(),
            "datadog_us".to_string(),
            datadog_us,
        ),
    ])
    .await;

    let client = connect_client(&harness.downstream_url).await;
    let list = client
        .peer()
        .list_all_resources()
        .await
        .expect("list_resources succeeds");

    // Four total: 2 upstreams * 2 resources each.
    assert_eq!(list.len(), 4, "got: {list:#?}");

    let uris: Vec<&str> = list.iter().map(|r| r.raw.uri.as_str()).collect();
    assert!(
        uris.contains(&"mcp-proxy://datadog_eu/datadog%3A%2F%2Fmcp%2Ftoolsets"),
        "expected EU toolsets URI; got {uris:?}"
    );
    assert!(
        uris.contains(&"mcp-proxy://datadog_us/datadog%3A%2F%2Fmcp%2Ftoolsets"),
        "expected US toolsets URI; got {uris:?}"
    );

    // The two upstreams' identical bare URIs must NOT collide downstream.
    let eu_count = uris
        .iter()
        .filter(|u| u.starts_with("mcp-proxy://datadog_eu/"))
        .count();
    let us_count = uris
        .iter()
        .filter(|u| u.starts_with("mcp-proxy://datadog_us/"))
        .count();
    assert_eq!(eu_count, 2, "EU upstream should contribute 2 resources");
    assert_eq!(us_count, 2, "US upstream should contribute 2 resources");

    harness.shutdown().await;
}

#[tokio::test]
async fn list_resource_templates_aggregates_and_namespaces() {
    // Templates get the same namespacing treatment. `{var}` placeholders
    // encode into `%7B...%7D` — the pinned codec decision from step 1.
    // Agents use `_gateway.expand_template(...)` to instantiate rather
    // than gsub-ing the namespaced form directly.
    let rootly = Fixture::default().with_templates(vec![
        template(
            "incident://{incident_id}",
            "incident",
            Some("incident by ID"),
        ),
        template("team://{team_id}", "team", Some("team by ID")),
    ]);

    let harness = start_harness(vec![("rootly".to_string(), "rootly".to_string(), rootly)]).await;

    let client = connect_client(&harness.downstream_url).await;
    let list = client
        .peer()
        .list_all_resource_templates()
        .await
        .expect("list_resource_templates succeeds");

    assert_eq!(list.len(), 2);
    let uris: Vec<&str> = list.iter().map(|t| t.raw.uri_template.as_str()).collect();
    // Per the codec's NON_ALPHANUMERIC policy, `_` also gets encoded
    // (to %5F). Not strictly required by URI-safety (underscore is
    // unreserved in RFC 3986), but our codec encodes everything that
    // isn't an ASCII alphanumeric — consistent and defensible.
    // If we ever loosen the policy, this test will flag it.
    assert!(
        uris.contains(&"mcp-proxy://rootly/incident%3A%2F%2F%7Bincident%5Fid%7D"),
        "expected namespaced incident template with encoded placeholders; got {uris:?}"
    );
    assert!(
        uris.contains(&"mcp-proxy://rootly/team%3A%2F%2F%7Bteam%5Fid%7D"),
        "expected namespaced team template with encoded placeholders; got {uris:?}"
    );

    // Also pin the load-bearing property: a template's encoded form
    // decodes back to the original `{var}` placeholder.
    let incident = list
        .iter()
        .find(|t| t.raw.uri_template.contains("incident"))
        .expect("incident template present");
    let (name, original) = mcp_client::resource_uri::decode(&incident.raw.uri_template)
        .expect("namespaced template URI must roundtrip");
    assert_eq!(name, "rootly");
    assert_eq!(original, "incident://{incident_id}");

    harness.shutdown().await;
}

#[tokio::test]
async fn read_resource_routes_to_correct_upstream() {
    // Property (b): a downstream `resources/read` carrying the EU
    // upstream's namespaced URI MUST route to the EU upstream and
    // nobody else. The prefix is load-bearing.
    let datadog_eu = Fixture::default().with_read("datadog://mcp/toolsets", "eu-toolsets-body");
    let datadog_us = Fixture::default().with_read("datadog://mcp/toolsets", "us-toolsets-body");

    let harness = start_harness(vec![
        (
            "datadog-eu".to_string(),
            "datadog_eu".to_string(),
            datadog_eu,
        ),
        (
            "datadog-us".to_string(),
            "datadog_us".to_string(),
            datadog_us,
        ),
    ])
    .await;

    let client = connect_client(&harness.downstream_url).await;

    // Read via the EU-namespaced URI.
    let eu_uri = "mcp-proxy://datadog_eu/datadog%3A%2F%2Fmcp%2Ftoolsets";
    let eu_result = client
        .peer()
        .read_resource(ReadResourceRequestParams::new(eu_uri.to_string()))
        .await
        .expect("EU read succeeds");
    let eu_body = text_of(&eu_result);
    assert_eq!(
        eu_body, "eu-toolsets-body",
        "EU URI must route to the EU upstream, not the US one"
    );

    // Read via the US-namespaced URI.
    let us_uri = "mcp-proxy://datadog_us/datadog%3A%2F%2Fmcp%2Ftoolsets";
    let us_result = client
        .peer()
        .read_resource(ReadResourceRequestParams::new(us_uri.to_string()))
        .await
        .expect("US read succeeds");
    let us_body = text_of(&us_result);
    assert_eq!(
        us_body, "us-toolsets-body",
        "US URI must route to the US upstream"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn read_resource_unknown_upstream_prefix_returns_error() {
    // Property (c): an `mcp-proxy://unknown_server/…` URI returns an
    // error, NOT an empty result and NOT a silent pass-through. The
    // inspection-grade detail: this must fail AFTER decoding succeeds,
    // because the decode shape is valid — only the lookup fails.
    let harness = start_harness(vec![(
        "real-upstream".to_string(),
        "real_upstream".to_string(),
        Fixture::default(),
    )])
    .await;

    let client = connect_client(&harness.downstream_url).await;

    let bad_uri = "mcp-proxy://never_registered/rootly%3A%2F%2Fincidents";
    let err = client
        .peer()
        .read_resource(ReadResourceRequestParams::new(bad_uri.to_string()))
        .await
        .expect_err("read must fail for unknown upstream");

    let msg = format!("{err}");
    assert!(
        msg.contains("never_registered") || msg.contains("unknown"),
        "error must name the missing upstream or clearly flag it as unknown; got: {msg}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn read_resource_rejects_non_mcp_proxy_uri() {
    // Defense-in-depth: a downstream client that forgets to use the
    // namespaced form and sends `rootly://incidents` directly must get
    // a decode error, not a routing miss. `invalid_params` is the right
    // shape because the request URI is malformed at the protocol level.
    let harness = start_harness(vec![(
        "rootly".to_string(),
        "rootly".to_string(),
        Fixture::default(),
    )])
    .await;

    let client = connect_client(&harness.downstream_url).await;

    let err = client
        .peer()
        .read_resource(ReadResourceRequestParams::new(
            "rootly://incidents".to_string(),
        ))
        .await
        .expect_err("non-namespaced URI must be rejected");

    let msg = format!("{err}");
    // The codec's error text includes "mcp-proxy" so the client dev
    // knows what shape to produce.
    assert!(
        msg.contains("mcp-proxy") || msg.contains("scheme"),
        "decode error must point at the scheme problem; got: {msg}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn list_resources_survives_one_upstream_failure() {
    // Property: bootstrap-best-effort posture extends to list-time
    // failures. A single flaky upstream that returns an error on
    // `list_resources` must not erase every other upstream's entries
    // from the downstream catalog.
    let working_fixture = Fixture::default().with_resources(vec![resource(
        "working://ok",
        "working-res",
        Some("still here"),
    )]);

    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());

    let (working_handle, working_task) = common::spawn_fake_upstream(
        "working-upstream",
        "working_upstream",
        FakeResourceServer {
            name: "working-upstream".to_string(),
            fixture: working_fixture,
        },
        &bus,
        &broker,
    )
    .await;
    let (broken_handle, broken_task) = common::spawn_fake_upstream(
        "broken-upstream",
        "broken_upstream",
        BrokenFixture,
        &bus,
        &broker,
    )
    .await;

    let harness = start_harness_with_handles(
        vec![working_handle, broken_handle],
        vec![working_task, broken_task],
        bus,
        broker,
    )
    .await;

    let client = connect_client(&harness.downstream_url).await;
    let list = client
        .peer()
        .list_all_resources()
        .await
        .expect("list survives one-upstream failure");

    assert_eq!(
        list.len(),
        1,
        "working upstream's resource must survive; got {list:#?}"
    );
    assert!(
        list[0].raw.uri.starts_with("mcp-proxy://working_upstream/"),
        "the one surviving entry must be the working upstream's"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn list_resource_templates_survives_one_upstream_failure() {
    // Property: the bootstrap-best-effort posture extends to
    // `list_resource_templates` as well — a single flaky upstream that
    // errors on the templates call must not erase every other
    // upstream's templates from the downstream catalog. Mirrors
    // `list_resources_survives_one_upstream_failure` but exercises the
    // templates path.
    let working_fixture = Fixture::default().with_templates(vec![template(
        "working://{id}",
        "working-template",
        Some("still here"),
    )]);

    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());

    let (working_handle, working_task) = common::spawn_fake_upstream(
        "working-upstream",
        "working_upstream",
        FakeResourceServer {
            name: "working-upstream".to_string(),
            fixture: working_fixture,
        },
        &bus,
        &broker,
    )
    .await;
    let (broken_handle, broken_task) = common::spawn_fake_upstream(
        "broken-upstream",
        "broken_upstream",
        BrokenFixture,
        &bus,
        &broker,
    )
    .await;

    let harness = start_harness_with_handles(
        vec![working_handle, broken_handle],
        vec![working_task, broken_task],
        bus,
        broker,
    )
    .await;

    let client = connect_client(&harness.downstream_url).await;
    let list = client
        .peer()
        .list_all_resource_templates()
        .await
        .expect("list_resource_templates survives one-upstream failure");

    assert_eq!(
        list.len(),
        1,
        "working upstream's template must survive; got {list:#?}"
    );
    assert!(
        list[0]
            .raw
            .uri_template
            .starts_with("mcp-proxy://working_upstream/"),
        "the one surviving entry must be the working upstream's"
    );

    harness.shutdown().await;
}

// A fake server that always errors on list_resources. Separate type so
// the "working" FakeResourceServer stays single-purpose. Test-only, so
// it lives in this file rather than with production code.
#[derive(Clone)]
struct BrokenFixture;

impl ServerHandler for BrokenFixture {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_resources().build())
            .with_server_info(Implementation::new("broken-upstream", "0.0.1"))
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        Err(ErrorData::internal_error(
            "broken upstream: list_resources always fails".to_string(),
            None,
        ))
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        Err(ErrorData::internal_error(
            "broken upstream: list_resource_templates always fails".to_string(),
            None,
        ))
    }
}

// -------- Helpers --------

/// Extract the text from the first content item of a `ReadResourceResult`.
/// Panics on mismatch because tests should fail loudly on unexpected shapes.
fn text_of(res: &ReadResourceResult) -> String {
    match res.contents.first().expect("at least one content item") {
        ResourceContents::TextResourceContents { text, .. } => text.clone(),
        ResourceContents::BlobResourceContents { .. } => {
            panic!("expected text content, got blob")
        }
    }
}
