// pattern: Imperative Shell
//
// End-to-end integration test for the agent-facing template workflow:

mod common;
//
//   1. Client calls `resources/templates/list` — gets namespaced templates.
//   2. Client decodes a namespaced template to recover (server, bare_template).
//   3. Client calls the gateway's `execute` tool with a Lua script that
//      invokes `_gateway.expand_template({server, template, params})` and
//      returns the resulting namespaced URI.
//   4. Client calls `resources/read` with that URI.
//   5. The upstream serves the resource content back, and the client sees it.
//
// This exercises the full loop that was the missing piece before this
// session: the Lua builtin is now the bridge that turns a template +
// params into a URI that `resources/read` can consume. Previously an
// agent would have had to manually `string.format` on an already-encoded
// namespaced template, which was clumsy and wrong for anything beyond
// Level 1 substitution.
//
// Step 4 is deliberately done via a separate MCP request, not from
// inside the Lua script. `resources/read` is a downstream method —
// scripts don't have access to it (deliberately; they might if a
// future feature axis calls for it, but that's not today). The test
// mirrors how a real agent would run: execute (for computation) →
// resources/read (for fetching).

use std::sync::Arc;

use mcp_client::{
    SamplingBroker, SharedSamplingBroker, UpstreamEventBus, UpstreamHandle, UpstreamManager,
};
use rmcp::{
    ClientHandler, ErrorData, RoleServer, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, ClientCapabilities, ClientInfo, Implementation,
        ListResourceTemplatesResult, ListResourcesResult, PaginatedRequestParams,
        RawResourceTemplate, ReadResourceRequestParams, ReadResourceResult, Resource,
        ResourceContents, ResourceTemplate, ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

// -------- Fake upstream: templates + read responses --------

/// Fake MCP upstream that advertises one template and can serve reads
/// for URIs matching an `incident://<id>` shape — regardless of which
/// `id` the client picks. This lets the test expand the template with
/// arbitrary params and still get a legible response body back.
#[derive(Clone)]
struct FakeRootlyLike;

impl ServerHandler for FakeRootlyLike {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_resources()
                .enable_resources_list_changed()
                .build(),
        )
        .with_server_info(Implementation::new("fake-rootly", "0.0.1"))
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        Ok(ListResourcesResult {
            resources: Vec::<Resource>::new(),
            next_cursor: None,
            meta: None,
        })
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        // One template: RFC 6570 Level 1. Real Rootly ships exactly
        // this shape (see the probe_resources example). Keeping the
        // test faithful to the production shape means a regression in
        // the real-world path would show up here too.
        let raw = RawResourceTemplate::new("incident://{incident_id}", "incident")
            .with_description("Incident by ID (test fixture)");
        let template = ResourceTemplate::new(raw, None);
        Ok(ListResourceTemplatesResult {
            resource_templates: vec![template],
            next_cursor: None,
            meta: None,
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        // Match `incident://{id}` by prefix. Anything else is an
        // unknown URI. Echo the id back in the response body so the
        // caller can assert routing happened correctly.
        let uri = request.uri.as_str();
        if let Some(id) = uri.strip_prefix("incident://") {
            Ok(ReadResourceResult::new(vec![ResourceContents::text(
                format!("incident body for id={id}"),
                uri.to_string(),
            )]))
        } else {
            Err(ErrorData::invalid_params(
                format!("unknown resource: {uri}"),
                None,
            ))
        }
    }
}

// -------- Downstream client --------

#[derive(Clone)]
struct PlainClient;

impl ClientHandler for PlainClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("expand-template-e2e-client", "0.0.1"),
        )
    }
}

// -------- Harness --------

struct Harness {
    upstream_handle: Option<Arc<UpstreamHandle>>,
    fake_upstream_task: tokio::task::JoinHandle<()>,
    downstream_url: String,
    ct: CancellationToken,
    server_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn shutdown(mut self) {
        match common::cancel_and_join_server(&self.ct, self.server_handle).await {
            common::ServerShutdown::Clean => {}
            common::ServerShutdown::ServeErr(e) => {
                panic!("server task returned error at shutdown: {e:?}")
            }
            common::ServerShutdown::Panicked(join_err) => {
                panic!("server task panicked at shutdown: {join_err}")
            }
            common::ServerShutdown::TimedOut => {
                eprintln!("warning: server task did not shut down within 3 s")
            }
        }
        // Drop the gateway-side upstream handle BEFORE awaiting the
        // fake upstream task: service.waiting() only returns when the
        // peer (gateway-side duplex half, held by UpstreamHandle)
        // disconnects. Joining the fake task while the Arc is alive
        // deadlocks.
        self.upstream_handle.take();
        if let Err(e) = self.fake_upstream_task.await {
            panic!("fake upstream task panicked at shutdown: {e}");
        }
    }
}

async fn start_harness() -> Harness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let upstreams = Arc::new(UpstreamManager::new());

    // Display name has a dash, lua_name uses an underscore — realistic
    // shape. The agent's Lua script will use the lua_name.
    let (upstream_handle, fake_upstream_task) =
        common::spawn_fake_upstream("fake-rootly", "fake_rootly", FakeRootlyLike, &bus, &broker)
            .await;
    upstreams.register(upstream_handle.clone()).await;

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
            common::test_app_context(upstreams, bus, broker).await,
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
        upstream_handle: Some(upstream_handle),
        fake_upstream_task,
        downstream_url,
        ct,
        server_handle,
    }
}

async fn connect_client(url: &str) -> rmcp::service::RunningService<rmcp::RoleClient, PlainClient> {
    let transport = rmcp::transport::StreamableHttpClientTransport::from_uri(url.to_string());
    PlainClient
        .serve(transport)
        .await
        .expect("downstream client connect")
}

/// Extract the text from the first content item of a `CallToolResult`.
/// Panics on mismatch — tests should fail loudly on unexpected shapes.
fn text_of_call_result(res: &rmcp::model::CallToolResult) -> String {
    let content = res.content.first().expect("at least one content item");
    content
        .as_text()
        .expect("expected text content")
        .text
        .clone()
}

// -------- Tests --------

#[tokio::test]
async fn agent_can_list_expand_and_read_a_template() {
    // The full loop. Broken into named steps so a failure points
    // cleanly at which link of the chain broke.
    let harness = start_harness().await;
    let client = connect_client(&harness.downstream_url).await;

    // ---- Step 1: list templates ----
    let templates = client
        .peer()
        .list_all_resource_templates()
        .await
        .expect("list_resource_templates must succeed");
    assert_eq!(
        templates.len(),
        1,
        "fake upstream advertises exactly one template; got {templates:#?}"
    );
    let template = &templates[0];
    let namespaced_template = template.raw.uri_template.as_str();
    assert!(
        namespaced_template.starts_with("mcp-proxy://fake_rootly/"),
        "template must be namespaced by the gateway; got {namespaced_template}"
    );

    // ---- Step 2: decode to recover (server, bare_template) ----
    let (server, bare_template) = mcp_client::resource_uri::decode(namespaced_template)
        .expect("namespaced template must decode");
    assert_eq!(server, "fake_rootly");
    assert_eq!(bare_template, "incident://{incident_id}");

    // ---- Step 3: run an execute script that calls expand_template ----
    //
    // Real agents build the script from data they just fetched. The
    // test mirrors this: we substitute the decoded template into the
    // script body rather than hardcoding the literal. If a future
    // change alters how the gateway returns templates, this test
    // fails at step 2 (decode), not at step 3 — which is a better
    // diagnostic locus.
    let script = format!(
        r#"
            local namespaced = _gateway.expand_template({{
                server = "{server}",
                template = "{bare_template}",
                params = {{ incident_id = "42" }},
            }})
            result(namespaced)
        "#,
    );
    let args = serde_json::json!({ "script": script });
    let call_result = client
        .peer()
        .call_tool(
            CallToolRequestParams::new("execute")
                .with_arguments(args.as_object().expect("args is an object").clone()),
        )
        .await
        .expect("execute must succeed");
    assert!(
        !call_result.is_error.unwrap_or(false),
        "execute returned is_error=true; content: {:#?}",
        call_result.content
    );
    let expanded_uri = text_of_call_result(&call_result);

    // Sanity check: the URI we got back is genuinely the gateway's
    // namespaced shape for incident://42 under fake_rootly.
    assert_eq!(expanded_uri, "mcp-proxy://fake_rootly/incident%3A%2F%2F42");

    // ---- Step 4: read_resource with the expanded URI ----
    let read_result = client
        .peer()
        .read_resource(ReadResourceRequestParams::new(expanded_uri))
        .await
        .expect("read_resource must succeed on the expanded URI");
    let content = read_result.contents.first().expect("at least one content");
    match content {
        ResourceContents::TextResourceContents { text, uri, .. } => {
            // The upstream echoed the id back. Confirm both the body
            // and the URI the upstream saw — both are load-bearing:
            // the body proves content made it back, and the URI
            // proves the gateway stripped the namespacing correctly
            // before forwarding.
            assert_eq!(text, "incident body for id=42");
            assert_eq!(uri, "incident://42");
        }
        other => panic!("expected text content, got {other:?}"),
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn expand_template_rejects_wrong_upstream_name_at_read_time() {
    // Property pin: if an agent's `expand_template` call uses the
    // wrong `server`, the call itself succeeds (produces a valid
    // namespaced URI for a server that doesn't exist) — and then the
    // follow-up `resources/read` fails with a clear error. We check
    // the read-time failure explicitly because the alternative
    // (rejecting inside `expand_template` by validating server names)
    // would require the Lua runtime to know about registered
    // upstreams, and we've kept that coupling out on purpose.
    let harness = start_harness().await;
    let client = connect_client(&harness.downstream_url).await;

    let script = r#"
        local uri = _gateway.expand_template({
            server = "no_such_upstream",
            template = "incident://{incident_id}",
            params = { incident_id = "42" },
        })
        result(uri)
    "#;
    let args = serde_json::json!({ "script": script });
    let call_result = client
        .peer()
        .call_tool(
            CallToolRequestParams::new("execute")
                .with_arguments(args.as_object().expect("args is an object").clone()),
        )
        .await
        .expect("execute with unknown-server template must succeed at the Lua level");
    assert!(
        !call_result.is_error.unwrap_or(false),
        "the Lua expand succeeds; the failure comes at read time"
    );
    let bogus_uri = text_of_call_result(&call_result);
    assert!(bogus_uri.starts_with("mcp-proxy://no_such_upstream/"));

    // Now the actual routing check: the gateway decodes, can't find
    // the upstream, and returns an error.
    let read_err = client
        .peer()
        .read_resource(ReadResourceRequestParams::new(bogus_uri))
        .await
        .expect_err("read must fail for unknown upstream");
    let msg = format!("{read_err}");
    assert!(
        msg.contains("no_such_upstream"),
        "error must name the missing upstream 'no_such_upstream'; got: {msg}"
    );

    harness.shutdown().await;
}
