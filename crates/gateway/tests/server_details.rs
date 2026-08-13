// pattern: Imperative Shell
//
// End-to-end tests for the `server-details` tool and for the hint-
// resolution path in `list-servers`. Both features were added together
// for the same reason (make upstream catalog signals cheaper to
// discover) so they share a test file.

mod common;
//
// Pinned properties:
//   (a) `server-details` on a known upstream returns the upstream's
//       advertised `InitializeResult.instructions` verbatim.
//   (b) `server-details` routes to the correct upstream by
//       `luaServerName`; an unknown name yields an `invalid_params`
//       error, not a silent empty response.
//   (c) Hint resolution prefers a config override over whatever the
//       upstream advertises. (Empty override falls through is covered
//       by the `hint::resolve_hint` unit tests; we don't re-pin it at
//       the integration level — the handler just hands its two inputs
//       to `resolve_hint` directly.)
//   (d) `list-servers` surfaces the resolved hint inline under each
//       server when one exists.
//
// In-process fake upstreams via `tokio::io::duplex` + `AsyncRwTransport`
// — same pattern as `tests/resources.rs`. No subprocesses.

use std::sync::Arc;

use mcp_client::{
    ResourceListCache, SamplingBroker, SharedSamplingBroker, ToolListCache, UpstreamConfig,
    UpstreamEventBus, UpstreamHandle, UpstreamKind, UpstreamManager,
};
use rmcp::{
    ClientHandler, ServiceExt,
    model::{
        CallToolRequestParams, ClientCapabilities, ClientInfo, Implementation, ServerCapabilities,
        ServerInfo,
    },
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

// -------- Fake upstream with configurable instructions --------

#[derive(Clone)]
struct FakeServer {
    name: String,
    version: String,
    instructions: Option<String>,
}

impl rmcp::ServerHandler for FakeServer {
    fn get_info(&self) -> ServerInfo {
        let base = ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(self.name.clone(), self.version.clone()));
        match &self.instructions {
            Some(text) => base.with_instructions(text.clone()),
            None => base,
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
            Implementation::new("server-details-test", "0.0.1"),
        )
    }
}

// -------- Harness --------

struct UpstreamSpec {
    name: String,
    lua_name: String,
    kind: UpstreamKind,
    /// Upstream-advertised `InitializeResult.instructions` string.
    /// `None` means the upstream sets no instructions (the common case
    /// in our probe — 7 of 10 real upstreams).
    instructions: Option<String>,
    /// Optional config-level hint override. `None` disables; when
    /// `Some`, the harness stamps it into the `UpstreamConfig` before
    /// registering with the manager.
    hint: Option<String>,
    /// Server-advertised implementation name (what the upstream
    /// reports for itself in `serverInfo.name`). Usually the same as
    /// `name` but the distinction matters for the rendered output.
    server_name: String,
    /// Server-advertised implementation version.
    server_version: String,
}

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
        // fake upstream tasks. Each fake task's service.waiting()
        // returns only when its duplex peer (an UpstreamHandle's
        // gateway-side half) disconnects; holding the Arcs here would
        // deadlock.
        self.upstream_handles.clear();
        for task in self.fake_upstream_tasks {
            if let Err(e) = task.await {
                panic!("fake upstream task panicked: {e}");
            }
        }
    }
}

async fn start_harness(upstreams: Vec<UpstreamSpec>) -> Harness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let manager = Arc::new(UpstreamManager::new());
    let tool_cache = Arc::new(ToolListCache::new());
    let resource_cache = Arc::new(ResourceListCache::new());

    let mut fake_tasks = Vec::new();
    let mut upstream_handles = Vec::new();

    for spec in upstreams {
        let server = FakeServer {
            name: spec.server_name.clone(),
            version: spec.server_version.clone(),
            instructions: spec.instructions.clone(),
        };

        let mut config = UpstreamConfig::new(spec.name.clone(), spec.lua_name.clone(), spec.kind);
        if let Some(h) = spec.hint {
            config = config.with_hint(h);
        }

        // Optional `.with_hint(..)` stamped onto the config is exactly
        // the case the config-taking spawner variant exists for.
        let (handle, task) =
            common::spawn_fake_upstream_with_config(config, server, &bus, &broker, 64 * 1024).await;
        fake_tasks.push(task);
        manager.register(handle.clone()).await;
        upstream_handles.push(handle);
    }

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
        fake_upstream_tasks: fake_tasks,
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

fn stdio_kind() -> UpstreamKind {
    UpstreamKind::Stdio {
        command: "unused-in-test".to_string(),
        args: vec![],
        env: Default::default(),
    }
}

fn call_server_details_params(lua_server_name: &str) -> CallToolRequestParams {
    let args = serde_json::json!({ "luaServerName": lua_server_name })
        .as_object()
        .expect("literal object")
        .clone();
    CallToolRequestParams::new("server-details").with_arguments(args)
}

fn text_content(result: &rmcp::model::CallToolResult) -> String {
    let mut parts = Vec::new();
    for block in &result.content {
        if let rmcp::model::RawContent::Text(t) = &block.raw {
            parts.push(t.text.clone());
        }
    }
    parts.join("\n")
}

async fn call_list_servers(
    client: &rmcp::service::RunningService<rmcp::RoleClient, PlainClient>,
) -> String {
    let result = client
        .peer()
        .call_tool(CallToolRequestParams::new("list-servers"))
        .await
        .expect("list-servers call should succeed");
    text_content(&result)
}

// -------- Tests --------

#[tokio::test]
async fn server_details_returns_upstream_instructions_verbatim() {
    // Property (a): the whole point of the tool is exposing the full
    // upstream-advertised `instructions` text. Truncation here would
    // defeat the purpose — the catalog hint is already the truncated
    // view. Motivating case: Glean's 2.5KB wall of usage guidelines
    // is worth paying for once, on demand.
    let full_instructions = "Line one: summary.\n\n\
                             Line two: details about tool selection.\n\
                             Line three: auth notes and limits.\n";

    let harness = start_harness(vec![UpstreamSpec {
        name: "datadog-us".to_string(),
        lua_name: "datadog_us".to_string(),
        kind: stdio_kind(),
        instructions: Some(full_instructions.to_string()),
        hint: None,
        server_name: "datadog-mcp".to_string(),
        server_version: "1.0.0".to_string(),
    }])
    .await;

    let client = connect_client(&harness.downstream_url).await;
    let result = client
        .peer()
        .call_tool(call_server_details_params("datadog_us"))
        .await
        .expect("server-details should succeed");

    let body = text_content(&result);
    let parsed: serde_json::Value =
        serde_json::from_str(&body).expect("server-details body is JSON");

    assert_eq!(parsed["name"], "datadog-us");
    assert_eq!(parsed["luaServerName"], "datadog_us");
    assert_eq!(parsed["transport"], "stdio");
    assert_eq!(parsed["serverImplementation"]["name"], "datadog-mcp");
    assert_eq!(parsed["serverImplementation"]["version"], "1.0.0");
    assert_eq!(
        parsed["instructions"].as_str().expect("instructions str"),
        full_instructions,
        "full instructions must survive verbatim"
    );

    // And the hint falls out of the upstream instructions via the
    // first-sentence extractor — pinned here so a future change to
    // hint policy also lights up a test at the integration layer.
    assert_eq!(parsed["hint"], "Line one: summary.");

    harness.shutdown().await;
}

#[tokio::test]
async fn server_details_rejects_unknown_upstream() {
    // Property (b): an unknown `luaServerName` must produce an error,
    // not a silent empty response. The agent needs to know when it
    // typed the wrong name.
    let harness = start_harness(vec![UpstreamSpec {
        name: "real".to_string(),
        lua_name: "real".to_string(),
        kind: stdio_kind(),
        instructions: None,
        hint: None,
        server_name: "real".to_string(),
        server_version: "0.0.1".to_string(),
    }])
    .await;

    let client = connect_client(&harness.downstream_url).await;
    let err = client
        .peer()
        .call_tool(call_server_details_params("does_not_exist"))
        .await
        .expect_err("unknown server must surface as an error");

    let text = format!("{err}");
    assert!(
        text.contains("unknown upstream server") && text.contains("does_not_exist"),
        "error must name the missing upstream: {text}"
    );
    // Property (b) also documents the JSON-RPC error code: an unknown
    // name must surface as `invalid_params`, not a generic internal
    // error. The string check alone would still pass if the handler
    // were changed to return `internal_error`; assert the code
    // directly so the rubric the file header promises is fully
    // enforced.
    assert!(
        matches!(
            &err,
            rmcp::ServiceError::McpError(e) if e.code == rmcp::model::ErrorCode::INVALID_PARAMS
        ),
        "expected invalid_params error code, got: {err:?}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn server_details_omits_instructions_when_upstream_sets_none() {
    // Property: the common case — an upstream that doesn't advertise
    // instructions. 7 of 10 real upstreams in the user's config today
    // are in this bucket. The tool must still return a useful
    // response; it just omits the instructions key.
    let harness = start_harness(vec![UpstreamSpec {
        name: "silent".to_string(),
        lua_name: "silent".to_string(),
        kind: stdio_kind(),
        instructions: None,
        hint: None,
        server_name: "silent-server".to_string(),
        server_version: "2.0.0".to_string(),
    }])
    .await;

    let client = connect_client(&harness.downstream_url).await;
    let result = client
        .peer()
        .call_tool(call_server_details_params("silent"))
        .await
        .expect("server-details should succeed");
    let body = text_content(&result);
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("JSON body");

    assert!(
        parsed.get("instructions").is_none(),
        "instructions key must be absent when upstream doesn't set it: {parsed}"
    );
    assert!(
        parsed.get("hint").is_none(),
        "hint key must also be absent when there's no source material: {parsed}"
    );
    // Identity still flows through.
    assert_eq!(parsed["serverImplementation"]["name"], "silent-server");

    harness.shutdown().await;
}

#[tokio::test]
async fn list_servers_renders_hints_and_config_override_wins() {
    // Property (c) + (d) in one test: with two upstreams — one
    // relying on upstream-advertised instructions, one with a config
    // override shadowing upstream instructions — list-servers surfaces
    // the resolved hint inline for both, and the override wins.
    let harness = start_harness(vec![
        UpstreamSpec {
            name: "from-upstream".to_string(),
            lua_name: "from_upstream".to_string(),
            kind: stdio_kind(),
            instructions: Some("Short upstream-advertised hint.".to_string()),
            hint: None,
            server_name: "u1".to_string(),
            server_version: "1".to_string(),
        },
        UpstreamSpec {
            name: "with-override".to_string(),
            lua_name: "with_override".to_string(),
            kind: stdio_kind(),
            instructions: Some("This should NOT appear in list-servers.".to_string()),
            hint: Some("operator-authored override".to_string()),
            server_name: "u2".to_string(),
            server_version: "1".to_string(),
        },
    ])
    .await;

    let client = connect_client(&harness.downstream_url).await;
    let rendered = call_list_servers(&client).await;

    // Both server headers render.
    assert!(rendered.contains("## from-upstream"));
    assert!(rendered.contains("## with-override"));

    // The upstream-advertised first sentence appears for the first.
    assert!(
        rendered.contains("hint: Short upstream-advertised hint."),
        "upstream-advertised hint must appear: {rendered}"
    );

    // The override appears for the second — AND the upstream's text
    // does NOT appear, proving override precedence.
    assert!(
        rendered.contains("hint: operator-authored override"),
        "config override hint must appear: {rendered}"
    );
    assert!(
        !rendered.contains("This should NOT appear"),
        "override must hide upstream content: {rendered}"
    );

    harness.shutdown().await;
}
