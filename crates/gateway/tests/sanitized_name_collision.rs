// pattern: Imperative Shell
//
// End-to-end proof that two upstream tools sanitizing to the same Lua
// identifier are quarantined: neither is callable from a script, and
// the agent's tool-details lookup surfaces the collision rather than
// silently picking a winner.

mod common;
//
// Why this matters: the inspection gate keys off the
// sanitized name. If the runtime were to silently install the second
// of two colliding tools while `tool-details` returned the first, an
// agent could legitimately inspect tool A's schema and unwittingly
// dispatch to tool B. This test pins the property end-to-end.

use std::sync::Arc;

use mcp_client::{
    SamplingBroker, SharedSamplingBroker, UpstreamEventBus, UpstreamHandle, UpstreamManager,
};
use rmcp::{
    ClientHandler, ErrorData, RoleServer, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResult, ClientCapabilities, ClientInfo, Content,
        Implementation, ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo,
        Tool,
    },
    service::RequestContext,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

// -------- Fake upstream with a colliding tool pair --------
//
// Both `safe-tool` and `safe.tool` sanitize to `safe_tool`. The server
// also exposes `unique` so we can assert that quarantine is scoped to
// the colliding pair, not the whole upstream.

#[derive(Clone)]
struct CollidingUpstream;

impl ServerHandler for CollidingUpstream {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("colliding-upstream", "0.0.1"))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
            serde_json::from_value(serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": true,
            }))
            .expect("static schema valid"),
        );
        Ok(ListToolsResult {
            tools: vec![
                Tool::new("safe-tool", "first colliding entry", schema.clone()),
                Tool::new("safe.tool", "second colliding entry", schema.clone()),
                // Reserved-word / already-prefixed pair: "nil" sanitizes to
                // "_nil" (reserved-word prefix), colliding with the literal
                // "_nil" which is already in prefixed form. Pins the
                // sanitizer's prefix-collision path end-to-end.
                Tool::new("nil", "reserved-word collision", schema.clone()),
                Tool::new("_nil", "already-prefixed collision", schema.clone()),
                Tool::new("unique", "the survivor", schema),
            ],
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        // Echo which tool was hit. If quarantine is broken, this surfaces
        // *which* of the colliders the runtime was about to dispatch.
        Ok(CallToolResult::success(vec![Content::text(format!(
            "called: {}",
            request.name
        ))]))
    }
}

#[derive(Clone)]
struct PlainDownstreamClient;

impl ClientHandler for PlainDownstreamClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("plain-downstream-for-collision", "0.0.1"),
        )
    }
}

struct Harness {
    upstream_handle: Option<Arc<UpstreamHandle>>,
    downstream_url: String,
    ct: CancellationToken,
    server_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    fake_upstream_task: tokio::task::JoinHandle<()>,
}

async fn start_harness() -> Harness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let upstreams = Arc::new(UpstreamManager::new());

    let (upstream_handle, fake_upstream_task) = common::spawn_fake_upstream(
        "colliding-upstream",
        "colliding_upstream",
        CollidingUpstream,
        &bus,
        &broker,
    )
    .await;
    upstreams.register(upstream_handle.clone()).await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().expect("local addr");
    let downstream_url = format!("http://{addr}/mcp");

    let ct = CancellationToken::new();
    let shutdown_ct = ct.clone();
    let bus_clone = Arc::clone(&bus);
    let upstreams_clone = Arc::clone(&upstreams);
    let broker_clone = broker.clone();
    let ct_for_serve = ct.clone();
    let supervisor_ct = ct.child_token();

    let server_handle = tokio::spawn(async move {
        gateway::serve_on(
            listener,
            common::test_app_context(upstreams_clone, bus_clone, broker_clone).await,
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
        downstream_url,
        ct,
        server_handle,
        fake_upstream_task,
    }
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
        // Drop the gateway-side upstream handle before awaiting the
        // fake upstream task, otherwise the duplex peer never sees
        // EOF and the await deadlocks.
        self.upstream_handle.take();
        if let Err(e) = self.fake_upstream_task.await {
            panic!("fake upstream task panicked: {e}");
        }
    }
}

// -------- Tests --------

#[tokio::test]
async fn tool_details_on_colliding_name_returns_ambiguous() {
    // Property: the agent must NOT be able to inspect a colliding name.
    // Returning the schema for "the first one" would let the agent
    // reason about tool A's interface while the runtime would dispatch
    // tool B (or vice versa) — silent capability bypass.
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    let args =
        serde_json::json!({ "luaServerName": "colliding_upstream", "luaToolName": "safe_tool" })
            .as_object()
            .expect("object literal")
            .clone();
    let result = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("tool-details").with_arguments(args))
        .await
        .expect("tool-details should complete at the protocol level");

    assert_eq!(
        result.is_error,
        Some(true),
        "ambiguous lookup must produce is_error=true"
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("error result has text content");
    assert!(
        text.contains("ambiguous"),
        "diagnostic must call out ambiguity; got: {text}"
    );
    assert!(
        text.contains("safe-tool") && text.contains("safe.tool"),
        "diagnostic must name both colliding originals so the operator can fix upstream; got: {text}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn execute_cannot_dispatch_colliding_tool() {
    // Property: even if the agent skipped tool-details and went
    // straight to execute (e.g. a script copied from another session,
    // or speculative use of a familiar name), the runtime must not
    // silently dispatch the script's `colliding_upstream.safe_tool(...)`
    // to either of the colliding tools. The Lua global must be absent.
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    // The colliding tool is not inspectable, so we can't even mark it
    // inspected via tool-details. We have to try execute directly.
    // The script will fail at the Lua level: `colliding_upstream.safe_tool`
    // is nil because we refused to install it.
    let script = r#"
        local fn = colliding_upstream.safe_tool
        if fn == nil then
            result("safe_tool was not installed")
        else
            result("safe_tool was installed (BUG): " .. tostring(fn))
        end
    "#
    .to_string();

    let args = serde_json::json!({ "script": script })
        .as_object()
        .expect("object literal")
        .clone();
    let result = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("execute").with_arguments(args))
        .await
        .expect("execute should complete at the protocol level");

    assert_eq!(
        result.is_error,
        Some(false),
        "introspection script should succeed: {:?}",
        result.content
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("script returned text");
    assert!(
        text.contains("not installed"),
        "colliding tool must be absent from the Lua global; got: {text}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn unique_sibling_of_colliding_tools_remains_callable() {
    // Property: collision-quarantine is scoped to the colliding pair,
    // not the whole upstream. The `unique` tool on the same upstream
    // must still be inspectable and callable.
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    // Inspect the unique tool first (the gate requires it).
    let inspect_args =
        serde_json::json!({ "luaServerName": "colliding_upstream", "luaToolName": "unique" })
            .as_object()
            .expect("object literal")
            .clone();
    let inspect_result = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("tool-details").with_arguments(inspect_args))
        .await
        .expect("tool-details should succeed for the unique tool");
    assert_eq!(
        inspect_result.is_error,
        Some(false),
        "unique tool must be inspectable despite siblings colliding: {:?}",
        inspect_result.content
    );

    let script = r#"
        local r = colliding_upstream.unique({})
        result(r)
    "#
    .to_string();
    let args = serde_json::json!({ "script": script })
        .as_object()
        .expect("object literal")
        .clone();
    let result = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("execute").with_arguments(args))
        .await
        .expect("execute should complete at the protocol level");

    assert_eq!(
        result.is_error,
        Some(false),
        "unique tool must dispatch normally; got: {:?}",
        result.content
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("script returned text");
    assert!(
        text.contains("called: unique"),
        "dispatch should hit the unique tool; got: {text}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn list_server_tools_flags_collisions_as_unavailable() {
    // Property: the agent's catalog view (list-server-tools) must show
    // the colliding tools under an "unavailable" section so a developer
    // debugging "why isn't safe_tool callable" can see the conflict
    // without reading gateway logs.
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    let args = serde_json::json!({ "luaServerName": "colliding_upstream" })
        .as_object()
        .expect("object literal")
        .clone();
    let result = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("list-server-tools").with_arguments(args))
        .await
        .expect("list-server-tools should succeed");

    assert_eq!(result.is_error, Some(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("listing has text content");

    assert!(
        text.contains("- unique"),
        "unique tool must appear as available: {text}"
    );
    assert!(
        text.contains("unavailable"),
        "unavailable section must exist when there are collisions: {text}"
    );
    assert!(
        text.contains("safe_tool") && text.contains("safe-tool") && text.contains("safe.tool"),
        "unavailable section must show the sanitized name and both originals: {text}"
    );
    // Reserved-word / already-prefixed pair: both "nil" (prefixed to
    // "_nil" by the sanitizer) and the literal "_nil" collide on the
    // sanitized identifier "_nil". Pins the prefix-collision path.
    // HashMap iteration makes the original order nondeterministic, so
    // check either legal ordering.
    assert!(
        text.contains("- _nil: nil, _nil") || text.contains("- _nil: _nil, nil"),
        "unavailable section must list the _nil sanitized name with both originals: {text}"
    );

    harness.shutdown().await;
}
