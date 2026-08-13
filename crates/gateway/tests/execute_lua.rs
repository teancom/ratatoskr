// pattern: Imperative Shell
//
// Integration test: Lua `execute` tool calls one upstream tool.

mod common;
//
// Setup (mirrors the sampling roundtrip harness in sampling_roundtrip.rs
// so the two tests share a mental model):
//   - Fake upstream MCP server (in-process) exposing a single `echo` tool
//     that returns its `message` argument back as text
//   - Real gateway (via serve_on), connected to the fake upstream over
//     tokio::io::duplex (no subprocess, no network)
//   - Real rmcp client that connects to the gateway over streamable HTTP
//     and calls the gateway's `execute` tool
//
// What we assert:
//   1. A Luau script that calls `fake_upstream.echo({message="hi"})` and
//      passes the result to `result(...)` returns "hi" to the calling
//      client. This is THE gate assertion.
//   2. Calling `execute` without a `script` argument fails with
//      invalid_params — rmcp surfaces this as an error, not a result.
//   3. A script that executes cleanly but forgets `result(...)` is
//      reported as an is_error CallToolResult carrying a clear message.

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

// -------- Fake upstream MCP server --------

#[derive(Clone)]
struct FakeUpstreamServer;

impl ServerHandler for FakeUpstreamServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("fake-upstream-for-execute", "0.0.1"))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
            serde_json::from_value(serde_json::json!({
                "type": "object",
                "properties": { "message": {"type": "string"} },
                "required": ["message"],
                "additionalProperties": false,
            }))
            .expect("static schema valid"),
        );
        Ok(ListToolsResult {
            tools: vec![Tool::new("echo", "Echoes back its message.", schema)],
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        if request.name.as_ref() != "echo" {
            return Err(ErrorData::invalid_params(
                format!("unknown tool: {}", request.name),
                None,
            ));
        }
        let message = request
            .arguments
            .as_ref()
            .and_then(|a| a.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(CallToolResult::success(vec![Content::text(format!(
            "echo: {message}"
        ))]))
    }
}

// -------- Minimal downstream client (no sampling needed) --------

#[derive(Clone)]
struct PlainDownstreamClient;

impl ClientHandler for PlainDownstreamClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("plain-downstream-for-execute", "0.0.1"),
        )
    }
}

// -------- Harness --------

struct Harness {
    _upstream_handle: Arc<UpstreamHandle>,
    downstream_url: String,
    ct: CancellationToken,
    server_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    _fake_upstream_task: tokio::task::JoinHandle<()>,
}

async fn start_harness() -> Harness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let upstreams = Arc::new(UpstreamManager::new());

    // Display name: "fake-upstream" — sanitized lua_name: "fake_upstream".
    let (upstream_handle, fake_upstream_task) = common::spawn_fake_upstream(
        "fake-upstream",
        "fake_upstream",
        FakeUpstreamServer,
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
            // Call the helper inside the spawned future so we don't
            // need an extra top-level `await`. `async move` already
            // handles the Arc moves.
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
        _upstream_handle: upstream_handle,
        downstream_url,
        ct,
        server_handle,
        _fake_upstream_task: fake_upstream_task,
    }
}

impl Harness {
    async fn shutdown(self) {
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
    }
}

// -------- Tests --------

#[tokio::test]
async fn execute_runs_lua_script_that_calls_upstream_tool() {
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    // Inspect first: the gateway gates execute-script tool calls on
    // whether the session has previously inspected the tool. Without
    // this step the execute below would fail with a NotInspected
    // error. This is the documented workflow, not a test shortcut.
    let inspect_args =
        serde_json::json!({ "luaServerName": "fake_upstream", "luaToolName": "echo" })
            .as_object()
            .expect("object literal")
            .clone();
    let inspect_result = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("tool-details").with_arguments(inspect_args))
        .await
        .expect("tool-details call should succeed");
    assert_eq!(
        inspect_result.is_error,
        Some(false),
        "tool-details should succeed before execute: {:?}",
        inspect_result.content
    );

    // The gate assertion: a Luau script that calls an upstream tool and
    // passes its result to result(...) yields that value back.
    let script = r#"
        local reply = fake_upstream.echo({ message = "hi" })
        result(reply)
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
        .expect("execute tool call should succeed");

    assert_eq!(
        result.is_error,
        Some(false),
        "execute should return a success result; got is_error={:?}, content={:?}",
        result.is_error,
        result.content
    );

    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("execute result has text content");
    assert!(
        text.contains("echo: hi"),
        "execute result should contain echoed message; got: {text}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn execute_without_script_arg_is_invalid_params() {
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    let args = serde_json::json!({})
        .as_object()
        .expect("object literal")
        .clone();
    let result = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("execute").with_arguments(args))
        .await;

    // The rmcp layer surfaces ErrorData::invalid_params as a transport-level
    // JSON-RPC error, so the client sees Err(...) here rather than Ok(r)
    // with is_error=true. That's the correct shape for "the call itself
    // was malformed".
    match result {
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                msg.contains("script") && msg.contains("required"),
                "error message should mention the missing script arg; got: {msg}"
            );
        }
        Ok(r) => panic!("expected Err(invalid_params) for missing script arg; got Ok({r:?})"),
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn execute_script_without_result_call_returns_is_error() {
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    // Script compiles and runs, but never calls result(...). Different
    // from invalid_params: here the script itself is the problem, so the
    // call succeeds at the protocol level and returns is_error=true with
    // a diagnostic the agent can read.
    let script = "local x = 1 + 1".to_string();
    let args = serde_json::json!({ "script": script })
        .as_object()
        .expect("object literal")
        .clone();
    let result = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("execute").with_arguments(args))
        .await
        .expect("execute tool call should complete at protocol level");

    assert_eq!(
        result.is_error,
        Some(true),
        "script without result() must produce is_error=true"
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("error result has text content");
    assert!(
        text.contains("result"),
        "error message should mention the missing result() call; got: {text}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn execute_rejects_uninspected_upstream_tool_call() {
    // Enforcement gate: an execute script that calls an upstream tool
    // the session hasn't inspected must fail with a clear diagnostic,
    // not silently dispatch. This is the structural property the
    // tool-details/inspect-tool-response workflow relies on.
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    // Deliberately DO NOT call tool-details first.
    let script = r#"
        local reply = fake_upstream.echo({ message = "hi" })
        result(reply)
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
        .expect("execute tool call should complete at protocol level");

    // Script-level failure: is_error=true, text names the tool and
    // points the agent at how to fix it.
    assert_eq!(
        result.is_error,
        Some(true),
        "uninspected tool call must produce is_error=true"
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("error result has text content");
    assert!(
        text.contains("fake_upstream") && text.contains("echo"),
        "error should name the offending tool; got: {text}"
    );
    assert!(
        text.contains("tool-details"),
        "error should point the agent at tool-details; got: {text}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn execute_allows_inspected_tool_call_across_scripts() {
    // Lifetime assertion: once a session has inspected a tool, it stays
    // inspected for subsequent execute calls. This is what makes the
    // 24-hour session keep-alive valuable — Claude inspects a tool
    // once, then reuses it freely across many scripts in the same
    // session.
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    // Inspect once.
    let inspect_args =
        serde_json::json!({ "luaServerName": "fake_upstream", "luaToolName": "echo" })
            .as_object()
            .expect("object literal")
            .clone();
    let r1 = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("tool-details").with_arguments(inspect_args))
        .await
        .expect("tool-details should succeed");
    assert_eq!(r1.is_error, Some(false));

    // Two separate execute calls, both succeed because the session
    // remembers the inspection.
    for round in 1..=2 {
        let script = format!(
            r#"
            local reply = fake_upstream.echo({{ message = "round-{round}" }})
            result(reply)
        "#
        );
        let args = serde_json::json!({ "script": script })
            .as_object()
            .expect("object literal")
            .clone();
        let result = client_service
            .peer()
            .call_tool(CallToolRequestParams::new("execute").with_arguments(args))
            .await
            .expect("execute should succeed at protocol level");
        assert_eq!(
            result.is_error,
            Some(false),
            "round {round}: execute should succeed after inspection"
        );
        let text = result
            .content
            .first()
            .and_then(|c| c.as_text())
            .map(|t| t.text.as_str())
            .expect("execute result has text content");
        assert!(
            text.contains(&format!("round-{round}")),
            "round {round}: content mismatch; got: {text}"
        );
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn inspect_tool_response_marks_inspection_for_execute() {
    // Property: inspect-tool-response is a second valid path to "I have
    // read this tool's contract" — it actually CALLS the tool. After
    // a successful inspect-tool-response, execute scripts must be able
    // to call the same tool without first going through tool-details.
    //
    // This pins a property that isn't otherwise exercised: the marking
    // happens in `call_inspect_tool_response`, not just in
    // `call_tool_details`.
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    // Probe the tool via inspect-tool-response (instead of tool-details).
    let inspect_args = serde_json::json!({
        "luaServerName": "fake_upstream",
        "luaToolName": "echo",
        "arguments": { "message": "probe" },
    })
    .as_object()
    .expect("object literal")
    .clone();
    let probe = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("inspect-tool-response").with_arguments(inspect_args))
        .await
        .expect("inspect-tool-response should succeed");
    assert_eq!(
        probe.is_error,
        Some(false),
        "inspect-tool-response must succeed against the live upstream: {:?}",
        probe.content
    );

    // Now execute, calling the same tool. Without the marking path
    // being exercised by inspect-tool-response, this would return
    // is_error=true with NotInspected.
    let script = r#"
        local reply = fake_upstream.echo({ message = "post-probe" })
        result(reply)
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
        .expect("execute should succeed at protocol level");
    assert_eq!(
        result.is_error,
        Some(false),
        "execute after inspect-tool-response must succeed; got {:?}",
        result.content
    );
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("execute returned text");
    assert!(
        text.contains("echo: post-probe"),
        "execute should have dispatched the tool successfully; got: {text}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn inspection_is_isolated_between_sessions() {
    // Critical security property at the wire level: the unit test in
    // tool_inspection.rs proves the store keeps sessions isolated, but
    // doesn't prove the gateway end-to-end actually distinguishes two
    // distinct MCP sessions hitting the same gateway. Two separate
    // `client_service` connections produce two distinct session IDs
    // via rmcp's streamable-http handshake; A's tool-details must not
    // grant B's execute permission to call the same tool.
    let harness = start_harness().await;

    // Session A: inspect, then run a quick script to confirm A is set up.
    let client_a = PlainDownstreamClient;
    let transport_a =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let session_a = client_a.serve(transport_a).await.expect("client A connect");

    let inspect_args =
        serde_json::json!({ "luaServerName": "fake_upstream", "luaToolName": "echo" })
            .as_object()
            .expect("object literal")
            .clone();
    let inspect_result = session_a
        .peer()
        .call_tool(CallToolRequestParams::new("tool-details").with_arguments(inspect_args))
        .await
        .expect("session A tool-details should succeed");
    assert_eq!(inspect_result.is_error, Some(false));

    // Sanity: A's execute now works (we proved this in another test,
    // but confirm here so the failure of B is unambiguous).
    let script = r#"
        local reply = fake_upstream.echo({ message = "from-A" })
        result(reply)
    "#
    .to_string();
    let args_a = serde_json::json!({ "script": &script })
        .as_object()
        .expect("object literal")
        .clone();
    let result_a = session_a
        .peer()
        .call_tool(CallToolRequestParams::new("execute").with_arguments(args_a))
        .await
        .expect("session A execute should complete");
    assert_eq!(
        result_a.is_error,
        Some(false),
        "session A execute should succeed after its own inspection: {:?}",
        result_a.content
    );
    let text_a = result_a
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("session A execute result has text");
    assert!(
        text_a.contains("from-A"),
        "session A content mismatch; got: {text_a}"
    );

    // Session B: a completely separate connection. NO inspection.
    // Calling the same tool must fail with NotInspected — A's inspection
    // must not leak across the session boundary.
    let client_b = PlainDownstreamClient;
    let transport_b =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let session_b = client_b.serve(transport_b).await.expect("client B connect");

    let args_b = serde_json::json!({ "script": &script })
        .as_object()
        .expect("object literal")
        .clone();
    let result_b = session_b
        .peer()
        .call_tool(CallToolRequestParams::new("execute").with_arguments(args_b))
        .await
        .expect("session B execute should complete at protocol level");
    assert_eq!(
        result_b.is_error,
        Some(true),
        "session B must NOT inherit session A's inspection — got is_error={:?}, content={:?}",
        result_b.is_error,
        result_b.content
    );
    let text_b = result_b
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("session B error result has text");
    assert!(
        text_b.contains("fake_upstream") && text_b.contains("echo"),
        "session B error must point at the un-inspected tool: {text_b}"
    );
    assert!(
        text_b.contains("tool-details"),
        "session B error must point at the remedy: {text_b}"
    );

    harness.shutdown().await;
}
