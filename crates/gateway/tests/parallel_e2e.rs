// pattern: Imperative Shell
//
// End-to-end proof that `_gateway.parallel` actually runs upstream tool
// calls concurrently. The shape mirrors `execute_lua.rs`: a fake

mod common;
// in-process MCP upstream + a real gateway over an in-memory duplex,
// plus a real rmcp client speaking streamable HTTP to the gateway.
//
// The parallelism proof is a wall-clock test — deliberate, because
// that's the observable property we care about. Each upstream tool
// call deliberately sleeps on the upstream side for a known duration;
// we assert the total `_gateway.parallel(N)` wall-clock stays under
// the sum of all sleeps. If the futures were executed serially, the
// total would be at least N * delay; if concurrent, it's ~delay plus
// a small overhead.
//
// The sleep duration is chosen to be large enough that the difference
// between serial and concurrent is unmistakable (500ms) but small
// enough not to slow the test suite down (total should finish in
// under ~1s with parallel dispatch).

use std::sync::Arc;
use std::time::{Duration, Instant};

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
//
// Exposes two tools:
//   - `slow_echo({ message, delay_ms })`: sleeps for delay_ms then
//     returns "echo: <message>". Load-bearing for the parallelism
//     wall-clock assertion.
//   - `fail_me({ reason })`: always returns an is_error result. Used
//     to prove per-task envelopes carry upstream errors back without
//     aborting the whole parallel call.

#[derive(Clone)]
struct FakeSlowUpstream;

impl ServerHandler for FakeSlowUpstream {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("fake-slow-upstream", "0.0.1"))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let echo_schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
            serde_json::from_value(serde_json::json!({
                "type": "object",
                "properties": {
                    "message": {"type": "string"},
                    "delay_ms": {"type": "number"}
                },
                "required": ["message"],
                "additionalProperties": false,
            }))
            .expect("static echo schema"),
        );
        let fail_schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
            serde_json::from_value(serde_json::json!({
                "type": "object",
                "properties": { "reason": {"type": "string"} },
                "required": [],
                "additionalProperties": false,
            }))
            .expect("static fail schema"),
        );
        Ok(ListToolsResult {
            tools: vec![
                Tool::new(
                    "slow_echo",
                    "Sleeps delay_ms then echoes message.",
                    echo_schema,
                ),
                Tool::new("fail_me", "Always returns is_error.", fail_schema),
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
        match request.name.as_ref() {
            "slow_echo" => {
                let message = request
                    .arguments
                    .as_ref()
                    .and_then(|a| a.get("message"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let delay_ms = request
                    .arguments
                    .as_ref()
                    .and_then(|a| a.get("delay_ms"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                // The whole point — actually block on the upstream
                // side so the gateway's parallelism (or lack thereof)
                // is observable from the outside.
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                Ok(CallToolResult::success(vec![Content::text(format!(
                    "echo: {message}"
                ))]))
            }
            "fail_me" => {
                let reason = request
                    .arguments
                    .as_ref()
                    .and_then(|a| a.get("reason"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("no reason given")
                    .to_string();
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "deliberate failure: {reason}"
                ))]))
            }
            other => Err(ErrorData::invalid_params(
                format!("unknown tool: {other}"),
                None,
            )),
        }
    }
}

// -------- Minimal downstream client --------

#[derive(Clone)]
struct PlainDownstreamClient;

impl ClientHandler for PlainDownstreamClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("plain-downstream-for-parallel", "0.0.1"),
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

    let (upstream_handle, fake_upstream_task) =
        common::spawn_fake_upstream("fake-slow", "fake_slow", FakeSlowUpstream, &bus, &broker)
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
    }
}

// Inspect a tool to satisfy the per-session inspection gate. Helper
// avoids repeating the boilerplate across tests.
async fn inspect_tool(
    client: &rmcp::service::RunningService<rmcp::RoleClient, PlainDownstreamClient>,
    lua_server: &str,
    lua_tool: &str,
) {
    let args = serde_json::json!({
        "luaServerName": lua_server,
        "luaToolName": lua_tool
    })
    .as_object()
    .expect("object literal")
    .clone();
    let result = client
        .peer()
        .call_tool(CallToolRequestParams::new("tool-details").with_arguments(args))
        .await
        .expect("tool-details call should succeed");
    assert_eq!(
        result.is_error,
        Some(false),
        "tool-details for {lua_server}.{lua_tool} must succeed: {:?}",
        result.content
    );
}

// Extract the text content from a successful execute call.
fn text_from(result: &CallToolResult) -> &str {
    result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("result has text content")
}

// -------- Tests --------

#[tokio::test]
async fn parallel_dispatch_runs_upstream_calls_concurrently() {
    // The load-bearing test. Three slow_echo calls, each sleeping
    // 500ms on the upstream side. If dispatch were serial, total
    // wall-clock would be ~1500ms. Concurrent dispatch completes
    // in a bit more than 500ms.
    //
    // We give ourselves ample slack — assert under 1200ms, well below
    // the 1500ms serial floor but generous enough that CI jitter
    // doesn't produce flakes.
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    inspect_tool(&client_service, "fake_slow", "slow_echo").await;

    let script = r#"
        local results = _gateway.parallel({
            { server = "fake_slow", tool = "slow_echo", args = { message = "a", delay_ms = 500 } },
            { server = "fake_slow", tool = "slow_echo", args = { message = "b", delay_ms = 500 } },
            { server = "fake_slow", tool = "slow_echo", args = { message = "c", delay_ms = 500 } },
        })
        result({
            count = #results,
            a = results[1].value,
            b = results[2].value,
            c = results[3].value,
            a_ok = results[1].ok,
            b_ok = results[2].ok,
            c_ok = results[3].ok,
        })
    "#
    .to_string();

    let args = serde_json::json!({ "script": script })
        .as_object()
        .expect("object literal")
        .clone();

    let start = Instant::now();
    let result = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("execute").with_arguments(args))
        .await
        .expect("execute call succeeds");
    let elapsed = start.elapsed();

    assert_eq!(
        result.is_error,
        Some(false),
        "parallel execute must succeed; content={:?}",
        result.content
    );

    // Parse the result as JSON to verify all three calls came back.
    let text = text_from(&result);
    let parsed: serde_json::Value =
        serde_json::from_str(text).expect("execute result parses as JSON");
    assert_eq!(parsed["count"], serde_json::json!(3));
    assert_eq!(parsed["a"], serde_json::json!("echo: a"));
    assert_eq!(parsed["b"], serde_json::json!("echo: b"));
    assert_eq!(parsed["c"], serde_json::json!("echo: c"));
    assert_eq!(parsed["a_ok"], serde_json::json!(true));
    assert_eq!(parsed["b_ok"], serde_json::json!(true));
    assert_eq!(parsed["c_ok"], serde_json::json!(true));

    // The parallelism assertion — three 500ms sleeps in under 1200ms
    // means they overlapped.
    assert!(
        elapsed < Duration::from_millis(1200),
        "parallel dispatch of three 500ms calls must complete well under 1500ms \
         (serial floor); got {elapsed:?}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn parallel_preserves_partial_failures_as_envelopes() {
    // One tool succeeds, one deliberately fails (is_error=true from
    // the upstream). The parallel call itself succeeds; per-task
    // envelopes reflect each outcome.
    //
    // The is_error upstream response isn't a Lua error (pcall wouldn't
    // catch it either), so it surfaces as ok=true with a value carrying
    // the is_error envelope. That's the documented behaviour.
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    inspect_tool(&client_service, "fake_slow", "slow_echo").await;
    inspect_tool(&client_service, "fake_slow", "fail_me").await;

    let script = r#"
        local results = _gateway.parallel({
            { server = "fake_slow", tool = "slow_echo", args = { message = "ok" } },
            { server = "fake_slow", tool = "fail_me", args = { reason = "on purpose" } },
        })
        -- fail_me returns is_error=true via the content envelope, which
        -- is a SUCCESSFUL upstream call from parallel's perspective
        -- (pcall semantics). So both tasks are ok=true; the caller
        -- inspects the value to decide.
        local second_value = results[2].value
        result({
            count = #results,
            first_ok = results[1].ok,
            first_value = results[1].value,
            second_ok = results[2].ok,
            second_value_is_error = (type(second_value) == "table") and second_value.isError or false,
        })
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
        .expect("execute call succeeds");

    assert_eq!(
        result.is_error,
        Some(false),
        "execute must succeed; content={:?}",
        result.content
    );
    let text = text_from(&result);
    let parsed: serde_json::Value = serde_json::from_str(text).expect("parses");
    assert_eq!(parsed["count"], serde_json::json!(2));
    assert_eq!(parsed["first_ok"], serde_json::json!(true));
    assert_eq!(parsed["first_value"], serde_json::json!("echo: ok"));
    assert_eq!(parsed["second_ok"], serde_json::json!(true));
    assert_eq!(parsed["second_value_is_error"], serde_json::json!(true));

    harness.shutdown().await;
}

#[tokio::test]
async fn parallel_empty_list_completes_without_upstream_contact() {
    // Property: `_gateway.parallel({})` completes without touching
    // any upstream. We can't directly observe "no upstream contact"
    // from the outside, but we CAN assert the call returns in
    // vanishingly small time (no cache miss, no dispatch).
    let harness = start_harness().await;

    let client = PlainDownstreamClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    let script = r#"
        local results = _gateway.parallel({})
        result({ count = #results })
    "#
    .to_string();

    let args = serde_json::json!({ "script": script })
        .as_object()
        .expect("object literal")
        .clone();
    let start = Instant::now();
    let result = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("execute").with_arguments(args))
        .await
        .expect("execute succeeds");
    let elapsed = start.elapsed();

    assert_eq!(
        result.is_error,
        Some(false),
        "execute must succeed; content={:?}",
        result.content
    );
    let text = text_from(&result);
    let parsed: serde_json::Value = serde_json::from_str(text).expect("parses");
    assert_eq!(parsed["count"], serde_json::json!(0));
    // Sanity bound — should be well under 500ms (the upstream doesn't
    // see this call at all).
    assert!(
        elapsed < Duration::from_millis(500),
        "empty parallel call should be essentially instant; got {elapsed:?}"
    );

    harness.shutdown().await;
}
