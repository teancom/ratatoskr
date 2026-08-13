// pattern: Imperative Shell
//
// End-to-end integration test for large-result offloading.

mod common;
//
// Properties verified:
//   1. Small result below threshold: returned verbatim to the agent.
//   2. Large result above threshold: replaced with a summary message
//      naming an execution ID and a shape tree — never the raw bytes.
//   3. Retrieval via `_gateway.get_result({id = ...})` in a follow-up
//      execute returns the stored data for in-Lua filtering.
//   4. Threshold = 0 disables offload entirely (large payload comes
//      back verbatim).
//   5. Session isolation: session B cannot read session A's stored
//      result even knowing the ID.

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

use gateway::config::GatewaySettings;

// -------- Fake upstream with an echo tool and a big_string tool --------

#[derive(Clone)]
struct FakeEchoServer;

impl ServerHandler for FakeEchoServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("fake-echo-for-offload", "0.0.1"))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
            serde_json::from_value(serde_json::json!({
                "type": "object",
                "properties": {
                    "message": { "type": "string" },
                },
                "additionalProperties": false,
            }))
            .expect("static schema"),
        );
        Ok(ListToolsResult {
            tools: vec![Tool::new(
                "echo",
                "Echoes back its message argument.",
                schema,
            )],
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
        Ok(CallToolResult::success(vec![Content::text(message)]))
    }
}

#[derive(Clone)]
struct PlainClient;

impl ClientHandler for PlainClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("offload-test-client", "0.0.1"),
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

/// Start a gateway whose offload threshold is `threshold_bytes`. Pass
/// 0 to disable offload entirely.
async fn start_harness(threshold_bytes: usize) -> Harness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let upstreams = Arc::new(UpstreamManager::new());

    // 256 KiB duplex buffer (vs. the common 64 KiB default): this
    // harness deliberately pushes payloads at/above the offload
    // threshold, so the default buffer would itself become a
    // bottleneck independent of the property under test.
    let (upstream_handle, fake_upstream_task) = common::spawn_fake_upstream_with_buf(
        "fake-echo",
        "fake_echo",
        FakeEchoServer,
        &bus,
        &broker,
        256 * 1024,
    )
    .await;
    upstreams.register(upstream_handle.clone()).await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().expect("local addr");
    let downstream_url = format!("http://{addr}/mcp");

    // Struct-update syntax so adding new `GatewaySettings` fields
    // downstream doesn't force every test to hand-set each one.
    let settings = GatewaySettings {
        result_size_threshold: threshold_bytes,
        ..GatewaySettings::default()
    };

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
            common::test_app_context_with_settings(
                upstreams_clone,
                bus_clone,
                broker_clone,
                settings,
            )
            .await,
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
                panic!("server task returned error on shutdown: {e}")
            }
            common::ServerShutdown::Panicked(join_err) => {
                panic!("server task panicked on shutdown: {join_err}")
            }
            // Unlike most harnesses, this one treats a shutdown timeout
            // as a hard failure rather than a warning: large in-flight
            // offload payloads must not linger past the budget.
            common::ServerShutdown::TimedOut => {
                panic!("server task did not stop within 3 s after cancellation")
            }
        }
    }
}

/// Inspect a tool to satisfy the NotInspected gate, then return.
/// Separated so individual tests can focus on the offload assertions
/// without re-pasting the inspection dance.
async fn inspect_echo(client: &rmcp::service::RunningService<rmcp::RoleClient, PlainClient>) {
    let args = serde_json::json!({ "luaServerName": "fake_echo", "luaToolName": "echo" })
        .as_object()
        .expect("object literal")
        .clone();
    let res = client
        .peer()
        .call_tool(CallToolRequestParams::new("tool-details").with_arguments(args))
        .await
        .expect("tool-details must succeed");
    assert_eq!(res.is_error, Some(false));
}

async fn run_execute(
    client: &rmcp::service::RunningService<rmcp::RoleClient, PlainClient>,
    script: &str,
) -> CallToolResult {
    let args = serde_json::json!({ "script": script })
        .as_object()
        .expect("object literal")
        .clone();
    client
        .peer()
        .call_tool(CallToolRequestParams::new("execute").with_arguments(args))
        .await
        .expect("execute tool call must succeed at the protocol level")
}

// -------- Tests --------

#[tokio::test]
async fn small_result_is_returned_verbatim() {
    // Property: a script that returns a tiny string (well below the
    // 1000-byte threshold) shows up in the response unchanged — no
    // offload metadata, no execution ID.
    let harness = start_harness(1000).await;
    let client = PlainClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    let result = run_execute(&client_service, r#"result("small")"#).await;
    assert_eq!(result.is_error, Some(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("must have text content");
    assert_eq!(text, "small", "small result must come back verbatim");
    assert!(
        !text.contains("Execution ID"),
        "small result must not carry offload metadata"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn large_result_is_offloaded_with_summary_only() {
    // Property: the script returns 2 KiB of payload against a 500-byte
    // threshold. Agent-facing text is the summary, NOT the raw bytes.
    let harness = start_harness(500).await;
    let client = PlainClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    let script = r#"
        local big = string.rep("A", 2000)
        result(big)
    "#;
    let result = run_execute(&client_service, script).await;
    assert_eq!(result.is_error, Some(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("must have text content");

    assert!(
        text.starts_with("Result offloaded:"),
        "expected offload header; got: {text}"
    );
    assert!(
        text.contains("Execution ID:"),
        "must name the execution ID: {text}"
    );
    assert!(
        text.contains("Structure:"),
        "must include structure block: {text}"
    );
    // The agent must NOT see the raw payload. "AAAAAAAA..." is the
    // canonical leak indicator for this script.
    assert!(
        !text.contains("AAAAAAAAAA"),
        "offload summary must not leak the raw payload: {text}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn get_result_retrieves_offloaded_payload_for_filtering() {
    // Property: a follow-up execute can use _gateway.get_result({id})
    // inside Lua to retrieve the stored payload, filter it, and
    // return only the filtered portion to the agent.
    //
    // Shape of the test:
    //   1. Run a script whose result is "Z" * 2000 — crosses the
    //      500-byte threshold, gets offloaded.
    //   2. Parse the execution ID out of the summary the agent sees.
    //   3. Run a follow-up script that fetches via get_result and
    //      returns the first 50 chars. The 50-char result is small,
    //      below the threshold, so it comes back verbatim.
    let harness = start_harness(500).await;
    let client = PlainClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    let script = r#"
        local big = string.rep("Z", 2000)
        result(big)
    "#;
    let first = run_execute(&client_service, script).await;
    let summary = first
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("first execute must return text");

    let execution_id = extract_execution_id(summary)
        .unwrap_or_else(|| panic!("summary must contain an Execution ID line: {summary}"));

    // Follow-up: retrieve and slice.
    let followup = format!(
        r#"
        local data = _gateway.get_result({{ id = "{execution_id}" }})
        result(string.sub(data, 1, 50))
        "#
    );
    let second = run_execute(&client_service, &followup).await;
    assert_eq!(second.is_error, Some(false));
    let filtered = second
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("follow-up execute must return text");
    assert_eq!(
        filtered.len(),
        50,
        "filtered result must be exactly 50 characters"
    );
    assert!(
        filtered.chars().all(|c| c == 'Z'),
        "filtered result must be Zs from the stored payload"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn threshold_zero_disables_offload_even_for_huge_results() {
    // Property: with threshold=0, even a 100 KB result comes back
    // verbatim. No offload metadata, no execution ID. This is the
    // "opt out" path for users who don't want the feature.
    let harness = start_harness(0).await;
    let client = PlainClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    let script = r#"
        local big = string.rep("Q", 100 * 1024)
        result(big)
    "#;
    let result = run_execute(&client_service, script).await;
    assert_eq!(result.is_error, Some(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("must have text content");
    assert!(
        !text.contains("Execution ID"),
        "threshold=0 must not trigger offload: {}",
        &text[..text.len().min(200)]
    );
    assert_eq!(
        text.len(),
        100 * 1024,
        "full payload must be returned; got {} bytes",
        text.len()
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn execute_offload_works_after_upstream_tool_call() {
    // Property: the offload path composes with upstream tool calls.
    // Script calls echo() to get a message back from the fake
    // upstream, then pads it to exceed the threshold — the result is
    // offloaded just as if it had been produced by pure Lua.
    //
    // This is the realistic shape: in production the large payloads
    // are Datadog log searches or Glean results, not hand-made
    // strings.
    let harness = start_harness(500).await;
    let client = PlainClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    inspect_echo(&client_service).await;

    let script = r#"
        local seed = fake_echo.echo({ message = "hello" })
        -- Upstream returns 5 chars; pad to blow through the threshold.
        local padded = seed .. string.rep("_", 1000)
        result(padded)
    "#;
    let result = run_execute(&client_service, script).await;
    assert_eq!(result.is_error, Some(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("must have text content");

    assert!(
        text.starts_with("Result offloaded:"),
        "composed script's oversized result must still take the offload path: {text}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn sessions_cannot_read_each_others_offloaded_results() {
    // Load-bearing isolation property: even knowing the execution ID,
    // a second session cannot retrieve another session's stored
    // payload. The store's session-keyed structure enforces this,
    // but proving it end-to-end guards against a future refactor
    // that accidentally globalized the key space.
    let harness = start_harness(500).await;
    let transport_a =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_a = PlainClient
        .serve(transport_a)
        .await
        .expect("client A connect");
    let transport_b =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_b = PlainClient
        .serve(transport_b)
        .await
        .expect("client B connect");

    // Session A stores something.
    let offload_script = r#"
        result(string.rep("S", 2000))
    "#;
    let first = run_execute(&client_a, offload_script).await;
    let summary = first
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("first execute produces text");
    let execution_id = extract_execution_id(summary)
        .unwrap_or_else(|| panic!("could not parse execution ID: {summary}"));

    // Session B tries to retrieve. get_result on an unknown (to B) ID
    // returns nil inside Lua — which the script below translates to
    // the string "missing".
    let leak_attempt = format!(
        r#"
        local data = _gateway.get_result({{ id = "{execution_id}" }})
        if data == nil then
            result("missing")
        else
            result("found")
        end
        "#
    );
    let leak_result = run_execute(&client_b, &leak_attempt).await;
    assert_eq!(
        leak_result.is_error,
        Some(false),
        "session-B get_result must not error"
    );
    let leak_text = leak_result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("leak attempt produces text");
    assert_eq!(
        leak_text, "missing",
        "session B must NOT read session A's stored result"
    );

    harness.shutdown().await;
}

/// Scrape the execution ID out of the offload summary text. Looks for
/// the "Execution ID: <id>" line; returns None if absent.
fn extract_execution_id(summary: &str) -> Option<String> {
    for line in summary.lines() {
        if let Some(id) = line.strip_prefix("Execution ID: ") {
            return Some(id.trim().to_string());
        }
    }
    None
}

// -------- inspect-tool-response offload tests --------
//
// Mirrors the `execute` offload properties for the `inspect-tool-response`
// path. The motivation: the gateway is the only layer that can enforce
// "no proxy response exceeds N bytes" as a structural guarantee — the
// agent has no way to predict how large an upstream's response will be
// before calling it. Without offload on this path, a probe call against
// an unknown tool could dump millions of characters into the agent's
// working memory with no warning.
//
// These tests pin the same safety property we already enforce for
// `execute`: over-threshold responses are stored server-side, replaced
// in the wire response with a summary + execution ID, and retrievable
// from a follow-up `execute` script via `_gateway.get_result`.

/// Call `inspect-tool-response` against the fake upstream's `echo` tool
/// with the given message. Returns the raw `CallToolResult` so tests
/// can assert on both `is_error` and content shape.
async fn run_inspect(
    client: &rmcp::service::RunningService<rmcp::RoleClient, PlainClient>,
    message: &str,
) -> CallToolResult {
    let args = serde_json::json!({
        "luaServerName": "fake_echo",
        "luaToolName": "echo",
        "arguments": { "message": message },
    })
    .as_object()
    .expect("object literal")
    .clone();
    client
        .peer()
        .call_tool(CallToolRequestParams::new("inspect-tool-response").with_arguments(args))
        .await
        .expect("inspect-tool-response tool call must succeed at the protocol level")
}

#[tokio::test]
async fn inspect_small_response_is_returned_verbatim() {
    // Property: inspect-tool-response below threshold passes the upstream
    // reply through untouched — the 90% case where the agent wants to
    // see exactly what the tool returned.
    let harness = start_harness(1000).await;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = PlainClient.serve(transport).await.expect("client connect");

    let result = run_inspect(&client_service, "small-payload").await;
    assert_eq!(result.is_error, Some(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("must have text content");
    assert_eq!(
        text, "small-payload",
        "small upstream response must come back verbatim"
    );
    assert!(
        !text.contains("Execution ID"),
        "small response must not carry offload metadata"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn inspect_large_response_is_offloaded_with_summary_only() {
    // Property: the upstream returns a response exceeding the threshold.
    // The agent-facing text must be the offload summary — execution ID,
    // structure block, retrieval snippet — NOT the raw payload bytes.
    //
    // The safety property this test pins: `inspect-tool-response`
    // against an unpredictable upstream cannot dump arbitrary byte
    // counts into the agent's context.
    let harness = start_harness(500).await;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = PlainClient.serve(transport).await.expect("client connect");

    // 2 KiB of "A"s. Comfortably over the 500-byte threshold once
    // wrapped in a CallToolResult/Content::text envelope.
    let big_message = "A".repeat(2000);
    let result = run_inspect(&client_service, &big_message).await;
    assert_eq!(result.is_error, Some(false));
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("must have text content");

    assert!(
        text.starts_with("Result offloaded:"),
        "expected offload header; got: {text}"
    );
    assert!(
        text.contains("Execution ID:"),
        "must name the execution ID: {text}"
    );
    assert!(
        text.contains("Structure:"),
        "must include structure block: {text}"
    );
    // The raw bytes must NOT reach the agent.
    assert!(
        !text.contains("AAAAAAAAAA"),
        "offload summary must not leak the raw payload: {text}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn inspect_offloaded_response_is_retrievable_via_get_result() {
    // Property: the end-to-end loop works. The agent gets an execution
    // ID from the `inspect-tool-response` summary, uses it inside a
    // follow-up `execute` script to retrieve the stored payload, and
    // can filter it down before returning. This is the critical
    // recovery path — without it, offload would be a black hole.
    let harness = start_harness(500).await;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = PlainClient.serve(transport).await.expect("client connect");

    let big_message = "Z".repeat(2000);
    let inspect_result = run_inspect(&client_service, &big_message).await;
    let summary = inspect_result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("inspect result must carry text");
    let execution_id = extract_execution_id(summary)
        .unwrap_or_else(|| panic!("summary must contain Execution ID line: {summary}"));

    // Follow-up: retrieve the stored payload via get_result and slice
    // into the structured response. The stored value is a full
    // CallToolResult envelope serialized from the upstream reply; the
    // Lua runtime rehydrates it back into a table, so the agent can
    // walk `data.content[1].text` without doing string manipulation.
    //
    // We return a 40-char prefix to prove the retrieval got the full
    // echoed payload, not a truncated or corrupted copy.
    let followup = format!(
        r#"
        local data = _gateway.get_result({{ id = "{execution_id}" }})
        local text = data.content[1].text
        result(string.sub(text, 1, 40))
        "#
    );
    let second = run_execute(&client_service, &followup).await;
    let second_text = second
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .unwrap_or("(no text)");
    assert_eq!(
        second.is_error,
        Some(false),
        "followup execute errored; content: {second_text}"
    );
    assert_eq!(
        second_text.len(),
        40,
        "filtered retrieval must be exactly 40 characters"
    );
    assert!(
        second_text.chars().all(|c| c == 'Z'),
        "filtered retrieval must be Zs from the stored payload: {second_text:?}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn inspect_threshold_zero_disables_offload() {
    // Property: with threshold=0, even a multi-KB upstream response
    // comes back verbatim through inspect-tool-response. Symmetric
    // with the execute-path test — the opt-out knob applies to both
    // paths the same way.
    let harness = start_harness(0).await;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = PlainClient.serve(transport).await.expect("client connect");

    let big_message = "Q".repeat(10 * 1024);
    let result = run_inspect(&client_service, &big_message).await;
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("must have text content");
    assert!(
        !text.contains("Execution ID"),
        "threshold=0 must not trigger offload on inspect path"
    );
    assert_eq!(
        text.len(),
        10 * 1024,
        "full upstream payload must be returned; got {} bytes",
        text.len()
    );

    harness.shutdown().await;
}
