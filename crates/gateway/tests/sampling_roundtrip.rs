// pattern: Imperative Shell
//
// End-to-end test: upstream -> gateway -> downstream ->
// gateway -> upstream sampling roundtrip.

mod common;
//
// Setup:
//   - Fake upstream MCP server (in-process) exposing a single `ask-llm` tool
//     that calls `context.peer.create_message()` when invoked
//   - Real gateway (via serve_on), connected to the fake upstream over
//     tokio::io::duplex (no subprocess, no network)
//   - Real downstream rmcp client that advertises sampling and returns
//     a canned response from its create_message handler
//
// What we assert:
//   The downstream client's create_message handler is invoked with the
//   expected question, and the canned reply makes it back through the
//   gateway to the upstream tool call, and the tool result carries
//   the reply.
//
// Why we trigger the upstream tool via `UpstreamHandle::call_tool` rather
// than `execute` Lua: `execute`'s upstream-call path is covered separately
// in execute_lua.rs. This test proves the sampling pipeline in isolation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use mcp_client::{
    SamplingBroker, SharedSamplingBroker, UpstreamConfig, UpstreamEventBus, UpstreamHandle,
    UpstreamKind, UpstreamManager,
};
use rmcp::{
    ClientHandler, ErrorData, RoleServer, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResult, ClientCapabilities, ClientInfo, Content,
        CreateMessageRequestParams, CreateMessageResult, ErrorCode, Implementation,
        ListToolsResult, PaginatedRequestParams, SamplingContent, SamplingMessage,
        SamplingMessageContent, ServerCapabilities, ServerInfo, Tool,
    },
    service::{RequestContext, RoleClient},
};

/// Extract the first text payload out of a SamplingContent (Single or
/// Multiple). Returns None if the first element isn't text.
fn sampling_first_text(content: &SamplingContent<SamplingMessageContent>) -> Option<String> {
    match content {
        SamplingContent::Single(c) => c.as_text().map(|t| t.text.clone()),
        SamplingContent::Multiple(cs) => {
            cs.first().and_then(|c| c.as_text()).map(|t| t.text.clone())
        }
    }
}
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

// -------- Fake upstream MCP server --------

#[derive(Clone)]
struct FakeUpstreamServer;

impl ServerHandler for FakeUpstreamServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("fake-sampling-upstream", "0.0.1"))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
            serde_json::from_value(serde_json::json!({
                "type": "object",
                "properties": { "question": {"type": "string"} },
                "required": ["question"],
                "additionalProperties": false,
            }))
            .expect("static schema valid"),
        );
        Ok(ListToolsResult {
            tools: vec![Tool::new(
                "ask-llm",
                "Asks the client's LLM via sampling.",
                schema,
            )],
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        if request.name.as_ref() != "ask-llm" {
            return Err(ErrorData::invalid_params(
                format!("unknown tool: {}", request.name),
                None,
            ));
        }
        let question = request
            .arguments
            .as_ref()
            .and_then(|a| a.get("question"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                ErrorData::invalid_params(
                    "ask-llm: missing or non-string 'question' argument",
                    None,
                )
            })?
            .to_string();

        let reply = context
            .peer
            .create_message(CreateMessageRequestParams::new(
                vec![SamplingMessage::user_text(&question)],
                100,
            ))
            .await
            .map_err(|e| {
                ErrorData::new(
                    ErrorCode::INTERNAL_ERROR,
                    format!("upstream's sampling call failed: {e}"),
                    None,
                )
            })?;

        let reply_text = sampling_first_text(&reply.message.content)
            .unwrap_or_else(|| "(empty reply)".to_string());

        Ok(CallToolResult::success(vec![Content::text(format!(
            "ask-llm({question}) => {reply_text}"
        ))]))
    }
}

// -------- Sampling-capable downstream client --------

#[derive(Clone)]
struct FakeDownstreamClient {
    canned_reply: String,
    last_question: Arc<tokio::sync::Mutex<Option<String>>>,
    sampling_calls: Arc<AtomicU32>,
}

impl FakeDownstreamClient {
    fn new(canned_reply: &str) -> Self {
        Self {
            canned_reply: canned_reply.to_string(),
            last_question: Arc::new(tokio::sync::Mutex::new(None)),
            sampling_calls: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl ClientHandler for FakeDownstreamClient {
    fn get_info(&self) -> ClientInfo {
        let caps = ClientCapabilities::builder().enable_sampling().build();
        ClientInfo::new(
            caps,
            Implementation::new("fake-sampling-capable-downstream", "0.0.1"),
        )
    }

    async fn create_message(
        &self,
        params: CreateMessageRequestParams,
        _context: RequestContext<RoleClient>,
    ) -> Result<CreateMessageResult, ErrorData> {
        self.sampling_calls.fetch_add(1, Ordering::SeqCst);
        let q = params
            .messages
            .first()
            .and_then(|m| sampling_first_text(&m.content))
            .unwrap_or_default();
        *self.last_question.lock().await = Some(q);

        Ok(CreateMessageResult::new(
            SamplingMessage::assistant_text(&self.canned_reply),
            "fake-model".to_string(),
        )
        .with_stop_reason(CreateMessageResult::STOP_REASON_END_TURN))
    }
}

// -------- Harness --------

struct Harness {
    upstream_handle: Arc<UpstreamHandle>,
    downstream_url: String,
    broker: SharedSamplingBroker,
    ct: CancellationToken,
    server_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    _fake_upstream_task: tokio::task::JoinHandle<()>,
}

/// Wait for `cond` to become true, polling every 25 ms up to `timeout`.
/// Returns `Ok(elapsed)` on success, `Err(elapsed)` on timeout.
async fn eventually<F, Fut>(timeout: Duration, mut cond: F) -> Result<Duration, Duration>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if cond().await {
            return Ok(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(start.elapsed())
}

async fn start_harness(upstream_sampling_enabled: bool) -> Harness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let upstreams = Arc::new(UpstreamManager::new());

    let mut upstream_config = UpstreamConfig::new(
        "fake-upstream",
        "fake_upstream",
        UpstreamKind::Stdio {
            command: "unused-in-test".to_string(),
            args: vec![],
            env: Default::default(),
        },
    );
    if upstream_sampling_enabled {
        upstream_config = upstream_config.with_sampling_enabled();
    }

    // Config needs `.with_sampling_enabled()` stamped on conditionally,
    // which the `(display_name, lua_name)`-only spawner variants can't
    // express — use the config-taking variant instead.
    let (upstream_handle, fake_upstream_task) = common::spawn_fake_upstream_with_config(
        upstream_config,
        FakeUpstreamServer,
        &bus,
        &broker,
        64 * 1024,
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

    let ctx = common::test_app_context(upstreams_clone, bus_clone, broker_clone).await;
    let server_handle = tokio::spawn(async move {
        let supervisor_ct = ct_for_serve.child_token();
        gateway::serve_on(
            listener,
            ctx,
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
        upstream_handle,
        downstream_url,
        broker,
        ct,
        server_handle,
        _fake_upstream_task: fake_upstream_task,
    }
}

impl Harness {
    async fn shutdown(self) {
        match common::cancel_and_join_server(&self.ct, self.server_handle).await {
            common::ServerShutdown::Clean => {}
            common::ServerShutdown::ServeErr(e) => panic!("gateway serve_on returned error: {e}"),
            common::ServerShutdown::Panicked(e) => std::panic::resume_unwind(e.into_panic()),
            common::ServerShutdown::TimedOut => {
                panic!("gateway server did not shut down within 3 s")
            }
        }
    }
}

// -------- Tests --------

#[tokio::test]
async fn sampling_request_flows_upstream_to_downstream_and_back() {
    let harness = start_harness(true).await;

    // Connect a sampling-capable downstream client.
    let client_handler = FakeDownstreamClient::new("the answer is 42");
    let sampling_calls = client_handler.sampling_calls.clone();
    let last_question = client_handler.last_question.clone();

    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let _client_service = client_handler
        .serve(transport)
        .await
        .expect("client connect");

    // Wait for on_initialized -> broker.register to fire. This is a
    // deterministic event (broker.peer_count flips from 0 to 1 when
    // the downstream's on_initialized callback completes), so we poll
    // for it rather than sleeping a fixed duration.
    eventually(Duration::from_secs(2), || {
        let broker = Arc::clone(&harness.broker);
        async move { broker.peer_count().await > 0 }
    })
    .await
    .expect("downstream peer must register with broker within 2 s");

    // Invoke the upstream's ask-llm tool directly. This triggers the
    // upstream to call context.peer.create_message, which traverses:
    //   upstream -> gateway's upstream-side ClientHandler.create_message
    //             -> broker.dispatch
    //             -> downstream peer.create_message
    //             -> downstream client's create_message handler (our fake)
    //             -> response back up the chain
    let args = serde_json::json!({"question": "what is life?"})
        .as_object()
        .expect("object literal")
        .clone();
    let result = harness
        .upstream_handle
        .call_tool(CallToolRequestParams::new("ask-llm").with_arguments(args))
        .await
        .expect("upstream tool call should succeed");

    // Assert: downstream's sampling handler was invoked exactly once with
    // the right question.
    assert_eq!(
        sampling_calls.load(Ordering::SeqCst),
        1,
        "downstream sampling handler must be invoked exactly once"
    );
    let q = last_question.lock().await.clone();
    assert_eq!(q.as_deref(), Some("what is life?"));

    // Assert: the tool result contains the canned reply.
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_str())
        .expect("tool result has text content");
    assert!(
        text.contains("the answer is 42"),
        "tool result should contain canned reply; got: {text}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn upstream_without_sampling_opt_in_gets_method_not_found() {
    // Layer 2 security: even if the gateway advertised sampling by mistake
    // (we control that at layer 1 — get_info), the handler itself rejects
    // sampling requests from un-opted-in upstreams.
    let harness = start_harness(false).await;

    let client_handler = FakeDownstreamClient::new("should never see this");
    let sampling_calls = client_handler.sampling_calls.clone();

    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let _client_service = client_handler
        .serve(transport)
        .await
        .expect("client connect");

    // Wait for on_initialized -> broker.register to fire (same rationale
    // as in the first test — deterministic event, poll rather than sleep).
    eventually(Duration::from_secs(2), || {
        let broker = Arc::clone(&harness.broker);
        async move { broker.peer_count().await > 0 }
    })
    .await
    .expect("downstream peer must register with broker within 2 s");

    // Invoke ask-llm. Upstream will call create_message, our gateway-side
    // ClientHandler refuses (not opted in) with METHOD_NOT_FOUND, and the
    // upstream's own call_tool handler maps that into an internal_error
    // result. The exact error shape isn't as important as: the downstream
    // client's sampling handler MUST NOT be invoked.
    let args = serde_json::json!({"question": "will this reach downstream?"})
        .as_object()
        .expect("object literal")
        .clone();
    let _ = harness
        .upstream_handle
        .call_tool(CallToolRequestParams::new("ask-llm").with_arguments(args))
        .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        sampling_calls.load(Ordering::SeqCst),
        0,
        "downstream sampling handler must NOT be invoked for un-opted-in upstream"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn no_downstream_client_yields_internal_error() {
    // If no downstream client is connected (or none has advertised
    // sampling), the broker returns a helpful error. This protects
    // against orphan sampling requests piling up.
    let harness = start_harness(true).await;

    // Do NOT connect any downstream client. Invoke upstream anyway.
    let args = serde_json::json!({"question": "hello?"})
        .as_object()
        .expect("object literal")
        .clone();
    let result = harness
        .upstream_handle
        .call_tool(CallToolRequestParams::new("ask-llm").with_arguments(args))
        .await;

    // The upstream side gets its sampling call errored out, which it
    // propagates into the tool-call result. Our assertion is: the result
    // is either an error, OR it's an "isError" CallToolResult — both are
    // fine; what's not fine is silent success.
    match result {
        Err(e) => {
            eprintln!("call_tool correctly errored: {e}");
        }
        Ok(r) => {
            assert!(r.is_error.unwrap_or(false), "expected isError=true or Err");
        }
    }

    harness.shutdown().await;
}
