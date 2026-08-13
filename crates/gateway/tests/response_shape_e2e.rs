// pattern: Imperative Shell
//
// End-to-end coverage for the `responseShape` / `responseShapeHint`
// fields in `tool-details`, which help agents pick the right
// `_gateway.*` builtin before they rediscover the response shape the
// hard way.

mod common;
//
// Three paths exercised:
//   1. Probe: inspect-tool-response first, tool-details second. The
//      probe populates the cache, the render reads it. Covers the
//      "text that is JSON" case because that's the one where the hint
//      is most agent-actionable (reach for unwrap_content + json_decode).
//   2. Schema: tool-details on a tool whose outputSchema describes a
//      structured payload, with no prior inspect. Schema alone
//      contributes `Structured`.
//   3. Absence: tool-details on a tool with neither schema signal nor
//      prior inspect. The responseShape fields must be OMITTED —
//      honest absence, not a confident guess.

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

// ---- Fake upstream with two tools: one JSON-in-text, one structured-declared ----

#[derive(Clone)]
struct FakeShapyServer;

impl ServerHandler for FakeShapyServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("fake-shapy", "0.0.1"))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        // Input schema every tool shares.
        let input_schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
            serde_json::from_value(serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false,
            }))
            .expect("static schema"),
        );

        // Tool #1: returns JSON-as-text. No output schema. Only the
        // probe path can classify this; schema alone cannot.
        let json_in_text = Tool::new(
            "get_incident",
            "Fetch an incident as JSON-in-text.",
            input_schema.clone(),
        );

        // Tool #2: declared structured output. Schema-only path should
        // classify this as `Structured` before any inspect fires.
        let mut structured_tool = Tool::new(
            "list_items",
            "List items as a structured array.",
            input_schema.clone(),
        );
        let output_schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
            serde_json::from_value(serde_json::json!({
                "type": "object",
                "properties": {
                    "items": {"type": "array"},
                    "total": {"type": "number"}
                }
            }))
            .expect("static schema"),
        );
        structured_tool.output_schema = Some(output_schema);

        // Tool #3: no schema signal, and we'll never inspect it. The
        // shape stays unknown, so the `responseShape` field must be
        // absent from tool-details output.
        let mystery = Tool::new(
            "mystery",
            "Opaque tool; shape is whatever it is.",
            input_schema,
        );

        Ok(ListToolsResult {
            tools: vec![json_in_text, structured_tool, mystery],
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
            "get_incident" => {
                // Real upstreams (Rootly, GitLab download_job_log, …)
                // ship JSON inside the text block. The classifier must
                // notice and emit `text-json`.
                let body = r#"{"id":"42","state":"open","severity":"sev2"}"#;
                Ok(CallToolResult::success(vec![Content::text(
                    body.to_string(),
                )]))
            }
            "list_items" => {
                // Not exercised in the schema-only test, but here for
                // completeness — match the declared output schema.
                let mut r = CallToolResult::success(vec![]);
                r.structured_content = Some(serde_json::json!({
                    "items": [{"id": 1}],
                    "total": 1,
                }));
                Ok(r)
            }
            "mystery" => Ok(CallToolResult::success(vec![Content::text(
                "prose answer".to_string(),
            )])),
            other => Err(ErrorData::invalid_params(
                format!("unknown tool: {other}"),
                None,
            )),
        }
    }
}

// ---- Downstream client ----

#[derive(Clone)]
struct PlainClient;

impl ClientHandler for PlainClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("response-shape-e2e-client", "0.0.1"),
        )
    }
}

// ---- Harness ----

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
            panic!("fake upstream task panicked at shutdown: {e}");
        }
    }
}

async fn start_harness() -> Harness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let upstreams = Arc::new(UpstreamManager::new());

    let (upstream_handle, fake_upstream_task) =
        common::spawn_fake_upstream("fake-shapy", "fake_shapy", FakeShapyServer, &bus, &broker)
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

/// Parse the text content of a `tool-details` result as JSON. Tool-details
/// wraps its JSON payload in a text block; this does the unwrap for us.
fn parse_details(res: &CallToolResult) -> serde_json::Value {
    let text = res
        .content
        .first()
        .expect("tool-details returned no content")
        .as_text()
        .expect("tool-details text block expected")
        .text
        .clone();
    serde_json::from_str(&text).expect("tool-details JSON must parse")
}

async fn call_details(
    client: &rmcp::service::RunningService<rmcp::RoleClient, PlainClient>,
    lua_tool: &str,
) -> CallToolResult {
    let mut req = CallToolRequestParams::new("tool-details");
    req = req.with_arguments(
        serde_json::json!({
            "luaServerName": "fake_shapy",
            "luaToolName": lua_tool,
        })
        .as_object()
        .expect("static json object")
        .clone(),
    );
    client
        .peer()
        .call_tool(req)
        .await
        .expect("tool-details call")
}

async fn call_inspect(
    client: &rmcp::service::RunningService<rmcp::RoleClient, PlainClient>,
    lua_tool: &str,
) -> CallToolResult {
    let mut req = CallToolRequestParams::new("inspect-tool-response");
    req = req.with_arguments(
        serde_json::json!({
            "luaServerName": "fake_shapy",
            "luaToolName": lua_tool,
            "arguments": {},
        })
        .as_object()
        .expect("static json object")
        .clone(),
    );
    client
        .peer()
        .call_tool(req)
        .await
        .expect("inspect-tool-response call")
}

// ---- Tests ----

#[tokio::test]
async fn probe_path_populates_text_json_shape_and_names_both_builtins() {
    // The F2 headline case: an upstream returns JSON inside a text
    // block. After one successful inspect, tool-details must surface
    // `text-json` with a hint that names BOTH unwrap_content and
    // json_decode — otherwise the agent still does the two-step
    // rediscovery the builtin set was designed to prevent.
    let harness = start_harness().await;
    let client = connect_client(&harness.downstream_url).await;

    // Inspect first (populates the cache).
    let inspect_result = call_inspect(&client, "get_incident").await;
    assert_eq!(
        inspect_result.is_error,
        Some(false),
        "inspect call must succeed for the cache write to fire"
    );

    // Then read details.
    let details = parse_details(&call_details(&client, "get_incident").await);

    assert_eq!(
        details["responseShape"], "text-json",
        "probe-populated shape must be text-json: {details}"
    );
    let hint = details["responseShapeHint"]
        .as_str()
        .expect("responseShapeHint must be a string");
    assert!(
        hint.contains("unwrap_content"),
        "text-json hint must name _gateway.unwrap_content: {hint}"
    );
    assert!(
        hint.contains("json_decode"),
        "text-json hint must name _gateway.json_decode: {hint}"
    );

    client.cancel().await.expect("client cancel");
    harness.shutdown().await;
}

#[tokio::test]
async fn schema_path_classifies_declared_structured_output_without_inspect() {
    // Cold start, no inspect. The tool's outputSchema is the only
    // signal available. The renderer must fall through to the schema
    // classifier and emit `structured`.
    //
    // This exercises the "first agent in a fresh gateway process"
    // experience: structured tools should hint from turn one, no
    // probe required.
    let harness = start_harness().await;
    let client = connect_client(&harness.downstream_url).await;

    let details = parse_details(&call_details(&client, "list_items").await);

    assert_eq!(
        details["responseShape"], "structured",
        "schema-derived shape must be structured: {details}"
    );
    let hint = details["responseShapeHint"]
        .as_str()
        .expect("responseShapeHint must be a string");
    assert!(
        hint.contains("structured_content"),
        "structured hint must point at structured_content: {hint}"
    );

    client.cancel().await.expect("client cancel");
    harness.shutdown().await;
}

#[tokio::test]
async fn absent_when_no_probe_and_no_schema_signal() {
    // The honest-absence property: when we genuinely don't know the
    // shape, the fields are OMITTED from the JSON (not emitted as
    // "unknown", not `null`). An agent that sees the fields missing
    // knows to try the tool and find out.
    let harness = start_harness().await;
    let client = connect_client(&harness.downstream_url).await;

    let details = parse_details(&call_details(&client, "mystery").await);

    assert!(
        details.get("responseShape").is_none(),
        "responseShape must be absent when shape is unknown: {details}"
    );
    assert!(
        details.get("responseShapeHint").is_none(),
        "responseShapeHint must be absent when shape is unknown: {details}"
    );

    client.cancel().await.expect("client cancel");
    harness.shutdown().await;
}

#[tokio::test]
async fn probe_observation_persists_across_sessions() {
    // The cache is process-wide, not session-scoped. A probe in
    // session A must populate the shape for session B's tool-details
    // call on the same tool. This is how a fresh agent benefits from
    // prior agents' work — the whole reason we chose a process-wide
    // cache over a session-scoped one.
    let harness = start_harness().await;

    // Session A: probe.
    {
        let client_a = connect_client(&harness.downstream_url).await;
        let inspect_result = call_inspect(&client_a, "get_incident").await;
        assert_eq!(
            inspect_result.is_error,
            Some(false),
            "probe in session A must succeed for the cache write to fire"
        );
        client_a.cancel().await.expect("client A cancel");
    }

    // Session B: read details, expect the probe-populated shape.
    let client_b = connect_client(&harness.downstream_url).await;
    let details = parse_details(&call_details(&client_b, "get_incident").await);

    assert_eq!(
        details["responseShape"], "text-json",
        "probe in session A must populate shape for session B: {details}"
    );

    client_b.cancel().await.expect("client B cancel");
    harness.shutdown().await;
}
