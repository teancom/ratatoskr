// pattern: Imperative Shell
//
// End-to-end property: the gateway's tool-list paths cache per-upstream
// tool lists and re-fetch only after the upstream publishes a
// `tools/list_changed` event.

mod common;
//
// Strategy:
//   - Fake upstream MCP server (in-process via tokio::io::duplex) whose
//     tool list is backed by an Arc<Mutex<Vec<Tool>>>. Tests mutate this
//     to simulate an upstream changing its advertised tools.
//   - Real gateway (via serve_on), wired to the fake upstream, with a
//     ToolListCache + invalidator task running (the production wiring).
//   - Real rmcp client that calls `list-server-tools` over streamable HTTP.
//
// The load-bearing assertions:
//   1. Calling `list-server-tools` twice with no tool-list change in
//      between sees the same snapshot even if we've mutated the backing
//      vec — this proves the cache is actually caching.
//   2. After mutating AND publishing `UpstreamEvent::ToolListChanged`,
//      the next `list-server-tools` sees the new tools — this proves
//      the invalidator does its job.
//
// Why both: (1) alone would pass even if invalidation silently broke,
// and (2) alone would pass even if the cache were bypassed (every call
// would naturally see fresh data). Pinning both pins the whole loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use mcp_client::{
    SamplingBroker, SharedSamplingBroker, ToolListCache, UpstreamConfig, UpstreamEvent,
    UpstreamEventBus, UpstreamHandle, UpstreamKind, UpstreamManager, connect_with_transport,
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
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

// -------- Fake upstream with a mutable tool list --------

/// Backing state for the fake upstream. Tool list lives behind an async
/// mutex so the test harness can mutate it between gateway calls without
/// racing the server's own `list_tools` handler.
///
/// When `page_size` is `Some(n)`, the server splits the tool list across
/// pages of `n` tools each and emits a `next_cursor` pointing at the
/// starting index of the next page. `None` keeps the legacy single-page
/// behaviour so the older tests are unaffected.
#[derive(Clone)]
struct MutableToolState {
    tools: Arc<AsyncMutex<Vec<Tool>>>,
    /// How many times the upstream's `list_tools` handler actually ran.
    /// The cache should keep this from climbing past 1 per invalidation
    /// (times the number of pages, when pagination is in effect).
    calls: Arc<AtomicU32>,
    /// If set, the server returns tools in pages of this size. The cursor
    /// is the stringified starting index of the next page. If absent, the
    /// server returns every tool on the first page with `next_cursor: None`.
    page_size: Option<usize>,
    /// If true, `list_tools` blocks indefinitely (well — for the test's
    /// lifetime; the harness shuts the server down). Models the SSE-bridge
    /// wedge that motivated the cache fetch_timeout. Cheap atomic flip
    /// from the test driver so we can toggle wedge state without
    /// rebuilding the harness.
    hang: Arc<std::sync::atomic::AtomicBool>,
}

impl MutableToolState {
    fn new(initial: Vec<Tool>) -> Self {
        Self {
            tools: Arc::new(AsyncMutex::new(initial)),
            calls: Arc::new(AtomicU32::new(0)),
            page_size: None,
            hang: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn with_page_size(initial: Vec<Tool>, page_size: usize) -> Self {
        Self {
            tools: Arc::new(AsyncMutex::new(initial)),
            calls: Arc::new(AtomicU32::new(0)),
            page_size: Some(page_size),
            hang: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    async fn replace_tools(&self, new: Vec<Tool>) {
        let mut guard = self.tools.lock().await;
        *guard = new;
    }

    fn call_count(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    /// Toggle the wedge. While `hang=true`, the next `list_tools`
    /// requests block in `pending().await` forever (or until the
    /// server task is cancelled at harness teardown).
    fn set_hang(&self, hang: bool) {
        self.hang.store(hang, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct MutableFakeServer {
    state: MutableToolState,
}

impl ServerHandler for MutableFakeServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("mutable-fake-upstream", "0.0.1"))
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        self.state.calls.fetch_add(1, Ordering::SeqCst);
        // Wedge mode: never resolve. Models a sick SSE bridge whose
        // underlying connection has died but whose `tools/list` request
        // is in-flight forever. The test's outer `tokio::time::timeout`
        // bounds what would otherwise be an infinite hang.
        if self.state.hang.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let tools = self.state.tools.lock().await.clone();
        match self.state.page_size {
            None => Ok(ListToolsResult {
                tools,
                next_cursor: None,
                meta: None,
            }),
            Some(page_size) => {
                // Cursor is the stringified starting index of the page to
                // return. Absent cursor means "start at 0".
                let start: usize = request
                    .as_ref()
                    .and_then(|r| r.cursor.as_deref())
                    .map(|c| {
                        c.parse::<usize>()
                            .expect("cursor emitted by this fake is always usize-parseable")
                    })
                    .unwrap_or(0);
                let end = (start + page_size).min(tools.len());
                let page: Vec<Tool> = tools[start..end].to_vec();
                let next_cursor = if end < tools.len() {
                    Some(end.to_string())
                } else {
                    None
                };
                Ok(ListToolsResult {
                    tools: page,
                    next_cursor,
                    meta: None,
                })
            }
        }
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(CallToolResult::success(vec![Content::text(format!(
            "called: {}",
            request.name
        ))]))
    }
}

// -------- Tool factory --------

fn simple_tool(name: &str, description: &str) -> Tool {
    let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false,
        }))
        .expect("static schema"),
    );
    Tool::new(name.to_string(), description.to_string(), schema)
}

// -------- Downstream client --------

#[derive(Clone)]
struct PlainClient;

impl ClientHandler for PlainClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("tool-list-cache-test-client", "0.0.1"),
        )
    }
}

// -------- Harness --------

struct Harness {
    _upstream_handle: Arc<UpstreamHandle>,
    state: MutableToolState,
    bus: Arc<UpstreamEventBus>,
    downstream_url: String,
    ct: CancellationToken,
    server_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    _fake_upstream_task: tokio::task::JoinHandle<()>,
}

async fn start_harness(initial_tools: Vec<Tool>) -> Harness {
    start_harness_with_state(MutableToolState::new(initial_tools)).await
}

/// Variant of `start_harness` that makes the fake upstream serve its
/// tool list across multiple pages of `page_size` each. Used to exercise
/// the cache's behaviour when the gateway must drive a paginated
/// `list_all_tools` fetch under the hood.
async fn start_harness_paginated(initial_tools: Vec<Tool>, page_size: usize) -> Harness {
    start_harness_with_state(MutableToolState::with_page_size(initial_tools, page_size)).await
}

async fn start_harness_with_state(state: MutableToolState) -> Harness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let upstreams = Arc::new(UpstreamManager::new());
    let tool_cache = Arc::new(ToolListCache::new());

    let state_for_server = state.clone();
    let server = MutableFakeServer {
        state: state_for_server,
    };
    let (upstream_handle, fake_upstream_task) = common::spawn_fake_upstream(
        "mutable-upstream",
        "mutable_upstream",
        server,
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
    let tool_cache_clone = Arc::clone(&tool_cache);
    let ct_for_serve = ct.clone();

    // Share the caller's `tool_cache` so the test can observe
    // invalidation from the outside.
    let ctx = gateway::AppContext {
        tool_cache: tool_cache_clone,
        ..common::test_app_context(upstreams_clone, bus_clone, broker_clone).await
    };
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
        _upstream_handle: upstream_handle,
        state,
        bus,
        downstream_url,
        ct,
        server_handle,
        _fake_upstream_task: fake_upstream_task,
    }
}

impl Harness {
    async fn shutdown(self) {
        let _ = common::cancel_and_join_server(&self.ct, self.server_handle).await;
    }
}

/// Call the gateway's `list-server-tools` tool and return the rendered
/// text payload.
async fn call_list_server_tools(
    client: &rmcp::service::RunningService<rmcp::RoleClient, PlainClient>,
    lua_server_name: &str,
) -> String {
    let args = serde_json::json!({ "luaServerName": lua_server_name })
        .as_object()
        .expect("object literal")
        .clone();
    let result = client
        .peer()
        .call_tool(CallToolRequestParams::new("list-server-tools").with_arguments(args))
        .await
        .expect("list-server-tools call should succeed");
    assert_eq!(result.is_error, Some(false), "expected success: {result:?}");
    result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.clone())
        .expect("list-server-tools returned text content")
}

/// Wait for `cond` to become true, polling every 25ms up to `timeout`.
async fn eventually<F>(timeout: Duration, mut cond: F) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

// -------- Tests --------

#[tokio::test]
async fn list_server_tools_is_cached_between_calls() {
    // Property: two consecutive `list-server-tools` calls on the same
    // upstream produce identical output, even if the upstream's
    // backing tool list has mutated. Cache freshness is driven by
    // `tools/list_changed` events, not by wall-clock refresh.
    let harness = start_harness(vec![
        simple_tool("alpha", "the first tool"),
        simple_tool("beta", "the second tool"),
    ])
    .await;

    let client = PlainClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    let first = call_list_server_tools(&client_service, "mutable_upstream").await;
    assert!(
        first.contains("alpha") && first.contains("beta"),
        "first call should see alpha/beta; got: {first}"
    );

    // Mutate the upstream's advertised list WITHOUT publishing an event.
    // A well-behaved upstream would publish; we're simulating a silent
    // mutation to prove the cache is what's holding us steady.
    harness
        .state
        .replace_tools(vec![
            simple_tool("gamma", "brand new tool"),
            simple_tool("delta", "another brand new tool"),
        ])
        .await;

    let second = call_list_server_tools(&client_service, "mutable_upstream").await;
    assert_eq!(
        first, second,
        "without a ToolListChanged event, the second call must see the cached snapshot"
    );
    assert!(
        !second.contains("gamma"),
        "cached snapshot must not leak the mutated state; got: {second}"
    );

    // The upstream server should have been asked for its tool list
    // exactly ONCE despite two downstream calls. Proves the cache
    // prevented a redundant RPC.
    let calls = harness.state.call_count();
    assert_eq!(
        calls, 1,
        "upstream list_tools should have been called once for two downstream calls; got {calls}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn list_server_tools_returns_every_page_of_a_paginated_upstream() {
    // Property: when the upstream returns its tool list across multiple
    // pages, the cache ends up holding the FULL list — every page's
    // tools show up in `list-server-tools`. Pins the auto-pagination
    // change to `UpstreamHandle::list_tools` (rmcp's `list_all_tools`):
    // if that ever regresses back to single-page `list_tools(None)`,
    // this test fails because the second page's tools are missing.
    //
    // Five tools split into pages of two means three upstream handler
    // hits per cold fetch (2 + 2 + 1). Two downstream `list-server-tools`
    // calls should therefore trigger three hits total (first call warms
    // the cache with three paginated server-side calls; second call is
    // served entirely from the cache).
    let harness = start_harness_paginated(
        vec![
            simple_tool("page_a_one", "page A tool 1"),
            simple_tool("page_a_two", "page A tool 2"),
            simple_tool("page_b_one", "page B tool 1"),
            simple_tool("page_b_two", "page B tool 2"),
            simple_tool("page_c_one", "page C tool 1"),
        ],
        2,
    )
    .await;

    let client = PlainClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    let first = call_list_server_tools(&client_service, "mutable_upstream").await;
    for expected in [
        "page_a_one",
        "page_a_two",
        "page_b_one",
        "page_b_two",
        "page_c_one",
    ] {
        assert!(
            first.contains(expected),
            "paginated fetch must expose every page's tools; missing {expected} in: {first}"
        );
    }

    // Warming the cache required walking three pages (2 + 2 + 1).
    let calls_after_warm = harness.state.call_count();
    assert_eq!(
        calls_after_warm, 3,
        "paginated cold-warm should hit upstream once per page; got {calls_after_warm}"
    );

    // Second downstream call must be served from the cache entirely —
    // no extra upstream handler hits regardless of page count.
    let second = call_list_server_tools(&client_service, "mutable_upstream").await;
    assert_eq!(
        first, second,
        "second downstream call should see the cached snapshot"
    );
    let calls_after_second = harness.state.call_count();
    assert_eq!(
        calls_after_second, 3,
        "second downstream call must not re-fetch any page; got {calls_after_second}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn tool_list_changed_event_invalidates_cache() {
    // Property: after the upstream's tool list changes AND the
    // invalidation event hits the bus, the next `list-server-tools`
    // call sees the new tools. This is the load-bearing property that
    // makes the cache safe in the presence of real upstreams adding/
    // removing tools at runtime.
    let harness = start_harness(vec![simple_tool("old_tool", "soon to be gone")]).await;

    let client = PlainClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    // Warm the cache.
    let before = call_list_server_tools(&client_service, "mutable_upstream").await;
    assert!(
        before.contains("old_tool"),
        "pre-invalidation call should see old_tool; got: {before}"
    );
    let calls_after_warm = harness.state.call_count();
    assert_eq!(
        calls_after_warm, 1,
        "warming the cache should have called upstream list_tools once; got {calls_after_warm}"
    );

    // Replace the upstream's advertised tools and fire the invalidation
    // event. In production the upstream itself would emit this via its
    // MCP transport; here we publish directly on the bus, which is the
    // same code path the ProxyUpstreamHandler uses on the real thing.
    harness
        .state
        .replace_tools(vec![
            simple_tool("new_tool", "the replacement"),
            simple_tool("also_new", "another replacement"),
        ])
        .await;

    // Ensure the invalidator task is subscribed before we publish.
    // Loop until at least one subscriber is there (the forwarder spawned
    // by the client's initialize race can also be counted; that's fine —
    // we just need any receiver present so our event isn't silently
    // dropped).
    //
    // Note: `eventually` calls this closure on each 25ms poll tick, so
    // `publish` may fire more than once before a subscriber appears and
    // we return `true`. That is safe because `ToolListChanged` is an
    // idempotent invalidation signal — the invalidator clears the slot;
    // a second clear on an already-empty slot is a no-op. If invalidation
    // ever grows stateful side effects (e.g. tracking dirty counts), this
    // pattern must switch to wait-then-publish-once (subscribe-presence
    // check outside, a single `publish` after the loop).
    let published_at_least_once = eventually(Duration::from_secs(1), || {
        harness.bus.publish(UpstreamEvent::ToolListChanged {
            upstream: "mutable-upstream".to_string(),
        }) > 0
    })
    .await;
    assert!(
        published_at_least_once,
        "could not observe a subscriber on the bus to deliver ToolListChanged"
    );

    // Wait until the next call actually sees the new tools. The
    // invalidator runs on its own task, so the cache clear happens
    // asynchronously after our publish. Poll `list-server-tools` until
    // it reflects the new state or the deadline passes.
    let start = std::time::Instant::now();
    let deadline = Duration::from_secs(2);
    let mut latest = String::new();
    while start.elapsed() < deadline {
        latest = call_list_server_tools(&client_service, "mutable_upstream").await;
        if latest.contains("new_tool") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert!(
        latest.contains("new_tool") && latest.contains("also_new"),
        "post-invalidation call should see the new tools; got: {latest}"
    );
    assert!(
        !latest.contains("old_tool"),
        "post-invalidation call must not see the old tool; got: {latest}"
    );

    // Upstream should have been re-queried at least once after
    // invalidation. Could be more if the poll loop retried; must be
    // at least 2.
    let calls = harness.state.call_count();
    assert!(
        calls >= 2,
        "upstream list_tools should have been called again after invalidation; got {calls}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn concurrent_misses_trigger_exactly_one_upstream_fetch() {
    // Property: N callers racing on a cold cache slot trigger exactly one
    // `list_tools` RPC to the upstream. The remaining N-1 callers block on
    // the slot mutex and observe the already-populated result on wake. This
    // is the single-flight guarantee documented in the module comment of
    // `crates/mcp-client/src/tool_list_cache.rs` (the per-slot async-mutex
    // paragraph). If the slot-mutex
    // were removed, each concurrent miss would fire its own RPC and
    // call_count would climb to N.
    //
    // To maximise the chance that all N tasks are actually racing (and not
    // serialising), we delay the upstream's `list_tools` response. We
    // achieve this by making the fake upstream hold the tools lock for a
    // small window: since `MutableToolState::replace_tools` takes the lock
    // briefly, we prime the server while holding the lock in the test
    // harness. That's not directly available, so instead we rely on the
    // fact that all N tasks are spawned before any is awaited — in the
    // cooperative Tokio scheduler that means they all progress to their
    // first `.await` point (the `call_list_server_tools` HTTP round-trip)
    // before any completes, giving the single-flight mutex a real
    // concurrent-miss scenario to serialise.
    const CONCURRENCY: usize = 8;

    let harness = start_harness(vec![simple_tool("solo", "only tool")]).await;

    let client = PlainClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = Arc::new(client.serve(transport).await.expect("client connect"));

    // Spawn all tasks before awaiting any of them so they enter the cache
    // simultaneously on a cold slot.
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let svc = Arc::clone(&client_service);
        handles.push(tokio::spawn(async move {
            call_list_server_tools(&svc, "mutable_upstream").await
        }));
    }

    // Collect results — all should succeed and see "solo".
    for handle in handles {
        let result = handle.await.expect("task should not panic");
        assert!(
            result.contains("solo"),
            "every concurrent caller must see the tool list; got: {result}"
        );
    }

    // Despite CONCURRENCY concurrent callers on a cold slot, the upstream
    // should have been asked exactly once. More than one means the
    // single-flight mutex is not protecting the fetch.
    let calls = harness.state.call_count();
    assert_eq!(
        calls, 1,
        "single-flight: {CONCURRENCY} concurrent misses must produce exactly 1 upstream fetch; got {calls}"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn execute_and_list_server_tools_share_the_cache() {
    // Property: two different tool-list code paths (the Lua runtime's
    // global injection and the gateway's `list-server-tools`) both go
    // through the same cache. A warm-up via `list-server-tools`
    // followed by an `execute` call should NOT re-fetch — one RPC total.
    let harness = start_harness(vec![simple_tool("shared", "same data from both paths")]).await;

    let client = PlainClient;
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(harness.downstream_url.clone());
    let client_service = client.serve(transport).await.expect("client connect");

    // Path A: list-server-tools.
    let _ = call_list_server_tools(&client_service, "mutable_upstream").await;
    assert_eq!(
        harness.state.call_count(),
        1,
        "list-server-tools should fetch once"
    );

    // To run `execute`, we need to inspect at least one tool first
    // (NotInspected gate), which also goes through the cache — and
    // since the cache is already warm, tool-details should NOT add
    // another upstream fetch.
    let inspect_args =
        serde_json::json!({ "luaServerName": "mutable_upstream", "luaToolName": "shared" })
            .as_object()
            .expect("object literal")
            .clone();
    let inspect_result = client_service
        .peer()
        .call_tool(CallToolRequestParams::new("tool-details").with_arguments(inspect_args))
        .await
        .expect("tool-details succeeds");
    assert_eq!(
        inspect_result.is_error,
        Some(false),
        "tool-details should succeed"
    );
    assert_eq!(
        harness.state.call_count(),
        1,
        "tool-details must reuse the warm cache"
    );

    // Path B: execute. The Lua runtime's install_upstream_globals
    // should also hit the cache rather than fetching.
    let script = r#"
        local _ = mutable_upstream.shared({})
        result("ok")
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
        .expect("execute succeeds");
    assert_eq!(result.is_error, Some(false), "execute should succeed");

    assert_eq!(
        harness.state.call_count(),
        1,
        "execute must reuse the warm cache: upstream list_tools should remain at 1"
    );

    harness.shutdown().await;
}

// -------- Slim cache-only fixture for the fetch-timeout regression --------

/// Minimal harness that connects an `UpstreamHandle` to a `MutableFakeServer`
/// over an in-memory duplex stream, without spinning up the gateway HTTP
/// server. Used by the tests below to exercise `ToolListCache::get` directly.
///
/// `call_timeout` is the per-call wall-clock deadline applied to the
/// constructed `UpstreamHandle` — this is what bounds `list_tools` when
/// the upstream wedges. (Earlier revisions of these tests bounded the
/// cache itself; the timeout now lives on the handle so a wedge gets
/// reflected on the health record. See `tool_list_cache.rs`'s type-
/// level docs.)
///
/// Returns `(handle, state, fake_upstream_task)` — the test must keep the
/// task alive and explicitly abort it at teardown.
async fn start_cache_fixture(
    state: MutableToolState,
    call_timeout: Duration,
) -> (
    Arc<UpstreamHandle>,
    MutableToolState,
    tokio::task::JoinHandle<()>,
) {
    let (gateway_side, upstream_side) = tokio::io::duplex(64 * 1024);
    let (gateway_read, gateway_write) = tokio::io::split(gateway_side);
    let (upstream_read, upstream_write) = tokio::io::split(upstream_side);

    let state_for_server = state.clone();
    let fake_upstream_task = tokio::spawn(async move {
        let server = MutableFakeServer {
            state: state_for_server,
        };
        let transport = rmcp::transport::async_rw::AsyncRwTransport::<RoleServer, _, _>::new_server(
            upstream_read,
            upstream_write,
        );
        if let Ok(service) = server.serve(transport).await {
            let _ = service.waiting().await;
        }
    });

    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());

    let upstream_config = UpstreamConfig::new(
        "wedgy-upstream",
        "wedgy_upstream",
        UpstreamKind::Stdio {
            command: "unused-in-test".to_string(),
            args: vec![],
            env: Default::default(),
        },
    );

    let live = connect_with_transport(&upstream_config, &bus, &broker, gateway_read, gateway_write)
        .await
        .expect("in-memory upstream connect");
    let handle = Arc::new(UpstreamHandle::new_with_call_timeout(
        upstream_config,
        live,
        call_timeout,
    ));
    (handle, state, fake_upstream_task)
}

#[tokio::test]
async fn cache_get_returns_timeout_error_when_upstream_list_tools_hangs() {
    // Regression: when an upstream's `list_tools` hangs (e.g. an SSE
    // bridge whose underlying connection died and is mid-reconnect-storm),
    // the cache used to hold the slot mutex forever — every subsequent
    // caller blocked at `slot.lock().await`. The handle now bounds
    // `list_tools` itself, so the cache observes a `ServiceError::Timeout`
    // and propagates it; `Duration` carried in the error is the
    // handle's `call_timeout`.
    let initial = vec![simple_tool("a", "first")];
    let state = MutableToolState::new(initial);
    state.set_hang(true);
    let (handle, state, fake_upstream_task) =
        start_cache_fixture(state, Duration::from_millis(150)).await;

    let cache = ToolListCache::new();

    let started = std::time::Instant::now();
    let result = cache.get(&handle).await;
    let elapsed = started.elapsed();

    match result {
        Err(rmcp::ServiceError::Timeout { timeout }) => {
            assert_eq!(
                timeout,
                Duration::from_millis(150),
                "timeout error must carry the handle's configured call_timeout"
            );
        }
        other => panic!("expected ServiceError::Timeout, got: {other:?}"),
    }

    // Property: the call returned within ~timeout + a small slack.
    // Bound generously to keep this stable on a loaded CI box without
    // making the failure mode unobservable.
    assert!(
        elapsed < Duration::from_secs(2),
        "cache.get must surface timeout within seconds, not hang; elapsed: {elapsed:?}"
    );

    // Sanity: we don't leak the fake upstream task into other tests.
    let _ = state; // keep `state` alive across the assertion
    fake_upstream_task.abort();
}

#[tokio::test]
async fn cache_releases_slot_after_timeout_so_next_caller_can_retry() {
    // Property: the load-bearing fix. Before this change, a single
    // hung `list_tools` poisoned the slot mutex permanently; every
    // subsequent `cache.get(...)` blocked at `slot.lock().await`. The
    // handle's per-call timeout drops the in-flight future on elapse,
    // which lets the cache's `slot.lock()` guard drop too — so when
    // the upstream recovers, the next caller succeeds.
    let state = MutableToolState::new(vec![simple_tool("a", "first")]);
    state.set_hang(true);
    let (handle, state, fake_upstream_task) =
        start_cache_fixture(state, Duration::from_millis(150)).await;

    let cache = ToolListCache::new();

    // First call hits the wedge.
    let first = cache.get(&handle).await;
    assert!(
        matches!(first, Err(rmcp::ServiceError::Timeout { .. })),
        "first call must time out: {first:?}"
    );

    // Upstream recovers.
    state.set_hang(false);

    // Second call must succeed within the timeout. If the slot were
    // still locked by the first call's dead guard, this would hang
    // and the outer test timeout would fail us.
    let second = tokio::time::timeout(Duration::from_secs(2), cache.get(&handle))
        .await
        .expect("second cache.get must not hang on a poisoned slot");
    let tools = second.expect("second cache.get must succeed once upstream recovers");
    assert_eq!(
        tools.len(),
        1,
        "recovered upstream should return its one tool"
    );
    assert_eq!(tools[0].name.as_ref(), "a");

    fake_upstream_task.abort();
}

#[tokio::test]
async fn handle_records_call_timeout_failure_when_list_tools_wedges() {
    // Property: the architectural fix. When `list_tools` times out at
    // the handle layer, the failure is recorded on the upstream's
    // `UpstreamHealth` record as `FailureKind::CallTimeout` and bumps
    // `consecutive_failures`. This is what lets the supervisor detect
    // a wedged-but-stdio-still-open upstream — the rootly bug from
    // 2026-04-30 that motivated this whole change.
    use mcp_client::{FailureKind, HealthState};

    let state = MutableToolState::new(vec![simple_tool("a", "first")]);
    state.set_hang(true);
    let (handle, state, fake_upstream_task) =
        start_cache_fixture(state, Duration::from_millis(150)).await;

    // Pre-condition: clean health record.
    let snap = handle.health_snapshot();
    assert_eq!(
        snap.consecutive_failures, 0,
        "fresh handle starts with zero failures"
    );
    assert_eq!(snap.state, HealthState::Healthy);

    // Drive three timeouts back-to-back so the supervisor's threshold
    // (default 3) would trip. We invoke `list_tools` directly rather
    // than through the cache because the cache memoizes the first
    // success — and we explicitly want N consecutive failures.
    for _ in 0..3 {
        let result = handle.list_tools().await;
        assert!(
            matches!(result, Err(rmcp::ServiceError::Timeout { .. })),
            "wedge must surface as Timeout: {result:?}"
        );
    }

    let snap = handle.health_snapshot();
    assert_eq!(
        snap.consecutive_failures, 3,
        "three list_tools timeouts must record three consecutive failures"
    );
    assert_eq!(
        snap.last_failure_kind,
        FailureKind::CallTimeout,
        "the recorded failure kind must be CallTimeout (transport-level — what the supervisor counts)"
    );

    let _ = state;
    fake_upstream_task.abort();
}
