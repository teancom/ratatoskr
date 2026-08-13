// pattern: Imperative Shell
//
// Coverage for the Lua-runtime deadline/cancellation contract.
//
// `run_script_blocking` does `handle.block_on(future)` on a oneshot/
// upstream-call future. If that future never completes (upstream stalls,
// peer dies silently, downstream request is cancelled), a blocking worker
// thread must not be pinned forever waiting on it.
//
// These tests pin the behaviour the runtime must guarantee:
//
//   1. A bounded wall-clock deadline per `execute`.
//   2. A cooperative cancellation signal from the caller, plumbed into the
//      blocking worker so a cancelled downstream request doesn't leak a
//      zombie thread.
//
// The fake upstream here exposes a single tool, `hang`, whose `call_tool`
// implementation `futures::future::pending().await`s forever — the shape of
// an await point that never completes. Without deadline/cancellation
// enforcement, these tests would block indefinitely; with it, they complete
// promptly with the expected error variants.
//
// Default-deadline test is pure Rust — no fake upstream required — so the
// 60-second contract stays pinned even if the integration plumbing changes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use lua_runtime::{ExecutionOutcome, LuaExecutionError, LuaRuntime};
use mcp_client::{
    SamplingBroker, SharedSamplingBroker, ToolListCache, UpstreamConfig, UpstreamEventBus,
    UpstreamHandle, UpstreamKind, UpstreamManager, connect_with_transport,
};
use rmcp::{
    ErrorData, RoleServer, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
};
use tokio_util::sync::CancellationToken;

// -------- Shared harness: fake upstream over an in-memory duplex --------
//
// Every fake-upstream test below wants the same plumbing: split a duplex
// pipe into gateway/upstream halves, serve `handler` on the upstream half,
// connect the gateway half through `connect_with_transport`, wrap the
// resulting `LiveUpstream` in an `UpstreamHandle`, and register it. Only the
// handler's `call_tool`/`list_tools` behaviour (hanging, slow, wedged) and
// the optional per-handle call_timeout vary across call sites, so those are
// the only parameters.
//
// Returns the serve-task `JoinHandle` and the registered `Arc<UpstreamHandle>`.
// Callers must hold onto both for as long as the harness is in use: the
// registered handle is also cloned into `manager` (so the manager alone
// keeps the connection alive), but the returned handle and task let a
// caller that wants explicit teardown (e.g. `start_mixed_harness`'s
// `task.abort()` loop) do so without reaching into the manager.
async fn spawn_fake_upstream<H>(
    name: &str,
    handler: H,
    manager: &UpstreamManager,
    bus: &Arc<UpstreamEventBus>,
    broker: &SharedSamplingBroker,
    call_timeout: Option<Duration>,
) -> (tokio::task::JoinHandle<()>, Arc<UpstreamHandle>)
where
    H: ServerHandler + Send + 'static,
{
    let (gateway_side, upstream_side) = tokio::io::duplex(64 * 1024);
    let (gateway_read, gateway_write) = tokio::io::split(gateway_side);
    let (upstream_read, upstream_write) = tokio::io::split(upstream_side);

    // Owned copy for the 'static spawned task; `name: &str` can't be
    // captured by the async block, which must outlive the caller's frame.
    let task_name = name.to_string();
    let server_task = tokio::spawn(async move {
        let transport = rmcp::transport::async_rw::AsyncRwTransport::<RoleServer, _, _>::new_server(
            upstream_read,
            upstream_write,
        );
        let serve_failure_msg = format!("{task_name} fake upstream serve must succeed");
        let service = handler.serve(transport).await.expect(&serve_failure_msg);
        let _ = service.waiting().await;
    });

    let upstream_config = UpstreamConfig::new(
        name,
        name,
        UpstreamKind::Stdio {
            command: "unused-in-test".to_string(),
            args: vec![],
            env: Default::default(),
        },
    );

    let connect_failure_msg = format!("{name} in-memory upstream connect");
    let live = connect_with_transport(&upstream_config, bus, broker, gateway_read, gateway_write)
        .await
        .expect(&connect_failure_msg);
    let handle = Arc::new(match call_timeout {
        Some(timeout) => UpstreamHandle::new_with_call_timeout(upstream_config, live, timeout),
        None => UpstreamHandle::new(upstream_config, live),
    });
    manager.register(handle.clone()).await;

    (server_task, handle)
}

// -------- Fake upstream that never answers call_tool --------
//
// Mirrors the `execute_lua.rs` / `parallel_e2e.rs` harness shape so the
// three tests have a consistent mental model. The only interesting
// departure: `call_tool` returns a pending future that resolves never.
// `list_tools` must still return promptly — the runtime calls it during
// global injection (before the script runs), and the bug we're pinning
// is specifically about the call_tool path.

#[derive(Clone)]
struct HangingUpstream;

impl ServerHandler for HangingUpstream {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("hanging-upstream", "0.0.1"))
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
                "additionalProperties": false,
            }))
            .expect("static schema valid"),
        );
        Ok(ListToolsResult {
            tools: vec![Tool::new(
                "hang",
                "Never returns. Used to pin execute deadline/cancel behaviour.",
                schema,
            )],
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        _request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        // The whole point: await a future that never completes.
        futures::future::pending::<()>().await;
        unreachable!("pending() never resolves by construction")
    }
}

// -------- Harness: spin up a HangingUpstream connected via duplex,
// return an UpstreamManager with one registered upstream named
// "hanger" (lua_name "hanger"). --------

struct HangHarness {
    upstreams: Arc<UpstreamManager>,
    _upstream_handle: Arc<UpstreamHandle>,
    _server_task: tokio::task::JoinHandle<()>,
}

async fn start_hang_harness() -> HangHarness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let upstreams = Arc::new(UpstreamManager::new());

    let (server_task, handle) =
        spawn_fake_upstream("hanger", HangingUpstream, &upstreams, &bus, &broker, None).await;

    HangHarness {
        upstreams,
        _upstream_handle: handle,
        _server_task: server_task,
    }
}

// A Lua script that calls `hanger.hang({})`. We don't care about the
// return — we care that the call itself hangs.
const HANG_SCRIPT: &str = r#"
local r = hanger.hang({})
result(r)
"#;

// -------- Tests --------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_times_out_when_upstream_never_responds() {
    let harness = start_hang_harness().await;

    // Tight deadline so the test finishes fast. The fix must respect
    // whatever deadline the runtime is configured with; we pick 150ms.
    let rt = LuaRuntime::new().with_execute_deadline(Duration::from_millis(150));

    let started = Instant::now();
    let outcome = rt
        .execute(
            HANG_SCRIPT.to_string(),
            Arc::clone(&harness.upstreams),
            None,
            None,
            None,
            CancellationToken::new(),
        )
        .await;
    let elapsed = started.elapsed();

    match outcome {
        Err(LuaExecutionError::Timeout { .. }) => {}
        other => panic!(
            "expected Err(LuaExecutionError::Timeout {{..}}); got {other:?} after {elapsed:?}"
        ),
    }

    // Must return soon after the deadline, not after an unrelated
    // long timer or rmcp-internal retry. 1s is generous enough for
    // CI jitter but tight enough to fail if the timeout isn't wired.
    assert!(
        elapsed < Duration::from_secs(1),
        "execute should return close to its 150ms deadline; took {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_returns_cancelled_when_token_fires() {
    let harness = start_hang_harness().await;

    // Long deadline so only the cancellation path can end the call —
    // if the token is ignored this test runs for 60s and CI kills it,
    // which correctly surfaces the regression.
    let rt = LuaRuntime::new().with_execute_deadline(Duration::from_secs(60));

    let ct = CancellationToken::new();
    let cancel_after = ct.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel_after.cancel();
    });

    let started = Instant::now();
    let outcome = rt
        .execute(
            HANG_SCRIPT.to_string(),
            Arc::clone(&harness.upstreams),
            None,
            None,
            None,
            ct,
        )
        .await;
    let elapsed = started.elapsed();

    match outcome {
        Err(LuaExecutionError::Cancelled) => {}
        other => {
            panic!("expected Err(LuaExecutionError::Cancelled); got {other:?} after {elapsed:?}")
        }
    }

    assert!(
        elapsed < Duration::from_secs(2),
        "execute should return shortly after the token is cancelled; took {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execute_succeeds_normally_with_uncancelled_token() {
    // Regression guard: adding the token/deadline plumbing must not
    // break pure-Lua happy path. No upstream needed.
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            "result(1 + 1)".to_string(),
            Arc::new(UpstreamManager::new()),
            None,
            None,
            None,
            CancellationToken::new(),
        )
        .await
        .expect("pure-Lua script should succeed");

    match outcome {
        ExecutionOutcome::Direct(v) => assert_eq!(v, serde_json::json!(2)),
        ExecutionOutcome::Offloaded { .. } => {
            panic!("expected Direct; got Offloaded from a tiny result");
        }
    }
}

#[test]
fn default_execute_deadline_is_sixty_seconds() {
    // Pin the operator-visible default so a future diff that wants to
    // change it has to update this test and explain why.
    assert_eq!(
        lua_runtime::runtime::DEFAULT_EXECUTE_DEADLINE,
        Duration::from_secs(60),
        "per-execute hard deadline must default to 60 seconds"
    );
}

// ---------------------------------------------------------------------------
// Deadline-boundary pins
// ---------------------------------------------------------------------------
//
// The production deadline is 60 s. The scaled deadline/delay pairs below
// preserve the below-bound/above-bound *ratio* of that production case
// (50 s legit work < 60 s bound, 70 s legit work > 60 s bound) at
// single-digit-hundreds-of-ms values so the suite stays fast:
//
//   * below-bound: 300 ms deadline, 50 ms upstream delay  (delay ≈ 17%)
//   * above-bound: 100 ms deadline, 500 ms upstream delay (delay ≈ 500%)
//
// These scale the 60:50 ratio (≈83%) and 60:70 ratio (≈117%) into
// comfortable-margin values that won't flake on a loaded CI runner.
// What's being pinned is the *boundary behaviour*: completing before the
// deadline succeeds, exceeding it surfaces `Timeout`. Pinning the exact
// 60 s default lives in `default_execute_deadline_is_sixty_seconds` above.
//
// We can't use `tokio::time::pause()` here because the `multi_thread`
// runtime flavour is required by `spawn_blocking` under the hood and
// paused-time auto-advance is only reliable on `current_thread`.

// A fake upstream whose `call_tool` sleeps for a configured duration,
// then returns a trivial `CallToolResult`. Mirrors `HangingUpstream`'s
// shape; only `call_tool` behaviour differs.
#[derive(Clone)]
struct SlowUpstream {
    delay: Duration,
}

impl ServerHandler for SlowUpstream {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("slow-upstream", "0.0.1"))
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
                "additionalProperties": false,
            }))
            .expect("static schema valid"),
        );
        Ok(ListToolsResult {
            tools: vec![Tool::new(
                "slow",
                "Sleeps for a configured duration, then returns. Used to pin execute \
                 deadline boundary behaviour.",
                schema,
            )],
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        _request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        tokio::time::sleep(self.delay).await;
        Ok(CallToolResult::success(vec![Content::text("done")]))
    }
}

// Parallel to `HangHarness`, but the fake upstream takes a configurable
// delay so the same harness builds both the "finishes before deadline"
// and "finishes after deadline" test cases.
struct SlowHarness {
    upstreams: Arc<UpstreamManager>,
    _upstream_handle: Arc<UpstreamHandle>,
    _server_task: tokio::task::JoinHandle<()>,
}

async fn start_slow_harness(delay: Duration) -> SlowHarness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let upstreams = Arc::new(UpstreamManager::new());

    let (server_task, handle) = spawn_fake_upstream(
        "slow",
        SlowUpstream { delay },
        &upstreams,
        &bus,
        &broker,
        None,
    )
    .await;

    SlowHarness {
        upstreams,
        _upstream_handle: handle,
        _server_task: server_task,
    }
}

const SLOW_SCRIPT: &str = r#"
local r = slow.slow({})
result(r)
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_succeeds_when_upstream_finishes_before_deadline() {
    // Pins: a script whose upstream work completes before the
    // configured deadline must succeed. Guards against a change that
    // accidentally makes the deadline hair-trigger — e.g. measuring
    // from the wrong epoch, or cancelling the script prematurely.
    //
    // Ratio mirrors the production 50 s-of-work-under-60 s-deadline case.
    let harness = start_slow_harness(Duration::from_millis(50)).await;

    let rt = LuaRuntime::new().with_execute_deadline(Duration::from_millis(300));

    let started = Instant::now();
    let outcome = rt
        .execute(
            SLOW_SCRIPT.to_string(),
            Arc::clone(&harness.upstreams),
            None,
            None,
            None,
            CancellationToken::new(),
        )
        .await;
    let elapsed = started.elapsed();

    match outcome {
        Ok(ExecutionOutcome::Direct(value)) => {
            // The `slow` tool returns a plain MCP CallToolResult; the
            // Lua side surfaces it as its JSON encoding. We don't
            // assert the exact content shape (the capture layer's
            // structure isn't what this test is pinning), just that
            // *some* value made it back. If nothing came back this
            // would have been `MissingResult`.
            assert!(
                !value.is_null(),
                "expected a non-null result, got: {value:?}"
            );
        }
        other => {
            panic!("expected Ok(ExecutionOutcome::Direct(..)); got {other:?} after {elapsed:?}")
        }
    }

    // Upstream delay is 50 ms; total elapsed should stay well under
    // the 300 ms deadline. A loose 1 s ceiling tolerates CI jitter
    // without hiding a regression that made the call 10× slower.
    assert!(
        elapsed < Duration::from_secs(1),
        "execute should complete shortly after the upstream work finishes; took {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_times_out_when_upstream_finishes_after_deadline() {
    // Pins: a script whose upstream work would take longer than the
    // configured deadline must surface `Timeout { deadline_ms }` with
    // the configured-deadline value, not e.g. the default. Guards
    // against a change that hard-codes the 60 s default inside the
    // Err arm.
    //
    // Ratio mirrors the production 70 s-of-work-over-60 s-deadline case.
    let harness = start_slow_harness(Duration::from_millis(500)).await;

    let rt = LuaRuntime::new().with_execute_deadline(Duration::from_millis(100));

    let started = Instant::now();
    let outcome = rt
        .execute(
            SLOW_SCRIPT.to_string(),
            Arc::clone(&harness.upstreams),
            None,
            None,
            None,
            CancellationToken::new(),
        )
        .await;
    let elapsed = started.elapsed();

    match outcome {
        Err(LuaExecutionError::Timeout { deadline_ms }) => {
            assert_eq!(
                deadline_ms, 100,
                "Timeout must echo the *configured* deadline, not the default"
            );
        }
        other => panic!(
            "expected Err(LuaExecutionError::Timeout {{..}}); got {other:?} after {elapsed:?}"
        ),
    }

    // Must return close to its 100 ms deadline, and well before the
    // 500 ms upstream delay would have resolved. A 1 s ceiling keeps
    // the assertion tolerant of CI jitter while still catching a
    // regression that dropped the timeout entirely.
    assert!(
        elapsed < Duration::from_secs(1),
        "execute should time out close to the deadline, not wait for the upstream; took {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(100),
        "execute should honour its deadline as a floor, not fire early; took {elapsed:?}"
    );
}

// -------- Install-loop tolerance under a wedged list_tools --------
//
// Different shape from `HangingUpstream`: this fixture's `list_tools`
// hangs forever, modelling the SSE-bridge wedge that motivated the
// timeout-on-list_tools fix. With the architectural fix in place:
//   - UpstreamHandle::list_tools is bounded by call_timeout and
//     resolves with Err(ServiceError::Timeout) on a wedge (also
//     recording FailureKind::CallTimeout on the health record)
//   - cache.get(...) on the wedged upstream propagates that Timeout
//   - install_upstream_globals catches the Err and SKIPS the bad
//     upstream rather than failing the whole script
// The test therefore expects an `execute` against a manager containing
// one healthy + one wedged upstream to (a) complete within the script's
// own deadline (not hang on the wedged one) and (b) succeed for scripts
// that only touch the healthy upstream.

#[derive(Clone)]
struct ListToolsHangingUpstream;

impl ServerHandler for ListToolsHangingUpstream {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("list-tools-hanging-upstream", "0.0.1"))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        // Models the SSE-bridge wedge: list_tools never resolves.
        futures::future::pending::<()>().await;
        unreachable!("pending() never resolves by construction")
    }

    async fn call_tool(
        &self,
        _request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        // Unreachable in practice: install_upstream_globals never gets
        // past list_tools, so no Lua function for this upstream is
        // ever installed and the script can't dial it.
        Ok(CallToolResult::success(vec![Content::text(
            "should never be reached",
        )]))
    }
}

/// Like `start_hang_harness`, but registers TWO upstreams: a healthy
/// one (`HangingUpstream`, which advertises `hang` but lists tools fast)
/// at lua_name `healthy`, and a `ListToolsHangingUpstream` whose
/// `list_tools` never returns at lua_name `wedged`.
async fn start_mixed_harness() -> (Arc<UpstreamManager>, Vec<tokio::task::JoinHandle<()>>) {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let upstreams = Arc::new(UpstreamManager::new());

    // Healthy upstream — list_tools returns fast, exposes `hang`.
    let (healthy_task, _healthy_handle) =
        spawn_fake_upstream("healthy", HangingUpstream, &upstreams, &bus, &broker, None).await;

    // Wedged upstream — list_tools hangs forever. Short call_timeout
    // (150 ms) on the handle so the wedge is observed quickly without
    // blowing the test's wall-clock budget; in production the
    // operator-configured `call_timeout` (default 20 s) plays this
    // role.
    let (wedged_task, _wedged_handle) = spawn_fake_upstream(
        "wedged",
        ListToolsHangingUpstream,
        &upstreams,
        &bus,
        &broker,
        Some(Duration::from_millis(150)),
    )
    .await;

    (upstreams, vec![healthy_task, wedged_task])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn install_loop_skips_upstream_whose_list_tools_hangs() {
    // Property: when one upstream's `list_tools` hangs, install_upstream_globals
    // must (a) NOT pin the blocking worker forever, (b) skip that upstream
    // with a warning, and (c) install the healthy upstream's globals so
    // a script touching only the healthy upstream still runs.
    //
    // Pre-fix behaviour: the slot mutex in ToolListCache was held across
    // the dead `list_tools` future, so install_upstream_globals blocked
    // on slot.lock().await indefinitely; the script never ran.
    let (upstreams, server_tasks) = start_mixed_harness().await;

    // The wedged upstream's handle has a 150 ms call_timeout (set in
    // `start_mixed_harness`), which is what bounds the install loop's
    // single hang. Default `ToolListCache` is fine here — it's just a
    // single-flight memoize now, not a timeout enforcer.
    let tool_cache = Arc::new(ToolListCache::new());

    // Script that only references the healthy upstream. If the install
    // loop completed (i.e. didn't hang on `wedged`), this should run.
    // We don't actually call `healthy.hang(...)` because that would
    // genuinely hang on the test's own healthy fixture; just check the
    // global was installed by attempting an attribute access.
    let script = r#"
        if type(healthy) == "table" and type(wedged) == "nil" then
            result("healthy installed; wedged skipped")
        else
            result("globals: healthy=" .. type(healthy) .. " wedged=" .. type(wedged))
        end
    "#;

    let rt = LuaRuntime::new().with_execute_deadline(Duration::from_secs(5));
    let started = Instant::now();
    let outcome = rt
        .execute(
            script.to_string(),
            Arc::clone(&upstreams),
            None,
            Some(tool_cache),
            None,
            CancellationToken::new(),
        )
        .await;
    let elapsed = started.elapsed();

    let value = match outcome {
        Ok(ExecutionOutcome::Direct(v)) => v,
        other => panic!(
            "expected Direct outcome (install-loop tolerated wedge); got {other:?} after {elapsed:?}"
        ),
    };
    assert_eq!(
        value.as_str(),
        Some("healthy installed; wedged skipped"),
        "wedged upstream should be absent from globals; got: {value:?}"
    );

    // Sanity ceiling: must have completed in well under the 5 s execute
    // deadline. The wedged upstream's 150 ms call_timeout fires once
    // during install; healthy's list_tools returns immediately. We give
    // 2 s of CI slack.
    assert!(
        elapsed < Duration::from_secs(2),
        "install loop must skip the wedged upstream within ~timeout; took {elapsed:?}"
    );

    for task in server_tasks {
        task.abort();
    }
}
