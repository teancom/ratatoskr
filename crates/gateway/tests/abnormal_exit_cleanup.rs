// pattern: Imperative Shell
//
// Subprocess-cleanup survivability matrix — integration tests for
// subprocess cleanup under abnormal exit paths that
// `shutdown_closes_upstreams.rs` does NOT cover.
//
// The graceful path (SIGINT/SIGTERM → cancellation token → axum drain →
// `close_all_upstreams`) is already pinned by
// `shutdown_closes_upstreams.rs`. The review noted four categories that
// weren't verified end-to-end:
//
//   1. Panic inside a handler task.
//   2. Panic inside a forwarder task.
//   3. Panic inside an invalidator task.
//   4. `serve_on` returning/aborted without a clean graceful shutdown
//      (e.g. tokio runtime shutdown, the main future returning Err).
//
// Why we simulate the panic category synthetically. The production
// spawn sites (`handler/mod.rs` session Drop, `session_forwarder.rs`
// forwarder, `tool_list_cache.rs`/`resource_list_cache.rs`
// invalidators) have narrow, panic-resistant bodies — they call
// infallible methods on shared caches/brokers. Triggering a real
// panic in them would require carrying test hooks in production.
// What matters is the STRUCTURAL invariant: none of those tasks
// hold an `UpstreamHandle` reference, so tokio's catch-panic-in-spawn
// semantics mean a panic in any of them cannot leak child processes.
//
// These tests pin that invariant by spawning a synthetic panicking
// task on the same runtime alongside a real stdio upstream, then
// asserting normal cleanup still reaps the child within
// `CLEANUP_BUDGET`. A future refactor that wires an `UpstreamHandle`
// into any of those tasks would break the invariant these tests
// encode.
//
// For case 4 we abort `serve_on`'s `JoinHandle` mid-flight, which
// mimics the runtime-shutdown / main-future-Err case — the explicit
// `close_all_upstreams` path never runs, and cleanup falls through to
// rmcp's Drop-based async-kill task. The test pins that this
// Drop-based path still reaps the child within budget.
//
// Fixture choice. We reuse `@modelcontextprotocol/server-everything`
// via `npx`, matching the existing upstream_cleanup.rs tests. That's
// the only MCP-speaking stdio fixture the repo pulls in; a non-MCP
// child would hang `connect()` on the initialize handshake. Each test
// pays one npx cold-start (~1–3 s) — acceptable for four tests.
//
// Scoped cleanup. Each test owns a `PidGuard` that force-kills the
// child's process group on Drop if our cleanup path failed to reap
// it. Scoped to the specific pid so parallel tests can't collide
// (same posture as upstream_cleanup.rs).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mcp_client::{
    SamplingBroker, SharedSamplingBroker, UpstreamConfig, UpstreamEventBus, UpstreamHandle,
    UpstreamKind, UpstreamManager, connect,
};
use nix::sys::signal::{Signal, kill, killpg};
use nix::unistd::Pid;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

mod common;

/// Same budget as `upstream_cleanup.rs`: 3 s of headroom is ample for
/// SIGKILL + reap on any stdio upstream we spawn in tests. Tightening
/// to match observed fast-machine times would turn a correctness
/// assertion into a flaky timing assertion.
const CLEANUP_BUDGET: Duration = Duration::from_secs(3);

async fn eventually<F>(timeout: Duration, mut cond: F) -> Result<Duration, Duration>
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return Ok(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(start.elapsed())
}

fn pid_alive(pid: u32) -> bool {
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

/// Scoped safety net. On Drop, force-kills the child's process group if
/// it's still alive. We spawn stdio upstreams with `ProcessGroup::leader()`
/// (see `connect::connect`), so the pid is the group id and `killpg`
/// reaches every descendant (npx, node, server-everything) in one
/// syscall. Scoped to one test's own child — never touches others.
struct PidGuard {
    pid: u32,
}

impl Drop for PidGuard {
    fn drop(&mut self) {
        if pid_alive(self.pid) {
            let _ = killpg(Pid::from_raw(self.pid as i32), Signal::SIGKILL);
        }
    }
}

/// Build an MCP-speaking stdio config. Uses `@modelcontextprotocol/server-everything`
/// because it's the only real MCP fixture this repo pulls in — a
/// non-MCP child would hang `connect()` on the initialize handshake.
fn everything_upstream(name: &str) -> UpstreamConfig {
    UpstreamConfig::new(
        name,
        name,
        UpstreamKind::Stdio {
            command: "npx".to_string(),
            args: vec![
                "-y".to_string(),
                "@modelcontextprotocol/server-everything".to_string(),
            ],
            env: HashMap::new(),
        },
    )
}

/// Spawn one real stdio upstream and register it in a fresh
/// `UpstreamManager`. Returns the pid (for liveness polling) and
/// the Arc<UpstreamManager> (for pass-through to AppContext).
async fn spawn_real_stdio_upstream(
    name: &str,
    bus: &UpstreamEventBus,
    broker: &SharedSamplingBroker,
) -> (u32, Arc<UpstreamManager>) {
    let config = everything_upstream(name);
    let live = connect(&config, bus, broker)
        .await
        .expect("connect to server-everything via npx");
    let pid = match &live {
        mcp_client::LiveUpstream::Stdio { pid, .. } => pid.expect("stdio upstream has pid"),
        _ => panic!("expected LiveUpstream::Stdio, got non-Stdio — wrong connect() return"),
    };

    let manager = Arc::new(UpstreamManager::new());
    manager
        .register(Arc::new(UpstreamHandle::new(config, live)))
        .await;
    (pid, manager)
}

/// Wait for axum to reach `accept()` by polling TCP connect against the
/// bound address. A fixed sleep is flaky under load (see AGENTS.md
/// "Sleeps only for negative assertions"); a successful TCP handshake is
/// the kernel's own "listener ready" signal.
async fn wait_for_serve_ready(addr: std::net::SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "serve_on never started accepting on {addr} within 5s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Common setup: real stdio upstream, bound listener, AppContext, and a
/// CancellationToken the test retains for its own teardown.
///
/// Returns `(pid, bus, manager, listener, ctx, ct)`. The test moves
/// the first five into `serve_on` (or a driver) and uses `ct` to
/// signal clean shutdown after its abnormal trigger has run.
async fn setup_stdio_harness(
    test_name: &'static str,
) -> (
    u32,
    Arc<UpstreamEventBus>,
    Arc<UpstreamManager>,
    TcpListener,
    gateway::AppContext,
    CancellationToken,
) {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let (pid, manager) = spawn_real_stdio_upstream(test_name, &bus, &broker).await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");

    let ctx =
        common::test_app_context(Arc::clone(&manager), Arc::clone(&bus), broker.clone()).await;

    let ct = CancellationToken::new();
    (pid, bus, manager, listener, ctx, ct)
}

/// Graceful-shutdown driver. Spawns `serve_on`, waits for readiness,
/// runs the caller-provided abnormal trigger, signals cancellation,
/// and awaits `serve_on` to completion. Returns the serve_on result so
/// the caller can assert on it.
///
/// The abnormal trigger runs on the same runtime as `serve_on`. Any
/// panic it raises is caught by tokio's spawn-panic machinery and
/// surfaces as a `JoinError` when we await the trigger's handle — we
/// deliberately DON'T propagate that panic into the test thread,
/// because the invariant we're pinning is "panic in sibling task
/// doesn't break child cleanup," not "panics propagate."
async fn run_with_abnormal_and_graceful_shutdown<F, Fut>(
    listener: TcpListener,
    ctx: gateway::AppContext,
    ct: CancellationToken,
    abnormal: F,
) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let addr = listener.local_addr().expect("local_addr");

    let serve_ct = ct.clone();
    let supervisor_ct = ct.child_token();
    let shutdown_ct = ct.clone();
    let shutdown = async move { shutdown_ct.cancelled().await };
    let serve_task = tokio::spawn(async move {
        gateway::serve::serve_on(listener, ctx, Vec::new(), serve_ct, supervisor_ct, shutdown).await
    });

    wait_for_serve_ready(addr).await;

    // Run the abnormal trigger on the same runtime as serve_on. We
    // don't await it here — a panicking trigger would otherwise
    // propagate into the test thread.
    let abnormal_handle = tokio::spawn(async move { abnormal().await });

    // Give the trigger a beat to run. 100 ms is plenty — every
    // abnormal trigger in this file does its work (panic or not)
    // synchronously or with one tokio::yield.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Swallow the trigger's JoinError (panic is expected in some
    // callers). We only await to avoid leaking a detached task.
    let _ = abnormal_handle.await;

    // Clean cancel: serve_on should now drain axum and run
    // close_all_upstreams, reaping the child.
    ct.cancel();

    tokio::time::timeout(Duration::from_secs(10), serve_task)
        .await
        .expect("serve_on must finish within 10 s after cancellation")
        .expect("serve_on task itself must not panic")
}

// ---------------------------------------------------------------------------
// Case 1: panic inside a handler task.
//
// Production spawn site: `gateway/src/handler/mod.rs`, in
// `impl Drop for SessionState`, which spawns a one-shot
// `broker.deregister(peer_id)` task. We simulate by spawning a task
// that panics immediately. The invariant is "panic in a handler-shaped
// task doesn't leak upstream children."
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_task_panic_does_not_leak_stdio_child() {
    let (pid, _bus, manager, listener, ctx, ct) = setup_stdio_harness("handler-panic").await;
    let _guard = PidGuard { pid };
    assert!(
        pid_alive(pid),
        "child pid {pid} must be alive before abnormal trigger"
    );

    let result = run_with_abnormal_and_graceful_shutdown(listener, ctx, ct, || async {
        // Simulate a panic in a handler-shaped task.
        panic!("synthetic handler-task panic");
    })
    .await;
    result.expect("serve_on must return Ok under graceful shutdown even with handler-task panic");

    // Drain invariant: after graceful shutdown, the manager is empty.
    assert!(
        manager.list().await.is_empty(),
        "UpstreamManager must be drained after graceful shutdown"
    );

    let reap_elapsed = eventually(CLEANUP_BUDGET, || !pid_alive(pid))
        .await
        .unwrap_or_else(|elapsed| {
            panic!(
                "child pid {pid} survived {elapsed:?} past graceful shutdown — \
                 CLEANUP INVARIANT VIOLATED under panic in handler-shaped task"
            )
        });
    eprintln!("[handler-panic] reaped {reap_elapsed:?} after graceful shutdown");
}

// ---------------------------------------------------------------------------
// Case 2: panic inside a forwarder task.
//
// Production spawn site: `gateway/src/session_forwarder.rs`, spawning
// a task that reads upstream events and notifies the session peer.
// We simulate by spawning a task that subscribes to the upstream bus
// and panics on first event. To ensure the panic actually fires, we
// publish a ResourceListChanged event on the bus right before
// cancellation.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarder_task_panic_does_not_leak_stdio_child() {
    let (pid, bus, manager, listener, ctx, ct) = setup_stdio_harness("forwarder-panic").await;
    let _guard = PidGuard { pid };
    assert!(
        pid_alive(pid),
        "child pid {pid} must be alive before abnormal trigger"
    );

    let bus_for_trigger = Arc::clone(&bus);
    let result = run_with_abnormal_and_graceful_shutdown(listener, ctx, ct, move || async move {
        // Simulate a forwarder task that panics on first event. A
        // real forwarder subscribes to the bus and notifies a peer;
        // this synthetic version subscribes to the same bus and
        // panics as soon as an event arrives. Publish the event
        // inline so the task panics before we await its handle.
        let mut rx = bus_for_trigger.subscribe();
        bus_for_trigger.publish(mcp_client::UpstreamEvent::ResourceListChanged {
            upstream: "forwarder-panic".into(),
        });
        let _ = rx.recv().await;
        panic!("synthetic forwarder-task panic");
    })
    .await;
    result.expect("serve_on must return Ok under graceful shutdown even with forwarder-task panic");

    assert!(
        manager.list().await.is_empty(),
        "UpstreamManager must be drained after graceful shutdown"
    );

    let reap_elapsed = eventually(CLEANUP_BUDGET, || !pid_alive(pid))
        .await
        .unwrap_or_else(|elapsed| {
            panic!(
                "child pid {pid} survived {elapsed:?} past graceful shutdown — \
                 CLEANUP INVARIANT VIOLATED under panic in forwarder-shaped task"
            )
        });
    eprintln!("[forwarder-panic] reaped {reap_elapsed:?} after graceful shutdown");
}

// ---------------------------------------------------------------------------
// Case 3: panic inside an invalidator task.
//
// Production spawn sites: `mcp-client/src/tool_list_cache.rs::spawn_invalidator`
// and `mcp-client/src/resource_list_cache.rs::spawn_resource_invalidator`.
// Both subscribe to the upstream bus and invalidate cache slots on
// ToolListChanged / ResourceListChanged. We simulate the same way
// as the forwarder case: subscribe, receive, panic.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalidator_task_panic_does_not_leak_stdio_child() {
    let (pid, bus, manager, listener, ctx, ct) = setup_stdio_harness("invalidator-panic").await;
    let _guard = PidGuard { pid };
    assert!(
        pid_alive(pid),
        "child pid {pid} must be alive before abnormal trigger"
    );

    let bus_for_trigger = Arc::clone(&bus);
    let result = run_with_abnormal_and_graceful_shutdown(listener, ctx, ct, move || async move {
        // Simulate an invalidator task that panics on first event.
        let mut rx = bus_for_trigger.subscribe();
        bus_for_trigger.publish(mcp_client::UpstreamEvent::ToolListChanged {
            upstream: "invalidator-panic".into(),
        });
        let _ = rx.recv().await;
        panic!("synthetic invalidator-task panic");
    })
    .await;
    result
        .expect("serve_on must return Ok under graceful shutdown even with invalidator-task panic");

    assert!(
        manager.list().await.is_empty(),
        "UpstreamManager must be drained after graceful shutdown"
    );

    let reap_elapsed = eventually(CLEANUP_BUDGET, || !pid_alive(pid))
        .await
        .unwrap_or_else(|elapsed| {
            panic!(
                "child pid {pid} survived {elapsed:?} past graceful shutdown — \
                 CLEANUP INVARIANT VIOLATED under panic in invalidator-shaped task"
            )
        });
    eprintln!("[invalidator-panic] reaped {reap_elapsed:?} after graceful shutdown");
}

// ---------------------------------------------------------------------------
// Case 4: serve_on aborted mid-flight — tokio runtime shutdown /
// main-future-Err analogue.
//
// We abort `serve_on`'s JoinHandle rather than sending a cancel. The
// explicit `close_all_upstreams` path never runs; cleanup falls
// through to rmcp's Drop-based async-kill task via
// `ChildWithCleanup::drop`. This pins that the Drop path still reaps
// the child within the cleanup budget when the graceful shutdown
// path is bypassed entirely.
//
// A test-runtime note: we spawn a dedicated tokio runtime for this
// test so the Drop-spawned kill task has a surviving runtime to run
// on after the serve_on frame unwinds. Inside the outer
// `#[tokio::test]` runtime, that's already true — we just have to
// avoid dropping the outer runtime before the cleanup completes.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_on_aborted_mid_flight_still_reaps_stdio_child() {
    let (pid, _bus, manager, listener, ctx, ct) = setup_stdio_harness("abort-mid-flight").await;
    let _guard = PidGuard { pid };
    assert!(pid_alive(pid), "child pid {pid} must be alive before abort");
    let addr = listener.local_addr().expect("local_addr");

    // Hold `manager` in the test so dropping `ctx` alone doesn't
    // release the registry. Production would release it; the test
    // retains a clone to observe the drain state post-cleanup.
    let serve_ct = ct.clone();
    let supervisor_ct = ct.child_token();
    let shutdown_ct = ct.clone();
    let shutdown = async move { shutdown_ct.cancelled().await };
    let serve_task = tokio::spawn(async move {
        gateway::serve::serve_on(listener, ctx, Vec::new(), serve_ct, supervisor_ct, shutdown).await
    });

    wait_for_serve_ready(addr).await;

    // Abort the serve_on task mid-flight. This drops its locals
    // (including the AppContext clone and its Arc<UpstreamManager>
    // reference) without running close_all_upstreams. rmcp's
    // Drop-based ChildWithCleanup should still reap the child.
    serve_task.abort();

    // Release our own UpstreamManager Arc so the last reference to
    // each handle goes away — otherwise the rmcp Service inside the
    // handle stays alive and the child is retained deliberately.
    // Dropping the Arc is the "abnormal exit path" we're simulating:
    // no one explicitly closed anything, Drop is the only actor.
    drop(manager);

    // Give the runtime time for rmcp's spawned kill task to execute.
    // It's bounded by the npx child's SIGTERM-then-SIGKILL ladder
    // (rmcp's ChildWithCleanup implementation); CLEANUP_BUDGET is
    // the same bound we use for the explicit close() path.
    let reap_elapsed = eventually(CLEANUP_BUDGET, || !pid_alive(pid))
        .await
        .unwrap_or_else(|elapsed| {
            panic!(
                "child pid {pid} survived {elapsed:?} past serve_on abort — \
                 CLEANUP INVARIANT VIOLATED on Drop-based cleanup path"
            )
        });
    eprintln!("[abort-mid-flight] reaped {reap_elapsed:?} after serve_on abort");
}

// ---------------------------------------------------------------------------
// Case 5: panic inside an UpstreamSupervisor task.
//
// Production spawn site: `gateway/src/serve.rs::spawn_stdio_supervisors`.
// Each supervisor holds `UpstreamConfig` + `Weak<UpstreamManager>` +
// `Arc<UpstreamHealth>` + misc refs — **NOT** an `Arc<UpstreamHandle>`.
// That structural invariant is what lets tokio's catch-panic-in-spawn
// semantics work without leaking a child: a panic in the supervisor
// drops only supervisor-local state, not the handle.
//
// We simulate the same way as the other panic cases: spawn a
// synthetic sibling task that panics immediately. The production
// supervisor body is narrow enough that carrying a test hook would
// cost more than it pays; the structural invariant is the property
// under test, and any task that respects the same invariant (no
// handle Arc stored on the struct) behaves the same under panic.
//
// If a future refactor stores an `Arc<UpstreamHandle>` on the
// supervisor struct (e.g., "let's cache the handle so we don't
// handle_by_name on every tick"), this test AND the other
// subprocess-cleanup survivability tests will keep passing — but the *supervisor*
// panic would start leaking, because its locals now include a
// handle Arc. That's why we also pin the structural invariant via
// code review / module-header documentation in `supervisor.rs`.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_task_panic_does_not_leak_stdio_child() {
    let (pid, _bus, manager, listener, ctx, ct) = setup_stdio_harness("supervisor-panic").await;
    let _guard = PidGuard { pid };
    assert!(
        pid_alive(pid),
        "child pid {pid} must be alive before abnormal trigger"
    );

    let result = run_with_abnormal_and_graceful_shutdown(listener, ctx, ct, || async {
        // Simulate a supervisor-shaped task panic. The structural
        // invariant (no handle Arc on the task's locals) is what
        // makes this safe; an actual panic in
        // `UpstreamSupervisor::run` during production would behave
        // identically.
        panic!("synthetic supervisor-task panic");
    })
    .await;
    result
        .expect("serve_on must return Ok under graceful shutdown even with supervisor-task panic");

    assert!(
        manager.list().await.is_empty(),
        "UpstreamManager must be drained after graceful shutdown"
    );

    let reap_elapsed = eventually(CLEANUP_BUDGET, || !pid_alive(pid))
        .await
        .unwrap_or_else(|elapsed| {
            panic!(
                "child pid {pid} survived {elapsed:?} past graceful shutdown — \
                 CLEANUP INVARIANT VIOLATED under panic in supervisor-shaped task"
            )
        });
    eprintln!("[supervisor-panic] reaped {reap_elapsed:?} after graceful shutdown");
}
