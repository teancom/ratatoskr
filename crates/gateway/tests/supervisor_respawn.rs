// pattern: Imperative Shell
//
// End-to-end respawn-after-SIGKILL integration tests against a real
// stdio upstream. The unit tests in
// `mcp-client/src/supervisor.rs` already pin the state-machine
// transitions against in-process fakes; these tests pin the
// production code path (`RealConnector` + real npx child + real
// `UpstreamManager::replace`) end-to-end.
//
// ## Fixture choice
//
// Reuses `@modelcontextprotocol/server-everything` via `npx` — the
// same fixture that `abnormal_exit_cleanup.rs` and `upstream_cleanup.rs`
// use. Each test pays one npx cold-start (~1–3 s for the first spawn
// plus another ~1–3 s for the respawn); acceptable for the respawn
// test's scope.
//
// ## Budget
//
// Respawn after SIGKILL takes: supervisor notices transport-closed
// (at most one `active_ping_interval`) + issues `connect::connect`
// (npx cold start ~1–3 s) + `UpstreamManager::replace`. Budget: 15 s.
// Tightening to match observed fast-machine times would turn a
// correctness assertion into a flaky timing assertion.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mcp_client::{
    HealthState, RealConnector, SamplingBroker, SharedSamplingBroker, SupervisionSettings,
    UpstreamConfig, UpstreamEventBus, UpstreamHandle, UpstreamKind, UpstreamManager,
    UpstreamSupervisor, connect,
};
use nix::sys::signal::{Signal, kill, killpg};
use nix::unistd::Pid;
use tokio_util::sync::CancellationToken;

const RESPAWN_BUDGET: Duration = Duration::from_secs(15);

fn pid_alive(pid: u32) -> bool {
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

/// Scoped safety net: on Drop, force-kill the pid's process group if
/// it's still around. Prevents a flaky test from leaking npx/node
/// descendants across runs.
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

async fn eventually<F>(timeout: Duration, mut cond: F) -> Result<Duration, Duration>
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return Ok(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(start.elapsed())
}

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

/// Build a supervisor wired to a real stdio child + real
/// `RealConnector`. Returns everything the test needs: the manager
/// (for `handle_by_name` lookups), the original pid (for SIGKILL),
/// the supervisor's JoinHandle, and the cancellation token.
async fn spawn_real_supervisor(
    name: &'static str,
    settings: SupervisionSettings,
) -> (
    Arc<UpstreamManager>,
    u32,
    tokio::task::JoinHandle<()>,
    CancellationToken,
) {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let config = everything_upstream(name);

    let live = connect(&config, &bus, &broker)
        .await
        .expect("connect to server-everything via npx");
    let pid = match &live {
        mcp_client::LiveUpstream::Stdio { pid, .. } => pid.expect("stdio upstream has pid"),
        _ => panic!("expected LiveUpstream::Stdio"),
    };

    let handle = Arc::new(UpstreamHandle::new_with_call_timeout(
        config.clone(),
        live,
        settings.call_timeout,
    ));
    let health = handle.health();

    let manager = Arc::new(UpstreamManager::new());
    manager.register(handle).await;

    let ct = CancellationToken::new();
    let supervisor = UpstreamSupervisor::new(
        config,
        settings,
        Arc::downgrade(&manager),
        health,
        bus,
        broker,
        ct.clone(),
        Box::new(RealConnector),
    );
    let task = tokio::spawn(supervisor.run());

    (manager, pid, task, ct)
}

/// Ignored by default because the test depends on `npx` being on PATH
/// and willing to fetch `@modelcontextprotocol/server-everything` from
/// the network on first run. Matches the `#[ignore]` discipline used
/// by `upstream_cleanup.rs` for the same reason — the other tests in
/// that file that need real npx are NOT gated because they're
/// representative of the gateway's production behaviour; same argument
/// applies here.
///
/// Run with: `cargo test -p gateway --test supervisor_respawn --
/// --include-ignored`.
#[tokio::test]
async fn supervisor_respawns_stdio_child_after_sigkill() {
    // Property: when the stdio child dies (simulated via SIGKILL on
    // its process group), the supervisor detects transport closure,
    // runs a respawn via `RealConnector`, installs the fresh handle
    // via `UpstreamManager::replace`, and the registered child has a
    // different pid.
    //
    // End-to-end coverage of the production path: `spawn_stdio_supervisors`
    // in `serve.rs` uses the exact same inputs this test constructs.
    let settings = SupervisionSettings {
        // Short poll interval so the supervisor notices the closed
        // transport quickly.
        active_ping_interval: Duration::from_millis(500),
        active_ping_timeout: Duration::from_millis(300),
        // Short backoff so a one-attempt respawn doesn't wait.
        respawn_backoff_base: Duration::from_millis(100),
        respawn_backoff_cap: Duration::from_millis(100),
        respawn_max_attempts: 3,
        // Default threshold is fine — we'll flip to Unhealthy via
        // transport-closed, not the counter.
        ..SupervisionSettings::default()
    };

    let (manager, original_pid, supervisor_task, ct) =
        spawn_real_supervisor("respawn-sigkill", settings).await;
    let _guard = PidGuard { pid: original_pid };

    // SIGKILL the original process group — bypasses rmcp's graceful
    // shutdown and simulates a container OOM / crash.
    killpg(Pid::from_raw(original_pid as i32), Signal::SIGKILL)
        .expect("SIGKILL the original upstream's process group");

    // Wait for the original pid to reap.
    eventually(RESPAWN_BUDGET, || !pid_alive(original_pid))
        .await
        .expect("original pid did not reap within budget");

    // Now wait for the supervisor to respawn. The health state will
    // be Healthy with a NEW pid on the registered handle.
    let deadline = Instant::now() + RESPAWN_BUDGET;
    let mut new_pid: Option<u32> = None;
    while Instant::now() < deadline {
        if let Some(handle) = manager.handle_by_name("respawn-sigkill").await
            && let Some(pid) = handle.child_pid()
            && pid != original_pid
            && handle.health().state() == HealthState::Healthy
        {
            new_pid = Some(pid);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let new_pid = new_pid.expect("supervisor did not respawn within budget");
    let _new_guard = PidGuard { pid: new_pid };
    assert_ne!(new_pid, original_pid, "respawn must install a fresh pid");
    assert!(pid_alive(new_pid), "respawned pid must be alive");

    // Clean up: cancel the supervisor and the manager.
    ct.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), supervisor_task).await;

    // Drain the manager so the new child is closed deterministically
    // instead of relying on Drop semantics at test-teardown.
    for handle_arc in manager.drain().await {
        if let Ok(handle) = Arc::try_unwrap(handle_arc) {
            let _ = handle.close().await;
        }
    }
}

#[tokio::test]
async fn supervisor_stays_quiet_when_upstream_is_healthy() {
    // Property: against a healthy upstream, the supervisor does NOT
    // respawn. The registered pid stays the same across a generous
    // observation window, and the health state stays Healthy.
    //
    // Regression guard: a future refactor that accidentally triggers
    // a respawn on a healthy upstream (e.g. a bug in the Healthy →
    // Suspect predicate) would break this test.
    let settings = SupervisionSettings {
        active_ping_interval: Duration::from_millis(200),
        active_ping_timeout: Duration::from_millis(150),
        // Short defaults so a failure would be quick — but we're
        // checking the supervisor doesn't fail.
        respawn_backoff_base: Duration::from_millis(50),
        respawn_backoff_cap: Duration::from_millis(50),
        ..SupervisionSettings::default()
    };

    let (manager, original_pid, supervisor_task, ct) =
        spawn_real_supervisor("stable-upstream", settings).await;
    let _guard = PidGuard { pid: original_pid };

    // Sleep discipline: this is a *negative*-assertion window
    // (per AGENTS.md "Sleeps only for negative assertions").
    // We're proving that nothing happens — that the supervisor does
    // NOT fire a respawn — over a 2 s window during which the
    // supervisor runs ~10 ticks at its 200 ms cadence. An
    // `eventually()` helper doesn't fit: there's no deterministic
    // signal that "a respawn wasn't triggered" we could poll for.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let current_handle = manager
        .handle_by_name("stable-upstream")
        .await
        .expect("upstream should still be registered");
    assert_eq!(
        current_handle.child_pid(),
        Some(original_pid),
        "pid must not change when upstream is healthy"
    );
    assert_eq!(
        current_handle.health().state(),
        HealthState::Healthy,
        "health must stay Healthy"
    );
    drop(current_handle);

    ct.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), supervisor_task).await;
    for handle_arc in manager.drain().await {
        if let Ok(handle) = Arc::try_unwrap(handle_arc) {
            let _ = handle.close().await;
        }
    }
}
