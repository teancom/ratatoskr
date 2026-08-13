//! Supervisor driver tests.
//!
//! Each test constructs a real `UpstreamManager` registered with a
//! real in-process fake upstream (via duplex channels), then drives
//! the supervisor's ticks directly to observe state transitions.
//!
//! The [`FakeConnector`] is scripted with a `VecDeque` of outcomes
//! so tests can pin specific respawn sequences ("first two attempts
//! fail with Connect error, third succeeds").
//!
//! Timing-sensitive loops (`run()` with sleeps) are exercised via
//! `tokio::time::pause()` on a `current_thread` runtime. The
//! supervisor does no `spawn_blocking`, so paused time auto-
//! advance is reliable for us.

use super::*;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use rmcp::{ServerHandler, ServiceExt};

use crate::connect::{LiveUpstream, connect_with_transport};
use crate::event_bus::UpstreamEventBus;
use crate::health::{FailureKind, HealthState};
use crate::sampling::SamplingBroker;
use crate::supervision::SupervisionSettings;
use crate::upstream::{UpstreamConfig, UpstreamHandle, UpstreamKind, UpstreamManager};

/// A do-nothing MCP server. The default `ServerHandler` impl is
/// enough — we only drive the initialize handshake plus the
/// occasional ping via rmcp's built-in handling.
struct FakeServer;
impl ServerHandler for FakeServer {}

type FakeHolder = rmcp::service::RunningService<rmcp::RoleServer, FakeServer>;

/// Build one live handle + hold the server-side RunningService.
/// Keeps the ServerHolder in scope for the life of the test so the
/// duplex stays open.
async fn make_live_handle(name: &str) -> (FakeHolder, Arc<UpstreamHandle>) {
    let (server_side, client_side) = tokio::io::duplex(64 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_side);
    let (cli_r, cli_w) = tokio::io::split(client_side);

    let bus = UpstreamEventBus::new();
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());

    let config = UpstreamConfig::new(
        name,
        name.replace('-', "_"),
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    );

    let (server_svc, live) = tokio::join!(
        FakeServer.serve((srv_r, srv_w)),
        connect_with_transport(&config, &bus, &broker, cli_r, cli_w),
    );

    let server_svc = server_svc.expect("fake server serve");
    let live = live.expect("connect_with_transport");
    let handle = Arc::new(UpstreamHandle::new_with_call_timeout(
        config,
        live,
        Duration::from_secs(1),
    ));
    (server_svc, handle)
}

/// Deliver scripted outcomes from a VecDeque. One outcome per
/// `connect()` call. Build outcomes with `FakeConnector::with_*`
/// helpers.
struct FakeConnector {
    outcomes: Mutex<VecDeque<FakeOutcome>>,
    /// Shared channel so a connect-success outcome can hand back a
    /// LiveUpstream the test has pre-built. We can't construct a
    /// LiveUpstream by value inside an enum (it's !Unpin and
    /// non-Clone), so we stash them in this parallel queue.
    live_slots: Mutex<VecDeque<LiveUpstream>>,
    calls: AtomicU32,
}

#[derive(Debug, Clone)]
enum FakeOutcome {
    /// Pop the next LiveUpstream from `live_slots` and return it.
    SuccessFromSlot,
    /// Return a scripted UpstreamError.
    Failure(FakeError),
}

#[derive(Debug, Clone)]
enum FakeError {
    Connect(String),
}

impl FakeConnector {
    fn new() -> Self {
        Self {
            outcomes: Mutex::new(VecDeque::new()),
            live_slots: Mutex::new(VecDeque::new()),
            calls: AtomicU32::new(0),
        }
    }

    fn enqueue_success(&self, live: LiveUpstream) {
        self.live_slots
            .lock()
            .expect("live_slots mutex poisoned")
            .push_back(live);
        self.outcomes
            .lock()
            .expect("outcomes mutex poisoned")
            .push_back(FakeOutcome::SuccessFromSlot);
    }

    fn enqueue_failure(&self, err: FakeError) {
        self.outcomes
            .lock()
            .expect("outcomes mutex poisoned")
            .push_back(FakeOutcome::Failure(err));
    }

    fn call_count(&self) -> u32 {
        self.calls.load(Ordering::Acquire)
    }
}

#[async_trait]
impl Connector for FakeConnector {
    async fn connect(
        &self,
        config: &UpstreamConfig,
        _bus: &UpstreamEventBus,
        _broker: &SharedSamplingBroker,
    ) -> Result<LiveUpstream, UpstreamError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let outcome = self
            .outcomes
            .lock()
            .expect("outcomes mutex poisoned")
            .pop_front()
            .unwrap_or(FakeOutcome::Failure(FakeError::Connect(
                "fake ran out of scripted outcomes".into(),
            )));
        match outcome {
            FakeOutcome::SuccessFromSlot => {
                let live = self
                    .live_slots
                    .lock()
                    .expect("live_slots mutex poisoned")
                    .pop_front()
                    .expect("success outcome scheduled without matching live slot");
                Ok(live)
            }
            FakeOutcome::Failure(FakeError::Connect(msg)) => Err(UpstreamError::Connect {
                name: config.name.clone(),
                source: anyhow::anyhow!("{msg}"),
            }),
        }
    }
}

/// Build a supervisor + all its deps around a live handle. Returns
/// the supervisor, the manager (for test assertions), the shared
/// broker/bus, the fake connector, the cancellation token, and the
/// server holder (kept alive so the duplex stays open).
struct Harness {
    supervisor: UpstreamSupervisor,
    manager: Arc<UpstreamManager>,
    connector: Arc<FakeConnector>,
    ct: CancellationToken,
    _server: FakeHolder,
}

async fn build_harness(name: &str, settings: SupervisionSettings) -> Harness {
    let (server, handle) = make_live_handle(name).await;
    let manager = Arc::new(UpstreamManager::new());
    manager.register(Arc::clone(&handle)).await;
    let health = handle.health();
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let ct = CancellationToken::new();
    let connector = Arc::new(FakeConnector::new());

    let supervisor = UpstreamSupervisor::new(
        handle.config.clone(),
        settings,
        Arc::downgrade(&manager),
        health,
        bus,
        broker,
        ct.clone(),
        Box::new(FakeConnectorProxy(Arc::clone(&connector))),
    );

    Harness {
        supervisor,
        manager,
        connector,
        ct,
        _server: server,
    }
}

/// Thin adapter letting tests share one `Arc<FakeConnector>`
/// between the supervisor (which takes `Box<dyn Connector>`) and
/// the test body (which wants to call `enqueue_*` on it).
struct FakeConnectorProxy(Arc<FakeConnector>);

#[async_trait]
impl Connector for FakeConnectorProxy {
    async fn connect(
        &self,
        config: &UpstreamConfig,
        bus: &UpstreamEventBus,
        broker: &SharedSamplingBroker,
    ) -> Result<LiveUpstream, UpstreamError> {
        self.0.connect(config, bus, broker).await
    }
}

/// Produce a fresh `LiveUpstream` wired to a new in-process fake
/// server. The caller keeps the `FakeHolder` alive so the duplex
/// doesn't close mid-test.
async fn fresh_live_upstream(config: &UpstreamConfig) -> (FakeHolder, LiveUpstream) {
    let (server_side, client_side) = tokio::io::duplex(64 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_side);
    let (cli_r, cli_w) = tokio::io::split(client_side);

    let bus = UpstreamEventBus::new();
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());

    let (server_svc, live) = tokio::join!(
        FakeServer.serve((srv_r, srv_w)),
        connect_with_transport(config, &bus, &broker, cli_r, cli_w),
    );
    let server_svc = server_svc.expect("fake server serve");
    let live = live.expect("connect_with_transport");
    (server_svc, live)
}

// ---- Healthy-state transitions ----

#[tokio::test]
async fn tick_healthy_is_a_noop_when_counter_is_below_threshold() {
    // Property: in Healthy with consecutive_failures < threshold
    // and transport open, one tick leaves state at Healthy.
    let settings = SupervisionSettings {
        unhealthy_threshold: 3,
        ..SupervisionSettings::default()
    };
    let mut h = build_harness("noop", settings).await;
    // Simulate 1 failure — below threshold.
    {
        let handle = h
            .manager
            .handle_by_name("noop")
            .await
            .expect("upstream must be registered");
        handle.health().record_failure(FailureKind::CallTimeout);
    }
    h.supervisor.tick().await;
    let state = {
        let handle = h
            .manager
            .handle_by_name("noop")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(state, HealthState::Healthy);
}

#[tokio::test]
async fn tick_healthy_flips_to_suspect_after_k_consecutive_failures() {
    // Property: in Healthy, reaching consecutive_failures ==
    // unhealthy_threshold triggers a flip to Suspect so the
    // supervisor starts actively probing.
    let settings = SupervisionSettings {
        unhealthy_threshold: 3,
        ..SupervisionSettings::default()
    };
    let mut h = build_harness("counter", settings).await;
    {
        let handle = h
            .manager
            .handle_by_name("counter")
            .await
            .expect("upstream must be registered");
        handle.health().record_failure(FailureKind::CallTimeout);
        handle.health().record_failure(FailureKind::CallTimeout);
        handle.health().record_failure(FailureKind::CallTimeout);
    }
    h.supervisor.tick().await;
    let state = {
        let handle = h
            .manager
            .handle_by_name("counter")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(state, HealthState::Suspect);
}

#[tokio::test]
async fn tick_healthy_flips_to_suspect_on_self_reported_streak_alone() {
    // Property: a sustained streak of self-reported errors (Error-level
    // notifications/message from the upstream) drives Healthy →
    // Suspect even when consecutive_failures stays at zero. This is
    // the dash-mcp pathology — every call_tool returns Ok(_) with
    // an error payload, so consecutive_failures never rises, but
    // the side-channel log lets the supervisor see the truth.
    //
    // Regression guard against a tempting implementation: funneling
    // the signal through `record_failure` doesn't work, because
    // `record_success` immediately zeroes it on the (apparent)
    // `Ok(_)` from the same tool call.
    let settings = SupervisionSettings {
        unhealthy_threshold: 3,
        ..SupervisionSettings::default()
    };
    let mut h = build_harness("self_reporter", settings).await;
    {
        let handle = h
            .manager
            .handle_by_name("self_reporter")
            .await
            .expect("upstream must be registered");
        // Three self-reported hits. record_success runs in between
        // each one (simulating a misleading-success call_tool) and
        // MUST NOT zero the counter — that's the whole point.
        handle.health().record_self_reported_error();
        handle.health().record_success();
        handle.health().record_self_reported_error();
        handle.health().record_success();
        handle.health().record_self_reported_error();
        handle.health().record_success();

        // Sanity: transport-level counter never moved.
        assert_eq!(handle.health().consecutive_failures(), 0);
        // But the self-reported counter is at threshold.
        assert_eq!(handle.health().consecutive_self_reported_errors(), 3);
    }
    h.supervisor.tick().await;
    let state = {
        let handle = h
            .manager
            .handle_by_name("self_reporter")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(
        state,
        HealthState::Suspect,
        "self-reported streak alone must demote, even with consecutive_failures=0"
    );
}

// ---- Suspect-state transitions ----

#[tokio::test]
async fn tick_suspect_does_not_promote_on_pings_alone() {
    // Property (new model — replaces the prior "K pings ⇒ Healthy"
    // promotion): the supervisor never flips Suspect → Healthy off
    // ping success alone. Promotion is owned by `record_success` on
    // a real call_tool / list_* outcome.
    //
    // Why: mcp-remote-style stdio bridges answer MCP `ping` locally
    // in the bridge's own shim before the message ever reaches the
    // remote upstream, so a bridge whose transport is dead can still
    // pass every ping while real RPCs hang (see supervisor.rs's "Lazy
    // active pings" section).
    //
    // We drive 5× the threshold worth of pings to make absolutely
    // sure no internal counter is fooling itself into a promote.
    let settings = SupervisionSettings {
        unhealthy_threshold: 2,
        active_ping_timeout: Duration::from_secs(1),
        active_ping_interval: Duration::from_secs(2),
        ..SupervisionSettings::default()
    };
    let mut h = build_harness("pinger", settings).await;
    {
        let handle = h
            .manager
            .handle_by_name("pinger")
            .await
            .expect("upstream must be registered");
        handle.health().set_state(HealthState::Suspect);
    }

    for _ in 0..(5 * settings.unhealthy_threshold) {
        h.supervisor.tick().await;
    }

    let state = {
        let handle = h
            .manager
            .handle_by_name("pinger")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(
        state,
        HealthState::Suspect,
        "many ping successes alone must NOT promote — promotion is owned by record_success"
    );
}

#[tokio::test]
async fn record_success_in_suspect_promotes_to_healthy_and_zeros_counter() {
    // Property: a real-traffic success (which ultimately flows
    // through `UpstreamHealth::record_success`) is the canonical
    // promotion signal. It both (a) zeros consecutive_failures and
    // (b) atomically CASes Suspect → Healthy. Without this the
    // upstream would stay in Suspect forever even after recovering.
    //
    // We drive the supervisor through a ping tick first to confirm
    // ping success doesn't promote (regression guard for the
    // previous test's claim), then call `record_success` to trigger
    // the CAS.
    let settings = SupervisionSettings {
        unhealthy_threshold: 2,
        active_ping_timeout: Duration::from_secs(1),
        active_ping_interval: Duration::from_secs(2),
        ..SupervisionSettings::default()
    };
    let mut h = build_harness("recoverer", settings).await;
    {
        let handle = h
            .manager
            .handle_by_name("recoverer")
            .await
            .expect("upstream must be registered");
        // Set up Suspect with the same counter state real failures
        // would leave behind.
        handle.health().record_failure(FailureKind::CallTimeout);
        handle.health().record_failure(FailureKind::CallTimeout);
        handle.health().set_state(HealthState::Suspect);
        assert_eq!(handle.health().consecutive_failures(), 2);
    }

    // Ping tick — proves we do NOT promote on ping alone.
    h.supervisor.tick().await;
    let mid_state = {
        let handle = h
            .manager
            .handle_by_name("recoverer")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(
        mid_state,
        HealthState::Suspect,
        "ping success must not promote — record_success owns that flip"
    );

    // Now simulate a real-traffic success.
    {
        let handle = h
            .manager
            .handle_by_name("recoverer")
            .await
            .expect("upstream must be registered");
        handle.health().record_success();
    }

    let snap = {
        let handle = h
            .manager
            .handle_by_name("recoverer")
            .await
            .expect("upstream must be registered");
        handle.health().snapshot()
    };
    assert_eq!(
        snap.state,
        HealthState::Healthy,
        "record_success must atomically promote Suspect → Healthy"
    );
    assert_eq!(
        snap.consecutive_failures, 0,
        "record_success must zero the failure counter, otherwise the next \
         tick_healthy would immediately re-flip the upstream to Suspect"
    );
}

#[tokio::test]
async fn tick_suspect_escalates_to_unhealthy_on_2k_consecutive_real_traffic_failures() {
    // Property: real-traffic escalation. While in Suspect, if
    // `consecutive_failures` reaches `2 * unhealthy_threshold`,
    // the supervisor flips to Unhealthy regardless of ping outcome.
    //
    // This catches the rootly-bridge pathology: the bridge passes
    // every ping (mcp-remote answers ping in its local shim before
    // the message hits the SSE wire) but blackholes real RPCs. The
    // bridge needs a respawn; under the old model pings would
    // perpetually claim "alive" and the supervisor would never
    // escalate. Now: 2K consecutive transport-level real-traffic
    // failures forces the flip.
    let settings = SupervisionSettings {
        unhealthy_threshold: 3,
        active_ping_timeout: Duration::from_secs(1),
        active_ping_interval: Duration::from_secs(2),
        ..SupervisionSettings::default()
    };
    let mut h = build_harness("escalator", settings).await;

    // Pile up 2K = 6 consecutive real-traffic failures and put the
    // supervisor in Suspect — exactly the state we'd be in if real
    // traffic kept timing out while pings kept passing.
    {
        let handle = h
            .manager
            .handle_by_name("escalator")
            .await
            .expect("upstream must be registered");
        for _ in 0..(2 * settings.unhealthy_threshold) {
            handle.health().record_failure(FailureKind::CallTimeout);
        }
        handle.health().set_state(HealthState::Suspect);
        assert_eq!(handle.health().consecutive_failures(), 6);
    }

    h.supervisor.tick().await;

    let state = {
        let handle = h
            .manager
            .handle_by_name("escalator")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(
        state,
        HealthState::Unhealthy,
        "2K consecutive real-traffic failures in Suspect must escalate \
         to Unhealthy regardless of ping health"
    );
}

#[tokio::test]
async fn tick_suspect_does_not_escalate_below_2k_failures() {
    // Property: the escalation is at exactly `2 * threshold`, not
    // earlier. With `2 * threshold - 1` failures the supervisor
    // stays in Suspect (waiting for either real-traffic recovery,
    // a transport closure, or one more failure). Pins the boundary
    // so a future tweak to the multiplier doesn't silently lower it.
    let settings = SupervisionSettings {
        unhealthy_threshold: 3,
        active_ping_timeout: Duration::from_secs(1),
        active_ping_interval: Duration::from_secs(2),
        ..SupervisionSettings::default()
    };
    let mut h = build_harness("almost-escalating", settings).await;

    {
        let handle = h
            .manager
            .handle_by_name("almost-escalating")
            .await
            .expect("upstream must be registered");
        // 2K - 1 = 5 — one less than the escalation threshold.
        for _ in 0..(2 * settings.unhealthy_threshold - 1) {
            handle.health().record_failure(FailureKind::CallTimeout);
        }
        handle.health().set_state(HealthState::Suspect);
        assert_eq!(handle.health().consecutive_failures(), 5);
    }

    h.supervisor.tick().await;

    let state = {
        let handle = h
            .manager
            .handle_by_name("almost-escalating")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(
        state,
        HealthState::Suspect,
        "(2K - 1) failures must not yet trigger escalation; the boundary \
         is exactly 2K"
    );
}

#[tokio::test]
async fn tick_suspect_flips_to_unhealthy_when_transport_closed() {
    // Property: in Suspect, `is_transport_closed()` returning true
    // short-circuits ping-based probing and flips straight to
    // Unhealthy. No amount of pinging brings a dead transport back.
    //
    // We simulate transport closure by calling `cancel()` on the
    // server-side RunningService, which closes rmcp's side of the
    // duplex. The client peer observes `is_transport_closed == true`
    // on its next poll.
    let settings = SupervisionSettings {
        unhealthy_threshold: 3,
        active_ping_timeout: Duration::from_millis(50),
        active_ping_interval: Duration::from_secs(2),
        ..SupervisionSettings::default()
    };
    let Harness {
        mut supervisor,
        manager,
        connector: _,
        ct: _,
        _server,
    } = build_harness("dying-transport", settings).await;
    {
        let handle = manager
            .handle_by_name("dying-transport")
            .await
            .expect("upstream must be registered");
        handle.health().set_state(HealthState::Suspect);
    }

    // Drop the server holder — this drops its `RunningService`,
    // which tears down the server-side transport. rmcp's client
    // peer notices the broken pipe on its next read.
    drop(_server);

    // Wait for rmcp's client peer to observe the closure. A poll
    // against `is_transport_closed()` beats a fixed sleep: it
    // returns the moment the condition is satisfied (fastest
    // path) and still surfaces a clear failure if it never
    // closes (slowest path), whereas a fixed sleep would either
    // flake under load or waste time on warm machines.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let closed = {
            let handle = manager
                .handle_by_name("dying-transport")
                .await
                .expect("upstream must be registered");
            handle.is_transport_closed()
        };
        if closed {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rmcp client never observed transport closure within 2 s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    supervisor.tick().await;

    let state = {
        let handle = manager
            .handle_by_name("dying-transport")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(
        state,
        HealthState::Unhealthy,
        "transport-closed probe must short-circuit Suspect to Unhealthy"
    );
}

// ---- Respawn flow ----

#[tokio::test]
async fn tick_unhealthy_transitions_to_respawning() {
    // Property: Unhealthy is a transient flag — one tick flips to
    // Respawning so the respawn flow starts. No connect attempt
    // fires on this tick.
    let settings = SupervisionSettings::default();
    let mut h = build_harness("toflip", settings).await;
    {
        let handle = h
            .manager
            .handle_by_name("toflip")
            .await
            .expect("upstream must be registered");
        handle.health().set_state(HealthState::Unhealthy);
    }
    h.supervisor.tick().await;
    let state = {
        let handle = h
            .manager
            .handle_by_name("toflip")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(state, HealthState::Respawning);
    assert_eq!(
        h.connector.call_count(),
        0,
        "tick_unhealthy must not call connect — that's the next tick's job"
    );
}

#[tokio::test]
async fn respawning_flips_to_healthy_on_successful_connect_and_replace() {
    // Property: from Respawning, a successful connect + replace:
    //   (a) invokes connector exactly once,
    //   (b) swaps the registered handle in the manager,
    //   (c) flips the NEW handle's health state to Healthy,
    //   (d) the supervisor's local health ref tracks the new record
    //       (observable via state() going forward).
    let settings = SupervisionSettings {
        respawn_max_attempts: 3,
        respawn_backoff_base: Duration::from_millis(1),
        respawn_backoff_cap: Duration::from_millis(1),
        ..SupervisionSettings::default()
    };
    let mut h = build_harness("respawn", settings).await;
    {
        let handle = h
            .manager
            .handle_by_name("respawn")
            .await
            .expect("upstream must be registered");
        handle.health().set_state(HealthState::Respawning);
    }

    // Capture the original handle's identity so we can later assert
    // the registered handle changed.
    let original_arc = h
        .manager
        .handle_by_name("respawn")
        .await
        .expect("upstream must be registered");

    // Script one success.
    let cfg = original_arc.config.clone();
    let (_server_new, live_new) = fresh_live_upstream(&cfg).await;
    h.connector.enqueue_success(live_new);

    h.supervisor.tick().await;

    assert_eq!(h.connector.call_count(), 1);

    let current_arc = h
        .manager
        .handle_by_name("respawn")
        .await
        .expect("upstream must be registered");
    assert!(
        !Arc::ptr_eq(&original_arc, &current_arc),
        "replace must have installed a fresh handle"
    );
    assert_eq!(current_arc.health().state(), HealthState::Healthy);
}

#[tokio::test]
async fn respawning_flips_to_dead_after_max_attempts() {
    // Property: after respawn_max_attempts consecutive connect
    // failures, the supervisor flips to Dead. Subsequent ticks
    // don't call connect until the rearm tick.
    let settings = SupervisionSettings {
        respawn_max_attempts: 3,
        respawn_backoff_base: Duration::from_millis(1),
        respawn_backoff_cap: Duration::from_millis(1),
        ..SupervisionSettings::default()
    };
    let mut h = build_harness("dying", settings).await;
    {
        let handle = h
            .manager
            .handle_by_name("dying")
            .await
            .expect("upstream must be registered");
        handle.health().set_state(HealthState::Respawning);
    }
    for _ in 0..3 {
        h.connector
            .enqueue_failure(FakeError::Connect("boom".into()));
    }

    // Three attempts — each in its own tick.
    for _ in 0..3 {
        h.supervisor.tick().await;
    }

    let state = {
        let handle = h
            .manager
            .handle_by_name("dying")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(state, HealthState::Dead);
    assert_eq!(
        h.connector.call_count(),
        3,
        "exactly respawn_max_attempts connect attempts must fire"
    );
}

#[tokio::test]
async fn dead_rearms_to_respawning_on_next_tick() {
    // Property: Dead is not terminal. One tick flips Dead →
    // Respawning so recovery can be attempted again.
    let settings = SupervisionSettings::default();
    let mut h = build_harness("corpse", settings).await;
    {
        let handle = h
            .manager
            .handle_by_name("corpse")
            .await
            .expect("upstream must be registered");
        handle.health().set_state(HealthState::Dead);
    }
    h.supervisor.tick().await;
    let state = {
        let handle = h
            .manager
            .handle_by_name("corpse")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(state, HealthState::Respawning);
}

#[tokio::test]
async fn successful_respawn_after_two_failures_resets_streak() {
    // Property: the failure streak is per-episode. After 2 failures
    // then 1 success, the next respawn episode starts with a fresh
    // counter — a future failure in the SAME test doesn't trip
    // Dead immediately.
    let settings = SupervisionSettings {
        respawn_max_attempts: 3,
        respawn_backoff_base: Duration::from_millis(1),
        respawn_backoff_cap: Duration::from_millis(1),
        ..SupervisionSettings::default()
    };
    let mut h = build_harness("recovering", settings).await;

    // Drive into Respawning.
    {
        let handle = h
            .manager
            .handle_by_name("recovering")
            .await
            .expect("upstream must be registered");
        handle.health().set_state(HealthState::Respawning);
    }

    // Script: fail, fail, success.
    h.connector
        .enqueue_failure(FakeError::Connect("one".into()));
    h.connector
        .enqueue_failure(FakeError::Connect("two".into()));
    let cfg = h
        .manager
        .handle_by_name("recovering")
        .await
        .expect("upstream must be registered")
        .config
        .clone();
    let (_server_new, live_new) = fresh_live_upstream(&cfg).await;
    h.connector.enqueue_success(live_new);

    // Three ticks to get through the three attempts.
    h.supervisor.tick().await;
    h.supervisor.tick().await;
    h.supervisor.tick().await;

    // Final state: Healthy (the success on attempt 3 succeeded).
    let state = {
        let handle = h
            .manager
            .handle_by_name("recovering")
            .await
            .expect("upstream must be registered");
        handle.health().state()
    };
    assert_eq!(state, HealthState::Healthy);
}

// ---- Structural invariant ----

#[tokio::test]
async fn supervisor_exits_when_manager_dropped() {
    // Property: the supervisor holds Weak<UpstreamManager>. Once
    // the last strong reference drops, `run()` exits promptly
    // without panicking — this is the graceful-shutdown path for
    // the case where the gateway tears down without cancelling the
    // supervisor's token.
    let settings = SupervisionSettings::default();
    let h = build_harness("ephemeral", settings).await;

    let supervisor = h.supervisor;
    let manager = h.manager;
    let _ct = h.ct;
    let _connector = h.connector;
    let _server = h._server;

    // Spawn the supervisor on its own task and drop the strong
    // manager reference.
    let run_handle = tokio::spawn(async move { supervisor.run().await });
    drop(manager);

    // The supervisor should notice within one tick-interval and
    // exit. Give it a generous window.
    tokio::time::timeout(Duration::from_secs(10), run_handle)
        .await
        .expect("supervisor run did not exit within budget")
        .expect("supervisor task should not panic");
}

// ---- Cancellation ----

/// Build a supervisor wired to a manager that has NO prior
/// registration under `name`. Models the bootstrap-failed-retry
/// case: gateway's `connect_configs` couldn't reach this upstream at
/// boot, so `spawn_bootstrap_retry_supervisors` builds a supervisor
/// with a born-Respawning health record and no registered handle.
/// The first successful respawn must register (not replace).
async fn build_harness_no_prior_handle(
    name: &str,
    settings: SupervisionSettings,
) -> (
    UpstreamSupervisor,
    Arc<UpstreamManager>,
    Arc<FakeConnector>,
    CancellationToken,
) {
    let config = UpstreamConfig::new(
        name,
        name.replace('-', "_"),
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    );
    let manager = Arc::new(UpstreamManager::new());
    // Born Respawning: matches what
    // `spawn_bootstrap_retry_supervisors` does at boot for configs
    // whose first connect failed.
    let health = Arc::new(UpstreamHealth::new());
    health.set_state(HealthState::Respawning);
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let ct = CancellationToken::new();
    let connector = Arc::new(FakeConnector::new());

    let supervisor = UpstreamSupervisor::new(
        config,
        settings,
        Arc::downgrade(&manager),
        health,
        bus,
        broker,
        ct.clone(),
        Box::new(FakeConnectorProxy(Arc::clone(&connector))),
    );
    (supervisor, manager, connector, ct)
}

#[tokio::test]
async fn respawning_registers_when_no_prior_handle_exists() {
    // Property: from Respawning with the manager carrying no prior
    // entry for this name, a successful connect must `register` the
    // new handle (not error with UnknownName). This is the
    // bootstrap-failed-retry happy path: gateway's
    // `spawn_bootstrap_retry_supervisors` spawns a born-Respawning
    // supervisor for each config whose first connect failed, and
    // the very next successful respawn becomes the upstream's first
    // appearance in the manager's catalog.
    let settings = SupervisionSettings {
        respawn_max_attempts: 3,
        respawn_backoff_base: Duration::from_millis(1),
        respawn_backoff_cap: Duration::from_millis(1),
        ..SupervisionSettings::default()
    };
    let (mut supervisor, manager, connector, _ct) =
        build_harness_no_prior_handle("late-boot", settings).await;

    // Pre-condition: nothing is registered.
    assert!(
        manager.handle_by_name("late-boot").await.is_none(),
        "harness invariant: no prior handle"
    );

    // Script one success against the supervisor's own config.
    let cfg = UpstreamConfig::new(
        "late-boot",
        "late_boot",
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    );
    let (_server_new, live_new) = fresh_live_upstream(&cfg).await;
    connector.enqueue_success(live_new);

    supervisor.tick().await;

    assert_eq!(
        connector.call_count(),
        1,
        "exactly one connect attempt must fire on the first Respawning tick"
    );
    let registered = manager
        .handle_by_name("late-boot")
        .await
        .expect("first successful respawn must register the handle");
    assert_eq!(
        registered.health().state(),
        HealthState::Healthy,
        "freshly-registered handle's health must be Healthy after respawn"
    );
}

#[tokio::test]
async fn respawning_no_prior_handle_failure_does_not_register() {
    // Property: a failed connect from a born-Respawning supervisor
    // must NOT register any handle in the manager. The supervisor
    // either retries (still Respawning, streak incremented) or
    // eventually goes Dead — but a phantom registration would leak
    // into `list-servers` as a healthy upstream that doesn't exist.
    let settings = SupervisionSettings {
        respawn_max_attempts: 3,
        respawn_backoff_base: Duration::from_millis(1),
        respawn_backoff_cap: Duration::from_millis(1),
        ..SupervisionSettings::default()
    };
    let (mut supervisor, manager, connector, _ct) =
        build_harness_no_prior_handle("never-up", settings).await;

    connector.enqueue_failure(FakeError::Connect("upstream unreachable".into()));
    supervisor.tick().await;

    assert_eq!(connector.call_count(), 1);
    assert!(
        manager.handle_by_name("never-up").await.is_none(),
        "a failed respawn must not register anything"
    );
}

#[tokio::test]
async fn respawning_no_prior_handle_full_cycle_eventually_registers() {
    // Property: the full born-Respawning → exhaust → Dead → rearm →
    // Respawning → success → register cycle works for a
    // bootstrap-failed config. This is the failure-then-recovery
    // path operators care about: an upstream that's broken for the
    // first few attempts of an episode, drops to Dead, sits for
    // dead_rearm_interval, and finally comes back online — the
    // supervisor must register the handle when it does.
    //
    // Drives the state machine directly via `tick().await` so the
    // test doesn't depend on real-time `dead_rearm_interval`. The
    // production timing is exercised by the live smoke test
    // separately; this is a regression guard against any future
    // change that breaks the state-transition wiring for the
    // no-prior-handle case.
    let settings = SupervisionSettings {
        respawn_max_attempts: 2,
        respawn_backoff_base: Duration::from_millis(1),
        respawn_backoff_cap: Duration::from_millis(1),
        ..SupervisionSettings::default()
    };
    let (mut supervisor, manager, connector, _ct) =
        build_harness_no_prior_handle("flaky-late-boot", settings).await;

    // Episode 1: two failures (= respawn_max_attempts) → supervisor
    // exhausts the episode and flips to Dead.
    connector.enqueue_failure(FakeError::Connect("not yet".into()));
    connector.enqueue_failure(FakeError::Connect("still not yet".into()));
    // Two Respawning ticks; on the second the supervisor sees
    // respawn_failure_streak >= max and flips to Dead.
    supervisor.tick().await;
    supervisor.tick().await;
    assert_eq!(connector.call_count(), 2);
    assert!(
        manager.handle_by_name("flaky-late-boot").await.is_none(),
        "no handle should be registered after the failure-only episode"
    );

    // Re-arm: one tick in Dead flips back to Respawning. The real
    // supervisor sleeps `dead_rearm_interval` between Dead ticks;
    // we just drive the tick directly.
    supervisor.tick().await;

    // Episode 2: scripted success on the first attempt.
    let cfg = UpstreamConfig::new(
        "flaky-late-boot",
        "flaky_late_boot",
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    );
    let (_server_new, live_new) = fresh_live_upstream(&cfg).await;
    connector.enqueue_success(live_new);

    supervisor.tick().await;

    assert_eq!(
        connector.call_count(),
        3,
        "two failed attempts in episode 1 plus one successful attempt in episode 2"
    );
    let registered = manager
        .handle_by_name("flaky-late-boot")
        .await
        .expect("eventual successful respawn must register the handle");
    assert_eq!(
        registered.health().state(),
        HealthState::Healthy,
        "freshly-registered handle's health must be Healthy after recovery"
    );
}

#[tokio::test]
async fn try_respawn_drops_live_upstream_on_post_connect_cancellation() {
    // Property: if the cancellation token fires after `connect`
    // returns successfully but before `register_or_replace` runs,
    // the supervisor must NOT register the handle (would leak a
    // child past `close_all_upstreams` because the registry is
    // already drained or about to be). The freshly-connected
    // `LiveUpstream` is dropped instead.
    let settings = SupervisionSettings {
        respawn_max_attempts: 3,
        respawn_backoff_base: Duration::from_millis(1),
        respawn_backoff_cap: Duration::from_millis(1),
        ..SupervisionSettings::default()
    };
    let (mut supervisor, manager, connector, ct) =
        build_harness_no_prior_handle("cancel-mid-respawn", settings).await;

    // Pre-cancel so the post-connect check fires.
    ct.cancel();

    // Script a success against the supervisor's own config so the
    // connector returns `Ok(live)`; the cancellation check after
    // `connect` should still cause the supervisor to drop it.
    let cfg = UpstreamConfig::new(
        "cancel-mid-respawn",
        "cancel_mid_respawn",
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    );
    let (_server_new, live_new) = fresh_live_upstream(&cfg).await;
    connector.enqueue_success(live_new);

    supervisor.tick().await;

    assert_eq!(
        connector.call_count(),
        1,
        "connect must still fire once — cancellation check is post-connect"
    );
    assert!(
        manager.handle_by_name("cancel-mid-respawn").await.is_none(),
        "post-connect cancellation must NOT register a phantom handle"
    );
}

#[tokio::test]
async fn supervisor_exits_on_cancellation() {
    // Property: cancelling the supervisor's token causes `run()` to
    // return promptly, even if the manager is still live.
    let settings = SupervisionSettings::default();
    let h = build_harness("cancelable", settings).await;

    let supervisor = h.supervisor;
    let _manager = h.manager;
    let ct = h.ct;
    let _connector = h.connector;
    let _server = h._server;

    let run_handle = tokio::spawn(async move { supervisor.run().await });
    ct.cancel();

    tokio::time::timeout(Duration::from_secs(10), run_handle)
        .await
        .expect("supervisor run did not exit within budget")
        .expect("supervisor task should not panic");
}
