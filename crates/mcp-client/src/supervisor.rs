// pattern: Imperative Shell
//
// Per-upstream liveness detection + auto-respawn.
//
// One [`UpstreamSupervisor`] task is spawned for each stdio upstream at
// gateway bootstrap. It observes the health record attached to the
// currently-registered handle, drives state transitions via
// [`UpstreamHealth::set_state`], and when the upstream is classified as
// sick, reconnects via the injected [`Connector`] and calls
// [`UpstreamManager::replace`] to swap in a fresh handle.
//
// ## Structural invariant (load-bearing)
//
// The supervisor struct must NOT store an [`Arc<UpstreamHandle>`]. It
// holds:
//
// - [`UpstreamConfig`] — so respawn knows what to reconnect.
// - [`Weak<UpstreamManager>`] — so lookups can find the current handle,
//   and so the supervisor gracefully exits once the manager is dropped.
// - [`Arc<UpstreamHealth>`] — the current generation's health record.
//   Replaced (via `health()` on the freshly-replaced handle) after
//   every successful respawn.
// - [`SupervisionSettings`] — resolved at spawn time.
// - [`Arc<UpstreamEventBus>`] + [`SharedSamplingBroker`] — needed by the
//   [`Connector`] to build fresh handles on respawn.
// - [`Box<dyn Connector>`] — the connect-on-respawn dep, trait-bounded
//   so tests can inject a fake without spawning real child processes.
// - [`CancellationToken`] — the supervisor's child of the gateway-wide
//   token, cancelled on shutdown.
//
// Transient handle access during one tick (via
// `manager.upgrade().handle_by_name(&name).await`) is permitted and
// necessary — pings and `is_transport_closed` probes both need a handle
// reference — but the Arc must not cross an `.await` that could panic
// without being locally bound. See
// `crates/gateway/tests/abnormal_exit_cleanup.rs` for the regression
// tests that will catch a structural drift here.
//
// ## State machine
//
// Shape (also drawn in `health.rs`'s module header):
//
// ```text
//     ┌─────────┐  K consecutive    ┌─────────┐
//     │ Healthy │─transport-level──▶│ Suspect │
//     └─────────┘   failures            └─────────┘
//          ▲     OR is_transport_closed   │
//          │                               │  Suspect → Unhealthy fires on ANY of:
//          │                               │   • is_transport_closed
//          │  Suspect → Healthy is owned   │   • K consecutive ping failures
//          │  by record_success on a       │   • consecutive_failures >= 2K
//          │  real call_tool / list_*      │     (real-traffic escalation —
//          │  success — pings alone do    │      the bridge is alive but
//          │  NOT promote.                  │      blackholing real RPCs)
//          │                               ▼
//          │                         ┌───────────┐
//          │                         │ Unhealthy │
//          │                         └───────────┘
//          │                              │
//          │                              ▼
//          │                         ┌────────────┐
//          │  replace + set_state(Healthy) ─────┐  │
//          ├──────────────────────────────────┬─┤  │ supervisor
//          │                                  │ │  ▼
//          │                             ┌────┴─┴────────┐
//          │                             │  Respawning   │
//          │                             └───────────────┘
//          │                                  │
//          │                                  │ N attempts failed
//          │                                  ▼
//          │                             ┌──────┐
//          │                             │ Dead │──rearm─▶ Respawning
//          │                             └──────┘
// ```
//
// ## Lazy active pings + promotion ownership
//
// Active pings only fire in the `Suspect` state. Steady-state Healthy
// traffic pays zero supervisor overhead; the supervisor's tick loop
// wakes, checks health, and sleeps again. Only when one of the
// real-traffic call sites — `UpstreamHandle::call_tool`,
// `list_tools`, `list_resources`, `list_resource_templates` (each
// timeout-bounded by `call_timeout` and routed through
// `health.record_failure(...)` on transport-level outcomes) — has
// recorded `unhealthy_threshold` consecutive transport-level failures
// (or `is_transport_closed()` returns true) does the supervisor start
// sending pings. The `list_*` methods carry their own timeout and
// health-record path for the same reason `call_tool` does: a wedged
// stdio bridge mid-reconnect can pin `tools/list` indefinitely without
// ever surfacing a failure, and an un-timeout-bounded call site is
// invisible to every gate below.
//
// **Promotion is owned by real-traffic outcomes.** Pings prove the
// *transport* is breathing, but a real tool call is the authoritative
// signal that the *upstream* is usable. The supervisor therefore does
// not promote Suspect → Healthy off ping success alone — that flip is
// owned exclusively by the atomic compare-exchange in
// [`UpstreamHealth::record_success`], which fires when a real
// `call_tool` / `list_tools` / `list_resources` /
// `list_resource_templates` succeeds.
//
// This rule exists because mcp-remote-style stdio bridges answer MCP
// `ping` *locally* in their own shim before the message ever crosses
// the underlying transport (e.g. an SSE bridge) to the remote
// upstream. A bridge whose transport is dead can still pass every
// ping the supervisor sends — yet a real RPC that has to actually
// traverse the bridge hangs forever. Promoting on ping success alone
// would oscillate the supervisor Healthy ↔ Suspect indefinitely (pings
// keep succeeding while real traffic keeps timing out) without ever
// escalating to respawn. Pinning promotion to real-traffic success
// closes that hole: the supervisor stays in Suspect until either real
// traffic recovers (CAS to Healthy via `record_success`) or the
// upstream breaks down further (escalation to Unhealthy, see below).
//
// ## Demotion paths in Suspect
//
// Three things can drive Suspect → Unhealthy → Respawning:
//
// 1. `is_transport_closed()` returns true — authoritative "the
//    transport is gone" signal.
// 2. **Ping failures**: `unhealthy_threshold` consecutive transport-
//    level ping failures (same knob the Healthy → Suspect transition
//    uses; one tunable for both probes). Catches a transport that
//    used to breathe and now doesn't.
// 3. **Real-traffic escalation**: `consecutive_failures` reaches
//    `2 * unhealthy_threshold`. Catches "ping is alive but real RPCs
//    keep timing out" — the rootly-bridge pathology. The factor of 2
//    is "the K that put us into Suspect, plus another K that piled
//    up after we landed there"; we want a sustained pattern, not a
//    single transient.
//
// ## Respawn
//
// On entering Respawning, the supervisor calls the injected
// [`Connector`] to build a fresh [`LiveUpstream`], wraps it in an
// [`UpstreamHandle`] with the same `call_timeout` the old handle had,
// and calls [`UpstreamManager::replace`] to swap it in atomically. On
// success, the supervisor updates its local `Arc<UpstreamHealth>`
// reference to point at the new handle's (fresh) record and returns
// to Healthy.
//
// On failure, the attempt counter increments and the supervisor sleeps
// for [`SupervisionSettings::respawn_delay`] before trying again, up to
// `respawn_max_attempts`. Exhausting the attempts moves to Dead; a
// `dead_rearm_interval` timer later, Dead flips back to Respawning.
//
// ## HTTP upstreams
//
// HTTP upstreams do not get a supervisor: rmcp's
// `StreamableHttpClientTransport` reconnects internally, and our health
// record still tracks consecutive failures so `call_tool` can gate on
// `TransientUnavailable` when the underlying transport gives up. The
// caller (`gateway::serve::bootstrap_upstreams`) is responsible for
// not spawning a supervisor for HTTP kinds.

use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::connect::LiveUpstream;
use crate::container_runtime::{RuntimeKind, detect_runtime_command};
use crate::container_runtime_probe::{PodmanMachineStatus, probe_podman_machine};
use crate::event_bus::UpstreamEventBus;
use crate::health::{HealthState, UpstreamHealth};
use crate::sampling::SharedSamplingBroker;
use crate::supervision::SupervisionSettings;
use crate::upstream::{UpstreamConfig, UpstreamError, UpstreamHandle, UpstreamManager};

/// Trait-bounded connect dependency. Production wires this to
/// [`crate::connect::connect`]; tests inject a [`FakeConnector`] (see
/// the tests module) that returns a scripted sequence of outcomes
/// without spawning real child processes.
///
/// The signature mirrors the production [`crate::connect::connect`]
/// function exactly, so wiring it up at the call site is one line.
// async_trait adds #[must_use] to an already must-use boxed Future.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait Connector: Send + Sync + 'static {
    async fn connect(
        &self,
        config: &UpstreamConfig,
        bus: &UpstreamEventBus,
        broker: &SharedSamplingBroker,
    ) -> Result<LiveUpstream, UpstreamError>;
}

/// Production connector: delegates to [`crate::connect::connect`]. A
/// zero-sized type — the supervisor holds `Box<dyn Connector>`, and a
/// boxed ZST is cheap.
pub struct RealConnector;

#[async_trait]
impl Connector for RealConnector {
    async fn connect(
        &self,
        config: &UpstreamConfig,
        bus: &UpstreamEventBus,
        broker: &SharedSamplingBroker,
    ) -> Result<LiveUpstream, UpstreamError> {
        crate::connect::connect(config, bus, broker).await
    }
}

/// Per-upstream liveness + respawn task.
///
/// Construct via [`UpstreamSupervisor::new`] and drive via
/// [`UpstreamSupervisor::run`]. Production spawns this on the gateway's
/// runtime; tests call it directly or drive a [`Self::tick`] invocation
/// at a time.
pub struct UpstreamSupervisor {
    config: UpstreamConfig,
    settings: SupervisionSettings,
    manager: Weak<UpstreamManager>,
    /// The currently-registered handle's health record. Updated after
    /// every successful `replace` so state reads go against the active
    /// generation — NEVER the displaced record.
    health: Arc<UpstreamHealth>,
    bus: Arc<UpstreamEventBus>,
    broker: SharedSamplingBroker,
    ct: CancellationToken,
    connector: Box<dyn Connector>,
    /// Consecutive ping failures observed in the Suspect state. Reset
    /// on successful ping or on any state transition out of Suspect.
    /// Private — the supervisor owns this, not the health record.
    ping_failure_streak: u32,
    /// Consecutive ping successes observed in the Suspect state.
    /// Maintained for observability only — this counter does NOT drive
    /// any flip. Promotion to Healthy is owned by
    /// [`UpstreamHealth::record_success`] (CAS Suspect → Healthy on a
    /// real call_tool / list_* success); the supervisor itself never
    /// flips Suspect → Healthy. Reset to 0 on any state transition out
    /// of Suspect or on a single ping failure.
    ping_success_streak: u32,
    /// How many respawn attempts in a row have failed in the current
    /// respawn episode. Reset after a successful respawn or after
    /// transitioning out of Respawning/Dead.
    respawn_failure_streak: u32,
}

impl UpstreamSupervisor {
    /// Build a supervisor. `health` should be the handle's current
    /// [`UpstreamHealth`] record (obtainable via
    /// [`UpstreamHandle::health`] on the freshly-registered handle).
    ///
    /// `connector` is trait-bounded for testability — production wires
    /// [`RealConnector`], which delegates to [`crate::connect::connect`].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: UpstreamConfig,
        settings: SupervisionSettings,
        manager: Weak<UpstreamManager>,
        health: Arc<UpstreamHealth>,
        bus: Arc<UpstreamEventBus>,
        broker: SharedSamplingBroker,
        ct: CancellationToken,
        connector: Box<dyn Connector>,
    ) -> Self {
        Self {
            config,
            settings,
            manager,
            health,
            bus,
            broker,
            ct,
            connector,
            ping_failure_streak: 0,
            ping_success_streak: 0,
            respawn_failure_streak: 0,
        }
    }

    /// Drive the supervisor loop to completion. Returns when the
    /// cancellation token fires or the [`UpstreamManager`] is dropped
    /// (the `Weak` fails to upgrade).
    ///
    /// Each iteration runs [`Self::tick`] and then sleeps for the
    /// state-appropriate interval. Sleeps are interruptible by the
    /// cancellation token so shutdown is prompt.
    pub async fn run(mut self) {
        info!(upstream = %self.config.name, "upstream supervisor starting");
        loop {
            if self.manager.strong_count() == 0 {
                info!(
                    upstream = %self.config.name,
                    "upstream manager dropped; supervisor exiting"
                );
                return;
            }

            let interval = self.tick_interval();
            self.tick().await;

            select! {
                biased;
                _ = self.ct.cancelled() => {
                    info!(
                        upstream = %self.config.name,
                        "supervisor cancelled; exiting"
                    );
                    return;
                }
                _ = tokio::time::sleep(interval) => {}
            }
        }
    }

    /// One iteration of the supervisor loop. Exposed for tests to drive
    /// state transitions deterministically without running `run()`'s
    /// sleep schedule.
    ///
    /// Reads the current [`HealthState`], observes inputs (counters,
    /// transport-closed probe), and either:
    ///
    /// - Leaves state unchanged (Healthy steady state, Respawning
    ///   mid-attempt, Dead pre-rearm).
    /// - Flips state via [`UpstreamHealth::set_state`].
    /// - Initiates a respawn (connect + replace).
    pub async fn tick(&mut self) {
        match self.health.state() {
            HealthState::Healthy => self.tick_healthy().await,
            HealthState::Suspect => self.tick_suspect().await,
            HealthState::Unhealthy => self.tick_unhealthy().await,
            HealthState::Respawning => self.tick_respawning().await,
            HealthState::Dead => self.tick_dead().await,
            // NeedsLogin is an OAuth-only state, and OAuth upstreams are
            // never supervised (rmcp handles their reconnect; see
            // `spawn_supervisors_for`). A supervised stdio upstream
            // therefore cannot legitimately be NeedsLogin. If we somehow
            // observe it (a corrupted state byte, or a future refactor
            // that routes OAuth through here), treat it like Dead —
            // slow-poll rather than busy-loop — instead of panicking and
            // taking the supervisor down.
            HealthState::NeedsLogin => self.tick_dead().await,
        }
    }

    /// Sleep interval to use AFTER the current tick, selected by
    /// current state.
    fn tick_interval(&self) -> Duration {
        match self.health.state() {
            HealthState::Healthy => self.settings.active_ping_interval,
            HealthState::Suspect => self.settings.active_ping_interval,
            // Unhealthy is a momentary transition state — the supervisor
            // flips to Respawning in tick_unhealthy and returns. A short
            // sleep here lets the next tick start the respawn promptly.
            HealthState::Unhealthy => Duration::from_millis(50),
            // Respawning also flows back on the next tick quickly;
            // between attempts the supervisor has already slept
            // respawn_delay inside tick_respawning itself.
            HealthState::Respawning => Duration::from_millis(50),
            HealthState::Dead => self.settings.dead_rearm_interval,
            // See `tick`: NeedsLogin shouldn't reach a supervisor; use the
            // same slow cadence as Dead defensively.
            HealthState::NeedsLogin => self.settings.dead_rearm_interval,
        }
    }

    /// In the Healthy state, the supervisor checks for signals that the
    /// upstream is becoming sick:
    ///
    /// - Consecutive transport-level `call_tool` failures have reached
    ///   the threshold → flip to Suspect (supervisor begins probing).
    /// - Consecutive self-reported errors (Error-level
    ///   `notifications/message` from the upstream) have reached the
    ///   same threshold → also flip to Suspect. This catches the case
    ///   where the upstream wraps its failure in a `Ok({error: "..."})`
    ///   payload — invisible to `consecutive_failures` because rmcp
    ///   sees a success, but loud via the log channel.
    /// - The transport is closed (the child exited or rmcp noticed the
    ///   HTTP stream died) → skip Suspect and go straight to Unhealthy,
    ///   since no amount of probing will bring a dead transport back.
    async fn tick_healthy(&mut self) {
        // Reset any stale counters from a prior episode — they don't
        // apply in Healthy steady state and their presence would be
        // confusing in tracing output.
        self.ping_failure_streak = 0;
        self.ping_success_streak = 0;
        self.respawn_failure_streak = 0;

        if self.transport_closed().await {
            self.flip(
                HealthState::Healthy,
                HealthState::Unhealthy,
                "transport closed",
            );
            return;
        }
        // Either signal can independently drive Healthy → Suspect.
        // `consecutive_failures` catches the classic "transport keeps
        // erroring" case; `consecutive_self_reported_errors` catches
        // the "upstream returns Ok(_) with an error payload while also
        // logging the cause" case (motivating bug: dash-mcp wrapping
        // its inability to reach Dash.app's HTTP API in a successful
        // tool result — every call looked fine to rmcp, only the
        // side-channel log told the truth). Each maps to its own
        // counter; the supervisor takes the worse of the two.
        let transport_failures = self.health.consecutive_failures();
        let self_reported = self.health.consecutive_self_reported_errors();
        let worst = transport_failures.max(self_reported);
        if worst >= self.settings.unhealthy_threshold {
            let trigger = if self_reported >= transport_failures {
                "self-reported"
            } else {
                "transport-level"
            };
            self.flip(
                HealthState::Healthy,
                HealthState::Suspect,
                &format!(
                    "{worst} consecutive {trigger} failures (threshold: {}, \
                     transport_failures={transport_failures}, \
                     self_reported={self_reported})",
                    self.settings.unhealthy_threshold
                ),
            );
        }
    }

    /// In Suspect, the supervisor actively probes with pings. Pings are
    /// **demotion-only evidence**: they can drive escalation to
    /// Unhealthy, but they do not promote back to Healthy. Promotion is
    /// owned exclusively by [`UpstreamHealth::record_success`] on a real
    /// `call_tool` / `list_tools` / `list_resources` /
    /// `list_resource_templates` outcome — only a real-traffic success
    /// proves the upstream is *usable*, not just breathing.
    ///
    /// Per tick:
    ///
    /// - Transport closed: flip straight to Unhealthy. The transport is
    ///   the authoritative signal.
    /// - Real-traffic escalation: if `consecutive_failures` has reached
    ///   `2 * unhealthy_threshold`, flip to Unhealthy regardless of ping
    ///   outcome. This catches a bridge that answers `ping` locally
    ///   while blackholing real RPCs (see the "Lazy active pings" module
    ///   section above for the mechanism).
    /// - Ping success: keeps the supervisor in Suspect. We do NOT flip
    ///   back to Healthy on K pings alone. If a concurrent real call
    ///   succeeds, `record_success`'s atomic CAS auto-promotes Suspect
    ///   → Healthy and `tick_healthy` takes over on the next tick.
    /// - Ping failure (transport-level): increment the failure streak;
    ///   at `unhealthy_threshold` flip to Unhealthy and trigger respawn.
    /// - Ping failure (peer-level): ignored — peer McpErrors aren't
    ///   indicative of transport sickness.
    async fn tick_suspect(&mut self) {
        if self.transport_closed().await {
            self.ping_failure_streak = 0;
            self.ping_success_streak = 0;
            self.flip(
                HealthState::Suspect,
                HealthState::Unhealthy,
                "transport closed",
            );
            return;
        }

        // Real-traffic escalation: if calls (call_tool / list_*) have
        // continued to fail while we've been in Suspect, escalate.
        // We use 2*threshold (= the K that put us into Suspect, plus
        // another K that piled up after) so this fires only on a
        // sustained pattern, not a transient flaky transport.
        //
        // `consecutive_failures` is monotonic until a `record_success`
        // resets it; the supervisor doesn't reset it on the
        // Healthy → Suspect flip, which is what makes "fresh failures
        // since entering Suspect" observable here as a strict increase
        // past the entry value.
        //
        // `consecutive_self_reported_errors` (driven by Error-level
        // `notifications/message`) is monotonic until decay or respawn.
        // It survives `record_success` precisely so the misleading-
        // success-envelope case can still escalate. Either counter
        // hitting 2*threshold flips us to Unhealthy.
        let transport_failures = self.health.consecutive_failures();
        let self_reported = self.health.consecutive_self_reported_errors();
        let worst = transport_failures.max(self_reported);
        let escalation_threshold = self.settings.unhealthy_threshold.saturating_mul(2);
        if worst >= escalation_threshold {
            self.ping_failure_streak = 0;
            self.ping_success_streak = 0;
            let trigger = if self_reported >= transport_failures {
                "self-reported"
            } else {
                "transport-level"
            };
            self.flip(
                HealthState::Suspect,
                HealthState::Unhealthy,
                &format!(
                    "{worst} consecutive {trigger} failures \
                     (escalation threshold in Suspect: {escalation_threshold}, \
                     transport_failures={transport_failures}, \
                     self_reported={self_reported})"
                ),
            );
            return;
        }

        let ping_outcome = self.ping_once().await;
        match ping_outcome {
            Ok(()) => {
                // Ping is alive — clear the ping-failure streak so a
                // single bad ping doesn't accumulate forever, but do
                // NOT flip to Healthy. Promotion requires real-traffic
                // evidence (record_success on a successful call_tool /
                // list_*).
                self.ping_failure_streak = 0;
                self.ping_success_streak = self.ping_success_streak.saturating_add(1);
                debug!(
                    upstream = %self.config.name,
                    streak = self.ping_success_streak,
                    "ping ok in Suspect; transport breathing but staying \
                     Suspect until real-traffic success"
                );
            }
            Err(e) if e.is_transport_level() => {
                self.ping_success_streak = 0;
                self.ping_failure_streak = self.ping_failure_streak.saturating_add(1);
                debug!(
                    upstream = %self.config.name,
                    streak = self.ping_failure_streak,
                    error = %e,
                    "ping failed in Suspect"
                );
                if self.ping_failure_streak >= self.settings.unhealthy_threshold {
                    self.ping_failure_streak = 0;
                    self.flip(
                        HealthState::Suspect,
                        HealthState::Unhealthy,
                        &format!(
                            "{} consecutive ping failures (threshold: {})",
                            self.settings.unhealthy_threshold, self.settings.unhealthy_threshold
                        ),
                    );
                }
            }
            Err(e) => {
                // Non-transport-level ping failures (peer-returned
                // McpError, unexpected response shape) are unusual but
                // not indicative of upstream sickness. Log and move on
                // without mutating counters.
                debug!(
                    upstream = %self.config.name,
                    error = %e,
                    "ping returned a non-transport-level error; ignoring"
                );
            }
        }
    }

    /// Unhealthy is a transient flag: the supervisor immediately flips
    /// to Respawning. The separate state exists for observability —
    /// operators see "Unhealthy" when the threshold tripped and
    /// "Respawning" when the attempt started.
    async fn tick_unhealthy(&mut self) {
        self.flip(
            HealthState::Unhealthy,
            HealthState::Respawning,
            "beginning respawn",
        );
    }

    /// In Respawning, the supervisor attempts one `connect + replace`
    /// cycle per tick. Each attempt runs inside a wait for
    /// `respawn_delay(attempt)` so the sequence is exponential-backoff
    /// on failure but fires immediately on the first attempt of an
    /// episode.
    async fn tick_respawning(&mut self) {
        // The first attempt fires without delay; subsequent attempts
        // wait for respawn_delay(n).
        let attempt = self.respawn_failure_streak.saturating_add(1);
        if attempt > 1 {
            let delay = self.settings.respawn_delay(attempt);
            debug!(
                upstream = %self.config.name,
                attempt,
                delay_ms = delay.as_millis() as u64,
                "waiting before next respawn attempt"
            );
            select! {
                biased;
                _ = self.ct.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }

        info!(
            upstream = %self.config.name,
            attempt,
            max = self.settings.respawn_max_attempts,
            "attempting respawn"
        );

        match self.try_respawn().await {
            Ok(()) => {
                self.respawn_failure_streak = 0;
                info!(
                    upstream = %self.config.name,
                    "respawn succeeded"
                );
            }
            Err(e) => {
                if let Some(manager) = self.manager.upgrade() {
                    manager
                        .record_connection_error(&self.config, &e.to_string())
                        .await;
                }
                self.respawn_failure_streak = attempt;
                warn!(
                    upstream = %self.config.name,
                    attempt,
                    max = self.settings.respawn_max_attempts,
                    error = %e,
                    "respawn attempt failed"
                );
                if self.respawn_failure_streak >= self.settings.respawn_max_attempts {
                    // Dead transitions are the single "things aren't
                    // recovering on their own" signal. Log-level error!
                    // surfaces them distinctly from the routine
                    // state-transition stream (which is info!) so
                    // operators filtering on severity see them.
                    error!(
                        upstream = %self.config.name,
                        attempts = self.settings.respawn_max_attempts,
                        "upstream respawn attempts exhausted; marking Dead (will retry after dead_rearm_interval)"
                    );
                    // Container-runtime-aware root-cause hint: if this
                    // is a containerized upstream and the resolved
                    // runtime is Podman, probe `podman machine list` so
                    // the log names the most common macOS recovery
                    // step ("start your podman machine") instead of
                    // just shouting "Dead" every 60 s.
                    self.maybe_log_podman_machine_diagnostic().await;
                    self.flip(
                        HealthState::Respawning,
                        HealthState::Dead,
                        &format!(
                            "{} respawn attempts exhausted",
                            self.settings.respawn_max_attempts
                        ),
                    );
                    // Reset the streak so when the rearm tick flips us
                    // back to Respawning, we start a fresh episode.
                    self.respawn_failure_streak = 0;
                }
            }
        }
    }

    /// In Dead, a single tick re-arms by flipping back to Respawning.
    /// The enclosing loop's `tick_interval` schedules the next wake-up
    /// at `dead_rearm_interval`, so this tick fires once per interval.
    async fn tick_dead(&mut self) {
        self.flip(
            HealthState::Dead,
            HealthState::Respawning,
            "dead rearm tick",
        );
    }

    /// Attempt one `connect + replace` cycle. On success, update the
    /// local health reference to the new handle's record and flip state
    /// to Healthy. On failure, return the error for the caller to count.
    async fn try_respawn(&mut self) -> Result<(), RespawnError> {
        // Pull the current call_timeout off the registered handle so
        // the replacement honours the operator's configured override.
        // Transient borrow — dropped before the connect() call below.
        let prior_call_timeout = {
            let Some(manager) = self.manager.upgrade() else {
                return Err(RespawnError::ManagerDropped);
            };
            match manager.handle_by_name(&self.config.name).await {
                Some(handle) => handle.call_timeout(),
                None => self.settings.call_timeout,
            }
        };

        let live = self
            .connector
            .connect(&self.config, &self.bus, &self.broker)
            .await
            .map_err(RespawnError::Connect)?;

        // Defense in depth against the
        // "successful-respawn-during-shutdown-leaks-handle" race: if
        // cancellation fired while we were inside `connect`, drop the
        // freshly-connected `live` here instead of registering it into
        // a manager that's about to be (or already has been) drained.
        // `LiveUpstream`'s `Drop` reaps the child best-effort, same as
        // the abort-mid-flight path. Without this check, a respawn
        // whose `connect` lands between `close_all_upstreams` draining
        // the registry and this supervisor's next post-tick
        // cancellation check would `register` a phantom handle that
        // never gets `close()`d. Shutdown-ordering in `serve_on` is
        // the primary safeguard; this is the supervisor-side belt
        // for any future caller that doesn't enforce the same order.
        if self.ct.is_cancelled() {
            debug!(
                upstream = %self.config.name,
                "respawn connect completed but supervisor was cancelled; \
                 dropping fresh upstream without registering"
            );
            drop(live);
            return Err(RespawnError::Cancelled);
        }

        let new_handle = Arc::new(UpstreamHandle::new_with_call_timeout(
            self.config.clone(),
            live,
            prior_call_timeout,
        ));
        // Grab the new health record BEFORE `register_or_replace`
        // consumes/shares the Arc so we can swap our local reference
        // even on success.
        let new_health = new_handle.health();

        let Some(manager) = self.manager.upgrade() else {
            return Err(RespawnError::ManagerDropped);
        };

        // `register_or_replace` is atomic — no TOCTOU window between
        // the "is name registered?" check and the insertion. Returns
        // `Some(displaced)` on the live-swap path (an existing handle
        // got swapped out and the caller closes it off-task), `None`
        // on the first-respawn-after-bootstrap-failure path.
        let displaced = match manager
            .register_or_replace(&self.config.name, Arc::clone(&new_handle))
            .await
        {
            Ok(Some(displaced)) => Some(displaced),
            Ok(None) => {
                info!(
                    upstream = %self.config.name,
                    "respawn registered a previously-unregistered upstream \
                     (bootstrap-failed config came back online)"
                );
                None
            }
            Err(e) => return Err(RespawnError::Replace(e)),
        };
        // A respawn can make a previously absent resource catalog reachable
        // after downstream clients have already listed resources. Broadcast
        // the lifecycle change so they refresh instead of keeping an empty
        // first snapshot forever.
        self.bus.publish(crate::UpstreamEvent::ResourceListChanged {
            upstream: self.config.name.clone(),
        });

        // Swap in the new record — future `state()` reads go against
        // the fresh generation. The displaced handle's record stays
        // alive via the returned Arc for any last logging.
        new_health.set_state(HealthState::Healthy);
        self.health = new_health;

        // Best-effort: close the displaced handle off-task. We don't
        // hold it across an await on our own critical path; spawn a
        // detached task so the child process reap doesn't block the
        // supervisor. The task holds the displaced Arc briefly and
        // drops it after close() — structural invariant preserved
        // because this is a fresh spawn, not a supervisor-owned ref.
        if let Some(displaced) = displaced {
            tokio::spawn(async move {
                match Arc::try_unwrap(displaced) {
                    Ok(handle) => {
                        if let Err(e) = handle.close().await {
                            warn!(error = %e, "displaced handle close failed");
                        }
                    }
                    Err(_still_shared) => {
                        // A concurrent call_tool is still borrowing the
                        // Arc. Dropping this Arc makes the strong count
                        // drop by one; the last remaining borrower's drop
                        // will kick the LiveUpstream's Drop path, which
                        // fire-and-forgets the child kill. This is the
                        // same contract the abort-path test pins.
                        debug!("displaced handle still shared; deferring close to Drop");
                    }
                }
            });
        }

        Ok(())
    }

    /// Transient `is_transport_closed()` probe against the currently
    /// registered handle. If the handle is absent (e.g. manager mid-
    /// drain at shutdown) or the manager has been dropped, returns
    /// false — the supervisor treats "can't probe" as "no evidence of
    /// closure" rather than triggering a spurious respawn.
    async fn transport_closed(&self) -> bool {
        let Some(manager) = self.manager.upgrade() else {
            return false;
        };
        match manager.handle_by_name(&self.config.name).await {
            Some(handle) => handle.is_transport_closed(),
            None => false,
        }
    }

    /// One ping against the currently registered handle. Transient
    /// handle borrow; the handle Arc is dropped before this function
    /// returns, preserving the "supervisor never stores a handle"
    /// invariant across awaits.
    async fn ping_once(&self) -> Result<(), UpstreamError> {
        let Some(manager) = self.manager.upgrade() else {
            return Err(UpstreamError::NotFound(self.config.name.clone()));
        };
        let Some(handle) = manager.handle_by_name(&self.config.name).await else {
            return Err(UpstreamError::NotFound(self.config.name.clone()));
        };
        handle.ping(self.settings.active_ping_timeout).await
    }

    /// Diagnostic hook fired once per Dead-transition for a
    /// containerized Podman upstream: probe `podman machine list`
    /// and, if no machine is running, log an `error!` pointing at
    /// the most likely operator fix. Silent no-op otherwise.
    ///
    /// Only fires for configs whose command is podman —
    /// Docker has no `machine` subcommand, and non-containerized
    /// upstreams would get a confusing unrelated diagnostic.
    ///
    /// Spawn failures / timeouts inside the probe itself are logged
    /// but don't propagate; this is a diagnostic aid, not a gate on
    /// the Dead transition.
    async fn maybe_log_podman_machine_diagnostic(&self) {
        let Some(configured) = detect_runtime_command(&self.config) else {
            return;
        };
        if configured != RuntimeKind::Podman {
            return;
        }

        match probe_podman_machine().await {
            PodmanMachineStatus::NoneRunning => {
                error!(
                    upstream = %self.config.name,
                    "no podman machine is Running — respawn failures are likely due to a stopped VM; \
                     run `podman machine start` to recover"
                );
            }
            PodmanMachineStatus::NoMachinesConfigured => {
                // On Linux this is normal (podman runs rootless on
                // the host, no VM). On macOS it means the user
                // never ran `podman machine init`. We can't tell
                // the two hosts apart from inside this code
                // without OS detection, so log at info rather than
                // error and let the operator judge.
                info!(
                    upstream = %self.config.name,
                    "no podman machines configured; on macOS try `podman machine init && podman machine start`"
                );
            }
            PodmanMachineStatus::Running => {
                debug!(
                    upstream = %self.config.name,
                    "podman machine probe: a machine is Running (respawn failures have another cause)"
                );
            }
            PodmanMachineStatus::ParseFailed(e) => {
                warn!(
                    upstream = %self.config.name,
                    error = %e,
                    "podman machine probe: unexpected output shape"
                );
            }
            PodmanMachineStatus::ProbeFailed(e) => {
                warn!(
                    upstream = %self.config.name,
                    error = %e,
                    "podman machine probe: runtime CLI error"
                );
            }
        }
    }

    /// Flip from `from` to `to` with an info-level log. Used for every
    /// observable state transition so `tail -f` on the gateway log
    /// shows the supervisor's decisions directly.
    fn flip(&self, from: HealthState, to: HealthState, reason: &str) {
        info!(
            upstream = %self.config.name,
            from = ?from,
            to = ?to,
            reason = %reason,
            "supervisor state transition"
        );
        self.health.set_state(to);
    }
}

/// Internal error type for one respawn attempt. Not exposed publicly
/// — the supervisor logs and counts these, it doesn't surface them.
#[derive(Debug, thiserror::Error)]
enum RespawnError {
    #[error("upstream manager dropped mid-respawn")]
    ManagerDropped,
    #[error("connect failed: {0}")]
    Connect(#[source] UpstreamError),
    #[error("replace failed: {0}")]
    Replace(#[source] crate::upstream::ReplaceError),
    /// Cancellation token fired during the connect window. Distinct
    /// from `Connect` (which would mean the upstream itself failed) —
    /// we don't want a shutdown to look like a respawn failure for
    /// streak-counting purposes. The caller's `tick_respawning` treats
    /// this the same as a connect failure for retry bookkeeping but
    /// the underlying log line distinguishes them.
    #[error("cancelled mid-respawn after successful connect")]
    Cancelled,
}

#[cfg(test)]
#[path = "supervisor_tests.rs"]
mod tests;
