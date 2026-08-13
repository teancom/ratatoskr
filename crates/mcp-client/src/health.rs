// pattern: Functional Core
//
// Per-upstream health state: a small bundle of atomics describing "how
// is this connection looking?" The structure is deliberately pure —
// it records outcomes, exposes snapshots, and proposes state transitions,
// but it does NOT drive respawn, issue pings, or talk to the event bus.
// Those are the `UpstreamSupervisor`'s concerns (see
// `gateway::upstream_supervisor`).
//
// ## Why atomics, not a single `Mutex<State>`
//
// `UpstreamHandle::call_tool` updates the counter on every outcome — a
// mutex here would serialise every concurrent call through one lock
// for the duration of the update. Atomics let N in-flight callers all
// bump `consecutive_failures` without contending beyond the cache
// line. The supervisor's read path is equally hot (polled on every
// tick of its state-machine loop), and a read against `AtomicU32` is
// a plain load.
//
// The consistency story: each atomic field is individually
// linearisable; snapshots are *not* a consistent cross-field read.
// That's fine because the supervisor's decision logic is tolerant of
// slightly-stale combinations — see `HealthSnapshot`'s docs for the
// contract.
//
// ## State machine
//
// ```text
//     ┌─────────┐   K consecutive    ┌─────────┐
//     │ Healthy │──transport-level──▶│ Suspect │
//     └─────────┘     failures       └─────────┘
//          ▲                              │
//          │ record_success on a real     │ Suspect → Unhealthy on ANY of:
//          │ call_tool / list_* outcome   │   • is_transport_closed
//          │ (atomic CAS in this module — │   • K consecutive ping failures
//          │  the only Suspect → Healthy  │   • consecutive_failures >= 2K
//          │  path; pings alone do NOT    │     (real-traffic escalation)
//          │  promote; see supervisor.rs) │
//          │                              ▼
//          │                         ┌───────────┐
//          │                         │ Unhealthy │
//          │                         └───────────┘
//          │                              │
//          │ replace() with fresh handle  │ supervisor
//          │ (handle swap in manager)     │ initiates
//          │                              ▼
//          │                         ┌────────────┐
//          └─────────────────────────│ Respawning │
//                                    └────────────┘
//                                         │
//                                         │ N respawn
//                                         │ attempts failed
//                                         ▼
//                                    ┌──────┐
//                                    │ Dead │──periodic re-arm──▶ Respawning
//                                    └──────┘
// ```
//
// The *data* structure here exposes the state and the counters; the
// *process* that drives transitions lives in the supervisor. Splitting
// the two keeps this module pure (functional-core) and the supervisor
// testable via state snapshots.

use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A snapshot of one upstream's health at an instant. Each field comes
/// from its own atomic load; there is no cross-field consistency
/// guarantee (a snapshot may report `state == Healthy` with a recent
/// `last_failure_at`, if another task flipped the state between the
/// two loads). Callers that need atomicity across fields should hold
/// an external lock — the supervisor doesn't need it, since its
/// decision logic is monotonic in `consecutive_failures` and tolerant
/// of a one-tick lag in `state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthSnapshot {
    pub state: HealthState,
    pub consecutive_failures: u32,
    pub last_failure_kind: FailureKind,
    /// Wall-clock of the most recent success, or `None` if the handle
    /// has never recorded one. Used by the supervisor to decide
    /// whether a Dead upstream has been "quiet" long enough to re-arm.
    pub last_success_at: Option<SystemTime>,
    /// Wall-clock of the most recent failure, or `None`. Used for
    /// operator-facing status output.
    pub last_failure_at: Option<SystemTime>,
    /// Streak counter for self-reported upstream errors
    /// (`FailureKind::UpstreamSelfReportedError`). Independent of
    /// `consecutive_failures`; see `record_self_reported_error`.
    pub consecutive_self_reported_errors: u32,
}

/// High-level operational state. Encoded as `u8` under the hood so it
/// fits in an atomic, but callers interact with the enum for
/// exhaustiveness.
///
/// The numeric values are stable and ordered: `Healthy < Suspect <
/// Unhealthy < Respawning < Dead`. The ordering lets `call_tool` gate
/// via `state >= Unhealthy` without importing a match arm — see
/// `UpstreamHandle::call_tool`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum HealthState {
    /// Steady state. Calls dispatch normally; counters all zero or
    /// resetting on success.
    Healthy = 0,
    /// K consecutive transport-level failures have been recorded. The
    /// supervisor is probing with pings; calls still dispatch to the
    /// underlying peer (so a brief transient doesn't block legitimate
    /// traffic) but the supervisor is watching closely.
    Suspect = 1,
    /// The upstream has been classified as unavailable. `call_tool`
    /// returns `UpstreamError::TransientUnavailable` without dispatching.
    /// The supervisor is about to respawn.
    Unhealthy = 2,
    /// The supervisor is mid-respawn. Same call-gating behaviour as
    /// `Unhealthy`; split out so observability can show "we're actively
    /// trying" vs "we've given up waiting and are about to give up
    /// entirely."
    Respawning = 3,
    /// The supervisor exhausted its respawn attempts. Stays here until
    /// a periodic re-arm tick flips it back to Respawning. Calls
    /// continue to fail fast with `TransientUnavailable` so the agent
    /// sees a clear "this upstream is down" signal rather than hanging.
    Dead = 4,
    /// An OAuth upstream's credentials can no longer authenticate — the
    /// refresh token is dead or absent, so silent refresh can't recover
    /// it. ONLY OAuth (`http-oauth`) upstreams ever enter this state, and
    /// they reach it one of two ways:
    ///   - at connect time, when the handshake fails with an auth error
    ///     (handled in `connect`/the bootstrap-retry loop), or
    ///   - mid-life, when a `call_tool` against a previously-healthy
    ///     upstream comes back with an auth failure (handled in
    ///     `UpstreamHandle::call_tool`).
    ///
    /// Unlike `Dead`, this is **not** a transient-outage state the
    /// gateway can wait out — it requires a human to re-run
    /// `rato login <name>`. It still gates calls (a call
    /// would just 401 again), but renders distinctly in `list-servers`
    /// with the exact re-login command so the fix is legible rather than
    /// buried in logs. The operator's `login` sends SIGHUP, which
    /// reconnects the upstream and clears this state.
    ///
    /// Appended after `Dead` (discriminant 5) deliberately: the numeric
    /// ordering `Healthy < … < Dead` is pinned by tests and documented as
    /// stable, so a new variant goes at the end rather than in the
    /// middle. Call-gating is driven by `gates_calls()` (a `matches!`),
    /// not by the numeric order, so placement at the end is purely
    /// cosmetic for the ordering and doesn't affect gating semantics.
    NeedsLogin = 5,
}

impl HealthState {
    /// True when `call_tool` should gate (return TransientUnavailable
    /// without dispatching). Suspect is excluded: we still try to call
    /// while probing.
    pub fn gates_calls(self) -> bool {
        matches!(
            self,
            HealthState::Unhealthy
                | HealthState::Respawning
                | HealthState::Dead
                | HealthState::NeedsLogin
        )
    }
}

impl From<HealthState> for u8 {
    fn from(s: HealthState) -> Self {
        s as u8
    }
}

impl TryFrom<u8> for HealthState {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(HealthState::Healthy),
            1 => Ok(HealthState::Suspect),
            2 => Ok(HealthState::Unhealthy),
            3 => Ok(HealthState::Respawning),
            4 => Ok(HealthState::Dead),
            5 => Ok(HealthState::NeedsLogin),
            other => Err(other),
        }
    }
}

/// What kind of failure most recently hit this upstream. Distinguishes
/// "transport is dead" (CallTimeout, TransportClosed) from "peer said
/// no" (McpError) so the supervisor can choose whether to respawn.
///
/// `None` is the default zero-valued variant — produced for a handle
/// that has never recorded a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FailureKind {
    /// No failure recorded yet.
    None = 0,
    /// Our own `tokio::time::timeout` wrapper fired.
    CallTimeout = 1,
    /// rmcp reported the transport is closed (child exited,
    /// HTTP stream terminated).
    TransportClosed = 2,
    /// rmcp's own request-level timeout fired (not currently wired in
    /// our call sites, but surfacing it keeps the taxonomy closed).
    RmcpTimeout = 3,
    /// `send` on the transport failed before rmcp could wait for a
    /// response (e.g. EPIPE on an stdio pipe that just closed).
    TransportSend = 4,
    /// Peer returned a JSON-RPC error — tool-shape noise, not
    /// upstream sickness. Recorded for observability but doesn't
    /// count toward `consecutive_failures`.
    PeerMcpError = 5,
    /// rmcp's `UnexpectedResponse` residual case.
    UnexpectedResponse = 6,
    /// Upstream emitted an `Error`-level (or worse) `notifications/message`
    /// log notification. The MCP call itself succeeded at the JSON-RPC
    /// layer, but the upstream is self-reporting that something is wrong —
    /// typically a tool returning a `{ "error": "..." }` envelope while
    /// also logging the underlying cause. The dash MCP server's "Health
    /// check failed for http://127.0.0.1:53545: unknown encoding: idna"
    /// is the motivating case: every `call_tool` returned `outcome=ok`,
    /// hiding the rot from the supervisor.
    ///
    /// Routed through its own counter, `consecutive_self_reported_errors`,
    /// which `record_success` does NOT zero — the "successful" Ok payload
    /// would otherwise immediately whitewash this signal.
    /// Has no corresponding `UpstreamError` variant — we never see
    /// this as a returned `Result` because the affected call returned
    /// `Ok(_)` with a misleading payload. The supervisor reads
    /// `max(consecutive_failures, consecutive_self_reported_errors)`
    /// when deciding demotion, so either signal can independently
    /// drive a respawn.
    UpstreamSelfReportedError = 7,
    /// An OAuth upstream's request failed because the credentials can no
    /// longer authenticate (refresh token dead/absent → rmcp's
    /// `AuthClient` surfaces an auth error). This is the FailureKind
    /// recorded alongside the `HealthState::NeedsLogin` transition.
    ///
    /// Explicitly **not** transport-level: it must NOT feed
    /// `consecutive_failures` (which drives Suspect/respawn for transient
    /// blips), because re-dialing won't help — only a human `login`
    /// will. It's a terminal-until-operator-action signal, recorded for
    /// observability and to stamp `last_failure_kind` so the snapshot and
    /// `list-servers` can explain *why* the upstream is in `NeedsLogin`.
    AuthExpired = 8,
}

impl From<FailureKind> for u8 {
    fn from(k: FailureKind) -> Self {
        k as u8
    }
}

impl TryFrom<u8> for FailureKind {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(FailureKind::None),
            1 => Ok(FailureKind::CallTimeout),
            2 => Ok(FailureKind::TransportClosed),
            3 => Ok(FailureKind::RmcpTimeout),
            4 => Ok(FailureKind::TransportSend),
            5 => Ok(FailureKind::PeerMcpError),
            6 => Ok(FailureKind::UnexpectedResponse),
            7 => Ok(FailureKind::UpstreamSelfReportedError),
            8 => Ok(FailureKind::AuthExpired),
            other => Err(other),
        }
    }
}

impl FailureKind {
    /// True for kinds that indicate the transport layer is sick (as
    /// opposed to a peer-returned tool-shape error). Mirrors
    /// `UpstreamError::is_transport_level`; the two must stay in sync
    /// because `UpstreamHandle::call_tool` classifies once and feeds
    /// both sides.
    ///
    /// `UpstreamSelfReportedError` is **not** transport-level here even
    /// though it's a sickness signal: it lives on its own counter
    /// (`consecutive_self_reported_errors`) that `record_success` does
    /// not zero, because a "successful" tool call that wraps an error
    /// in its payload mustn't be allowed to whitewash the
    /// side-channel signal. Routing it through `consecutive_failures`
    /// would do exactly that. See `record_self_reported_error`.
    pub fn is_transport_level(self) -> bool {
        matches!(
            self,
            FailureKind::CallTimeout
                | FailureKind::TransportClosed
                | FailureKind::RmcpTimeout
                | FailureKind::TransportSend
        )
    }
}

/// How long the `consecutive_self_reported_errors` streak persists
/// without a fresh signal. After this window elapses, the next
/// self-reported error starts a new streak (counter = 1) rather than
/// extending the old one.
///
/// Picked to be:
///   - Long enough that a slow-drip sick upstream (one Error log per
///     minute) can accumulate to the demotion threshold.
///   - Short enough that a single transient blip an hour ago doesn't
///     combine with a fresh one to trigger spurious demotion.
///
/// Unlike `consecutive_failures`, this counter is NOT zeroed by
/// `record_success` — the success-envelope-with-error-payload case is
/// exactly the pathology that motivated this whole signal class, so
/// letting a fake success clear it would defeat the purpose. Decay is
/// the only natural recovery; respawn (which mints a fresh
/// `UpstreamHealth`) is the hard reset.
pub const SELF_REPORT_DECAY: Duration = Duration::from_secs(60);

/// Per-upstream health record. Designed to be wrapped in an `Arc` and
/// shared between `UpstreamHandle` (which writes) and the supervisor
/// (which reads and drives state transitions).
///
/// Every field is an atomic; snapshots via `snapshot()` are eventually
/// consistent across the struct. See the module-level note on why
/// this is the right trade-off for the hot-path on `call_tool`.
pub struct UpstreamHealth {
    state: AtomicU8,
    consecutive_failures: AtomicU32,
    last_failure_kind: AtomicU8,
    /// Unix microseconds of the last success. `0` means "never".
    /// Stored as i64 (not u64) so `SystemTime::now().duration_since`
    /// errors (clock went backwards) can round to 0 without wrapping.
    last_success_micros: AtomicI64,
    /// Unix microseconds of the last failure. `0` means "never".
    last_failure_micros: AtomicI64,
    /// Streak counter for `FailureKind::UpstreamSelfReportedError`.
    /// Lives separately from `consecutive_failures` because
    /// `record_success` MUST NOT zero it — the motivating bug is an
    /// upstream returning `Ok(_)` with an error payload, which makes
    /// every success look real to rmcp. See `record_self_reported_error`
    /// and `SELF_REPORT_DECAY`.
    consecutive_self_reported_errors: AtomicU32,
    /// Unix microseconds of the last self-reported error.
    /// `0` means "never". Used by `record_self_reported_error` to
    /// implement decay: if more than `SELF_REPORT_DECAY` has elapsed
    /// since the last hit, the next hit starts a fresh streak rather
    /// than extending a stale one.
    last_self_reported_micros: AtomicI64,
}

impl std::fmt::Debug for UpstreamHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamHealth")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl Default for UpstreamHealth {
    fn default() -> Self {
        Self::new()
    }
}

impl UpstreamHealth {
    /// Fresh record: `Healthy`, zero counters, no recorded events.
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(HealthState::Healthy.into()),
            consecutive_failures: AtomicU32::new(0),
            last_failure_kind: AtomicU8::new(FailureKind::None.into()),
            last_success_micros: AtomicI64::new(0),
            last_failure_micros: AtomicI64::new(0),
            consecutive_self_reported_errors: AtomicU32::new(0),
            last_self_reported_micros: AtomicI64::new(0),
        }
    }

    /// Record a successful call. Resets the consecutive-failure
    /// counter; if the state was `Suspect`, promotes back to
    /// `Healthy` **iff** there is no outstanding self-reported-error
    /// streak — see below. Does NOT modify
    /// `Unhealthy` / `Respawning` / `Dead` states — those are
    /// supervisor-driven and must not be cleared by a stray in-flight
    /// success racing with a state transition.
    ///
    /// **Why the self-reported gate on Suspect → Healthy?** The
    /// motivating dash-mcp pathology is an upstream that returns
    /// `Ok(_)` with an `{ "error": "..." }` payload — rmcp sees
    /// success, `record_success` fires, but the upstream is sick.
    /// Without this gate, every misleading success would CAS-promote
    /// the upstream back to Healthy, undoing the demotion driven by
    /// `consecutive_self_reported_errors` and trapping the supervisor
    /// in an endless Healthy ↔ Suspect oscillation. With the gate,
    /// Suspect → Healthy still happens for transport-level recovery
    /// (counter is naturally zero) but NOT for misleading successes
    /// while the side channel is still complaining. Decay or respawn
    /// eventually clears the streak; until then, the promotion is
    /// blocked.
    ///
    /// Verified live: without this gate, the supervisor flips
    /// Healthy → Suspect at every counter increment, `record_success`
    /// immediately flips it back, and the 2*threshold escalation to
    /// Unhealthy is never reached.
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.last_success_micros
            .store(now_micros(), Ordering::Release);

        // Side-channel sickness gate: skip the Suspect → Healthy CAS
        // when there's an outstanding self-reported-error streak. The
        // "success" we just observed may be the misleading kind.
        if self.consecutive_self_reported_errors() > 0 {
            return;
        }

        // Auto-promote Suspect → Healthy. Use compare_exchange so we
        // don't stomp on a concurrent supervisor-driven transition to
        // Unhealthy/Respawning — the supervisor's write wins.
        let suspect = HealthState::Suspect as u8;
        let healthy = HealthState::Healthy as u8;
        let _ = self
            .state
            .compare_exchange(suspect, healthy, Ordering::AcqRel, Ordering::Acquire);
    }

    /// Record a failure of the given kind. The
    /// consecutive-failure counter only increments for transport-level
    /// kinds (see `FailureKind::is_transport_level`); peer-returned
    /// `McpError` is recorded for observability but doesn't contribute
    /// to the counter that might trigger a respawn.
    ///
    /// Returns the new counter value for transport-level failures (so
    /// the caller can log it without a second load), or `None` for
    /// non-counting kinds.
    pub fn record_failure(&self, kind: FailureKind) -> Option<u32> {
        self.last_failure_kind.store(kind.into(), Ordering::Release);
        self.last_failure_micros
            .store(now_micros(), Ordering::Release);

        if kind.is_transport_level() {
            // `fetch_add` returns the PREVIOUS value; add 1 to get the
            // new one so callers can compare against the threshold
            // without off-by-one guessing.
            let previous = self.consecutive_failures.fetch_add(1, Ordering::AcqRel);
            Some(previous.saturating_add(1))
        } else {
            None
        }
    }

    /// Record an Error-level `notifications/message` from the upstream.
    /// Bumps a *separate* streak counter that `record_success` does NOT
    /// zero — see the module-level docs and the `SELF_REPORT_DECAY`
    /// constant for the rationale. Returns the new counter value so
    /// the caller can log it without a second load.
    ///
    /// Decay: if the previous hit was more than `SELF_REPORT_DECAY`
    /// ago, the streak resets to 1 (fresh observation) instead of
    /// extending the stale one. This keeps a single Error log a week
    /// ago from compounding with a fresh one today.
    ///
    /// Also stamps `last_failure_kind` + `last_failure_micros` so the
    /// snapshot surfaces the most-recent signal regardless of which
    /// counter it landed on.
    pub fn record_self_reported_error(&self) -> u32 {
        let now = now_micros();
        let decay_micros = SELF_REPORT_DECAY.as_micros() as i64;
        let previous_micros = self.last_self_reported_micros.load(Ordering::Acquire);
        let elapsed = now.saturating_sub(previous_micros);
        let decayed = previous_micros == 0 || elapsed >= decay_micros;

        self.last_self_reported_micros.store(now, Ordering::Release);
        self.last_failure_kind.store(
            FailureKind::UpstreamSelfReportedError.into(),
            Ordering::Release,
        );
        self.last_failure_micros.store(now, Ordering::Release);

        if decayed {
            // Fresh streak: store 1 directly rather than fetch_add over
            // a possibly-large stale value.
            self.consecutive_self_reported_errors
                .store(1, Ordering::Release);
            1
        } else {
            let previous = self
                .consecutive_self_reported_errors
                .fetch_add(1, Ordering::AcqRel);
            previous.saturating_add(1)
        }
    }

    /// Unconditionally set the state. Used by the supervisor to drive
    /// transitions; `call_tool`'s success/failure paths go through
    /// `record_success` / `record_failure` which only perform the
    /// narrow Suspect→Healthy auto-promotion.
    pub fn set_state(&self, new_state: HealthState) {
        self.state.store(new_state.into(), Ordering::Release);
    }

    /// Transition into [`HealthState::NeedsLogin`], returning `true` only
    /// if this call was the *edge* — i.e. the state was something other
    /// than `NeedsLogin` immediately before. Returns `false` if it was
    /// already `NeedsLogin`.
    ///
    /// This is the dedup primitive behind the human-facing re-auth
    /// notification: an OAuth upstream that just lost its token will fail
    /// EVERY subsequent `call_tool` with an auth error, and each one calls
    /// this — but only the first should fire a desktop alert, or a chatty
    /// agent retrying the upstream would bury the user in identical
    /// notifications. By making the transition atomic (a CAS loop) and
    /// returning the edge, the caller gets "did I just become NeedsLogin?"
    /// without a separate load-then-store race between two failing calls.
    ///
    /// Unconditional `set_state(NeedsLogin)` remains available for callers
    /// that don't care about the edge (e.g. tests); this method is the one
    /// the notification path uses.
    pub fn enter_needs_login(&self) -> bool {
        let target = HealthState::NeedsLogin as u8;
        // CAS loop: succeed by swapping any non-target value to target.
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if current == target {
                // Already NeedsLogin — not an edge, nothing to do.
                return false;
            }
            match self.state.compare_exchange_weak(
                current,
                target,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,                // We performed the transition.
                Err(observed) => current = observed, // Retry with the fresh value.
            }
        }
    }

    /// The current state. Cheap; use freely on the hot path.
    pub fn state(&self) -> HealthState {
        HealthState::try_from(self.state.load(Ordering::Acquire))
            // Impossible: set_state only writes valid discriminants.
            // Panic in debug, fall back to Healthy in release so a
            // corrupted state can't brick the gateway.
            .unwrap_or_else(|raw| {
                debug_assert!(false, "UpstreamHealth: invalid state byte {raw}");
                HealthState::Healthy
            })
    }

    /// Current consecutive-failure count. Compare against the
    /// supervisor's configured threshold to decide Healthy → Suspect.
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Acquire)
    }

    /// Current consecutive self-reported-error count. The supervisor
    /// combines this with `consecutive_failures` via `max` when
    /// deciding demotion — either signal can independently drive the
    /// upstream toward respawn.
    pub fn consecutive_self_reported_errors(&self) -> u32 {
        self.consecutive_self_reported_errors
            .load(Ordering::Acquire)
    }

    /// Reset both consecutive-failure counters without touching the
    /// state. Used by the supervisor after a successful respawn — the
    /// new handle starts fresh even though the *record* lives across
    /// the swap.
    ///
    /// (Today, respawn replaces the `UpstreamHandle` and its
    /// `UpstreamHealth` together, so this method is documentation for
    /// the future case where a respawn reuses the same record. Kept
    /// here so the reset is a single, named operation rather than
    /// ad-hoc atomic stores scattered across the supervisor.)
    pub fn reset_counter(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.consecutive_self_reported_errors
            .store(0, Ordering::Release);
    }

    /// Eventually-consistent snapshot of all fields. Cheap but not
    /// atomic across fields — a racing writer may produce a snapshot
    /// that never existed as a single instant.
    pub fn snapshot(&self) -> HealthSnapshot {
        let state = self.state();
        let consecutive_failures = self.consecutive_failures();
        let last_failure_kind =
            FailureKind::try_from(self.last_failure_kind.load(Ordering::Acquire))
                .unwrap_or(FailureKind::None);
        let last_success_at =
            micros_to_system_time(self.last_success_micros.load(Ordering::Acquire));
        let last_failure_at =
            micros_to_system_time(self.last_failure_micros.load(Ordering::Acquire));
        let consecutive_self_reported_errors = self.consecutive_self_reported_errors();
        HealthSnapshot {
            state,
            consecutive_failures,
            last_failure_kind,
            last_success_at,
            last_failure_at,
            consecutive_self_reported_errors,
        }
    }
}

/// Best-effort "now in Unix microseconds." Returns `0` on the
/// should-never-happen branch of clock-went-backwards; the atomic
/// field treats `0` as "never recorded" so this degrades gracefully.
fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_micros()).ok())
        .unwrap_or(0)
}

/// Convert a stored micros value to a `SystemTime`. `0` means "never
/// recorded" and yields `None`.
fn micros_to_system_time(micros: i64) -> Option<SystemTime> {
    if micros <= 0 {
        return None;
    }
    let duration = Duration::from_micros(micros as u64);
    UNIX_EPOCH.checked_add(duration)
}

#[cfg(test)]
#[path = "health_tests.rs"]
mod tests;
