// pattern: Functional Core
//
// Configuration surface for the per-upstream supervisor (see
// `gateway::upstream_supervisor`). Kept separate from the runtime
// `UpstreamHealth` record because these are pure configuration — a
// bundle of durations and thresholds parsed from the config file and
// baked in at boot.
//
// ## Two-layer resolution
//
// Each upstream resolves its settings in this order, with each layer
// overlaying only the fields it explicitly sets:
//
//   1. Built-in defaults ([`SupervisionSettings::default`]).
//   2. Gateway-wide config (`upstreamSupervision` at the top level of
//      the config file, parsed into
//      [`SupervisionSettings::merge_partial`]).
//   3. Per-upstream overrides (`supervision` on the mcp-client entry,
//      parsed as [`PartialSupervisionSettings`]).
//
// The merge is "set-if-present": a partial's `None` field leaves the
// base unchanged, a `Some(x)` overwrites. This mirrors the config
// precedence used by most supervisor implementations in the wild and
// keeps the operator's config minimal — you only override the knobs
// you care about.
//
// ## Validation
//
// Parsing into `PartialSupervisionSettings` accepts any shape; merging
// returns the concrete `SupervisionSettings` with no invalid values
// because each field's setter validates. Negative durations don't
// exist at the `Duration` boundary; zero is only valid for a narrow
// set of fields (e.g. `unhealthy_threshold = 0` is a user error —
// see [`SupervisionError`]).

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Default per-call wall-clock deadline. Single source of truth lives
/// on the handle ([`crate::upstream::DEFAULT_CALL_TIMEOUT`]).
pub const DEFAULT_CALL_TIMEOUT: Duration = crate::upstream::DEFAULT_CALL_TIMEOUT;

/// Default count of consecutive transport-level failures that flips
/// Healthy → Suspect. 3 is a middle ground: 1 is too twitchy
/// (single-call transient triggers probe), 10 is too permissive
/// (user waits through 10 × call_timeout before the supervisor
/// notices).
pub const DEFAULT_UNHEALTHY_THRESHOLD: u32 = 3;

/// Default cadence for active pings while in Suspect state. Only
/// fires when the supervisor is probing; steady-state traffic pays
/// nothing. 5 s is short enough to promote back to Healthy quickly
/// after a false alarm, long enough to avoid flooding a genuinely
/// sick upstream with ping traffic on top of its problems.
pub const DEFAULT_ACTIVE_PING_INTERVAL: Duration = Duration::from_secs(5);

/// Default per-ping deadline. Shorter than `call_timeout` because a
/// ping response is trivial (empty body) — a slow ping response is
/// itself a strong signal the upstream is unhealthy. 3 s leaves
/// headroom for network jitter on a remote stdio-via-SSE upstream
/// (like `mcp-remote`) without giving a wedged upstream the benefit
/// of the doubt.
pub const DEFAULT_ACTIVE_PING_TIMEOUT: Duration = Duration::from_secs(3);

/// Default cap on respawn attempts before going Dead. 5 attempts with
/// exponential backoff (1 s → 2 s → 4 s → 8 s → 16 s) covers ~31 s
/// of total retry budget — enough to ride out a transient container
/// restart or an SSE endpoint's brief 503, not so many that a dead
/// upstream eats unbounded supervisor cycles.
pub const DEFAULT_RESPAWN_MAX_ATTEMPTS: u32 = 5;

/// Default base for exponential respawn backoff. Each attempt doubles
/// the delay up to `respawn_backoff_cap`.
pub const DEFAULT_RESPAWN_BACKOFF_BASE: Duration = Duration::from_secs(1);

/// Default cap on respawn backoff. 30 s is long enough to let a
/// misbehaving upstream's transient settle, short enough that a
/// manually-fixed upstream comes back within a reasonable
/// investigation window.
pub const DEFAULT_RESPAWN_BACKOFF_CAP: Duration = Duration::from_secs(30);

/// Default interval for the Dead → Respawning re-arm tick. We're not
/// giving up forever: once every 60 s, a Dead upstream gets another
/// chance. Gives the operator a loose "fix the container, wait a
/// minute, traffic resumes" recovery path without requiring a
/// gateway restart.
pub const DEFAULT_DEAD_REARM_INTERVAL: Duration = Duration::from_secs(60);

/// Fully-resolved supervisor settings for one upstream. Every field
/// is mandatory (no `Option`) — operators either accept the default
/// or override via the partial form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupervisionSettings {
    /// Per-call wall-clock deadline applied inside
    /// `UpstreamHandle::call_tool`. On expiry we bail with
    /// [`crate::UpstreamError::CallTimeout`] and record a
    /// transport-level failure on the handle's health record.
    pub call_timeout: Duration,

    /// Count of consecutive transport-level failures that flips
    /// Healthy → Suspect. Non-zero; `0` is a user error rejected at
    /// config-parse time.
    pub unhealthy_threshold: u32,

    /// Cadence for the supervisor's active ping while in Suspect.
    /// Not applied in Healthy (zero overhead when idle).
    pub active_ping_interval: Duration,

    /// Per-ping deadline. Must be less than `active_ping_interval`
    /// so a slow ping doesn't delay the next one — validated at
    /// parse time.
    pub active_ping_timeout: Duration,

    /// Cap on consecutive respawn attempts. After this many failures
    /// the supervisor moves to Dead.
    pub respawn_max_attempts: u32,

    /// Base for exponential backoff between respawn attempts.
    /// Attempt N waits `min(base * 2^(N-1), cap)`.
    pub respawn_backoff_base: Duration,

    /// Cap on respawn backoff. No attempt ever waits longer than this.
    pub respawn_backoff_cap: Duration,

    /// Interval between Dead → Respawning re-arm attempts. The
    /// supervisor periodically probes whether a Dead upstream can
    /// come back without requiring operator intervention.
    pub dead_rearm_interval: Duration,
}

impl Default for SupervisionSettings {
    fn default() -> Self {
        Self {
            call_timeout: DEFAULT_CALL_TIMEOUT,
            unhealthy_threshold: DEFAULT_UNHEALTHY_THRESHOLD,
            active_ping_interval: DEFAULT_ACTIVE_PING_INTERVAL,
            active_ping_timeout: DEFAULT_ACTIVE_PING_TIMEOUT,
            respawn_max_attempts: DEFAULT_RESPAWN_MAX_ATTEMPTS,
            respawn_backoff_base: DEFAULT_RESPAWN_BACKOFF_BASE,
            respawn_backoff_cap: DEFAULT_RESPAWN_BACKOFF_CAP,
            dead_rearm_interval: DEFAULT_DEAD_REARM_INTERVAL,
        }
    }
}

impl SupervisionSettings {
    /// Apply a partial's `Some` fields atop `self`, returning the
    /// merged result. `None` fields in the partial leave `self`'s
    /// corresponding field unchanged.
    ///
    /// This is pure — no I/O, no env. The caller uses it to resolve
    /// per-upstream settings from `global.merge_partial(&per_upstream)`.
    pub fn merge_partial(self, partial: &PartialSupervisionSettings) -> Self {
        Self {
            call_timeout: partial
                .call_timeout_ms
                .map_or(self.call_timeout, Duration::from_millis),
            unhealthy_threshold: partial
                .unhealthy_threshold
                .unwrap_or(self.unhealthy_threshold),
            active_ping_interval: partial
                .active_ping_interval_ms
                .map_or(self.active_ping_interval, Duration::from_millis),
            active_ping_timeout: partial
                .active_ping_timeout_ms
                .map_or(self.active_ping_timeout, Duration::from_millis),
            respawn_max_attempts: partial
                .respawn_max_attempts
                .unwrap_or(self.respawn_max_attempts),
            respawn_backoff_base: partial
                .respawn_backoff_base_ms
                .map_or(self.respawn_backoff_base, Duration::from_millis),
            respawn_backoff_cap: partial
                .respawn_backoff_cap_ms
                .map_or(self.respawn_backoff_cap, Duration::from_millis),
            dead_rearm_interval: partial
                .dead_rearm_interval_ms
                .map_or(self.dead_rearm_interval, Duration::from_millis),
        }
    }

    /// Cross-field validation. Per-field validation (e.g. non-zero
    /// threshold) lives at parse time on `PartialSupervisionSettings`;
    /// this catches combinations that are individually valid but
    /// semantically nonsense — e.g. ping timeout >= interval would
    /// pile up unfinished pings.
    pub fn validate(&self) -> Result<(), SupervisionError> {
        if self.unhealthy_threshold == 0 {
            return Err(SupervisionError::ZeroUnhealthyThreshold);
        }
        if self.active_ping_timeout >= self.active_ping_interval {
            return Err(SupervisionError::PingTimeoutExceedsInterval {
                timeout_ms: self.active_ping_timeout.as_millis() as u64,
                interval_ms: self.active_ping_interval.as_millis() as u64,
            });
        }
        if self.respawn_backoff_cap > Duration::from_secs(3600) {
            return Err(SupervisionError::BackoffCapExceedsOneHour);
        }
        if self.respawn_backoff_base > self.respawn_backoff_cap {
            return Err(SupervisionError::BackoffBaseExceedsCap {
                base_ms: self.respawn_backoff_base.as_millis() as u64,
                cap_ms: self.respawn_backoff_cap.as_millis() as u64,
            });
        }
        if self.call_timeout.is_zero() {
            return Err(SupervisionError::ZeroCallTimeout);
        }
        Ok(())
    }

    /// Compute the backoff delay for respawn attempt `n` (1-indexed).
    /// Returns `base * 2^(n-1)`, capped at `cap`.
    ///
    /// Attempt 0 is "there was no previous attempt" — the supervisor
    /// shouldn't call this with `n == 0`; we saturate to `base` as a
    /// defensive default rather than returning `0`.
    pub fn respawn_delay(&self, attempt: u32) -> Duration {
        if attempt <= 1 {
            return self.respawn_backoff_base;
        }
        // 2^(attempt - 1), clamped so the shift doesn't overflow.
        let shift = attempt.saturating_sub(1).min(32);
        let multiplier = 1u128.checked_shl(shift).unwrap_or(u128::MAX);
        let base_ms = self.respawn_backoff_base.as_millis();
        let delay_ms = base_ms.saturating_mul(multiplier);
        let cap_ms = self.respawn_backoff_cap.as_millis();
        let bounded = delay_ms.min(cap_ms);
        Duration::from_millis(u64::try_from(bounded).unwrap_or(u64::MAX))
    }
}

/// Configuration overlay: each field is optional. Used both for the
/// gateway-wide `upstreamSupervision` key AND for per-upstream
/// `supervision` overrides — same shape at both layers.
///
/// All fields are named `*_ms` for durations so the config-file shape
/// is plain JSON integers (matching the existing `executeDeadlineMs`
/// convention on `GatewaySettings`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct PartialSupervisionSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unhealthy_threshold: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_ping_interval_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_ping_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub respawn_max_attempts: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub respawn_backoff_base_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub respawn_backoff_cap_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_rearm_interval_ms: Option<u64>,
}

/// Errors produced by `SupervisionSettings::validate` and the
/// gateway's parse-time validators.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SupervisionError {
    #[error(
        "unhealthyThreshold must be greater than zero; a zero threshold \
         would transition every upstream to Suspect on the first failure"
    )]
    ZeroUnhealthyThreshold,

    #[error(
        "activePingTimeoutMs ({timeout_ms}) must be less than \
         activePingIntervalMs ({interval_ms}); otherwise pings would pile up"
    )]
    PingTimeoutExceedsInterval { timeout_ms: u64, interval_ms: u64 },

    #[error(
        "respawnBackoffBaseMs ({base_ms}) must not exceed \
         respawnBackoffCapMs ({cap_ms})"
    )]
    BackoffBaseExceedsCap { base_ms: u64, cap_ms: u64 },

    #[error("respawnBackoffCapMs must not exceed 3600000 (one hour)")]
    BackoffCapExceedsOneHour,

    #[error(
        "callTimeoutMs must be greater than zero; a zero deadline \
         causes every call to time out before it can make progress"
    )]
    ZeroCallTimeout,
}

#[cfg(test)]
#[path = "supervision_tests.rs"]
mod tests;
