// pattern: Functional Core
//
// Pure config-reconciliation logic for the `reload-config` tool. Given
// the currently-running upstream set and a freshly-parsed one, compute a
// structured diff (what to add / remove / change / leave alone) and a
// human-readable report of which gateway-wide settings changed.
//
// No I/O, no clock, no env. The imperative shell (`serve::apply_diff`,
// `handler::admin`) reads the file, calls these functions, then acts on
// the result. Keeping the classification pure makes it exhaustively
// table-testable without spawning a single upstream.

use std::collections::HashMap;

use mcp_client::UpstreamConfig;

use crate::config::GatewaySettings;

/// The result of reconciling the running upstream set against a
/// freshly-parsed config. Every upstream name present in either input
/// lands in exactly one bucket.
///
/// Keyed by display `name`. A rename reads as a remove of the old name
/// plus an add of the new one — which is correct: the Lua-global
/// identity is derived from `name`, so a renamed upstream genuinely is a
/// different upstream from the agent's perspective.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UpstreamDiff {
    /// Present in the new config, absent from the running set.
    pub added: Vec<UpstreamConfig>,
    /// Display names present in the running set, absent from the new config.
    pub removed: Vec<String>,
    /// Present in both under the same name, but the config differs. The
    /// stored value is the NEW config (what to connect).
    pub changed: Vec<UpstreamConfig>,
    /// Display names present in both with byte-identical config — left
    /// untouched by reload (no reconnect, no disruption).
    pub unchanged: Vec<String>,
}

impl UpstreamDiff {
    /// True when the running set already matches the new config — reload
    /// has no upstream work to do. Settings may still differ (see
    /// [`settings_changes`]); this only speaks to the upstream roster.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

/// Classify each upstream into added / removed / changed / unchanged by
/// comparing the running set against the freshly-parsed one. Pure: the
/// inputs are owned slices, the output is owned, no side effects.
///
/// Equality is the full structural `PartialEq` on `UpstreamConfig`
/// (command/url/args/env/headers/hint/sampling/supervision) — any
/// observable difference in how the upstream would be connected counts
/// as "changed".
pub fn diff_upstreams(current: &[UpstreamConfig], next: &[UpstreamConfig]) -> UpstreamDiff {
    let current_by_name: HashMap<&str, &UpstreamConfig> =
        current.iter().map(|c| (c.name.as_str(), c)).collect();
    let next_by_name: HashMap<&str, &UpstreamConfig> =
        next.iter().map(|c| (c.name.as_str(), c)).collect();

    let mut diff = UpstreamDiff::default();

    for next_cfg in next {
        match current_by_name.get(next_cfg.name.as_str()) {
            None => diff.added.push(next_cfg.clone()),
            Some(current_cfg) => {
                if *current_cfg == next_cfg {
                    diff.unchanged.push(next_cfg.name.clone());
                } else {
                    diff.changed.push(next_cfg.clone());
                }
            }
        }
    }

    for current_cfg in current {
        if !next_by_name.contains_key(current_cfg.name.as_str()) {
            diff.removed.push(current_cfg.name.clone());
        }
    }

    diff
}

/// Move any upstream named in `force_reconnect` from the diff's
/// `unchanged` bucket into `changed`, so the reload's connect-new-first
/// path re-attempts a connection even though the *config* didn't change.
///
/// ## Why this exists
///
/// An OAuth upstream whose token died mid-life is flipped to
/// `HealthState::NeedsLogin` by `call_tool`, but its declarative config
/// is untouched — so a plain `diff_upstreams` classifies it as
/// `unchanged` and a reload would skip it. After the operator re-runs
/// `login` (which writes fresh credentials and sends SIGHUP), we WANT
/// that upstream reconnected. This promotion is the bridge: the SIGHUP
/// reload re-attempts exactly the NeedsLogin upstreams.
///
/// The `changed` path uses `apply_diff`'s connect-new-first semantics, so
/// this is safe even if `login` did NOT actually run (e.g. a stray
/// SIGHUP): the reconnect fast-fails with `OAuthReauthRequired`, the old
/// NeedsLogin handle is left registered, and nothing is lost. A
/// successful reconnect swaps in a fresh, Healthy handle.
///
/// Pure: takes the running set (to recover the upstream's current config,
/// since the diff's `unchanged` bucket holds only names) and mutates the
/// diff in place. Names in `force_reconnect` that aren't in `unchanged`
/// (already added/changed/removed, or never present) are ignored — the
/// caller can pass the full NeedsLogin set without pre-filtering.
pub fn promote_for_reconnect(
    diff: &mut UpstreamDiff,
    running: &[UpstreamConfig],
    force_reconnect: &std::collections::HashSet<String>,
) {
    if force_reconnect.is_empty() {
        return;
    }
    let running_by_name: HashMap<&str, &UpstreamConfig> =
        running.iter().map(|c| (c.name.as_str(), c)).collect();

    // Partition `unchanged`: names we're promoting move to `changed`
    // (carrying their current config, which is also the desired config
    // since they're unchanged); the rest stay put.
    let mut still_unchanged = Vec::with_capacity(diff.unchanged.len());
    for name in std::mem::take(&mut diff.unchanged) {
        // Promote only if it's both in the force set AND still has a known
        // config. If the name is force-listed but its config vanished
        // (shouldn't happen — it came from the running set), leave it out
        // of both buckets rather than fabricate a config.
        if let Some(cfg) = force_reconnect
            .contains(&name)
            .then(|| running_by_name.get(name.as_str()))
            .flatten()
        {
            diff.changed.push((*cfg).clone());
            continue;
        }
        still_unchanged.push(name);
    }
    diff.unchanged = still_unchanged;
}

/// Human-readable lines describing which gateway-wide settings changed
/// between the running config and the freshly-parsed one. Empty when
/// they are identical.
///
/// Settings are NOT hot-reloaded (they are baked into per-session
/// handlers and the Lua runtime at boot), so the `reload-config` tool
/// uses this only to TELL the operator a `restart-gateway` is needed —
/// it never mutates the running settings.
pub fn settings_changes(running: &GatewaySettings, parsed: &GatewaySettings) -> Vec<String> {
    let mut lines = Vec::new();
    if running.result_size_threshold != parsed.result_size_threshold {
        lines.push(format!(
            "result_size_threshold: {} -> {}",
            running.result_size_threshold, parsed.result_size_threshold
        ));
    }
    if running.execute_deadline_ms != parsed.execute_deadline_ms {
        lines.push(format!(
            "execute_deadline_ms: {} -> {}",
            running.execute_deadline_ms, parsed.execute_deadline_ms
        ));
    }
    if running.supervision != parsed.supervision {
        // The supervision block has eight knobs; a field-by-field render
        // would bloat the reload result for little operator value. The
        // actionable signal is "supervision changed — restart to apply",
        // and the operator can diff their own config file for the detail.
        lines.push("supervision: changed".to_string());
    }
    if running.port != parsed.port {
        lines.push(format!("port: {} -> {}", running.port, parsed.port));
    }
    lines
}

#[cfg(test)]
#[path = "reconcile_tests.rs"]
mod tests;
