// pattern: Imperative Shell
//
// Admin / control-plane tools: `reload-config` and `restart-gateway`.
// These are the gateway's self-management surface — they let an operator
// (via Claude) pick up config edits without remembering a `launchctl`
// incantation.
//
// `reload-config` is the careful one: parse the file, and only on a clean
// parse apply the upstream diff (connect added, drain removed, reconnect
// changed) while leaving unchanged upstreams untouched. It never restarts
// and never hot-swaps gateway-wide settings — it only reports when those
// changed so the operator knows a restart is needed.
//
// `restart-gateway` is the blunt one: cancel the root shutdown token so
// the process drains and exits, then launchd respawns it fresh. It drops
// the current MCP session, so it's deliberately a separate tool.
//
// The reconcile logic (`diff_upstreams`, `settings_changes`) and the
// manager mutation (`apply_diff`) are pure / injectable and live
// elsewhere (Functional Core + the `serve` shell); this file is the thin
// orchestration that wires file I/O → diff → apply → report.

use std::fmt::Write as _;
use std::time::Duration;

use super::*;
use crate::serve;

/// How long to wait after returning the `restart-gateway` response
/// before cancelling the shutdown token. Gives rmcp's HTTP layer time to
/// flush the tool result back to the client before axum's graceful
/// shutdown tears the connection down — otherwise the agent sees a
/// dropped connection instead of the confirmation. 250ms is comfortably
/// longer than a localhost response flush and short enough that the
/// restart still feels immediate.
const RESTART_FLUSH_GRACE: Duration = Duration::from_millis(250);

impl GatewayHandler {
    /// `reload-config`: re-read the config file and reconcile the live
    /// upstream set to it. Invalid config changes nothing. Settings
    /// changes are reported but not applied (restart required).
    pub(super) async fn call_reload_config(&self) -> Result<CallToolResult, ErrorData> {
        // Delegate to the shared reconcile path (also used by the SIGHUP
        // handler) so the two can't drift. Locking, parse-error handling,
        // diff/apply, and supervisor spawning all live there; this layer
        // only renders the operator-facing report.
        let text = match serve::reconcile_from_disk(&self.state.ctx).await {
            serve::ReconcileResult::ParseError(e) => format!(
                "Config NOT reloaded — the config file is missing or invalid, so nothing \
                 was changed. The running configuration is untouched.\n\nError: {e}"
            ),
            serve::ReconcileResult::Applied {
                outcome,
                settings_lines,
            } => render_reload_report(&outcome, &settings_lines),
        };

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    /// `reconnect-upstream`: force one named upstream through the
    /// reconnect path even though its config is unchanged. The workhorse
    /// is the same shared reconcile as `reload-config`; the only
    /// difference is the explicit force set, which promotes the target
    /// out of the `unchanged` bucket (see `promote_for_reconnect`).
    ///
    /// Exists because stdio launchers that mint short-lived credentials
    /// at process start (the argocd SSO-token launcher is the archetype)
    /// have no other agent-reachable refresh path: `reload-config` skips
    /// unchanged upstreams by design, and `restart-gateway` is a
    /// sledgehammer that drops every upstream and the MCP session itself.
    pub(super) async fn call_reconnect_upstream(
        &self,
        request: &CallToolRequestParams,
    ) -> Result<CallToolResult, ErrorData> {
        let lua_server_name = parse_string_arg(request, "luaServerName")?;
        let (display_name, result) = serve::reconnect_from_disk(&self.state.ctx, &lua_server_name)
            .await
            .map_err(|message| ErrorData::invalid_params(message, None))?;
        let text = match result {
            serve::ReconcileResult::ParseError(e) => format!(
                "Upstream NOT reconnected — the config file is missing or invalid, so nothing \
                 was changed. Fix the config (or restore it) and retry.\n\nError: {e}"
            ),
            serve::ReconcileResult::Applied {
                outcome,
                settings_lines,
            } => render_reconnect_report(
                display_name
                    .as_deref()
                    .expect("applied reconnect has a resolved target"),
                &outcome,
                &settings_lines,
            ),
        };

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    /// `restart-gateway`: confirm, then (after a short flush grace)
    /// cancel the root shutdown token so the process drains, exits, and
    /// is respawned by launchd with the new config + settings.
    pub(super) fn call_restart_gateway(&self) -> Result<CallToolResult, ErrorData> {
        let shutdown = self.state.ctx.shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(RESTART_FLUSH_GRACE).await;
            tracing::info!("restart-gateway: cancelling root token to trigger clean restart");
            shutdown.cancel();
        });

        Ok(CallToolResult::success(vec![Content::text(
            "Gateway restarting: it will drain upstreams, exit, and be respawned by the process \
             supervisor (launchd) with the updated config and settings. This MCP session will \
             drop momentarily — reconnect in a few seconds.",
        )]))
    }
}

/// Render the operator-facing summary of a reload. Pure: takes the
/// outcome of the apply step plus the settings-change lines and produces
/// the text block. Kept free-standing (not a method) so it's unit-
/// testable without a handler.
fn render_reload_report(outcome: &serve::DiffOutcome, settings_lines: &[String]) -> String {
    let mut out = String::new();

    let added_or_changed = outcome.connected.len();
    let removed = outcome.removed.len();

    if added_or_changed == 0 && removed == 0 && outcome.failed.is_empty() {
        out.push_str("Config reloaded: no upstream changes (the running set already matches).");
    } else {
        out.push_str("Config reloaded.\n");
        let _ = writeln!(
            out,
            "  connected (added/changed): {added_or_changed}\n  removed: {removed}",
        );
        if added_or_changed > 0 {
            let names: Vec<&str> = outcome.connected.iter().map(|c| c.name.as_str()).collect();
            let _ = writeln!(out, "  → connected: {}", names.join(", "));
        }
        if removed > 0 {
            let _ = writeln!(out, "  → removed: {}", outcome.removed.join(", "));
        }
    }

    if !outcome.failed.is_empty() {
        out.push_str("\nFailures (these upstreams could not be connected):\n");
        for (name, err) in &outcome.failed {
            let _ = writeln!(out, "  - {name}: {err}");
        }
        if !outcome.kept_old_on_failure.is_empty() {
            let _ = writeln!(
                out,
                "  (still running on their PREVIOUS config — the edit did not take effect: {})",
                outcome.kept_old_on_failure.join(", ")
            );
        }
    }

    if !settings_lines.is_empty() {
        out.push_str(
            "\nGateway-wide settings changed but were NOT applied (they require a restart):\n",
        );
        for line in settings_lines {
            let _ = writeln!(out, "  - {line}");
        }
        out.push_str("Run restart-gateway to apply the settings changes.");
    }

    out
}

/// Render the operator-facing summary of a `reconnect-upstream` call.
/// Pure, like [`render_reload_report`], and free-standing for the same
/// unit-testability reason.
///
/// Leads with the fate of the REQUESTED upstream (the thing the caller
/// asked about), then appends the standard reload report only when the
/// reconcile also picked up unrelated config-file changes — the common
/// case is "nothing else changed" and the report would be noise.
fn render_reconnect_report(
    target: &str,
    outcome: &serve::DiffOutcome,
    settings_lines: &[String],
) -> String {
    let mut out = String::new();

    let target_connected = outcome.connected.iter().any(|c| c.name == target);
    let target_failure = outcome.failed.iter().find(|(name, _)| name == target);

    if target_connected {
        let _ = writeln!(
            out,
            "Reconnected `{target}`: the connection was torn down and re-established from \
             scratch (for stdio upstreams the launcher command re-ran, so start-time \
             credentials are fresh)."
        );
    } else if let Some((_, err)) = target_failure {
        let _ = writeln!(out, "Reconnect FAILED for `{target}`: {err}");
        if outcome.kept_old_on_failure.iter().any(|n| n == target) {
            let _ = writeln!(
                out,
                "The previous connection is still registered — calls may keep failing the \
                 same way they did before."
            );
        }
    } else {
        // Neither connected nor failed: the config file no longer lists
        // the upstream (the reconcile removed it), which trumps the
        // reconnect request.
        let _ = writeln!(
            out,
            "`{target}` was not reconnected: the config file no longer defines it, so the \
             reconcile removed it instead. Re-add it to the config and run reload-config."
        );
    }

    // Surface any OTHER changes the reconcile applied along the way, so
    // side effects of the shared reload path are never silent.
    let other_connected = outcome.connected.len() - usize::from(target_connected);
    let other_failed = outcome.failed.len() - usize::from(target_failure.is_some());
    if other_connected > 0 || other_failed > 0 || !outcome.removed.is_empty() {
        out.push_str("\nThe reconcile also applied pending config changes:\n");
        out.push_str(&render_reload_report(outcome, settings_lines));
    } else if !settings_lines.is_empty() {
        out.push('\n');
        out.push_str(
            "Gateway-wide settings changed but were NOT applied (they require a restart):\n",
        );
        for line in settings_lines {
            let _ = writeln!(out, "  - {line}");
        }
        out.push_str("Run restart-gateway to apply the settings changes.");
    }

    out
}

#[cfg(test)]
#[path = "admin_tests.rs"]
mod tests;
