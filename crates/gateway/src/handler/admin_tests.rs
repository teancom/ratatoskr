use super::{render_reconnect_report, render_reload_report};

#[test]
fn configured_unavailable_message_points_to_exact_reconnect_argument() {
    let cfg = mcp_client::UpstreamConfig::new(
        "example-server",
        "example_server",
        mcp_client::UpstreamKind::Stdio {
            command: "missing-command".into(),
            args: vec![],
            env: Default::default(),
        },
    );
    let message = super::super::unavailable_upstream_message("example_server", &[cfg]);
    assert_eq!(
        message,
        "upstream server `example-server` (Lua name `example_server`) is configured but unavailable. Call reconnect-upstream with luaServerName `example_server` to retry the connection and obtain its startup diagnostic"
    );
    assert_eq!(
        super::super::unavailable_upstream_message("missing", &[]),
        "unknown upstream server: missing; available Lua names: "
    );
}
use crate::AppContext;
use crate::serve::DiffOutcome;
use mcp_client::{SamplingBroker, UpstreamConfig, UpstreamEventBus, UpstreamKind, UpstreamManager};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn cfg(name: &str) -> UpstreamConfig {
    UpstreamConfig::new(
        name,
        name,
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    )
}

/// Minimal `AppContext` for the restart test: only `shutdown` is
/// exercised, but every field must be present.
fn minimal_ctx() -> AppContext {
    let manager = Arc::new(UpstreamManager::new());
    let subscriptions = Arc::new(crate::subscription_registry::SubscriptionRegistry::new());
    let subscription_service = Arc::new(crate::subscription_service::SubscriptionService::new(
        Arc::clone(&subscriptions),
        Arc::clone(&manager),
    ));
    AppContext {
        upstreams: manager,
        bus: Arc::new(UpstreamEventBus::new()),
        broker: Arc::new(SamplingBroker::new()),
        inspection: crate::tool_inspection::ToolInspectionStore::new(),
        tool_cache: Arc::new(mcp_client::ToolListCache::new()),
        resource_cache: Arc::new(mcp_client::ResourceListCache::new()),
        result_store: Arc::new(crate::execution_result_store::ExecutionResultStore::new()),
        subscriptions,
        subscription_service,
        response_shapes: crate::response_shape_cache::ResponseShapeCache::new(),
        settings: crate::config::GatewaySettings::default(),
        lua: Arc::new(lua_runtime::LuaRuntime::new()),
        shutdown: CancellationToken::new(),
        supervisor_cancel: CancellationToken::new(),
        supervisors: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        reload_lock: Arc::new(tokio::sync::Mutex::new(())),
    }
}

#[tokio::test]
async fn restart_gateway_cancels_root_shutdown_token() {
    // Property: invoking restart-gateway cancels the root `shutdown`
    // token — the exact signal `serve_on` selects on for a clean
    // shutdown. We don't exercise the real process exit, only that
    // the tool pulls that one lever. The cancel fires from a spawned
    // task after the flush grace, so we wait on the token.
    let ctx = minimal_ctx();
    let shutdown = ctx.shutdown.clone();

    let handler = crate::handler::GatewayHandler::new(ctx);
    let result = handler.call_restart_gateway();
    assert!(
        result.is_ok(),
        "restart-gateway must return a success result"
    );
    assert!(
        !shutdown.is_cancelled(),
        "the token must NOT be cancelled synchronously — the response flushes first"
    );

    tokio::time::timeout(Duration::from_secs(5), shutdown.cancelled())
        .await
        .expect("restart-gateway must cancel the root shutdown token after the flush grace");
    assert!(shutdown.is_cancelled());
}

#[test]
fn no_changes_reports_already_matching() {
    let report = render_reload_report(&DiffOutcome::default(), &[]);
    assert!(report.contains("no upstream changes"));
    assert!(!report.contains("restart-gateway"));
}

#[test]
fn reports_connected_and_removed_names() {
    let outcome = DiffOutcome {
        connected: vec![cfg("alpha")],
        removed: vec!["beta".to_string()],
        ..Default::default()
    };
    let report = render_reload_report(&outcome, &[]);
    assert!(report.contains("connected: alpha"));
    assert!(report.contains("removed: beta"));
}

#[test]
fn settings_change_appends_restart_hint() {
    let lines = vec!["execute_deadline_ms: 60000 -> 90000".to_string()];
    let report = render_reload_report(&DiffOutcome::default(), &lines);
    assert!(report.contains("require a restart"));
    assert!(report.contains("execute_deadline_ms: 60000 -> 90000"));
    assert!(
        report.contains("Run restart-gateway"),
        "a settings change must tell the operator how to apply it"
    );
}

// ── render_reconnect_report (reconnect-upstream) ──

#[test]
fn reconnect_success_leads_with_target_and_stays_quiet_about_the_rest() {
    // Property: a clean single-upstream reconnect reports exactly that —
    // no reload-report noise when nothing else changed.
    let outcome = DiffOutcome {
        connected: vec![cfg("argocd")],
        ..Default::default()
    };
    let report = render_reconnect_report("argocd", &outcome, &[]);
    assert!(report.contains("Reconnected `argocd`"));
    assert!(
        report.contains("credentials are fresh"),
        "the success message must explain WHY reconnecting helps (launcher re-ran)"
    );
    assert!(
        !report.contains("also applied"),
        "no unrelated-changes section when the reconnect was the only change"
    );
}

#[test]
fn reconnect_failure_reports_error_and_kept_old_connection() {
    // Property: a failed reconnect names the error AND warns that the old
    // (degraded) connection is still serving — the caller must not assume
    // the problem went away.
    let outcome = DiffOutcome {
        failed: vec![("argocd".to_string(), "launcher exited 1".to_string())],
        kept_old_on_failure: vec!["argocd".to_string()],
        ..Default::default()
    };
    let report = render_reconnect_report("argocd", &outcome, &[]);
    assert!(report.contains("Reconnect FAILED for `argocd`: launcher exited 1"));
    assert!(report.contains("previous connection is still registered"));
}

#[test]
fn reconnect_of_upstream_dropped_from_config_reports_removal() {
    // Property: when the config file no longer defines the target, the
    // shared reconcile removes it — the report must say the removal
    // trumped the reconnect rather than claiming success or failure.
    let outcome = DiffOutcome {
        removed: vec!["argocd".to_string()],
        ..Default::default()
    };
    let report = render_reconnect_report("argocd", &outcome, &[]);
    assert!(report.contains("no longer defines it"));
    assert!(!report.contains("Reconnected `argocd`"));
}

#[test]
fn reconnect_surfaces_unrelated_config_changes_it_applied() {
    // Property: the reconnect rides the shared reload path, so pending
    // config edits get applied as a side effect — those must be reported,
    // never silent.
    let outcome = DiffOutcome {
        connected: vec![cfg("argocd"), cfg("brand-new-upstream")],
        removed: vec!["old-upstream".to_string()],
        ..Default::default()
    };
    let report = render_reconnect_report("argocd", &outcome, &[]);
    assert!(report.contains("Reconnected `argocd`"));
    assert!(report.contains("also applied pending config changes"));
    assert!(report.contains("brand-new-upstream"));
    assert!(report.contains("removed: old-upstream"));
}

#[test]
fn failed_changed_upstream_notes_previous_config_still_running() {
    let outcome = DiffOutcome {
        failed: vec![("svc".to_string(), "connect refused".to_string())],
        kept_old_on_failure: vec!["svc".to_string()],
        ..Default::default()
    };
    let report = render_reload_report(&outcome, &[]);
    assert!(report.contains("svc: connect refused"));
    assert!(report.contains("PREVIOUS config"));
}
