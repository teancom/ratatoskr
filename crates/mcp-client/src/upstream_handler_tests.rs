//! Unit tests for ProxyUpstreamHandler callback behaviour.
//!
//! Strategy: use connect_with_transport with tokio::io::duplex to create an
//! in-process MCP client+server pair. The fake server triggers notifications
//! and sampling requests; we assert on the event bus or error code.

use std::sync::Arc;
use std::time::Duration;

use rmcp::{
    ServerHandler, ServiceExt,
    model::{CreateMessageRequestParams, ResourceUpdatedNotificationParam},
};

use crate::event_bus::UpstreamEventBus;
use crate::events::UpstreamEvent;
use crate::sampling::SamplingBroker;
use crate::upstream::{UpstreamConfig, UpstreamKind};

/// A do-nothing MCP server. All trait methods use the ServerHandler defaults.
struct FakeServer;
impl ServerHandler for FakeServer {}

/// Spin up an in-process pair: FakeServer on one side, connect_with_transport
/// (ProxyUpstreamHandler) on the other. Returns (server_service, bus, live_upstream).
///
/// The caller holds `server_service` to keep the connection alive and can call
/// `server_service.peer().notify_*()` to trigger handler callbacks.
async fn make_handler_pair(
    sampling_enabled: bool,
) -> (
    rmcp::service::RunningService<rmcp::RoleServer, FakeServer>,
    UpstreamEventBus,
    crate::connect::LiveUpstream,
) {
    let (server_side, client_side) = tokio::io::duplex(64 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_side);
    let (cli_r, cli_w) = tokio::io::split(client_side);

    let bus = UpstreamEventBus::new();
    let broker = Arc::new(SamplingBroker::new());

    let mut config = UpstreamConfig::new(
        "test-upstream",
        "test_upstream",
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    );
    if sampling_enabled {
        config = config.with_sampling_enabled();
    }

    let (server_svc, live) = tokio::join!(
        FakeServer.serve((srv_r, srv_w)),
        crate::connect::connect_with_transport(&config, &bus, &broker, cli_r, cli_w),
    );

    (
        server_svc.expect("fake server serve"),
        bus,
        live.expect("connect_with_transport"),
    )
}

// ---- notification → bus forwarding tests ----

#[tokio::test]
async fn on_tool_list_changed_publishes_tool_list_changed_event() {
    // Property: on_tool_list_changed must publish UpstreamEvent::ToolListChanged
    // carrying the upstream name. This is the primary fan-out signal that
    // invalidates downstream tool caches.
    let (server_svc, bus, _live) = make_handler_pair(false).await;
    let mut rx = bus.subscribe();

    server_svc
        .peer()
        .notify_tool_list_changed()
        .await
        .expect("notify_tool_list_changed");

    let event = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("event bus receive timed out")
        .expect("bus closed");

    match event {
        UpstreamEvent::ToolListChanged { upstream } => {
            assert_eq!(upstream, "test-upstream");
        }
        other => panic!("expected ToolListChanged, got {other:?}"),
    }

    let _ = server_svc.cancel().await;
}

#[tokio::test]
async fn on_resource_list_changed_publishes_resource_list_changed_event() {
    // Property: on_resource_list_changed must publish
    // UpstreamEvent::ResourceListChanged so downstream sessions know to
    // re-fetch the resource listing.
    let (server_svc, bus, _live) = make_handler_pair(false).await;
    let mut rx = bus.subscribe();

    server_svc
        .peer()
        .notify_resource_list_changed()
        .await
        .expect("notify_resource_list_changed");

    let event = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("event bus receive timed out")
        .expect("bus closed");

    match event {
        UpstreamEvent::ResourceListChanged { upstream } => {
            assert_eq!(upstream, "test-upstream");
        }
        other => panic!("expected ResourceListChanged, got {other:?}"),
    }

    let _ = server_svc.cancel().await;
}

#[tokio::test]
async fn on_prompt_list_changed_publishes_prompt_list_changed_event() {
    // Property: on_prompt_list_changed must publish
    // UpstreamEvent::PromptListChanged so downstream sessions know to
    // re-fetch the prompt listing.
    let (server_svc, bus, _live) = make_handler_pair(false).await;
    let mut rx = bus.subscribe();

    server_svc
        .peer()
        .notify_prompt_list_changed()
        .await
        .expect("notify_prompt_list_changed");

    let event = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("event bus receive timed out")
        .expect("bus closed");

    match event {
        UpstreamEvent::PromptListChanged { upstream } => {
            assert_eq!(upstream, "test-upstream");
        }
        other => panic!("expected PromptListChanged, got {other:?}"),
    }

    let _ = server_svc.cancel().await;
}

#[tokio::test]
async fn on_resource_updated_publishes_resource_updated_with_bare_uri() {
    // Property: on_resource_updated must publish UpstreamEvent::ResourceUpdated
    // carrying the BARE upstream URI (not the namespaced mcp-proxy:// form).
    // The forwarder translates at fan-out time; the bus must carry raw URIs.
    let (server_svc, bus, _live) = make_handler_pair(false).await;
    let mut rx = bus.subscribe();

    server_svc
        .peer()
        .notify_resource_updated(ResourceUpdatedNotificationParam::new("incident://99"))
        .await
        .expect("notify_resource_updated");

    let event = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("event bus receive timed out")
        .expect("bus closed");

    match event {
        UpstreamEvent::ResourceUpdated { upstream, uri } => {
            assert_eq!(upstream, "test-upstream");
            assert_eq!(uri, "incident://99", "bus must carry the bare upstream URI");
            assert!(
                !uri.starts_with("mcp-proxy://"),
                "bus must NOT carry the namespaced mcp-proxy:// form"
            );
        }
        other => panic!("expected ResourceUpdated, got {other:?}"),
    }

    let _ = server_svc.cancel().await;
}

// ---- sampling security gate tests ----

#[tokio::test]
async fn create_message_returns_method_not_found_when_sampling_disabled() {
    // Property: defense-in-depth layer 2 — even if a malicious upstream
    // sends sampling/createMessage when sampling_enabled = false, the
    // handler must return METHOD_NOT_FOUND and never forward to the broker.
    // The security contract from the module header is tested here.
    let (server_svc, _bus, _live) = make_handler_pair(false).await;

    let params = CreateMessageRequestParams::new(vec![], 1);
    let result = server_svc.peer().create_message(params).await;

    match result {
        Err(e) => {
            // rmcp wraps the JSON-RPC error; the code must be METHOD_NOT_FOUND.
            let msg = format!("{e:?}");
            assert!(
                msg.contains("MethodNotFound") || msg.contains("-32601"),
                "expected METHOD_NOT_FOUND (-32601), got: {msg}"
            );
        }
        Ok(_) => panic!("expected METHOD_NOT_FOUND error, but create_message succeeded"),
    }

    let _ = server_svc.cancel().await;
}

#[tokio::test]
async fn create_message_with_sampling_enabled_reaches_broker() {
    // Property: when sampling_enabled = true, create_message must forward
    // through to the broker. With no downstream peer registered, the
    // broker returns INTERNAL_ERROR (not METHOD_NOT_FOUND) — which proves
    // the security gate was passed and the broker was consulted.
    let (server_svc, _bus, _live) = make_handler_pair(true).await;

    let params = CreateMessageRequestParams::new(vec![], 1);
    let result = server_svc.peer().create_message(params).await;

    match result {
        Err(e) => {
            // The broker has no registered peers, so it returns INTERNAL_ERROR.
            // METHOD_NOT_FOUND would mean the gate wrongly blocked the request.
            let msg = format!("{e:?}");
            assert!(
                msg.contains("InternalError") || msg.contains("-32603"),
                "expected INTERNAL_ERROR from empty broker, got: {msg} \
                 (METHOD_NOT_FOUND would indicate the sampling gate wrongly fired)"
            );
        }
        Ok(_) => {
            panic!("expected an error (no downstream peers), but create_message succeeded")
        }
    }

    let _ = server_svc.cancel().await;
}

// ---- Error-log → supervisor signal tests ----

use rmcp::model::{LoggingLevel, LoggingMessageNotificationParam};

use super::{ERROR_LOG_DEBOUNCE, ProxyUpstreamHandler};
use crate::health::{FailureKind, HealthState};

/// Wait for a condition with a 1s budget. Used instead of a fixed
/// `sleep` so the test stays fast when the notification arrives
/// quickly (which it does in-process) but doesn't flake if a slow
/// CI host needs a bit more time.
async fn wait_for<F: Fn() -> bool>(predicate: F, desc: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {desc}");
}

#[tokio::test]
async fn one_error_log_increments_self_reported_counter_by_one() {
    // Property: a single Error-level notifications/message lands on
    // the shared health record's self-reported counter. This is the
    // foundational signal that drives Healthy → Suspect once the
    // supervisor sees enough of them. Without this, the bug that
    // motivated UpstreamSelfReportedError stays invisible to the
    // supervisor (the tool call returned Ok with a misleading
    // payload; only the side-channel log reveals the rot).
    //
    // Crucially: the signal lands on `consecutive_self_reported_errors`,
    // NOT on `consecutive_failures` — see the
    // `record_success_does_not_clear_self_reported_counter` test
    // for why that distinction matters in the dash-mcp pathology.
    let (server_svc, _bus, live) = make_handler_pair(false).await;
    let health = live.health();
    assert_eq!(
        health.consecutive_self_reported_errors(),
        0,
        "fresh record is zero"
    );
    assert_eq!(health.state(), HealthState::Healthy);

    server_svc
        .peer()
        .notify_logging_message(LoggingMessageNotificationParam {
            level: LoggingLevel::Error,
            logger: Some("upstream_internal".to_string()),
            data: serde_json::json!({ "message": "Health check failed: idna codec unavailable" }),
        })
        .await
        .expect("notify_logging_message");

    wait_for(
        || health.consecutive_self_reported_errors() >= 1,
        "consecutive_self_reported_errors to advance past 0",
    )
    .await;

    // Exactly one increment for one Error log.
    assert_eq!(health.consecutive_self_reported_errors(), 1);
    // The signal must NOT bleed into consecutive_failures — that
    // counter is reserved for transport-level signals so it can be
    // legitimately zeroed by record_success on a real-traffic
    // recovery.
    assert_eq!(
        health.consecutive_failures(),
        0,
        "self-reported signal must NOT touch consecutive_failures"
    );
    // The recorded kind is the new variant — proves the helper
    // chose UpstreamSelfReportedError.
    assert_eq!(
        health.snapshot().last_failure_kind,
        FailureKind::UpstreamSelfReportedError
    );

    let _ = server_svc.cancel().await;
}

#[tokio::test]
async fn rapid_error_logs_are_debounced_to_one_increment() {
    // Property: ERROR_LOG_DEBOUNCE prevents a tight retry loop in
    // the upstream from driving the failure counter past the
    // demotion threshold in milliseconds. The real-traffic signal
    // (one increment per tool-call timeout) is the intended cadence;
    // the debounce keeps the side-channel signal at the same tempo.
    let (server_svc, _bus, live) = make_handler_pair(false).await;
    let health = live.health();

    // Fire three Error logs back-to-back. The first counts; the
    // other two land inside the 1s debounce window and must not.
    for i in 0..3 {
        server_svc
            .peer()
            .notify_logging_message(LoggingMessageNotificationParam {
                level: LoggingLevel::Error,
                logger: None,
                data: serde_json::json!({ "n": i }),
            })
            .await
            .expect("notify_logging_message");
    }

    wait_for(
        || health.consecutive_self_reported_errors() >= 1,
        "first Error log to be counted",
    )
    .await;

    // Brief settle window so any late-counted increments would
    // show up if the debounce were broken. 50ms is generous
    // compared to the in-process notification round-trip but well
    // under ERROR_LOG_DEBOUNCE (1s).
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        health.consecutive_self_reported_errors(),
        1,
        "debounce must suppress rapid follow-ups: a tight retry loop in the upstream cannot drive demotion"
    );

    let _ = server_svc.cancel().await;
}

#[tokio::test]
async fn warning_level_log_does_not_count_toward_failure() {
    // Property: only Error and worse feed the supervisor. Warnings
    // and Info messages are diagnostic, not state-affecting — an
    // upstream that emits debug-level chatter shouldn't get bounced.
    let (server_svc, _bus, live) = make_handler_pair(false).await;
    let health = live.health();

    server_svc
        .peer()
        .notify_logging_message(LoggingMessageNotificationParam {
            level: LoggingLevel::Warning,
            logger: None,
            data: serde_json::json!({ "msg": "not fatal" }),
        })
        .await
        .expect("notify_logging_message");

    // No condition to wait on; instead give the notification
    // long enough to be processed by the client's notification
    // task. If the counter were going to advance, it would have
    // by now in-process.
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        health.consecutive_self_reported_errors(),
        0,
        "Warning-level log must not feed the supervisor"
    );

    let _ = server_svc.cancel().await;
}

#[test]
fn record_success_does_not_clear_self_reported_counter() {
    // Property: the self-reported counter survives record_success.
    // This is the crux of why the counter exists separately — the
    // motivating dash-mcp bug is an upstream that returns Ok(_) at
    // the JSON-RPC layer (rmcp sees success → record_success →
    // consecutive_failures zeroed) while the failure leaks out
    // through a side-channel Error log. If record_success cleared
    // the self-reported counter too, every misleading-success
    // payload would immediately whitewash the signal and the
    // supervisor would never demote.
    //
    // Unit-level test: bump the counter, fire record_success, prove
    // the counter stays. Pins the invariant that record_success only
    // zeroes `consecutive_failures`, never the self-reported counter;
    // a regression here would let a misleading-success payload
    // silently clear evidence of a self-reported error.
    let h = crate::health::UpstreamHealth::new();
    h.record_self_reported_error();
    h.record_self_reported_error();
    // Backdating the timestamp would have rolled the second hit
    // into a fresh streak; we want both hits in the same streak,
    // so we don't backdate here — the in-process gap is
    // microseconds, well under SELF_REPORT_DECAY.
    assert_eq!(h.consecutive_self_reported_errors(), 2);

    // Now simulate the misleading-success path: the tool call
    // returned Ok(_) so record_success fires.
    h.record_success();

    assert_eq!(
        h.consecutive_self_reported_errors(),
        2,
        "record_success must NOT zero the self-reported counter — the dash-mcp bug returns Ok(_) with an error payload and would otherwise whitewash the signal"
    );
    // And the standard counter still behaves normally: it's zero
    // because record_success cleared the transport-level streak
    // (which was already zero).
    assert_eq!(h.consecutive_failures(), 0);
}

#[test]
fn helper_debounce_resets_after_window() {
    // Property: once the debounce window has elapsed, the helper
    // counts the next Error log again. Without this, a slow-drip
    // sick upstream (one Error per minute) would only ever bump
    // the counter once and never reach demotion.
    //
    // Unit-level test — exercises the helper directly with a
    // controllable timeline by reaching into the debounce slot.
    // The end-to-end test above already covers the integration;
    // this test guards the boundary.
    let bus = UpstreamEventBus::new();
    let broker = Arc::new(SamplingBroker::new());
    let health = Arc::new(crate::health::UpstreamHealth::new());
    let h = ProxyUpstreamHandler::new("test".into(), bus, broker, false, Arc::clone(&health));

    h.maybe_count_error_log_toward_health("first");
    assert_eq!(health.consecutive_self_reported_errors(), 1);

    // Backdate the last-counted timestamp to make the next call
    // see itself as past the debounce window. This is the only
    // way to test "time passed" without a real sleep.
    {
        let mut slot = h
            .last_counted_error_log
            .lock()
            .expect("last_counted_error_log mutex poisoned");
        *slot = Some(std::time::Instant::now() - ERROR_LOG_DEBOUNCE - Duration::from_millis(10));
    }

    h.maybe_count_error_log_toward_health("second");
    assert_eq!(
        health.consecutive_self_reported_errors(),
        2,
        "second Error log past the debounce window must count"
    );
}
