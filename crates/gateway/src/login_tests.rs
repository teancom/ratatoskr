use super::*;

fn http_oauth_config(callback_port: u16, client_id: Option<&str>) -> mcp_client::UpstreamConfig {
    mcp_client::UpstreamConfig::new(
        "Slack".to_string(),
        "slack".to_string(),
        UpstreamKind::HttpOAuth {
            url: "https://slack.example/mcp".to_string(),
            scopes: vec![],
            client_name: None,
            callback_port,
            client_id: client_id.map(String::from),
        },
    )
}

#[test]
fn static_client_id_with_ephemeral_port_is_rejected() {
    // A pre-registered client's redirect URI must use the exact port the
    // provider has registered; callback_port 0 (ephemeral) can never
    // match, and letting it through would fail as an opaque provider-side
    // redirect_uri-mismatch AFTER the browser dance. params_from_config
    // must reject it up front with an actionable message.
    let err = params_from_config(&http_oauth_config(0, Some("preregistered-42")))
        .expect_err("clientId + ephemeral callbackPort must be rejected");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("callbackPort"),
        "error must name the missing field; got: {msg}"
    );
}

#[test]
fn static_client_id_with_pinned_port_resolves() {
    let params = params_from_config(&http_oauth_config(3118, Some("preregistered-42")))
        .expect("pinned port + clientId is the valid static-flow shape");
    assert_eq!(params.client_id.as_deref(), Some("preregistered-42"));
    assert_eq!(params.callback_port, 3118);
}

#[test]
fn dcr_flow_allows_ephemeral_port() {
    // No clientId → DCR registers whatever redirect we send, so an
    // ephemeral port stays fine there.
    let params = params_from_config(&http_oauth_config(0, None))
        .expect("DCR flow must keep accepting callback_port 0");
    assert!(params.client_id.is_none());
}

#[test]
fn parses_code_and_state() {
    let p = parse_callback_query("/callback?code=abc123&state=xyz789").expect("parse");
    assert_eq!(p.code, "abc123");
    assert_eq!(p.state, "xyz789");
}

#[test]
fn parses_with_extra_params_and_reordered() {
    let p = parse_callback_query("/callback?state=s1&iss=https%3A%2F%2Fp&code=c1").expect("parse");
    assert_eq!(p.code, "c1");
    assert_eq!(p.state, "s1");
}

#[test]
fn percent_encoded_values_are_decoded() {
    let p = parse_callback_query("/callback?code=a%2Fb&state=x%3Dy").expect("parse");
    assert_eq!(p.code, "a/b");
    assert_eq!(p.state, "x=y");
}

#[test]
fn provider_error_surfaces() {
    let err = parse_callback_query("/callback?error=access_denied&state=s")
        .expect_err("denied consent must error");
    assert!(err.to_string().contains("access_denied"), "{err}");
}

#[test]
fn missing_code_errors() {
    let err = parse_callback_query("/callback?state=s").expect_err("missing code");
    assert!(err.to_string().contains("code"), "{err}");
}

#[test]
fn missing_state_errors() {
    let err = parse_callback_query("/callback?code=c").expect_err("missing state");
    assert!(err.to_string().contains("state"), "{err}");
}

#[test]
fn request_target_extracts_get_path() {
    let req = "GET /callback?code=c&state=s HTTP/1.1\r\nHost: localhost\r\n\r\n";
    assert_eq!(
        request_target(req).as_deref(),
        Some("/callback?code=c&state=s")
    );
}

#[test]
fn request_target_rejects_non_get() {
    let req = "POST /callback HTTP/1.1\r\n\r\n";
    assert!(request_target(req).is_none());
}

// ── SIGHUP-on-login reload notification (Unix) ──

#[cfg(unix)]
#[test]
fn notify_reload_no_pidfile_is_no_gateway_running() {
    let dir = tempfile::tempdir().expect("tempdir");
    let missing = dir.path().join("gateway.pid");
    assert!(matches!(
        notify_gateway_reload_at(&missing),
        ReloadNotify::NoGatewayRunning
    ));
}

#[cfg(unix)]
#[test]
fn notify_reload_stale_pid_is_no_gateway_running() {
    // A PID that is almost certainly not a live process. Even in the
    // vanishingly unlikely case it is, the liveness check returning
    // "alive" would still not be wrong — but 0x7FFF_FFFE is above the
    // default pid_max on macOS/Linux, so `kill(_, 0)` returns ESRCH.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("gateway.pid");
    std::fs::write(&path, "2147483646").expect("seed stale pid");
    assert!(matches!(
        notify_gateway_reload_at(&path),
        ReloadNotify::NoGatewayRunning
    ));
}

#[cfg(unix)]
#[test]
fn notify_reload_garbage_pidfile_is_failed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("gateway.pid");
    std::fs::write(&path, "not-a-number").expect("seed garbage");
    assert!(matches!(
        notify_gateway_reload_at(&path),
        ReloadNotify::Failed(_)
    ));
}

#[cfg(unix)]
#[test]
fn notify_reload_live_process_gets_signalled() {
    // Spawn a real child that ignores SIGHUP, write its PID, and assert
    // we report Signalled. Using a child (rather than our own PID) means
    // the delivered SIGHUP can't disturb the test runner.
    use std::process::{Command, Stdio};

    // `sh -c 'trap "" HUP; sleep 30'` — install SIGHUP-ignore then sleep,
    // so the SIGHUP we send neither kills it nor escapes to us.
    let mut child = Command::new("sh")
        .args(["-c", "trap \"\" HUP; sleep 30"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sleeper");

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("gateway.pid");
    std::fs::write(&path, child.id().to_string()).expect("write child pid");

    let outcome = notify_gateway_reload_at(&path);

    // Clean up the child regardless of the assertion result.
    let _ = child.kill();
    let _ = child.wait();

    match outcome {
        ReloadNotify::Signalled(pid) => assert_eq!(pid, child.id()),
        other => panic!("expected Signalled for a live process, got {other:?}"),
    }
}
