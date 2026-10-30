use super::*;
use crate::sampling::SamplingBroker;
use crate::upstream::UpstreamConfig;
use std::time::Instant;

// Pins actionable launcher stderr in connection errors without leaking configured credentials.
#[cfg(unix)]
#[tokio::test]
async fn stdio_failure_includes_redacted_launcher_diagnostics() {
    let bus = UpstreamEventBus::new();
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let config = UpstreamConfig::new(
        "launcher", "launcher",
        UpstreamKind::Stdio {
            command: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), "printf '%s\\n' 'launcher: run ride-cli login to renew session' \"$TEST_CREDENTIAL\" 'Authorization: Bearer raw-bearer-value' >&2; exit 1".to_string()],
            env: HashMap::from([("TEST_CREDENTIAL".to_string(), "opaque-secret-value".to_string())]),
        },
    );
    let result = tokio::time::timeout(Duration::from_secs(3), connect(&config, &bus, &broker))
        .await
        .expect("failed launcher must return promptly");
    match result {
        Err(UpstreamError::Connect { name, source }) => {
            assert_eq!(name, "launcher");
            let text = source.to_string();
            assert!(
                text.contains("launcher: run ride-cli login to renew session"),
                "{text}"
            );
            assert!(!text.contains("opaque-secret-value"), "{text}");
            assert!(!text.contains("raw-bearer-value"), "{text}");
        }
        Err(other) => panic!("expected Connect, got {other:?}"),
        Ok(_) => panic!("launcher unexpectedly connected"),
    }
}

// Pins child ownership and continued stderr draining after a successful handshake.
#[cfg(unix)]
#[tokio::test]
async fn stdio_success_survives_stderr_pressure() {
    let config = UpstreamConfig::new("healthy", "healthy", UpstreamKind::Stdio {
        command: "/bin/sh".to_string(),
        args: vec!["-c".to_string(), r#"
read -r initialize
printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-03-26","capabilities":{},"serverInfo":{"name":"test","version":"1"}}}'
read -r initialized
i=0
while [ "$i" -lt 10000 ]; do printf '%s\n' 'healthy server stderr chatter' >&2; i=$((i+1)); done
read -r ping
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{}}'
read -r wait_for_close
"#.to_string()],
        env: HashMap::new(),
    });
    let bus = UpstreamEventBus::new();
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    tokio::time::timeout(Duration::from_secs(5), async {
        let live = connect(&config, &bus, &broker).await.expect("handshake");
        live.peer()
            .send_request(rmcp::model::ClientRequest::PingRequest(
                rmcp::model::PingRequest::default(),
            ))
            .await
            .expect("child still serves requests after stderr pressure");
        live.close().await.expect("close connection");
    })
    .await
    .expect("stderr pipe must not block successful child");
}

/// An OAuth upstream with no persisted credentials must fail with the
/// actionable [`UpstreamError::OAuthLoginRequired`] — and must do so
/// *fast*, before any network I/O. We point it at an empty state dir
/// (so `initialize_from_store` finds no file) and at a URL that would
/// hang/refuse if we actually dialed it; the assertion is that we
/// return well under the 30 s handshake budget, proving the
/// credential gate short-circuits before discovery.
#[tokio::test]
async fn oauth_connect_without_credentials_fails_fast_and_actionably() {
    let empty_state_dir = tempfile::tempdir().expect("tempdir");
    let bus = UpstreamEventBus::new();
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());

    let config = UpstreamConfig::new(
        "Datadog-Acme",
        "datadog_acme",
        UpstreamKind::HttpOAuth {
            // A non-routable TEST-NET-1 address (RFC 5737). If the gate
            // failed to short-circuit, dialing this would burn the full
            // handshake timeout — the < 5 s assertion below would fail.
            url: "https://192.0.2.1/api/unstable/mcp-server/mcp".to_string(),
            scopes: vec![],
            client_name: None,
            callback_port: 0,
            client_id: None,
        },
    );

    let health = Arc::new(UpstreamHealth::new());
    let handler = ProxyUpstreamHandler::new(
        config.name.clone(),
        bus.clone(),
        broker.clone(),
        config.dangerously_enable_sampling,
        Arc::clone(&health),
    );

    let start = Instant::now();
    let result = connect_oauth_headless(
        handler,
        &config,
        "https://192.0.2.1/api/unstable/mcp-server/mcp",
        empty_state_dir.path(),
        health,
    )
    .await;
    let elapsed = start.elapsed();

    match result {
        Err(UpstreamError::OAuthLoginRequired { name }) => {
            assert_eq!(name, "Datadog-Acme");
        }
        Err(other) => panic!("expected OAuthLoginRequired, got error {other:?}"),
        Ok(_) => panic!("expected OAuthLoginRequired, but connect unexpectedly succeeded"),
    }
    assert!(
        elapsed < Duration::from_secs(5),
        "no-credentials gate must short-circuit before any network I/O; took {elapsed:?}"
    );
}
