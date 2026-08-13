use super::*;
use crate::sampling::SamplingBroker;
use crate::upstream::UpstreamConfig;
use std::time::Instant;

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
