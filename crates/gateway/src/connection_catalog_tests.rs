use super::*;
use crate::list_servers::{CatalogEntry, render};
use std::collections::HashMap;

fn config(name: &str) -> UpstreamConfig {
    UpstreamConfig::new(
        name,
        name,
        UpstreamKind::Stdio {
            command: "/missing/catalog-test-command".into(),
            args: vec![],
            env: HashMap::from([("PRIVATE_TOKEN".into(), "fixture-secret-value".into())]),
        },
    )
}

#[tokio::test]
async fn initial_failure_catalog_retains_latest_safe_diagnostic_and_removes_it() {
    // Property: failed bootstrap entries remain discoverable, with bounded safe diagnostics.
    let cfg = config("failed");
    let (manager, failed) = connect_configs(
        vec![cfg.clone()],
        &config::GatewaySettings::default(),
        |_| async {
            Err(UpstreamError::NotFound(
                "launcher exited: fixture-secret-value".into(),
            ))
        },
    )
    .await;
    assert_eq!(failed, vec![cfg.clone()]);
    assert!(manager.list().await.is_empty());
    assert_eq!(manager.configured().await, vec![cfg.clone()]);
    let first = manager
        .connection_error("failed")
        .await
        .expect("boot diagnostic");
    assert!(!first.contains("fixture-secret-value"));
    manager
        .record_connection_error(&cfg, "launcher executable missing")
        .await;
    let error = manager
        .connection_error("failed")
        .await
        .expect("latest diagnostic");
    assert_eq!(error, "launcher executable missing");
    let rendered = render(
        &[CatalogEntry::new(&cfg, None).with_connection_error(Some(&error))],
        std::time::SystemTime::UNIX_EPOCH,
    );
    assert!(rendered.contains("Disconnected"));
    assert!(rendered.contains("launcher executable missing"));
    assert!(rendered.contains("reconnect-upstream with {\"luaServerName\":\"failed\"}"));
    manager.set_configured(vec![]).await;
    manager
        .record_connection_error(&cfg, "late retry failure")
        .await;
    assert!(manager.configured().await.is_empty());
    assert_eq!(manager.connection_error("failed").await, None);
}

#[tokio::test]
async fn http_diagnostic_never_retains_transport_credentials() {
    // Property: arbitrary HTTP response bodies and URL credentials cannot enter discovery output.
    let cfg = UpstreamConfig::new(
        "http",
        "http",
        UpstreamKind::Http {
            url: "https://user:password@example.test/mcp?key=secret".into(),
            headers: HashMap::from([("X-Custom".into(), "header-secret".into())]),
        },
    );
    let manager = UpstreamManager::new();
    manager.set_configured(vec![cfg.clone()]).await;
    manager
        .record_connection_error(
            &cfg,
            "https://user:password@example.test/mcp?key=secret response header-secret body-secret",
        )
        .await;
    let error = manager
        .connection_error("http")
        .await
        .expect("safe diagnostic");
    for secret in ["password", "key=secret", "header-secret", "body-secret"] {
        assert!(!error.contains(secret));
    }
    assert!(error.contains("HTTP connection failed"));
}

#[tokio::test]
async fn stdio_diagnostics_are_bounded_and_strip_controls() {
    // Property: large launcher output cannot grow retained state or inject terminal controls.
    let cfg = config("bounded");
    let manager = UpstreamManager::new();
    manager
        .record_connection_error(&cfg, &"safe line\n".repeat(10_000))
        .await;
    assert!(
        manager
            .connection_error("bounded")
            .await
            .expect("bounded")
            .len()
            <= 16 * 1024
    );
    manager
        .record_connection_error(&cfg, "\u{1b}missing executable\r")
        .await;
    assert_eq!(
        manager.connection_error("bounded").await.as_deref(),
        Some("missing executable")
    );
}
