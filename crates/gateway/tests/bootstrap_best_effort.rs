// pattern: Imperative Shell
//
// Regression test for the bootstrap bug discovered during the first real-
// config smoke test: a single failing upstream would `?`-short-circuit the
// whole `bootstrap_upstreams` and the gateway would never reach
// `listener.bind`. The fix is that connect_configs is best-effort per
// upstream — failures are logged and the gateway proceeds with whatever
// worked.
//
// Instead of spinning up real stdio or HTTP upstreams, we inject a fake
// connector into `connect_configs` that succeeds or fails per upstream
// name. That keeps this test fast and hermetic. The injected connector
// also bypasses `mcp_client::connect()` entirely — no real `serve().await`
// handshake runs against a subprocess or socket — so any timeout added
// to `mcp_client::connect()` (e.g. a handshake timeout) has no effect
// on these tests.

use std::collections::HashMap;
use std::sync::Arc;

use gateway::serve::connect_configs;
use mcp_client::{LiveUpstream, UpstreamConfig, UpstreamError, UpstreamHealth, UpstreamKind};
use rmcp::RoleClient;
use rmcp::{ClientHandler, ServiceExt};

/// Build a config with a stdio transport — the specific kind doesn't matter
/// because we inject the connector.
fn stdio_cfg(name: &str) -> UpstreamConfig {
    UpstreamConfig::new(
        name,
        name.replace('-', "_"),
        UpstreamKind::Stdio {
            command: "does-not-matter".to_string(),
            args: vec![],
            env: HashMap::new(),
        },
    )
}

/// Minimal client handler for standing up an in-memory `LiveUpstream` on
/// demand. The test's fake connector returns one of these to simulate a
/// successful upstream connection without any real I/O.
#[derive(Clone, Default)]
struct NoopClient;
impl ClientHandler for NoopClient {}

/// Create a real `LiveUpstream` backed by an in-memory duplex. This stands
/// in for "a connected upstream" without spawning a child or opening a
/// socket. The `_driver` JoinHandle keeps the server side alive for the
/// lifetime of the returned tuple.
async fn fake_connected_upstream() -> (LiveUpstream, tokio::task::JoinHandle<()>) {
    let (client_side, server_side) = tokio::io::duplex(1024);
    let (client_read, client_write) = tokio::io::split(client_side);
    let (server_read, server_write) = tokio::io::split(server_side);

    // Drive the server side: accept an initialize request and nothing else.
    // For this test we only care that the client's `initialize` handshake
    // completes — after that, the fake upstream can idle.
    let driver = tokio::spawn(async move {
        use rmcp::ServerHandler;

        #[derive(Clone, Default)]
        struct IdleServer;
        impl ServerHandler for IdleServer {}

        let transport =
            rmcp::transport::async_rw::AsyncRwTransport::<rmcp::RoleServer, _, _>::new_server(
                server_read,
                server_write,
            );
        let service = IdleServer.serve(transport).await;
        if let Ok(s) = service {
            // Idle forever — the test will cancel on drop.
            let _ = s.waiting().await;
        }
    });

    let client_transport =
        rmcp::transport::async_rw::AsyncRwTransport::<RoleClient, _, _>::new_client(
            client_read,
            client_write,
        );
    // into_dyn erases the handler type so the resulting RunningService
    // matches LiveUpstream's expected `Box<dyn DynService<RoleClient>>`.
    // This is the same pattern `connect_with_transport` uses in production.
    let service = NoopClient
        .into_dyn()
        .serve(client_transport)
        .await
        .expect("in-memory client should initialize");
    // Reuse the `LiveUpstream::Http` variant as a PID-less stand-in.
    // `connect_with_transport` does exactly the same thing, so this shape
    // matches the test helper the other integration tests use.
    (
        LiveUpstream::Http {
            service,
            health: Arc::new(UpstreamHealth::new()),
        },
        driver,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_failing_upstream_does_not_block_others() {
    // Shape: two upstreams, first fails, second succeeds. If the old
    // `?`-short-circuit behavior regressed, `manager.list()` would be
    // empty (first `?` aborts before reaching the second config).
    let configs = vec![stdio_cfg("broken"), stdio_cfg("working")];

    let (live_ok, driver) = fake_connected_upstream().await;
    let live_ok = Arc::new(tokio::sync::Mutex::new(Some(live_ok)));

    let settings = gateway::config::GatewaySettings::default();
    let (manager, failed) = connect_configs(configs, &settings, |cfg| {
        let live_ok = Arc::clone(&live_ok);
        async move {
            if cfg.name == "broken" {
                Err(UpstreamError::NotFound("simulated failure".into()))
            } else {
                let live = live_ok
                    .lock()
                    .await
                    .take()
                    .expect("fake upstream consumed only once");
                Ok(live)
            }
        }
    })
    .await;

    let registered = manager.list().await;
    assert_eq!(
        registered.len(),
        1,
        "exactly one upstream should have registered; got {registered:?}"
    );
    assert_eq!(
        registered[0].name, "working",
        "the working upstream should be the one registered"
    );

    // The failed config must be surfaced separately so `run()` can
    // spawn a bootstrap-retry supervisor for it. Without this, a
    // first-boot failure silently drops the upstream from the catalog
    // forever — exactly the bug this contract guards against.
    assert_eq!(
        failed.len(),
        1,
        "exactly one config should be returned as failed; got {failed:?}"
    );
    assert_eq!(
        failed[0].name, "broken",
        "the broken upstream should be the one returned as failed"
    );

    // Cancel the fake upstream's driver task explicitly — dropping the
    // JoinHandle would only detach it, matching the `.abort()` discipline
    // used in `resources.rs` and other gateway integration tests.
    driver.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_failing_produces_empty_manager_not_error() {
    // If every upstream fails, the gateway still starts — it just has
    // nothing to proxy to. A zero-upstream gateway is a valid (if
    // useless) MCP server; better to have it up and complaining in logs
    // than silently refusing to start.
    let configs = vec![stdio_cfg("broken-a"), stdio_cfg("broken-b")];

    let settings = gateway::config::GatewaySettings::default();
    let (manager, failed) = connect_configs(configs, &settings, |_cfg| async {
        Err::<LiveUpstream, _>(UpstreamError::NotFound("simulated".into()))
    })
    .await;

    assert!(
        manager.list().await.is_empty(),
        "no upstreams should register when all fail"
    );
    // Every config must come back as failed so `run()` can spawn a
    // bootstrap-retry supervisor for each — preserving the recovery
    // path even when nothing comes up at boot.
    assert_eq!(
        failed.len(),
        2,
        "all configs should be returned as failed; got {failed:?}"
    );
    let names: std::collections::BTreeSet<&str> = failed.iter().map(|c| c.name.as_str()).collect();
    assert!(
        names.contains("broken-a") && names.contains("broken-b"),
        "both failing configs should appear in the failed list; got {names:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_config_list_produces_empty_manager() {
    // Edge case: zero upstreams (user's config has `"mcpClients": {}`).
    // Should succeed silently with an empty manager.
    let settings = gateway::config::GatewaySettings::default();
    let (manager, failed) = connect_configs(Vec::new(), &settings, |_cfg| async {
        Err::<LiveUpstream, _>(UpstreamError::NotFound("should not be called".into()))
    })
    .await;
    assert!(manager.list().await.is_empty());
    assert!(
        failed.is_empty(),
        "no configs in, no failed configs out; got {failed:?}"
    );
}
