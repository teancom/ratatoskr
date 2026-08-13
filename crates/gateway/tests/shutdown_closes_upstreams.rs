// pattern: Imperative Shell
//
// Regression test for a gap exposed while investigating "podman cleanup":
// production never called `UpstreamHandle::close()` on shutdown. It relied
// on `Drop` for the `RunningService`, which is async-fire-and-forget.
// The close-within-budget test in upstream_cleanup.rs proved the close()
// path works — but production never ran it. After this fix, `serve_on` drains the
// UpstreamManager and explicitly closes each handle before returning.
//
// This test spins up `serve_on` with one in-memory fake upstream
// registered, triggers shutdown, and asserts that when serve_on returns
// the manager is empty — i.e. close_all_upstreams ran the drain path.

mod common;

use std::sync::Arc;
use std::time::Duration;

use mcp_client::{
    LiveUpstream, SamplingBroker, SharedSamplingBroker, UpstreamConfig, UpstreamEventBus,
    UpstreamHandle, UpstreamHealth, UpstreamKind, UpstreamManager,
};
use rmcp::{ClientHandler, RoleClient, ServiceExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

/// Minimal in-memory client handler. No protocol logic; we only need a
/// `RunningService` that can be `.cancel()`-ed cleanly.
#[derive(Clone, Default)]
struct NoopClient;
impl ClientHandler for NoopClient {}

/// Spawn an in-memory duplex "upstream": a connected `LiveUpstream` on
/// the client side, plus a server-side driver that will exit when the
/// client cancels. Returns both so the test can keep the server task
/// alive for the duration of the run.
async fn fake_connected_upstream() -> (LiveUpstream, tokio::task::JoinHandle<()>) {
    let (client_side, server_side) = tokio::io::duplex(1024);
    let (client_read, client_write) = tokio::io::split(client_side);
    let (server_read, server_write) = tokio::io::split(server_side);

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
        if let Ok(service) = IdleServer.serve(transport).await {
            let _ = service.waiting().await;
        }
    });

    let client_transport =
        rmcp::transport::async_rw::AsyncRwTransport::<RoleClient, _, _>::new_client(
            client_read,
            client_write,
        );
    let service = NoopClient
        .into_dyn()
        .serve(client_transport)
        .await
        .expect("in-memory handshake must complete");
    (
        LiveUpstream::Http {
            service,
            health: Arc::new(UpstreamHealth::new()),
        },
        driver,
    )
}

/// Build a minimal config for the fake upstream. Kind shape doesn't
/// matter since no one ever reconnects through it.
fn dummy_config(name: &str) -> UpstreamConfig {
    UpstreamConfig::new(
        name,
        name,
        UpstreamKind::Http {
            url: "http://127.0.0.1:0/unused".to_string(),
            headers: std::collections::HashMap::new(),
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_on_drains_manager_during_graceful_shutdown() {
    // Property: after `serve_on` completes a graceful shutdown, the
    // `UpstreamManager` is drained — every registered handle has been
    // explicitly `close()`-ed rather than left to async fire-and-forget
    // `Drop`. This pins the fix for the "podman cleanup" regression
    // where production never called `UpstreamHandle::close()` on
    // shutdown. A failure here means `close_all_upstreams` regressed to
    // a no-op (or the drain path stopped running before return) and
    // upstream processes could be leaked at shutdown again.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    // Capture the bound address before `serve_on` takes ownership of the
    // listener, so the readiness probe below can target it.
    let addr = listener
        .local_addr()
        .expect("listener must expose local_addr");
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let manager = Arc::new(UpstreamManager::new());

    // Register one in-memory upstream. Keep the server-side driver alive
    // via `_server` — dropping it would close the transport prematurely
    // and race with our deliberate close path.
    let (live, _server) = fake_connected_upstream().await;
    manager
        .register(Arc::new(UpstreamHandle::new(dummy_config("fake"), live)))
        .await;

    assert_eq!(
        manager.list().await.len(),
        1,
        "upstream should be registered before shutdown"
    );

    let ct = CancellationToken::new();
    let shutdown_ct = ct.clone();
    let shutdown = async move {
        shutdown_ct.cancelled().await;
    };

    let bus_clone = Arc::clone(&bus);
    let manager_for_serve = Arc::clone(&manager);
    let broker_clone = broker.clone();
    let ct_for_serve = ct.clone();
    let supervisor_ct = ct.child_token();

    let ctx = common::test_app_context(manager_for_serve, bus_clone, broker_clone).await;
    let serve_task = tokio::spawn(async move {
        gateway::serve::serve_on(
            listener,
            ctx,
            Vec::new(),
            ct_for_serve,
            supervisor_ct,
            shutdown,
        )
        .await
    });

    // Wait until axum's accept loop is actually serving so the shutdown
    // path is the same one production takes (shutdown-after-up rather
    // than shutdown-before-up). A fixed sleep here is flaky under load:
    // on a slow CI runner the window can expire before axum is accepting,
    // silently exercising the shutdown-before-up path instead. A TCP
    // connect succeeding against `addr` is a deterministic readiness
    // signal — the kernel only completes the handshake once axum has
    // reached `accept()`.
    let ready_deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < ready_deadline,
            "serve_on never started accepting on {addr} within 5s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Trigger shutdown.
    ct.cancel();

    // serve_on should return Ok within a reasonable window: no real
    // clients, in-memory transport, should be ~instant.
    let serve_result = tokio::time::timeout(Duration::from_secs(5), serve_task)
        .await
        .expect("serve_on must not hang past 5s during shutdown")
        .expect("serve_on task panicked");
    serve_result.expect("serve_on must return Ok on graceful shutdown");

    // The drain invariant: after serve_on returns, the manager is empty.
    // If close_all_upstreams regressed to no-op, this would fail.
    assert!(
        manager.list().await.is_empty(),
        "manager should be drained after graceful shutdown"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_on_exits_within_drain_grace_even_with_open_connection() {
    // Regression test for the `restart-gateway` wedge found during e2e:
    // axum's `with_graceful_shutdown` waits for EXISTING connections to
    // drain before returning, and rmcp's streamable-HTTP transport keeps
    // a client connection open across requests. A still-attached client
    // (notably the very one that invoked `restart-gateway`) would block
    // the drain — and thus the process exit — indefinitely.
    //
    // The fix bounds the drain by `SHUTDOWN_DRAIN_GRACE` (3s). This test
    // holds a raw TCP connection open across shutdown and asserts
    // `serve_on` STILL returns within the grace + margin, rather than
    // hanging on the open socket. Without the bound, this hangs and the
    // outer timeout fails the test.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().expect("local_addr");

    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let manager = Arc::new(UpstreamManager::new());

    let ct = CancellationToken::new();
    let shutdown_ct = ct.clone();
    let shutdown = async move {
        shutdown_ct.cancelled().await;
    };

    let ct_for_serve = ct.clone();
    let supervisor_ct = ct.child_token();
    let ctx =
        common::test_app_context(Arc::clone(&manager), Arc::clone(&bus), broker.clone()).await;
    let serve_task = tokio::spawn(async move {
        gateway::serve::serve_on(
            listener,
            ctx,
            Vec::new(),
            ct_for_serve,
            supervisor_ct,
            shutdown,
        )
        .await
    });

    // Wait until axum is accepting.
    let ready_deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < ready_deadline,
            "serve_on never started accepting on {addr} within 5s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Open a connection and HOLD it for the lifetime of the test — this
    // is the "client that won't let go" that would wedge the drain if
    // handled naively.
    let _held_open = TcpStream::connect(addr)
        .await
        .expect("must be able to open a connection to hold");

    ct.cancel();

    // With the bound, serve_on must return within SHUTDOWN_DRAIN_GRACE
    // (3s) plus generous cleanup margin — NOT hang on the open socket.
    // The outer 8s timeout is the actual assertion: pre-fix this hung.
    let serve_result = tokio::time::timeout(Duration::from_secs(8), serve_task)
        .await
        .expect("serve_on must return within the bounded drain even with an open connection")
        .expect("serve_on task panicked");
    serve_result.expect("serve_on must return Ok after a bounded (forced) drain");
}
