//! Unit tests covering the broker's edge cases independent of the gateway
//! harness. Integration coverage lives in
//! `crates/gateway/tests/sampling_roundtrip.rs`; these tests are fast,
//! deterministic, and do not require a full gateway stack.

use super::*;
use rmcp::{ServerHandler, ServiceExt, serve_client};

// ---- minimal in-process server used to obtain a real Peer<RoleServer> ----

/// A do-nothing MCP server. All methods use the ServerHandler defaults.
struct MinimalServer;
impl ServerHandler for MinimalServer {}

/// Spin up an in-process server+client pair and return the
/// `Peer<RoleServer>` that the server holds for the connected client.
/// The returned `RunningService` must stay alive for the duration of
/// the test (drop it to shut down the connection).
async fn make_peer() -> (
    rmcp::service::RunningService<RoleServer, MinimalServer>,
    rmcp::service::RunningService<rmcp::RoleClient, ()>,
) {
    let (server_side, client_side) = tokio::io::duplex(64 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_side);
    let (cli_r, cli_w) = tokio::io::split(client_side);

    // Spawn the server and client concurrently — each blocks until the
    // MCP initialize handshake completes.
    let (server_svc, client_svc) = tokio::join!(
        MinimalServer.serve((srv_r, srv_w)),
        serve_client((), (cli_r, cli_w)),
    );
    (
        server_svc.expect("server serve"),
        client_svc.expect("client serve"),
    )
}

// ---- tests ----

#[tokio::test]
async fn dispatch_returns_error_when_no_peers_registered() {
    // Property: dispatching to a broker with zero registered peers returns
    // an Err carrying INTERNAL_ERROR. The empty-peer branch must not hang,
    // panic, or produce a misleading error variant.
    let broker = SamplingBroker::new();
    let params = CreateMessageRequestParams::new(vec![], 1);
    let err = broker
        .dispatch(params)
        .await
        .expect_err("dispatch with no peers must return Err");
    assert_eq!(
        err.code,
        ErrorCode::INTERNAL_ERROR,
        "expected INTERNAL_ERROR for empty peer list, got {:?}",
        err.code
    );
}

#[tokio::test]
async fn deregister_unknown_id_is_safe() {
    // Property: calling deregister with a PeerId that was never
    // registered must be a safe no-op — no panic, no spurious insertion,
    // and peer_count stays at 0.
    let broker = SamplingBroker::new();
    let phantom = PeerId(999);
    broker.deregister(phantom).await;
    assert_eq!(
        broker.peer_count().await,
        0,
        "deregister of unknown id must leave peer_count at 0"
    );
}

#[tokio::test]
async fn register_then_deregister_removes_peer() {
    // Property: the lifecycle 0 -> 1 -> 0 is enforced by the RwLock.
    // After register the count is 1 and is_registered returns true; after
    // deregister both reset to 0 / false.
    let broker = SamplingBroker::new();
    let (server_svc, _client_svc) = make_peer().await;

    let peer: Peer<RoleServer> = server_svc.peer().clone();
    let id = broker.register(peer).await;

    assert_eq!(
        broker.peer_count().await,
        1,
        "peer_count must be 1 after register"
    );
    assert!(
        broker.is_registered(id).await,
        "is_registered must be true after register"
    );

    broker.deregister(id).await;

    assert_eq!(
        broker.peer_count().await,
        0,
        "peer_count must be 0 after deregister"
    );
    assert!(
        !broker.is_registered(id).await,
        "is_registered must be false after deregister"
    );

    // Explicit shutdown to avoid the RunningService drop-warning.
    let _ = server_svc.cancel().await;
}
