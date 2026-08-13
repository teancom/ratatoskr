// pattern: Imperative Shell
//
// Sampling broker: routes upstream-initiated `sampling/createMessage`
// requests to a connected downstream client peer.
//
// Security note: this broker only accepts peer registrations that have
// advertised the `sampling` capability. The per-upstream opt-in (whether
// a specific upstream is ALLOWED to send sampling requests) is enforced
// at a different layer — in the upstream handler — before the request
// ever reaches the broker. This file is about "given a permitted request,
// how do we dispatch it".

use std::sync::Arc;

use rmcp::{
    ErrorData, RoleServer,
    model::{CreateMessageRequestParams, CreateMessageResult, ErrorCode},
    service::Peer,
};
use tokio::sync::RwLock;

/// An opaque identifier for a registered downstream peer. Sessions use this
/// to deregister themselves on shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId(u64);

#[derive(Default)]
pub struct SamplingBroker {
    inner: RwLock<BrokerState>,
}

#[derive(Default)]
struct BrokerState {
    /// Monotonic counter for PeerId allocation. Never reused even after
    /// deregister — makes lifecycle bugs easier to spot in logs.
    next_id: u64,
    peers: Vec<Registration>,
}

struct Registration {
    id: PeerId,
    peer: Peer<RoleServer>,
}

impl SamplingBroker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a downstream peer as eligible for sampling requests.
    /// Callers must have verified the peer's ClientCapabilities include
    /// `sampling` before calling — this method does not re-check.
    pub async fn register(&self, peer: Peer<RoleServer>) -> PeerId {
        let mut state = self.inner.write().await;
        state.next_id += 1;
        let id = PeerId(state.next_id);
        state.peers.push(Registration { id, peer });
        tracing::debug!(peer_id = ?id, peer_count = state.peers.len(), "downstream peer registered for sampling");
        id
    }

    pub async fn deregister(&self, id: PeerId) {
        let mut state = self.inner.write().await;
        let before = state.peers.len();
        state.peers.retain(|r| r.id != id);
        let after = state.peers.len();
        if before == after {
            tracing::warn!(peer_id = ?id, "deregister called for unknown PeerId — already removed or never registered");
        }
        tracing::debug!(peer_id = ?id, removed = before - after, remaining = after, "peer deregistered");
    }

    /// Dispatch a sampling request. Picks the oldest registered peer
    /// (first in the list) — deliberately the simplest thing that works
    /// for a single-operator gateway. Load-balancing or letting upstreams
    /// target specific downstreams would only matter with real concurrent
    /// downstream sessions.
    ///
    /// Returns an MCP error if no peer is currently available.
    pub async fn dispatch(
        &self,
        params: CreateMessageRequestParams,
    ) -> Result<CreateMessageResult, ErrorData> {
        let peer = {
            let state = self.inner.read().await;
            state.peers.first().map(|r| r.peer.clone())
        };

        let peer = peer.ok_or_else(|| {
            ErrorData::new(
                ErrorCode::INTERNAL_ERROR,
                "no downstream client with sampling capability is connected",
                None,
            )
        })?;

        peer.create_message(params).await.map_err(|e| {
            tracing::warn!(error = %e, "downstream peer.create_message failed");
            ErrorData::new(
                ErrorCode::INTERNAL_ERROR,
                format!("downstream sampling failed: {e}"),
                None,
            )
        })
    }

    /// Test/observability helper: how many peers are currently registered.
    pub async fn peer_count(&self) -> usize {
        self.inner.read().await.peers.len()
    }

    /// Return `true` if the given `id` is currently in the registered peer
    /// list. Cheaper than `peer_count()` for a caller that already has the
    /// specific `PeerId` it wants to confirm (e.g. a test polling for
    /// "my peer is live" without conflating it with other peers).
    pub async fn is_registered(&self, id: PeerId) -> bool {
        self.inner.read().await.peers.iter().any(|r| r.id == id)
    }
}

/// Shared handle to the broker. Used by both upstreams (to dispatch
/// sampling requests) and downstream sessions (to register their peers).
pub type SharedSamplingBroker = Arc<SamplingBroker>;

#[cfg(test)]
#[path = "sampling_tests.rs"]
mod tests;
