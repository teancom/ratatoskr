// pattern: Imperative Shell
//
// Bridges upstream events onto a specific downstream session's peer.
//
// Architecture:
//   - Each downstream session holds an Arc<OnceLock<Peer<RoleServer>>>.
//   - On_initialized, we Set the peer (idempotent).
//   - At Set time (not Handler::new time) we spawn the forwarder task,
//     because we need the peer to exist before the task can do anything
//     useful. The spawn happens at most once per session.
//
// The forwarder owns a broadcast::Receiver and translates events into
// downstream notifications. It lives until either:
//   - The broadcast channel closes (bus dropped), or
//   - The cancellation token fires (session shutting down).
//
// `ResourceUpdated` is the only event variant that needs per-session
// filtering: the MCP spec requires `resources/updated` to be delivered
// only to peers that previously called `resources/subscribe` for the
// URI in question. The forwarder consults `SubscriptionRegistry`
// keyed on its own session ID and drops updates that don't belong
// to it. The list-changed variants are broadcast-style: every session
// that can see the gateway's downstream resource catalog sees the
// invalidation.

use std::sync::Arc;

use mcp_client::{UpstreamEvent, UpstreamEventBus, resource_uri};
use rmcp::{RoleServer, model::ResourceUpdatedNotificationParam, service::Peer};
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;

use crate::subscription_registry::{SubscriptionRegistry, UpstreamSubKey};

pub struct SessionForwarder {
    /// Cancelling this token stops the forwarder task. Dropping the
    /// SessionForwarder (via Drop) does the same — belt and suspenders.
    cancel: CancellationToken,
}

impl Drop for SessionForwarder {
    fn drop(&mut self) {
        // End of session: tell the forwarder task to exit. Without this,
        // the task would live until the broadcast channel itself closes
        // (which happens only when all event bus clones drop — including
        // the one held by the gateway, i.e. at process exit).
        self.cancel.cancel();
    }
}

impl SessionForwarder {
    /// Spawn a forwarder task that reads from the bus and notifies the
    /// given peer. The returned handle can be cancelled to stop forwarding
    /// when the session ends.
    ///
    /// `session_id` is `None` for stdio transports where there's no
    /// `Mcp-Session-Id` header to key subscriptions against. In that
    /// case the forwarder drops every `ResourceUpdated` event — stdio
    /// has no `resources/subscribe` path (see `GatewayHandler::subscribe`),
    /// so no update could be legitimately destined for that session
    /// anyway. List-changed events still flow normally.
    pub fn spawn(
        peer: Peer<RoleServer>,
        bus: Arc<UpstreamEventBus>,
        subscriptions: Arc<SubscriptionRegistry>,
        session_id: Option<String>,
    ) -> Self {
        let cancel = CancellationToken::new();
        let child_cancel = cancel.clone();

        tokio::spawn(async move {
            let mut rx = bus.subscribe();
            // Drop the bus Arc immediately after subscribing. If we held on to
            // it, the task's own clone would keep the underlying broadcast
            // Sender alive forever and the channel would never close even
            // after every other holder dropped — turning the "bus dropped"
            // shutdown signal (RecvError::Closed below) into a deadlock.
            drop(bus);
            tracing::debug!("session forwarder started");
            loop {
                tokio::select! {
                    _ = child_cancel.cancelled() => {
                        tracing::debug!("session forwarder cancelled");
                        break;
                    }
                    recv = rx.recv() => {
                        match recv {
                            Ok(event) => {
                                if let Err(e) = forward(
                                    &peer,
                                    &event,
                                    &subscriptions,
                                    session_id.as_deref(),
                                ).await {
                                    // Distinguish permanent (peer gone) from transient failures.
                                    // Transport-level errors on an outbound notification mean
                                    // the session's downstream transport is dead; there is no
                                    // recovery path for this task — every subsequent bus event
                                    // would generate the same warn, flooding logs until the
                                    // cancellation token eventually fires. Log once at debug
                                    // and exit. Transient errors (protocol-level, timeouts,
                                    // etc.) stay at warn and continue the loop, matching the
                                    // shape of the RecvError::Closed arm below.
                                    match e {
                                        rmcp::ServiceError::TransportClosed
                                        | rmcp::ServiceError::TransportSend(_) => {
                                            tracing::debug!(
                                                event = ?event,
                                                error = %e,
                                                "downstream peer gone; session forwarder exiting"
                                            );
                                            break;
                                        }
                                        _ => {
                                            tracing::warn!(
                                                event = ?event,
                                                error = %e,
                                                "failed to forward upstream event to downstream peer"
                                            );
                                        }
                                    }
                                }
                            }
                            Err(RecvError::Lagged(skipped)) => {
                                tracing::warn!(
                                    skipped,
                                    "downstream session lagged on upstream events; some notifications dropped"
                                );
                                // Fallthrough: broadcast receiver recovers automatically.
                            }
                            Err(RecvError::Closed) => {
                                tracing::debug!("upstream event bus closed; forwarder exiting");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Self { cancel }
    }
}

async fn forward(
    peer: &Peer<RoleServer>,
    event: &UpstreamEvent,
    subscriptions: &SubscriptionRegistry,
    session_id: Option<&str>,
) -> Result<(), rmcp::ServiceError> {
    match event {
        UpstreamEvent::ToolListChanged { upstream } => {
            // Semantic note: the gateway's downstream-facing tool list is
            // FIXED (ping, list-servers, list-server-tools, tool-details,
            // inspect-tool-response, execute). Upstream tool changes don't
            // affect what downstream sees via tools/list, so forwarding
            // would mis-signal "our tool list changed" when it hasn't.
            //
            // So: log the event, don't forward it.
            // Downstream still sees fresh tool data
            // whenever it calls list-server-tools (cache gets invalidated
            // automatically in the upstream client session).
            tracing::info!(
                %upstream,
                "upstream tools/list_changed — logged but not forwarded (gateway tools are fixed)"
            );
            Ok(())
        }
        UpstreamEvent::ResourceListChanged { upstream } => {
            // info!, not debug!: these events are rare (reload, respawn,
            // bootstrap retry) and each one is the only durable evidence
            // that a downstream session was told to refresh its resource
            // catalog — exactly what an operator needs when a client UI
            // shows a stale "0 resources" snapshot after a restart.
            tracing::info!(%upstream, "forwarding resources/list_changed to downstream");
            peer.notify_resource_list_changed().await
        }
        UpstreamEvent::ResourceUpdated { upstream, uri } => {
            // Session filter: only sessions that previously subscribed
            // to (upstream, uri) are entitled to this notification —
            // EITHER via a concrete subscription to the bare URI, OR
            // via a template subscription whose matcher accepts it.
            // Stdio sessions never have `resources/subscribe` wired
            // up, so `session_id == None` means "drop".
            let Some(session_id) = session_id else {
                return Ok(());
            };

            let concrete_hit = subscriptions.session_is_subscribed(
                session_id,
                &UpstreamSubKey::new(upstream.clone(), uri.clone()),
            );
            let template_hit = !concrete_hit
                && subscriptions
                    .template_sessions_for(upstream, uri)
                    .iter()
                    .any(|s| s == session_id);

            if !concrete_hit && !template_hit {
                return Ok(());
            }

            // Translate the upstream-native URI into the gateway's
            // namespaced form before handing it downstream. The
            // client subscribed with a `mcp-proxy://...` URI and
            // expects updates under the same scheme — delivering
            // the bare upstream URI would leak the routing layer
            // and break the client's subscription bookkeeping.
            //
            // Note: for TEMPLATE subscribers we still deliver the
            // namespaced CONCRETE URI, not the template URI. The
            // client subscribed to `mcp-proxy://rootly/incident%3A%2F%2F%7Bid%7D`
            // but expects to see updates scoped to real incidents:
            // `mcp-proxy://rootly/incident%3A%2F%2F42`. This matches
            // how MCP's spec frames template subscriptions (subscribe
            // to the SHAPE, get updates for concrete INSTANCES).
            let namespaced = resource_uri::encode(upstream, uri);
            tracing::debug!(
                %upstream,
                %uri,
                %session_id,
                %namespaced,
                concrete_hit,
                template_hit,
                "forwarding resources/updated to subscribed session"
            );
            peer.notify_resource_updated(ResourceUpdatedNotificationParam { uri: namespaced })
                .await
        }
        UpstreamEvent::PromptListChanged { upstream } => {
            tracing::debug!(%upstream, "forwarding prompts/list_changed to downstream");
            peer.notify_prompt_list_changed().await
        }
    }
}
