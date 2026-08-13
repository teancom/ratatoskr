// pattern: Imperative Shell
//
// Thin wrapper around `LocalSessionManager` that drops the tool-
// inspection store's per-session entry when rmcp's streamable-http
// server closes a session.
//
// Without this, inspection records would accumulate for the lifetime
// of the gateway process. Single-dev-laptop usage wouldn't hit memory
// pressure for a long time — each entry is a small `HashSet<(String,
// String)>` — but leaving the growth unbounded is a latent landmine
// we'd rather not have.
//
// Coverage note: rmcp's LocalSessionManager only calls `close_session`
// on an explicit HTTP DELETE with `Mcp-Session-Id`. A session that
// expires via keep-alive timeout does NOT call close_session; the
// worker just stops and the entry in the session map becomes stale.
// We end up with the same orphan condition rmcp has on its own
// `sessions` map (worker handle, channels, etc.) — every inspection
// entry we leak is dwarfed by the rmcp-internal session record it
// shadows. If session GC ever matters, fix it in rmcp; mirroring the
// limitation here costs us a fraction of what rmcp itself loses.

use futures::Stream;
use rmcp::model::{ClientJsonRpcMessage, ServerJsonRpcMessage};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::session::{SessionId, SessionManager};

use crate::app_context::AppContext;

/// Wraps `LocalSessionManager`, delegating every trait method, and
/// adds a side effect to `close_session` that clears the inspection
/// store entry, offloaded results, and subscription state for that
/// session.
///
/// Holds the inner manager by value: `StreamableHttpService::new` takes
/// `Arc<S: SessionManager>`, so the outer Arc supplies all the shared
/// ownership we need. The trait methods only require `&self`.
///
/// `ctx` carries the process-wide handles this manager needs on
/// session close: the inspection store, result store, subscription
/// registry, and upstream registry (for teardown RPCs). See
/// `app_context.rs` for the sharing model.
pub struct InspectingSessionManager {
    inner: LocalSessionManager,
    ctx: AppContext,
}

impl InspectingSessionManager {
    pub fn new(inner: LocalSessionManager, ctx: AppContext) -> Self {
        Self { inner, ctx }
    }
}

impl SessionManager for InspectingSessionManager {
    // Inherit associated types from the wrapped manager. Using the
    // same concrete types means callers swap from
    // `Arc<LocalSessionManager>` to `Arc<InspectingSessionManager>`
    // without any other type annotations changing.
    type Error = <LocalSessionManager as SessionManager>::Error;
    type Transport = <LocalSessionManager as SessionManager>::Transport;

    async fn create_session(&self) -> Result<(SessionId, Self::Transport), Self::Error> {
        self.inner.create_session().await
    }

    async fn initialize_session(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        self.inner.initialize_session(id, message).await
    }

    async fn has_session(&self, id: &SessionId) -> Result<bool, Self::Error> {
        self.inner.has_session(id).await
    }

    async fn close_session(&self, id: &SessionId) -> Result<(), Self::Error> {
        // Forget our session-scoped caches first, then delegate. With
        // today's `LocalSessionManager` the ordering is academic (see
        // the module-level note on error behavior), but preserving
        // store-first ordering is cheap discipline against a future
        // inner that leaves the session reachable on its own error.
        //
        // We clear every session-scoped cache in this one place so
        // the "when rmcp closes a session, what also needs to go?"
        // question has exactly one answer: add it here.
        self.ctx.inspection.forget_session(id.as_ref());
        self.ctx.result_store.forget_session(id.as_ref());

        // Subscription teardown is one await: the service owns both
        // the registry drop and the upstream unsubscribe fan-out,
        // concrete + template. Before the service extraction this
        // lived as two free helpers in this file; folding them into
        // `drop_session` means the policy "what does orphan cleanup
        // look like?" has exactly one home.
        self.ctx
            .subscription_service
            .drop_session(id.as_ref())
            .await;

        tracing::debug!(
            session_id = %id.as_ref(),
            "cleared tool inspection, execution-result, and subscription state for closed session"
        );
        self.inner.close_session(id).await
    }

    async fn create_stream(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<
        impl Stream<Item = rmcp::transport::streamable_http_server::session::ServerSseMessage>
        + Send
        + Sync
        + 'static,
        Self::Error,
    > {
        self.inner.create_stream(id, message).await
    }

    async fn accept_message(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        self.inner.accept_message(id, message).await
    }

    async fn create_standalone_stream(
        &self,
        id: &SessionId,
    ) -> Result<
        impl Stream<Item = rmcp::transport::streamable_http_server::session::ServerSseMessage>
        + Send
        + Sync
        + 'static,
        Self::Error,
    > {
        self.inner.create_standalone_stream(id).await
    }

    async fn resume(
        &self,
        id: &SessionId,
        last_event_id: String,
    ) -> Result<
        impl Stream<Item = rmcp::transport::streamable_http_server::session::ServerSseMessage>
        + Send
        + Sync
        + 'static,
        Self::Error,
    > {
        self.inner.resume(id, last_event_id).await
    }
}

#[cfg(test)]
#[path = "inspecting_session_manager_tests.rs"]
mod tests;
