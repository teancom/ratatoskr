// pattern: Imperative Shell
//
// Subscriptions family (handler side): the `resources/subscribe` and
// `resources/unsubscribe` MCP trait methods. This module is a thin
// adapter — the ref-counted state machine, rollback policy, and
// upstream fan-out all live in `crate::subscription_service::SubscriptionService`.
//
// Responsibilities left at this layer:
//
//   1. Require an HTTP session. Stdio transport has no session
//      identity to ref-count against, so subscriptions are rejected
//      with `invalid_params` at this boundary. The service never sees
//      the stdio case.
//   2. Map `SubscribeError` / `UnsubscribeError` (domain errors) to
//      JSON-RPC `ErrorData` (wire errors). Decode/compile/unknown-
//      upstream → `invalid_params`; upstream-call failures →
//      `internal_error`. Matches the pre-service behaviour verbatim
//      so the wire contract doesn't drift.
//
// State-machine tests live alongside the service in
// `crate::subscription_service::tests`; this module has no test code
// because every line is a five-line delegation.

use rmcp::ErrorData;

use super::*;
use crate::subscription_service::{SubscribeError, UnsubscribeError};

impl GatewayHandler {
    /// Handle `resources/subscribe`: require an HTTP session, then
    /// delegate to the service.
    pub(super) async fn call_subscribe(
        &self,
        request: SubscribeRequestParams,
        context: &RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        let session_id = require_session(session_id_from_context(context))?;
        let Some(session_id) = session_id else {
            // stdio transport has no session identity to ref-count
            // against. Subscriptions in stdio would have no way to
            // flow updates back (there's no persistent notification
            // stream to push to), so reject cleanly rather than
            // quietly dropping the call on the floor.
            return Err(ErrorData::invalid_params(
                "resources/subscribe requires an HTTP session; stdio transport is not supported"
                    .to_string(),
                None,
            ));
        };
        self.state
            .ctx
            .subscription_service
            .subscribe(&session_id, &request.uri)
            .await
            .map_err(subscribe_error_to_rmcp)
    }

    /// Handle `resources/unsubscribe`: mirror of `call_subscribe`.
    pub(super) async fn call_unsubscribe(
        &self,
        request: UnsubscribeRequestParams,
        context: &RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        let session_id = require_session(session_id_from_context(context))?;
        let Some(session_id) = session_id else {
            return Err(ErrorData::invalid_params(
                "resources/unsubscribe requires an HTTP session; stdio transport is not supported"
                    .to_string(),
                None,
            ));
        };
        self.state
            .ctx
            .subscription_service
            .unsubscribe(&session_id, &request.uri)
            .await
            .map_err(unsubscribe_error_to_rmcp)
    }
}

/// Translate a `SubscribeError` into the JSON-RPC `ErrorData` the MCP
/// protocol expects downstream. Preserves the pre-service wire
/// messages verbatim so client-side error-handling code doesn't need
/// to change.
///
/// Policy:
///   - Client-fixable errors (decode, unknown upstream, unsupported
///     or malformed template) → `invalid_params`. The caller's
///     request is structurally wrong in a way they can correct.
///   - Upstream call failures → `internal_error`. The request was
///     well-formed; the failure is infrastructural.
fn subscribe_error_to_rmcp(err: SubscribeError) -> ErrorData {
    match err {
        SubscribeError::DecodeUri { .. }
        | SubscribeError::UnknownUpstream { .. }
        | SubscribeError::UnsupportedTemplate { .. }
        | SubscribeError::MalformedTemplate { .. } => {
            ErrorData::invalid_params(err.to_string(), None)
        }
        SubscribeError::UpstreamCall { .. } => ErrorData::internal_error(err.to_string(), None),
    }
}

/// Translate an `UnsubscribeError` into `ErrorData`. Same policy as
/// `subscribe_error_to_rmcp` — client-fixable errors become
/// `invalid_params`, upstream failures become `internal_error`.
fn unsubscribe_error_to_rmcp(err: UnsubscribeError) -> ErrorData {
    match err {
        UnsubscribeError::DecodeUri { .. } | UnsubscribeError::UnknownUpstream { .. } => {
            ErrorData::invalid_params(err.to_string(), None)
        }
        UnsubscribeError::UpstreamCall { .. } => ErrorData::internal_error(err.to_string(), None),
    }
}
