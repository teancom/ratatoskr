//! pattern: Imperative Shell
//!
//! Owns the ref-counted subscription protocol. The handler is a thin
//! adapter that translates between MCP-level `ErrorData` and this
//! service's domain errors; this module owns the state-machine
//! transitions, rollback policy, and upstream RPC fan-out.
//!
//! Layering:
//!
//!   - `SubscriptionRegistry` is the pure state structure (add /
//!     remove / drop_session / fan-out lookup). No RPCs, no I/O.
//!     Indexes concrete and template subscriptions separately because
//!     the upstream wire-shape differs.
//!   - `SubscriptionService` (this file) owns the registry plus the
//!     upstream registry, decides when to fan out a `resources/subscribe`
//!     / `resources/unsubscribe` RPC based on the ref-count delta, and
//!     rolls back registry writes when the upstream call fails.
//!   - Handler layer calls into this service; see
//!     `crate::handler::subscriptions`.
//!
//! Why pull this out of the handler:
//!
//! - Session-close teardown in `InspectingSessionManager` used to call
//!   two free helper functions (`teardown_orphaned_subscriptions` and
//!   `teardown_orphaned_template_subscriptions`). Both are folded into
//!   `drop_session` here, so `close_session` is one await.
//! - The handler's subscribe/unsubscribe methods drop to ~10 lines
//!   each — just the "session id required" check plus a domain-to-MCP
//!   error mapper.
//! - Tests that pin state-machine behaviour now live next to the state
//!   machine, not next to the HTTP surface.
//!
//! Rollback discipline: every registry write that depends on a
//! subsequent upstream RPC reverses itself on upstream failure. Without
//! rollback, a transient upstream failure would permanently wedge the
//! ref-count — either "upstream already subscribed" from our POV when
//! the upstream has nothing registered, or a zombie subscription still
//! streaming us updates after the last downstream session has gone.

use std::sync::Arc;

use mcp_client::{
    UpstreamManager, resource_uri,
    template_matcher::{CompileError, CompiledTemplate},
};
use thiserror::Error;

use crate::subscription_registry::{
    SubscribeDelta, SubscriptionRegistry, UnsubscribeDelta, UpstreamSubKey, UpstreamTemplateKey,
};

/// Errors surfaced by `SubscriptionService::subscribe`. Each variant
/// carries the context the handler needs to build a JSON-RPC error
/// payload — the handler decides whether to map a given variant to
/// `invalid_params` or `internal_error`.
#[derive(Debug, Error)]
pub enum SubscribeError {
    /// The namespaced URI decoded successfully but named an upstream
    /// no longer registered. Typically client error (the catalog is
    /// stale), so the handler maps this to `invalid_params`.
    #[error("unknown upstream server: {upstream}")]
    UnknownUpstream { upstream: String },

    /// The namespaced URI failed to decode. Malformed scheme, missing
    /// authority, bad percent-encoding. `invalid_params` at the
    /// handler boundary.
    #[error("failed to decode resource URI '{uri}': {source}")]
    DecodeUri {
        uri: String,
        #[source]
        source: resource_uri::DecodeError,
    },

    /// The decoded template string was rejected by the RFC 6570
    /// compiler with a level-3/4 construct the gateway doesn't
    /// support. `invalid_params` — the client's request is wrong in a
    /// fixable way.
    #[error(
        "template '{template}' uses an RFC 6570 construct the gateway does not support (Level 1-2 only): {source}"
    )]
    UnsupportedTemplate {
        template: String,
        #[source]
        source: CompileError,
    },

    /// The decoded template string was malformed (unterminated
    /// expression, empty expression, invalid var character).
    /// `invalid_params`.
    #[error("template '{template}' is malformed: {source}")]
    MalformedTemplate {
        template: String,
        #[source]
        source: CompileError,
    },

    /// The upstream `resources/subscribe` RPC failed after we already
    /// updated the registry. The registry write has been rolled back
    /// so a client retry gets a fresh `FirstSubscriber` delta.
    /// `internal_error` at the handler boundary — the client's request
    /// was well-formed; the upstream failed.
    #[error("upstream '{upstream}' subscribe failed: {source}")]
    UpstreamCall {
        upstream: String,
        #[source]
        source: rmcp::ServiceError,
    },
}

/// Errors surfaced by `SubscriptionService::unsubscribe`. No template
/// compile errors here — unsubscribe is keyed by the raw string, and
/// the compile only matters for rollback (which is best-effort anyway).
#[derive(Debug, Error)]
pub enum UnsubscribeError {
    #[error("unknown upstream server: {upstream}")]
    UnknownUpstream { upstream: String },

    #[error("failed to decode resource URI '{uri}': {source}")]
    DecodeUri {
        uri: String,
        #[source]
        source: resource_uri::DecodeError,
    },

    #[error("upstream '{upstream}' unsubscribe failed: {source}")]
    UpstreamCall {
        upstream: String,
        #[source]
        source: rmcp::ServiceError,
    },
}

/// Service that owns the ref-counted subscription state machine plus
/// the upstream fan-out policy.
///
/// Cheap to clone — both fields are `Arc`.
#[derive(Clone)]
pub struct SubscriptionService {
    registry: Arc<SubscriptionRegistry>,
    upstreams: Arc<UpstreamManager>,
}

impl SubscriptionService {
    pub fn new(registry: Arc<SubscriptionRegistry>, upstreams: Arc<UpstreamManager>) -> Self {
        Self {
            registry,
            upstreams,
        }
    }

    /// Subscribe `session_id` to `namespaced_uri`. Branches on the
    /// decoded upstream URI shape:
    ///   - Contains `{` → compiled as an RFC 6570 template and stored
    ///     in the template index.
    ///   - Otherwise → stored in the concrete index.
    ///
    /// The first subscriber per `(upstream, key)` triggers an upstream
    /// `resources/subscribe`; subsequent subscribers just bump the ref
    /// count.
    pub async fn subscribe(
        &self,
        session_id: &str,
        namespaced_uri: &str,
    ) -> Result<(), SubscribeError> {
        let (lua_name, upstream_uri) =
            resource_uri::decode(namespaced_uri).map_err(|source| SubscribeError::DecodeUri {
                uri: namespaced_uri.to_string(),
                source,
            })?;

        let handle = self
            .upstreams
            .handle_by_lua_name(&lua_name)
            .await
            .ok_or_else(|| SubscribeError::UnknownUpstream {
                upstream: lua_name.clone(),
            })?;

        if looks_like_template(&upstream_uri) {
            self.subscribe_template_inner(session_id, lua_name, upstream_uri, handle)
                .await
        } else {
            self.subscribe_concrete_inner(session_id, lua_name, upstream_uri, handle)
                .await
        }
    }

    /// Unsubscribe `session_id` from `namespaced_uri`. Mirror of
    /// `subscribe`; the last-subscriber delta triggers an upstream
    /// `resources/unsubscribe`.
    pub async fn unsubscribe(
        &self,
        session_id: &str,
        namespaced_uri: &str,
    ) -> Result<(), UnsubscribeError> {
        let (lua_name, upstream_uri) =
            resource_uri::decode(namespaced_uri).map_err(|source| UnsubscribeError::DecodeUri {
                uri: namespaced_uri.to_string(),
                source,
            })?;

        if looks_like_template(&upstream_uri) {
            self.unsubscribe_template_inner(session_id, lua_name, upstream_uri)
                .await
        } else {
            self.unsubscribe_concrete_inner(session_id, lua_name, upstream_uri)
                .await
        }
    }

    /// Tear down every subscription owned by `session_id`, both
    /// concrete and template. Best-effort on the upstream RPCs: an
    /// upstream that's gone or fails to unsubscribe gets logged and
    /// skipped, because the local registry state has already been
    /// dropped — the alternative would be to block session close on a
    /// flaky upstream.
    ///
    /// Called from `InspectingSessionManager::close_session`. Folds
    /// the pre-service `teardown_orphaned_subscriptions` +
    /// `teardown_orphaned_template_subscriptions` helpers into one
    /// entry point so "when rmcp closes a session, what also needs to
    /// go?" has one answer.
    pub async fn drop_session(&self, session_id: &str) {
        let orphaned_concrete = self.registry.drop_session(session_id);
        let orphaned_templates = self.registry.drop_session_templates(session_id);
        self.teardown_concrete(session_id, orphaned_concrete).await;
        self.teardown_templates(session_id, orphaned_templates)
            .await;
    }

    // ---- internal helpers ----

    async fn subscribe_concrete_inner(
        &self,
        session_id: &str,
        lua_name: String,
        upstream_uri: String,
        handle: Arc<mcp_client::UpstreamHandle>,
    ) -> Result<(), SubscribeError> {
        let key = UpstreamSubKey::new(lua_name.clone(), upstream_uri.clone());
        let delta = self.registry.add_concrete(session_id, key.clone());

        match delta {
            SubscribeDelta::FirstSubscriber => {
                let params = rmcp::model::SubscribeRequestParams::new(upstream_uri);
                if let Err(source) = handle.subscribe(params).await {
                    // Roll back the registry entry so a retry gets a
                    // fresh FirstSubscriber delta. Without rollback a
                    // transient upstream failure would permanently
                    // mark the key as "upstream already subscribed"
                    // from our point of view, silently breaking
                    // delivery forever.
                    self.registry.remove_concrete(session_id, &key);
                    return Err(SubscribeError::UpstreamCall {
                        upstream: lua_name,
                        source,
                    });
                }
                tracing::debug!(
                    %lua_name,
                    uri = %key.uri,
                    %session_id,
                    "first concrete subscriber; issued upstream subscribe"
                );
            }
            SubscribeDelta::AdditionalSubscriber => {
                tracing::debug!(
                    %lua_name,
                    uri = %key.uri,
                    %session_id,
                    "additional concrete subscriber; upstream already subscribed"
                );
            }
            SubscribeDelta::AlreadySubscribed => {
                tracing::debug!(
                    %lua_name,
                    uri = %key.uri,
                    %session_id,
                    "session already subscribed to concrete key; no-op"
                );
            }
        }
        Ok(())
    }

    async fn subscribe_template_inner(
        &self,
        session_id: &str,
        lua_name: String,
        template_str: String,
        handle: Arc<mcp_client::UpstreamHandle>,
    ) -> Result<(), SubscribeError> {
        // Compile first: Level 3-4 rejected here with a clean
        // `UnsupportedTemplate` so the client sees "unsupported
        // operator" rather than "subscribe timed out" or silent
        // no-delivery.
        let compiled = CompiledTemplate::compile(&template_str).map_err(|e| match e {
            CompileError::UnsupportedOperator { .. }
            | CompileError::ListExpression { .. }
            | CompileError::UnsupportedModifier { .. }
            | CompileError::MultipleFragments
            | CompileError::FragmentNotLast => SubscribeError::UnsupportedTemplate {
                template: template_str.clone(),
                source: e,
            },
            CompileError::UnterminatedExpression
            | CompileError::EmptyExpression
            | CompileError::InvalidVarChar { .. } => SubscribeError::MalformedTemplate {
                template: template_str.clone(),
                source: e,
            },
        })?;

        let key = UpstreamTemplateKey::new(lua_name.clone(), template_str.clone());
        let delta = self
            .registry
            .add_template(session_id, key.clone(), compiled);

        match delta {
            SubscribeDelta::FirstSubscriber => {
                let params = rmcp::model::SubscribeRequestParams::new(template_str);
                if let Err(source) = handle.subscribe(params).await {
                    self.registry.remove_template(session_id, &key);
                    return Err(SubscribeError::UpstreamCall {
                        upstream: lua_name,
                        source,
                    });
                }
                tracing::debug!(
                    %lua_name,
                    template = %key.template,
                    %session_id,
                    "first template subscriber; issued upstream subscribe"
                );
            }
            SubscribeDelta::AdditionalSubscriber => {
                tracing::debug!(
                    %lua_name,
                    template = %key.template,
                    %session_id,
                    "additional template subscriber; upstream already subscribed"
                );
            }
            SubscribeDelta::AlreadySubscribed => {
                tracing::debug!(
                    %lua_name,
                    template = %key.template,
                    %session_id,
                    "session already subscribed to template; no-op"
                );
            }
        }
        Ok(())
    }

    async fn unsubscribe_concrete_inner(
        &self,
        session_id: &str,
        lua_name: String,
        upstream_uri: String,
    ) -> Result<(), UnsubscribeError> {
        let key = UpstreamSubKey::new(lua_name.clone(), upstream_uri.clone());
        let delta = self.registry.remove_concrete(session_id, &key);

        match delta {
            UnsubscribeDelta::LastSubscriberGone => {
                // Resolve the handle before we have any chance of needing
                // rollback — if the upstream is gone we re-add the session
                // to the registry first, so a retry sees `LastSubscriberGone`
                // again and re-attempts the upstream teardown.
                let handle = match self.upstreams.handle_by_lua_name(&lua_name).await {
                    Some(h) => h,
                    None => {
                        self.registry.add_concrete(session_id, key);
                        return Err(UnsubscribeError::UnknownUpstream { upstream: lua_name });
                    }
                };
                let params = rmcp::model::UnsubscribeRequestParams::new(upstream_uri);
                if let Err(source) = handle.unsubscribe(params).await {
                    // Roll back the registry removal so the session
                    // entry re-appears. Without rollback the upstream
                    // keeps streaming ResourceUpdated events for a
                    // subscription that has no downstream tracking —
                    // a zombie that lives until gateway restart. With
                    // rollback, the client's retry sees
                    // LastSubscriberGone and re-attempts the upstream
                    // call, mirroring the subscribe rollback pattern.
                    self.registry.add_concrete(session_id, key);
                    return Err(UnsubscribeError::UpstreamCall {
                        upstream: lua_name,
                        source,
                    });
                }
                tracing::debug!(
                    %lua_name,
                    uri = %key.uri,
                    %session_id,
                    "last concrete subscriber gone; issued upstream unsubscribe"
                );
            }
            UnsubscribeDelta::OtherSubscribersRemain => {
                tracing::debug!(
                    %lua_name,
                    uri = %key.uri,
                    %session_id,
                    "concrete subscriber left; others remain"
                );
            }
            UnsubscribeDelta::NotSubscribed => {
                tracing::debug!(
                    %lua_name,
                    uri = %key.uri,
                    %session_id,
                    "unsubscribe from unknown concrete sub; no-op"
                );
            }
        }
        Ok(())
    }

    async fn unsubscribe_template_inner(
        &self,
        session_id: &str,
        lua_name: String,
        template_str: String,
    ) -> Result<(), UnsubscribeError> {
        let key = UpstreamTemplateKey::new(lua_name.clone(), template_str.clone());
        let delta = self.registry.remove_template(session_id, &key);

        match delta {
            UnsubscribeDelta::LastSubscriberGone => {
                // Mirror the concrete rollback pattern. If the upstream
                // call fails after we've already removed the registry
                // entry, re-add the session so a retry can succeed. We
                // re-compile from `template_str` on the rollback path;
                // compilation is cheap and deterministic so this avoids
                // cloning the `CompiledTemplate` on the happy path.
                let handle = match self.upstreams.handle_by_lua_name(&lua_name).await {
                    Some(h) => h,
                    None => {
                        if let Ok(compiled) = CompiledTemplate::compile(&template_str) {
                            self.registry.add_template(session_id, key, compiled);
                        }
                        return Err(UnsubscribeError::UnknownUpstream { upstream: lua_name });
                    }
                };
                let params = rmcp::model::UnsubscribeRequestParams::new(template_str.clone());
                if let Err(source) = handle.unsubscribe(params).await {
                    if let Ok(compiled) = CompiledTemplate::compile(&template_str) {
                        self.registry.add_template(session_id, key, compiled);
                    }
                    return Err(UnsubscribeError::UpstreamCall {
                        upstream: lua_name,
                        source,
                    });
                }
                tracing::debug!(
                    %lua_name,
                    template = %key.template,
                    %session_id,
                    "last template subscriber gone; issued upstream unsubscribe"
                );
            }
            UnsubscribeDelta::OtherSubscribersRemain => {
                tracing::debug!(
                    %lua_name,
                    template = %key.template,
                    %session_id,
                    "template subscriber left; others remain"
                );
            }
            UnsubscribeDelta::NotSubscribed => {
                tracing::debug!(
                    %lua_name,
                    template = %key.template,
                    %session_id,
                    "unsubscribe from unknown template; no-op"
                );
            }
        }
        Ok(())
    }

    /// Issue upstream `resources/unsubscribe` for every concrete key
    /// whose last subscriber was the session that just closed.
    ///
    /// Best-effort: an upstream that fails (gone, crashed, doesn't
    /// support unsubscribe, whatever) gets logged and skipped. The
    /// registry entry has already been removed, so a failed upstream
    /// teardown just means the upstream keeps streaming us updates
    /// we'll ignore — wasteful but not incorrect. A gateway restart
    /// clears any drift.
    async fn teardown_concrete(&self, session_id: &str, orphaned: Vec<UpstreamSubKey>) {
        if orphaned.is_empty() {
            return;
        }
        tracing::debug!(
            %session_id,
            orphan_count = orphaned.len(),
            "tearing down upstream concrete subscriptions orphaned by session close"
        );
        for key in orphaned {
            let Some(handle) = self.upstreams.handle_by_lua_name(&key.upstream).await else {
                // Upstream is gone — nothing to unsubscribe from.
                // Silent: this can happen during gateway shutdown where
                // the manager has already been drained.
                tracing::debug!(
                    %session_id,
                    upstream = %key.upstream,
                    uri = %key.uri,
                    "upstream no longer registered; skipping orphan unsubscribe"
                );
                continue;
            };
            let params = rmcp::model::UnsubscribeRequestParams::new(key.uri.clone());
            if let Err(e) = handle.unsubscribe(params).await {
                tracing::warn!(
                    %session_id,
                    upstream = %key.upstream,
                    uri = %key.uri,
                    error = %e,
                    "upstream orphan unsubscribe failed; dropping local state anyway"
                );
            }
        }
    }

    /// Template-side counterpart to [`Self::teardown_concrete`]. The
    /// upstream RPC carries the original template string, not a
    /// concrete URI — the upstream remembers its subscriptions by the
    /// shape it accepted.
    async fn teardown_templates(&self, session_id: &str, orphaned: Vec<UpstreamTemplateKey>) {
        if orphaned.is_empty() {
            return;
        }
        tracing::debug!(
            %session_id,
            orphan_count = orphaned.len(),
            "tearing down upstream template subscriptions orphaned by session close"
        );
        for key in orphaned {
            let Some(handle) = self.upstreams.handle_by_lua_name(&key.upstream).await else {
                tracing::debug!(
                    %session_id,
                    upstream = %key.upstream,
                    template = %key.template,
                    "upstream no longer registered; skipping orphan template unsubscribe"
                );
                continue;
            };
            let params = rmcp::model::UnsubscribeRequestParams::new(key.template.clone());
            if let Err(e) = handle.unsubscribe(params).await {
                tracing::warn!(
                    %session_id,
                    upstream = %key.upstream,
                    template = %key.template,
                    error = %e,
                    "upstream orphan template unsubscribe failed; dropping local state anyway"
                );
            }
        }
    }
}

/// Heuristic: any `{` in the bare URI means a Level 1-2+ template.
/// Upstream-native URIs in practice don't contain literal `{`, and if
/// one ever did, the author would need to percent-encode it anyway —
/// so this heuristic is safe and avoids compiling every subscribe URI
/// through the matcher just to discover it was concrete.
fn looks_like_template(uri: &str) -> bool {
    uri.contains('{')
}

#[cfg(test)]
#[path = "subscription_service_tests.rs"]
mod tests;
