// pattern: Imperative Shell
//
// Session-scoped registry of live upstream MCP connections. Owns the handles;
// cleanup happens when the UpstreamManager is dropped or close() is called.
//
// ## Subprocess-cleanup survivability matrix
//
// stdio upstreams spawn a child process (typically `npx … mcp-server-…`
// under a `ProcessGroup::leader()` so the pid is the group id). The
// gateway guarantees reap of that child under the following exit paths,
// with the strength of the guarantee varying by path:
//
// | Exit path                                         | Mechanism                                               | Strength        | Pinned by                                              |
// |---------------------------------------------------|---------------------------------------------------------|-----------------|--------------------------------------------------------|
// | Graceful shutdown (SIGINT/SIGTERM)                | `serve_on` → `close_all_upstreams` → `UpstreamHandle::close()` → `service.cancel().await` | **Guaranteed** — bounded by test budget                     | `crates/gateway/tests/shutdown_closes_upstreams.rs` + `crates/gateway/tests/upstream_cleanup.rs::upstream_handle_close_reaps_child_within_budget` |
// | Panic in a handler-shaped task (SessionState Drop spawn) | Panic caught by tokio, no handle reference held by task | **Guaranteed** (structural — task never owned the handle) | `crates/gateway/tests/abnormal_exit_cleanup.rs::handler_task_panic_does_not_leak_stdio_child` |
// | Panic in a forwarder task (`session_forwarder.rs`) | Same — forwarder holds a Peer, not an UpstreamHandle    | **Guaranteed** (structural)                              | `crates/gateway/tests/abnormal_exit_cleanup.rs::forwarder_task_panic_does_not_leak_stdio_child` |
// | Panic in an invalidator task (`tool_list_cache.rs`, `resource_list_cache.rs`) | Same — invalidator holds a cache, not an UpstreamHandle | **Guaranteed** (structural)                              | `crates/gateway/tests/abnormal_exit_cleanup.rs::invalidator_task_panic_does_not_leak_stdio_child` |
// | Panic in an `UpstreamSupervisor` task (`mcp-client::supervisor`) | Same — supervisor holds `UpstreamConfig` + `Weak<UpstreamManager>` + `Arc<UpstreamHealth>`, **NOT** an `UpstreamHandle` Arc | **Guaranteed** (structural) | `crates/gateway/tests/abnormal_exit_cleanup.rs::supervisor_task_panic_does_not_leak_stdio_child` |
// | `serve_on` aborted / dropped mid-flight (runtime shutdown, main-future Err) | Drop of `UpstreamHandle` → `RunningService::drop` → rmcp's `ChildWithCleanup::drop` fires a fire-and-forget kill task | **Best-effort** — requires the tokio runtime to still be alive long enough for the kill task to execute (measured ~380 ms on a warm cache, bounded by `CLEANUP_BUDGET` in tests) | `crates/gateway/tests/abnormal_exit_cleanup.rs::serve_on_aborted_mid_flight_still_reaps_stdio_child` |
// | SIGKILL of the gateway process itself             | OS delivers SIGHUP to the process group (we spawn with `ProcessGroup::leader()`); kernel reaps | **Best-effort** — depends on the process group still being intact at SIGKILL time                   | Not integration-tested (requires a child Cargo-test process; out-of-scope for in-tree tests) |
//
// "Structural" guarantees rely on none of the spawned tasks holding an
// `UpstreamHandle` clone. If a future refactor changes that — e.g., a
// supervisor task that caches a handle so it can avoid `handle_by_name`
// on every tick — the corresponding panic-survivability tests will
// start failing and the matrix entry needs updating. The tests in
// `abnormal_exit_cleanup.rs` exist specifically to catch that class of
// regression.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::connect::LiveUpstream;
use crate::health::{FailureKind, HealthSnapshot, UpstreamHealth};

/// Default per-call wall-clock deadline for `UpstreamHandle::call_tool`.
///
/// 20 s is generous for every legitimate MCP tool call we've measured
/// (Glean search cold-cache, Datadog deep queries, GitLab issue fetches)
/// and tight enough that a wedged upstream surfaces as a typed
/// [`UpstreamError::CallTimeout`] well before the gateway's 60 s
/// execute-level backstop (`GatewaySettings::execute_deadline_ms`) fires
/// a generic "script exceeded deadline."
///
/// Operators who have a specific upstream that legitimately needs longer
/// (e.g. a deep-query data tool) should configure the
/// per-upstream override rather than moving this default up — the point
/// of the default is that a wedged *typical* upstream is detected fast.
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum UpstreamKind {
    Http {
        url: String,
        #[serde(default)]
        headers: HashMap<String, String>,
    },
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: HashMap<String, String>,
    },
    /// An HTTP streamable upstream that authenticates via OAuth 2.0
    /// (authorization-code + PKCE, with discovery and dynamic client
    /// registration) rather than static `headers`.
    ///
    /// Unlike [`Http`](Self::Http), this carries no `headers` map — the
    /// `Authorization` bearer is owned end-to-end by rmcp's `AuthClient`,
    /// which injects it on every request and refreshes it silently.
    /// Credentials are obtained once via the interactive `login`
    /// subcommand and persisted by a
    /// [`FileCredentialStore`](crate::credential_store::FileCredentialStore);
    /// the gateway's headless `connect()` only ever *loads* them.
    ///
    /// `#[serde(rename = "http-oauth")]` is mandatory: the enum's
    /// `rename_all = "lowercase"` would otherwise serialize this variant
    /// as `"httpoauth"`, while the config author writes
    /// `"type": "http-oauth"`.
    #[serde(rename = "http-oauth")]
    HttpOAuth {
        url: String,
        /// OAuth scopes to request at login. Empty (the default) lets the
        /// authorization server pick its default scope set — Datadog, for
        /// instance, advertises an empty `scopes_supported` and grants a
        /// default set. Only used by the `login` flow; ignored at headless
        /// connect time (the granted scopes are baked into the persisted
        /// credentials).
        #[serde(default)]
        scopes: Vec<String>,
        /// Client name presented during dynamic client registration.
        /// Cosmetic — appears in the OAuth provider's authorized-apps list.
        /// Defaults to `"ratatoskr"` at login time when unset.
        #[serde(default)]
        client_name: Option<String>,
        /// Localhost port for the OAuth redirect callback during `login`.
        /// `0` (the default) requests an ephemeral OS-assigned port, which
        /// avoids fixed-port collisions; the actual port is read back from
        /// the bound listener and used in the redirect URI. Only relevant
        /// to the interactive `login` flow.
        #[serde(default)]
        callback_port: u16,
        /// Pre-registered OAuth client_id for providers that do NOT support
        /// dynamic client registration (RFC 7591) — e.g. Slack's MCP server,
        /// which only accepts requests from apps registered ahead of time.
        /// When set, the `login` flow skips DCR entirely and authorizes as
        /// this client (public client + PKCE, no secret). The provider must
        /// have `http://localhost:<callback_port>/callback` among the
        /// client's registered redirect URIs, so `callback_port` should be
        /// pinned to the registered port rather than left ephemeral.
        /// Ignored at headless connect time (the client_id used for refresh
        /// comes from the persisted credentials).
        #[serde(default)]
        client_id: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamConfig {
    pub name: String,
    pub lua_name: String,
    #[serde(flatten)]
    pub kind: UpstreamKind,

    /// Opt-in flag for sampling. When false (default), the gateway does NOT
    /// advertise sampling capability to this upstream and silently drops
    /// any sampling/createMessage request it sends.
    ///
    /// The `dangerously_` prefix is deliberate: sampling gives an
    /// upstream the ability to consume the client's LLM credits and
    /// potentially exfiltrate data, so the opt-in name makes the
    /// operator think twice.
    #[serde(default)]
    pub dangerously_enable_sampling: bool,

    /// Optional one-line hint shown alongside this upstream in
    /// `list-servers` output. Lets the operator tag each upstream with
    /// its canonical use ("incident investigation", "Jira/Confluence",
    /// "internal company search") so an agent scanning the catalog
    /// knows which upstream to drill into for a given task.
    ///
    /// When unset, the gateway falls back to extracting the first
    /// sentence of the upstream's advertised
    /// `InitializeResult.instructions` string (if any). See
    /// `gateway::hint::resolve_hint` for the priority order.
    ///
    /// Authored hints are trusted verbatim — no char cap. The cap
    /// only applies to the fallback path, which defends against
    /// upstreams that dump their whole README into `instructions`.
    #[serde(default)]
    pub hint: Option<String>,

    /// Per-upstream supervisor overrides. Each present field
    /// overrides the gateway-wide default for this upstream only.
    /// Absent fields (or a missing key entirely) fall through to
    /// whatever the gateway-wide `upstreamSupervision` or the
    /// built-in defaults provide.
    ///
    /// See [`crate::supervision`] for the full knob surface and
    /// merge semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervision: Option<crate::supervision::PartialSupervisionSettings>,
}

impl UpstreamConfig {
    /// Construct a config with safe defaults (sampling disabled, no
    /// hint). Call `.with_sampling_enabled()` / `.with_hint(...)` to
    /// opt in.
    pub fn new(name: impl Into<String>, lua_name: impl Into<String>, kind: UpstreamKind) -> Self {
        Self {
            name: name.into(),
            lua_name: lua_name.into(),
            kind,
            dangerously_enable_sampling: false,
            hint: None,
            supervision: None,
        }
    }

    pub fn with_sampling_enabled(mut self) -> Self {
        self.dangerously_enable_sampling = true;
        self
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Attach per-upstream supervision overrides. Useful in tests and
    /// for programmatic config construction; the config-file parser
    /// reads the same shape from JSON.
    pub fn with_supervision(
        mut self,
        overrides: crate::supervision::PartialSupervisionSettings,
    ) -> Self {
        self.supervision = Some(overrides);
        self
    }
}

/// Live upstream: config plus an owned rmcp client service.
///
/// Drop semantics: dropping `UpstreamHandle` drops the inner `LiveUpstream`,
/// which in turn drops rmcp's service. For stdio upstreams, the `TokioChildProcess`'s
/// `ChildWithCleanup::drop` spawns a kill task on the tokio runtime to terminate
/// the child. Known correctness boundary: SIGKILL of the gateway still
/// orphans children — an OS-level concern, handled via process groups at
/// spawn time.
pub struct UpstreamHandle {
    pub config: UpstreamConfig,
    live: LiveUpstream,
    /// Per-call deadline applied inside [`UpstreamHandle::call_tool`].
    /// Defaults to [`DEFAULT_CALL_TIMEOUT`]; set via
    /// [`UpstreamHandle::new_with_call_timeout`] at construction so every
    /// handle captures whatever the operator configured at boot.
    ///
    /// This is the first line of defence against a wedged upstream: when
    /// a tool call doesn't return within the deadline, we bail with a
    /// typed [`UpstreamError::CallTimeout`] that names the upstream and
    /// echoes the configured deadline. Callers that previously waited for
    /// the Lua-runtime's 60 s execute-level backstop now see the
    /// more specific error in < `call_timeout`, and the
    /// [`UpstreamSupervisor`](crate) (see gateway) uses the failure
    /// signal to drive health tracking and respawn.
    call_timeout: Duration,
    /// Health record observed by the supervisor and gated-against by
    /// `call_tool`. Each `call_tool` outcome is classified into a
    /// [`FailureKind`] (or success) and recorded here; the
    /// supervisor polls the snapshot to drive state transitions.
    ///
    /// Wrapped in `Arc` so the supervisor can hold its own reference
    /// even when a respawn replaces the handle — though today's design
    /// gives each fresh handle a fresh record, the Arc keeps the
    /// option open for a future refactor that lets a record outlive a
    /// replace. See `health.rs` for the state-machine contract.
    health: Arc<UpstreamHealth>,
}

impl UpstreamHandle {
    /// Construct a handle that uses the default per-call timeout
    /// ([`DEFAULT_CALL_TIMEOUT`]). Prefer
    /// [`UpstreamHandle::new_with_call_timeout`] in production where the
    /// operator-configured value is already in scope; this convenience
    /// constructor is retained for tests that don't care about the
    /// deadline and for callers ported before the timeout landed.
    pub fn new(config: UpstreamConfig, live: LiveUpstream) -> Self {
        Self::new_with_call_timeout(config, live, DEFAULT_CALL_TIMEOUT)
    }

    /// Construct a handle with an explicit per-call deadline. Adopts
    /// the [`UpstreamHealth`] record already attached to `live` (created
    /// by `connect()` / `connect_with_transport()` and shared with the
    /// [`ProxyUpstreamHandler`] that observes upstream notifications).
    /// This shared identity is what lets a side-channel signal like
    /// `FailureKind::UpstreamSelfReportedError` — recorded by the
    /// handler — be visible to the supervisor reading off the handle.
    pub fn new_with_call_timeout(
        config: UpstreamConfig,
        live: LiveUpstream,
        call_timeout: Duration,
    ) -> Self {
        let health = live.health();
        Self {
            config,
            live,
            call_timeout,
            health,
        }
    }

    /// The per-call deadline this handle enforces inside `call_tool`.
    /// Exposed so supervisors and tests can observe the effective value
    /// rather than guessing at the default.
    pub fn call_timeout(&self) -> Duration {
        self.call_timeout
    }

    /// Shared reference to the health record. Intended for the
    /// supervisor, which needs to drive state transitions (set_state)
    /// and read counters across many ticks without cloning the
    /// snapshot every time.
    pub fn health(&self) -> Arc<UpstreamHealth> {
        Arc::clone(&self.health)
    }

    /// Snapshot of the current health. Cheap; callers that only need
    /// a point-in-time view should use this rather than holding
    /// `health()` live.
    pub fn health_snapshot(&self) -> HealthSnapshot {
        self.health.snapshot()
    }

    /// Synchronous liveness probe: true when rmcp has noticed the
    /// underlying transport is closed (child exited, HTTP stream
    /// terminated). Cheap enough to poll on every call-site entry; the
    /// supervisor uses it as a definitive "the connection is gone"
    /// signal that doesn't need K-consecutive-failures to trigger.
    pub fn is_transport_closed(&self) -> bool {
        self.live.peer().is_transport_closed()
    }

    /// Process ID of the upstream child process, if this is a stdio upstream.
    /// Returns None for HTTP upstreams. Used by tests to verify cleanup.
    pub fn child_pid(&self) -> Option<u32> {
        self.live.child_pid()
    }

    /// Call a tool on this upstream with a per-call wall-clock deadline.
    ///
    /// The deadline is enforced by [`tokio::time::timeout`]; when it
    /// fires, the in-flight future is dropped and we return
    /// [`UpstreamError::CallTimeout`] tagged with the upstream name and
    /// the configured `deadline_ms`. Any error returned directly by
    /// rmcp (transport closed, peer-generated `McpError`, rmcp's own
    /// internal timeout) is forwarded as
    /// [`UpstreamError::ServiceCall`] so callers can still distinguish
    /// "we gave up" from "the peer told us no."
    ///
    /// Every outcome (success or failure) is recorded on the handle's
    /// [`UpstreamHealth`] record: successes reset the
    /// consecutive-failure counter and auto-promote Suspect → Healthy;
    /// transport-level failures increment the counter; peer-returned
    /// `McpError` is recorded for observability but doesn't contribute
    /// to the counter that drives respawn. The supervisor polls the
    /// record to drive state transitions.
    ///
    /// Used by the Lua bridge and by the sampling test fixture; the
    /// production path is: agent sends Lua script → gateway's execute
    /// tool → lua-runtime host function → this call.
    pub async fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParams,
    ) -> Result<rmcp::model::CallToolResult, UpstreamError> {
        // Supervisor-driven fast-fail: if health has been classified
        // as Unhealthy/Respawning/Dead, short-circuit without
        // dispatching. Cheap atomic read; safe to do on every call.
        //
        // We don't gate Suspect — the supervisor is still probing
        // there, and letting calls through lets a recovering upstream
        // self-heal without the supervisor needing to flip back to
        // Healthy first.
        let current_state = self.health.state();
        if current_state.gates_calls() {
            // No timing needed on the fast-fail path — it's always <1us
            // and the `outcome` label below already distinguishes this
            // shape from real-dispatch paths.
            tracing::debug!(
                upstream = %self.config.name,
                tool = %request.name,
                outcome = "gated_transient",
                state = ?current_state,
                "upstream call_tool gated by health state"
            );
            return Err(UpstreamError::TransientUnavailable {
                name: self.config.name.clone(),
                state: current_state,
            });
        }

        // Capture the tool name before we hand `request` to rmcp —
        // CallToolRequestParams doesn't give us a borrow after move.
        let tool_name = request.name.to_string();
        let start = std::time::Instant::now();
        let outcome =
            tokio::time::timeout(self.call_timeout, self.live.peer().call_tool(request)).await;
        let duration_ms = start.elapsed().as_millis() as u64;

        match outcome {
            Ok(Ok(result)) => {
                self.health.record_success();
                // Per-call visibility lives at debug: it's the field
                // you reach for when answering "which tool got slow?"
                // but not something you want on by default in a
                // steady-state info feed.
                tracing::debug!(
                    upstream = %self.config.name,
                    tool = %tool_name,
                    duration_ms,
                    outcome = "ok",
                    "upstream call_tool complete"
                );
                Ok(result)
            }
            Ok(Err(source)) => {
                // OAuth-only re-auth detection: if this is an `http-oauth`
                // upstream and the failure is an auth error (dead refresh
                // token → rmcp's AuthClient surfaces it through the erased
                // transport boundary), this is NOT a transient flap. Flip
                // the health record to NeedsLogin so `list-servers` renders
                // the re-login command, and return the actionable
                // OAuthReauthRequired rather than a generic ServiceCall.
                //
                // Guarded on the OAuth kind deliberately: a plain `http`
                // upstream whose static bearer 401s is a config problem the
                // operator fixes by editing headers, not by running `login`
                // — misreporting it as needs-login would send them down the
                // wrong path. For OAuth, `login` is exactly the fix.
                if matches!(self.config.kind, UpstreamKind::HttpOAuth { .. })
                    && is_auth_failure(&source)
                {
                    self.health.record_failure(FailureKind::AuthExpired);
                    // Edge-triggered: `enter_needs_login` returns true only
                    // on the Healthy→NeedsLogin transition, so a retrying
                    // agent hammering this failing upstream fires exactly
                    // one human-facing notification per incident — not one
                    // per failed call.
                    if self.health.enter_needs_login() {
                        tracing::warn!(
                            upstream = %self.config.name,
                            tool = %tool_name,
                            duration_ms,
                            outcome = "auth_expired",
                            error = %source,
                            "OAuth upstream can no longer authenticate; marking NeedsLogin and \
                             notifying the operator to re-run `login`"
                        );
                        crate::reauth_notify::notify(&self.config.name);
                    } else {
                        // Already NeedsLogin — stay quiet (no repeat alert),
                        // but keep a debug breadcrumb for the failing call.
                        tracing::debug!(
                            upstream = %self.config.name,
                            tool = %tool_name,
                            outcome = "auth_expired",
                            "OAuth upstream still NeedsLogin (notification already fired)"
                        );
                    }
                    return Err(UpstreamError::OAuthReauthRequired {
                        name: self.config.name.clone(),
                    });
                }

                let kind = failure_kind_from_service_error(&source);
                self.health.record_failure(kind);
                tracing::debug!(
                    upstream = %self.config.name,
                    tool = %tool_name,
                    duration_ms,
                    outcome = "service_error",
                    failure_kind = ?kind,
                    error = %source,
                    "upstream call_tool returned service error"
                );
                Err(UpstreamError::ServiceCall {
                    name: self.config.name.clone(),
                    source,
                })
            }
            Err(_elapsed) => {
                self.health.record_failure(FailureKind::CallTimeout);
                tracing::warn!(
                    upstream = %self.config.name,
                    tool = %tool_name,
                    duration_ms,
                    timeout_ms = u64::try_from(self.call_timeout.as_millis()).unwrap_or(u64::MAX),
                    outcome = "timeout",
                    "upstream call_tool timed out"
                );
                Err(UpstreamError::CallTimeout {
                    name: self.config.name.clone(),
                    deadline_ms: u64::try_from(self.call_timeout.as_millis()).unwrap_or(u64::MAX),
                })
            }
        }
    }

    /// List the upstream's tools with full rmcp metadata (name,
    /// description, input schema, annotations). Callers that only care
    /// about names just `.iter().map(|t| &t.name)` — no need for two
    /// variants.
    ///
    /// Paginates automatically via rmcp's `list_all_tools`, so callers
    /// get the complete tool list even for upstreams that ship more than
    /// one page.
    ///
    /// Used at Lua VM setup (to decide which tool-named functions to
    /// inject into the per-upstream Lua table) AND by the gateway's
    /// `tool-details` / `list-server-tools` tools, which need the
    /// descriptions and schemas.
    ///
    /// ## Bounded by `call_timeout`; flows through the health record
    ///
    /// Wrapped by [`Self::list_with_health`] in the same way `call_tool`
    /// is: the in-flight rmcp future is dropped if `self.call_timeout`
    /// elapses, every outcome is reflected on the handle's
    /// [`UpstreamHealth`] record (success → reset counters, transport-
    /// level error → bump counter, timeout → bump counter as
    /// [`FailureKind::CallTimeout`]), and on timeout the returned error
    /// is [`rmcp::ServiceError::Timeout`] so callers see one canonical
    /// shape regardless of whether the timeout came from rmcp's own
    /// per-request deadline or from this wrapper.
    ///
    /// Why this matters: a stdio upstream whose subprocess is alive but
    /// silently no longer responding (mcp-remote with a dead SSE stream
    /// mid-reconnect, for example) leaves `is_transport_closed()`
    /// reporting `false`. Without this wrapper, a wedged `list_tools`
    /// would never resolve, never increment the failure counter, and
    /// the supervisor would see a "healthy" upstream forever.
    pub async fn list_tools(&self) -> Result<Vec<rmcp::model::Tool>, rmcp::ServiceError> {
        self.list_with_health("list_tools", self.live.peer().list_all_tools())
            .await
    }

    /// List the upstream's resources. Paginates automatically via
    /// rmcp's `list_all_resources`, so callers don't have to worry
    /// about cursors even for upstreams that ship more than one page.
    ///
    /// Used by `ResourceListCache` on a cache miss. Separate from
    /// `list_resource_templates` because MCP exposes them as two
    /// distinct endpoints with independent paginations.
    ///
    /// Same timeout + health-record contract as [`Self::list_tools`].
    pub async fn list_resources(&self) -> Result<Vec<rmcp::model::Resource>, rmcp::ServiceError> {
        self.list_with_health("list_resources", self.live.peer().list_all_resources())
            .await
    }

    /// List the upstream's resource templates. Paginates automatically.
    /// See `list_resources` for context.
    ///
    /// Same timeout + health-record contract as [`Self::list_tools`].
    pub async fn list_resource_templates(
        &self,
    ) -> Result<Vec<rmcp::model::ResourceTemplate>, rmcp::ServiceError> {
        self.list_with_health(
            "list_resource_templates",
            self.live.peer().list_all_resource_templates(),
        )
        .await
    }

    /// Common wrapper for the three `list_*` passthroughs. Mirrors
    /// `call_tool`'s timeout-and-health-record pattern, factored to
    /// avoid three identical copies. Private — callers go through the
    /// typed methods above so the operation name in logs is correct
    /// without callers having to remember to pass it.
    ///
    /// `op` is a static string used in tracing fields only; it has no
    /// behavioural effect. Pass the rmcp method name (`"list_tools"`,
    /// `"list_resources"`, `"list_resource_templates"`) so log scraping
    /// stays clean.
    async fn list_with_health<T>(
        &self,
        op: &'static str,
        future: impl std::future::Future<Output = Result<T, rmcp::ServiceError>>,
    ) -> Result<T, rmcp::ServiceError> {
        let start = std::time::Instant::now();
        let outcome = tokio::time::timeout(self.call_timeout, future).await;
        let duration_ms = start.elapsed().as_millis() as u64;

        match outcome {
            Ok(Ok(value)) => {
                self.health.record_success();
                tracing::debug!(
                    upstream = %self.config.name,
                    op,
                    duration_ms,
                    outcome = "ok",
                    "upstream list complete"
                );
                Ok(value)
            }
            Ok(Err(source)) => {
                let kind = failure_kind_from_service_error(&source);
                self.health.record_failure(kind);
                tracing::debug!(
                    upstream = %self.config.name,
                    op,
                    duration_ms,
                    outcome = "service_error",
                    failure_kind = ?kind,
                    error = %source,
                    "upstream list returned service error"
                );
                Err(source)
            }
            Err(_elapsed) => {
                self.health.record_failure(FailureKind::CallTimeout);
                tracing::warn!(
                    upstream = %self.config.name,
                    op,
                    duration_ms,
                    timeout_ms = u64::try_from(self.call_timeout.as_millis()).unwrap_or(u64::MAX),
                    outcome = "timeout",
                    "upstream list timed out"
                );
                // ServiceError::Timeout is the canonical "rmcp's deadline
                // fired" shape; reusing it for our wrapper-level deadline
                // means callers (ToolListCache, ResourceListCache, the
                // resources handler) handle one error variant rather
                // than two near-identical ones. The carried `Duration`
                // is the configured `call_timeout`, so observability
                // sees the same value tracing did.
                Err(rmcp::ServiceError::Timeout {
                    timeout: self.call_timeout,
                })
            }
        }
    }

    /// Read a resource's content from this upstream. Thin pass-through
    /// — the gateway's `resources/read` handler builds the request
    /// params after URI decoding and routing, then calls this.
    pub async fn read_resource(
        &self,
        request: rmcp::model::ReadResourceRequestParams,
    ) -> Result<rmcp::model::ReadResourceResult, rmcp::ServiceError> {
        self.live.peer().read_resource(request).await
    }

    /// Subscribe to updates for `uri` on this upstream. Pass-through over
    /// rmcp's `peer.subscribe`. Callers supply the bare (upstream-native)
    /// URI — the gateway's `SubscriptionRegistry` is responsible for
    /// decoding the namespaced form before calling this.
    ///
    /// Ref-counting policy lives one layer up: the registry invokes this
    /// only for the FIRST downstream session that subscribes to a given
    /// `(upstream, uri)` pair. Subsequent sessions don't re-call the
    /// upstream.
    pub async fn subscribe(
        &self,
        request: rmcp::model::SubscribeRequestParams,
    ) -> Result<(), rmcp::ServiceError> {
        self.live.peer().subscribe(request).await.map(|_| ())
    }

    /// Unsubscribe from updates for `uri` on this upstream. Pass-through
    /// over rmcp's `peer.unsubscribe`. The registry invokes this only
    /// when the LAST downstream session for a given `(upstream, uri)`
    /// pair drops the subscription.
    pub async fn unsubscribe(
        &self,
        request: rmcp::model::UnsubscribeRequestParams,
    ) -> Result<(), rmcp::ServiceError> {
        self.live.peer().unsubscribe(request).await.map(|_| ())
    }

    /// The upstream's self-advertised `InitializeResult.instructions`
    /// string, cloned from the peer's cached ServerInfo.
    ///
    /// Returns `None` when either the handshake hasn't completed
    /// (pre-`initialize`) or the upstream didn't set the field — many
    /// servers leave it empty. Surfaced here rather than exposing the
    /// full peer because `instructions` is the only field downstream
    /// code has needed to date; widening the surface only when a
    /// concrete use case appears keeps `UpstreamHandle` narrow.
    ///
    /// The string is cloned into an owned `String` so callers don't
    /// need to hold references into the peer's internal storage.
    pub fn server_instructions(&self) -> Option<String> {
        self.live
            .peer()
            .peer_info()
            .and_then(|info| info.instructions.clone())
    }

    /// The upstream's self-advertised implementation identity (name
    /// and version) from `InitializeResult.serverInfo`. Returns
    /// `None` before the handshake completes.
    ///
    /// Both fields are present as a unit because rmcp models them as
    /// required strings on `Implementation`. If rmcp adds optional
    /// fields in the future, a new method (or a changed return type)
    /// will be needed — this signature does not accommodate a
    /// "name-only" result.
    pub fn server_implementation(&self) -> Option<(String, String)> {
        self.live.peer().peer_info().map(|info| {
            (
                info.server_info.name.clone(),
                info.server_info.version.clone(),
            )
        })
    }

    /// Send an MCP `ping` request with a per-ping wall-clock deadline,
    /// returning `Ok(())` on a well-formed empty response and an
    /// [`UpstreamError`] otherwise.
    ///
    /// Used by the supervisor ([`crate::supervisor`]) as its active
    /// probe when the health state is `Suspect`. Rmcp 1.5.0 doesn't
    /// expose a `peer.ping()` helper on `Peer<RoleClient>` — we go
    /// through `send_request(ClientRequest::PingRequest(...))` and
    /// match the `ServerResult::EmptyResult` success shape by hand.
    ///
    /// Classification and health recording: ping outcomes flow through
    /// the same `failure_kind_from_service_error` classifier as
    /// [`call_tool`](Self::call_tool) so Suspect → Unhealthy transitions
    /// are driven by the same transport-level predicate. Unlike
    /// `call_tool`, ping **does not** record on the health record
    /// itself — the supervisor owns the ping-streak counters and
    /// decides whether K ping failures means "flip to Unhealthy."
    /// Keeping ping record-free avoids a category error where a single
    /// ping failure out of the blue would bump the same counter that
    /// `call_tool` uses to detect real-traffic wedges.
    ///
    /// **Ping is demotion-only evidence in Suspect.** Ping success does
    /// NOT promote Suspect → Healthy: that flip is owned exclusively by
    /// [`UpstreamHealth::record_success`] on a real-traffic outcome
    /// (call_tool / list_*). And Suspect → Unhealthy now has two
    /// ping-independent triggers — K consecutive ping failures *or*
    /// `consecutive_failures >= 2 * unhealthy_threshold` (real-traffic
    /// escalation, catches "ping passes, real RPCs hang"). See
    /// `supervisor.rs`'s "Lazy active pings + promotion ownership"
    /// section for the full rationale.
    pub async fn ping(&self, timeout: Duration) -> Result<(), UpstreamError> {
        use rmcp::model::{ClientRequest, PingRequest, ServerResult};

        let outcome = tokio::time::timeout(
            timeout,
            self.live
                .peer()
                .send_request(ClientRequest::PingRequest(PingRequest::default())),
        )
        .await;

        match outcome {
            Ok(Ok(ServerResult::EmptyResult(_))) => Ok(()),
            Ok(Ok(_other)) => Err(UpstreamError::ServiceCall {
                name: self.config.name.clone(),
                source: rmcp::ServiceError::UnexpectedResponse,
            }),
            Ok(Err(source)) => Err(UpstreamError::ServiceCall {
                name: self.config.name.clone(),
                source,
            }),
            Err(_elapsed) => Err(UpstreamError::CallTimeout {
                name: self.config.name.clone(),
                deadline_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            }),
        }
    }

    /// Explicit graceful shutdown. Preferred over relying on Drop because
    /// Drop's async-kill-task can race with runtime shutdown.
    pub async fn close(self) -> anyhow::Result<()> {
        self.live.close().await
    }
}

/// Minimal `Debug` that shows the config identity and current call
/// timeout — enough for `Result::expect_err` formatting in tests and
/// for operator-facing log lines, without trying to pretty-print the
/// `LiveUpstream` (which owns a rmcp `RunningService` whose Debug
/// output is verbose and not useful here). The health record's full
/// snapshot is available via `health_snapshot()` when callers want
/// detail.
impl std::fmt::Debug for UpstreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamHandle")
            .field("name", &self.config.name)
            .field("lua_name", &self.config.lua_name)
            .field("call_timeout", &self.call_timeout)
            .field("health_state", &self.health.state())
            .finish_non_exhaustive()
    }
}

/// Classify a rmcp `ServiceError` into a `FailureKind` for the health
/// record. Pairs with [`UpstreamError::is_transport_level`]; the two
/// must agree on what counts as transport-level. Cross-reference:
/// `health::FailureKind::is_transport_level` + `upstream_error_classifications`
/// / `failure_kind_classification_matches_transport_level_predicate`
/// tests pin the invariant on each side.
fn failure_kind_from_service_error(err: &rmcp::ServiceError) -> FailureKind {
    match err {
        rmcp::ServiceError::TransportClosed => FailureKind::TransportClosed,
        rmcp::ServiceError::TransportSend(_) => FailureKind::TransportSend,
        rmcp::ServiceError::Timeout { .. } => FailureKind::RmcpTimeout,
        rmcp::ServiceError::McpError(_) => FailureKind::PeerMcpError,
        rmcp::ServiceError::UnexpectedResponse => FailureKind::UnexpectedResponse,
        // ServiceError is #[non_exhaustive]; any future variant lands
        // here and is conservatively classified as peer-level (the
        // least-disruptive default — no respawn triggered).
        _ => FailureKind::PeerMcpError,
    }
}

/// Stable substrings emitted by rmcp 1.5's OAuth layer when a request
/// fails because the credentials can no longer authenticate (dead/absent
/// refresh token, expired token with no refresh, or a 401 from the
/// resource server). These are the `Display` outputs of the relevant
/// `rmcp::transport::auth::AuthError` and `StreamableHttpError` variants,
/// verified against the installed rmcp-1.5.0 source:
///
///   - `AuthError::AuthorizationRequired` → `"OAuth authorization required"`
///     (the dominant case: `get_access_token` / `try_refresh_or_reauth`
///     return this when there's no refresh token or the refresh was
///     rejected — see rmcp `auth.rs`)
///   - `AuthError::AuthorizationFailed(_)` → `"OAuth authorization failed: …"`
///   - `AuthError::TokenRefreshFailed(_)` → `"OAuth token refresh failed: …"`
///   - `AuthError::TokenExpired` → `"Token expired"`
///   - `StreamableHttpError::AuthRequired` → `"Auth required"`
///   - `StreamableHttpError::Auth(_)` → `"Auth error: …"` (wraps the above)
///
/// Matched case-insensitively against the error's full `Display` chain.
/// This is deliberately string-based: by the time an auth failure reaches
/// our `call_tool` / connect sites it has been erased through rmcp's
/// `ServiceError` / transport boundary, which exposes no typed auth
/// variant we can match on. The `auth_markers_are_current_for_rmcp_1_5`
/// test pins these strings against freshly-constructed rmcp error values,
/// so an rmcp upgrade that changes the wording trips a test rather than
/// silently regressing detection.
///
/// Fail-safe: an unrecognized string simply isn't classified as auth, so
/// the worst case of a wording drift is reverting to today's behavior
/// (generic failure), never a false "needs login" on an unrelated error.
const AUTH_FAILURE_MARKERS: &[&str] = &[
    "oauth authorization required",
    "oauth authorization failed",
    "oauth token refresh failed",
    "token expired",
    "auth required",
    "auth error",
    "unauthorized",
    "401",
];

/// True when `text` contains any of [`AUTH_FAILURE_MARKERS`] (case-
/// insensitive). Pure helper over an already-lowercased haystack is
/// avoided in favor of lowercasing here so callers can't forget.
fn text_has_auth_marker(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    AUTH_FAILURE_MARKERS.iter().any(|m| lower.contains(m))
}

/// Determine whether an error (typically an `rmcp::ServiceError` from a
/// failed `call_tool`, or a connect/handshake error) indicates the
/// upstream's OAuth credentials can no longer authenticate and a human
/// `login` is required.
///
/// Walks the full `source()` chain so a marker buried under several
/// layers of transport wrapping is still found — rmcp nests the auth
/// error inside `StreamableHttpError` inside the transport's client
/// error inside `ServiceError`, and only the innermost link carries the
/// `"OAuth authorization required"` text.
///
/// See [`AUTH_FAILURE_MARKERS`] for the matched strings and the rationale
/// for string-matching at this boundary.
pub fn is_auth_failure(err: &(dyn std::error::Error + 'static)) -> bool {
    // Check the top-level Display first, then each source in the chain.
    if text_has_auth_marker(&err.to_string()) {
        return true;
    }
    let mut current = err.source();
    while let Some(src) = current {
        if text_has_auth_marker(&src.to_string()) {
            return true;
        }
        current = src.source();
    }
    false
}

/// Two-index registry keyed by both display `name` and sanitized
/// `lua_name`. Each registered handle is stored in both maps; the
/// `Arc<UpstreamHandle>` is the shared owner, so lookups by either key
/// return the same live object.
///
/// Keeping the maps in sync is the sole responsibility of
/// `UpstreamManager::register` / `drain` — no other code path mutates
/// this struct directly. The duplicate-handling branches in `register`
/// evict the *other* index's entry for the Arc being displaced, so a
/// stale `by_lua_name` pointer cannot outlive its `by_name` partner
/// (or vice versa).
#[derive(Default)]
struct Registry {
    by_name: HashMap<String, Arc<UpstreamHandle>>,
    by_lua_name: HashMap<String, Arc<UpstreamHandle>>,
}

/// Session-scoped registry of upstream connections.
///
/// `inner` is the canonical async-locked map; every read/write to the
/// upstream catalog goes through it.
#[derive(Default)]
pub struct UpstreamManager {
    inner: RwLock<Registry>,
}

impl UpstreamManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a handle under both its display `name` and its sanitized
    /// `lua_name`.
    ///
    /// Duplicate-handling policy (symmetric on both keys):
    ///   - `debug_assert!` in debug builds — a dev-time shout that two
    ///     upstream configs collide on one of the two keys. In practice
    ///     this is already caught upstream (`config::parse_full` rejects
    ///     `lua_name` collisions at parse time via `LuaNameCollision`,
    ///     and the display-name space is flat), so tripping this in
    ///     debug means a test built an inconsistent manager directly.
    ///   - `tracing::warn!` + replace in release — matches the
    ///     "log at warn, continue" policy used by `connect_configs` and
    ///     keeps the process running under misconfiguration.
    ///
    /// Cross-index consistency: when replacing an existing handle on
    /// either key, the other key's entry for the displaced Arc is
    /// removed as well, so a stale pointer never remains. This means
    /// a well-formed registration (no collisions) inserts into both
    /// maps; a collision on one key evicts at most one entry from the
    /// other.
    pub async fn register(&self, handle: Arc<UpstreamHandle>) {
        let mut guard = self.inner.write().await;
        let name = handle.config.name.clone();
        let lua_name = handle.config.lua_name.clone();

        debug_assert!(
            !guard.by_name.contains_key(&name),
            "duplicate upstream name '{name}': previous connection will be displaced"
        );
        debug_assert!(
            !guard.by_lua_name.contains_key(&lua_name),
            "duplicate upstream lua_name '{lua_name}': previous connection will be displaced \
             (config parse should have caught this via LuaNameCollision)"
        );

        if let Some(displaced) = guard.by_name.get(&name) {
            tracing::warn!(
                upstream_name = %name,
                "duplicate upstream name: displacing existing connection; \
                 check your config for two upstreams with the same 'name' field"
            );
            // Evict the displaced Arc's lua_name entry so it doesn't
            // outlive its partner in by_name.
            let displaced_lua = displaced.config.lua_name.clone();
            guard.by_lua_name.remove(&displaced_lua);
        }
        if let Some(displaced) = guard.by_lua_name.get(&lua_name) {
            tracing::warn!(
                upstream_lua_name = %lua_name,
                "duplicate upstream lua_name: displacing existing connection; \
                 two upstreams sanitize to the same Lua identifier"
            );
            // Evict the displaced Arc's display-name entry too.
            let displaced_name = displaced.config.name.clone();
            guard.by_name.remove(&displaced_name);
        }

        guard.by_name.insert(name, Arc::clone(&handle));
        guard.by_lua_name.insert(lua_name, handle);
    }

    /// Returns a snapshot of upstream configs. Cloning here is deliberate:
    /// callers iterate freely without holding the lock.
    pub async fn list(&self) -> Vec<UpstreamConfig> {
        let guard = self.inner.read().await;
        guard.by_name.values().map(|h| h.config.clone()).collect()
    }

    /// Look up a live handle by display name. Used by the Lua bridge, which
    /// needs to dispatch calls to the actual upstream after identifying it
    /// by `lua_name` → display name → handle.
    pub async fn handle_by_name(&self, name: &str) -> Option<Arc<UpstreamHandle>> {
        let guard = self.inner.read().await;
        guard.by_name.get(name).cloned()
    }

    /// Look up a live handle by its sanitized Lua identifier.
    ///
    /// The subscription registry keys by `lua_name` (the same thing the
    /// namespaced URI decoder produces), so session-close cleanup and
    /// the Lua bridge's tool dispatch need a direct `lua_name → handle`
    /// lookup. Backed by the `by_lua_name` secondary index — a single
    /// HashMap get, symmetric with `handle_by_name`.
    pub async fn handle_by_lua_name(&self, lua_name: &str) -> Option<Arc<UpstreamHandle>> {
        let guard = self.inner.read().await;
        guard.by_lua_name.get(lua_name).cloned()
    }

    /// Returns the child PID for a named upstream, if any. Test helper.
    pub async fn child_pid(&self, name: &str) -> Option<u32> {
        let guard = self.inner.read().await;
        guard.by_name.get(name).and_then(|h| h.child_pid())
    }

    /// Take ownership of every registered handle, emptying the manager.
    ///
    /// Used during graceful shutdown: callers drain, then call
    /// `UpstreamHandle::close()` on each entry to get deterministic child
    /// cleanup instead of relying on Drop's async-fire-and-forget path.
    ///
    /// Returns the handles from `by_name`. Clearing `by_lua_name` first
    /// drops its strong references so the Arcs returned from `by_name`
    /// carry only the external strong count — typically 1 after the
    /// HTTP server has drained, but if a tool call is still in flight
    /// the shared count will be >1. Callers that need to actually
    /// consume the handle via `Arc::try_unwrap` should handle the
    /// "still shared" case.
    pub async fn drain(&self) -> Vec<Arc<UpstreamHandle>> {
        let mut guard = self.inner.write().await;
        guard.by_lua_name.clear();
        guard.by_name.drain().map(|(_, handle)| handle).collect()
    }

    /// Atomically swap the handle registered under `name`, returning the
    /// displaced Arc so the caller can `close()` it.
    ///
    /// Used by the supervisor (see `gateway::upstream_supervisor`) to
    /// install a freshly-respawned upstream without visiting a state
    /// where the registry has zero entries for this name. The swap
    /// happens under a single write-lock acquisition so a concurrent
    /// `handle_by_name` either sees the old handle or the new one —
    /// never an absence.
    ///
    /// ## Invariants the supervisor must respect
    ///
    /// - `name` must currently be registered. A replace against an
    ///   unknown name is a programmer error (the supervisor should have
    ///   a registered upstream to supervise); we return
    ///   [`ReplaceError::UnknownName`] so callers can log and no-op
    ///   rather than silently inserting a new entry.
    /// - `new_handle.config.name` must equal `name`. We enforce this
    ///   as a safety rail: a supervisor that accidentally builds a
    ///   handle for "server-a" and tries to install it under "server-b"
    ///   is a bug, and we want to catch it at the swap rather than
    ///   silently corrupting the registry.
    /// - `new_handle.config.lua_name` must equal the displaced handle's
    ///   `lua_name`. The Lua global's identity is a load-bearing piece
    ///   of the downstream surface — an agent's Lua script binds
    ///   `gitlab.create_issue` at session setup. Silently re-indexing
    ///   a replace would break that binding. We return
    ///   [`ReplaceError::LuaNameMismatch`] so the supervisor logs and
    ///   rejects the swap rather than corrupting the Lua-global space.
    ///
    /// ## What doesn't change
    ///
    /// - The `lua_name` key in `by_lua_name` continues to point at the
    ///   same slot; we just store a new Arc there. An in-flight caller
    ///   that looked up via `handle_by_lua_name` before the swap and
    ///   is now mid-`call_tool` on the old Arc keeps that Arc alive
    ///   until its call completes — the Arc refcount handles this
    ///   naturally.
    /// - The displaced Arc's `UpstreamHealth` record does not carry
    ///   forward. A fresh respawn gets a fresh health record, so the
    ///   consecutive-failure counter doesn't inherit the wedged
    ///   upstream's tally.
    pub async fn replace(
        &self,
        name: &str,
        new_handle: Arc<UpstreamHandle>,
    ) -> Result<Arc<UpstreamHandle>, ReplaceError> {
        // Safety-rail checks: perform BEFORE taking the write lock so
        // a buggy supervisor doesn't block reads while we reject its
        // request.
        if new_handle.config.name != name {
            return Err(ReplaceError::NameMismatch {
                requested: name.to_string(),
                new_handle_name: new_handle.config.name.clone(),
            });
        }

        let mut guard = self.inner.write().await;
        let Some(old) = guard.by_name.get(name).cloned() else {
            return Err(ReplaceError::UnknownName(name.to_string()));
        };

        if old.config.lua_name != new_handle.config.lua_name {
            return Err(ReplaceError::LuaNameMismatch {
                name: name.to_string(),
                old_lua_name: old.config.lua_name.clone(),
                new_lua_name: new_handle.config.lua_name.clone(),
            });
        }

        let new_lua_name = new_handle.config.lua_name.clone();
        guard
            .by_name
            .insert(name.to_string(), Arc::clone(&new_handle));
        guard.by_lua_name.insert(new_lua_name, new_handle);

        tracing::info!(
            upstream = %name,
            "upstream handle replaced (supervisor respawn or operator-driven swap)"
        );

        Ok(old)
    }

    /// Atomically register-or-replace: if `name` is already registered,
    /// swap (same contract as [`Self::replace`]); otherwise insert a
    /// fresh entry. The whole operation runs under one write lock — no
    /// TOCTOU window between the existence check and the insertion.
    ///
    /// Used by the supervisor's `try_respawn` so a respawn that
    /// succeeds against a previously-unregistered config (the
    /// bootstrap-failed-retry case) can register without giving up
    /// the lock between the failed `replace` and the fallback
    /// `register` calls.
    ///
    /// Returns `Some(displaced)` when an existing handle was swapped
    /// out (caller closes it off-task), `None` when this was a fresh
    /// insertion.
    ///
    /// Errors:
    /// - [`ReplaceError::NameMismatch`] — `handle.config.name != name`.
    ///   Same safety rail [`Self::replace`] enforces; a supervisor that
    ///   built the wrong handle is a bug we want surfaced.
    /// - [`ReplaceError::LuaNameMismatch`] — `handle.config.lua_name`
    ///   differs from an existing entry's `lua_name`, OR (on fresh
    ///   insertion) collides with a different name's `lua_name` already
    ///   in the registry. Either case would silently corrupt the
    ///   Lua-global space, so we reject loudly.
    pub async fn register_or_replace(
        &self,
        name: &str,
        new_handle: Arc<UpstreamHandle>,
    ) -> Result<Option<Arc<UpstreamHandle>>, ReplaceError> {
        if new_handle.config.name != name {
            return Err(ReplaceError::NameMismatch {
                requested: name.to_string(),
                new_handle_name: new_handle.config.name.clone(),
            });
        }

        let new_lua_name = new_handle.config.lua_name.clone();
        let mut guard = self.inner.write().await;

        if let Some(old) = guard.by_name.get(name) {
            // Replacement path: preserve the load-bearing Lua-global
            // identity invariant (same constraint `replace` enforces).
            if old.config.lua_name != new_lua_name {
                return Err(ReplaceError::LuaNameMismatch {
                    name: name.to_string(),
                    old_lua_name: old.config.lua_name.clone(),
                    new_lua_name,
                });
            }
            let displaced = old.clone();
            guard
                .by_name
                .insert(name.to_string(), Arc::clone(&new_handle));
            guard.by_lua_name.insert(new_lua_name, new_handle);
            tracing::info!(
                upstream = %name,
                "upstream handle replaced (register_or_replace; existing name)"
            );
            return Ok(Some(displaced));
        }

        // Fresh-insertion path: the display name is new, but the
        // lua_name might collide with a different upstream already in
        // the registry. Reject — silently displacing a different
        // upstream's lua_name would break its agent-visible Lua
        // binding.
        if let Some(other) = guard.by_lua_name.get(&new_lua_name) {
            return Err(ReplaceError::LuaNameMismatch {
                name: name.to_string(),
                old_lua_name: other.config.name.clone(),
                new_lua_name,
            });
        }

        guard
            .by_name
            .insert(name.to_string(), Arc::clone(&new_handle));
        guard.by_lua_name.insert(new_lua_name, new_handle);
        tracing::info!(
            upstream = %name,
            "upstream handle registered (register_or_replace; fresh insertion)"
        );
        Ok(None)
    }

    /// Remove the upstream registered under `name`, evicting it from
    /// both lookup indexes, and return the displaced handle so the
    /// caller can `close()` it. Returns `None` if no upstream is
    /// registered under `name`.
    ///
    /// This is the single-upstream counterpart to [`Self::drain`]:
    /// `drain` empties the whole registry at shutdown; `remove` excises
    /// exactly one entry, which is what the reload diff's "removed" path
    /// needs. The eviction discipline mirrors [`Self::register`] —
    /// `by_lua_name` is evicted alongside `by_name`.
    pub async fn remove(&self, name: &str) -> Option<Arc<UpstreamHandle>> {
        let mut guard = self.inner.write().await;
        let removed = guard.by_name.remove(name)?;
        let lua_name = removed.config.lua_name.clone();
        guard.by_lua_name.remove(&lua_name);

        tracing::info!(
            upstream = %name,
            "upstream handle removed (reload diff or operator-driven removal)"
        );
        Some(removed)
    }
}

/// Errors produced by [`UpstreamManager::replace`] and
/// [`UpstreamManager::register_or_replace`]. Each variant corresponds
/// to a programmer-error class the supervisor can log without touching
/// the registry — both methods are all-or-nothing, so any error leaves
/// the existing registry contents in place.
#[derive(Debug, thiserror::Error)]
pub enum ReplaceError {
    /// No upstream is currently registered under this display name.
    /// Probably the supervisor and the shutdown path raced — the
    /// manager drained before the respawn could swap. Caller should
    /// log and abandon the respawn.
    #[error("cannot replace unknown upstream {0:?}")]
    UnknownName(String),

    /// The new handle's `config.name` doesn't match the name passed
    /// to `replace`. Caught as a safety rail so a supervisor that
    /// built the wrong handle can't corrupt the registry.
    #[error(
        "replace target name {requested:?} does not match new handle's config.name {new_handle_name:?}"
    )]
    NameMismatch {
        requested: String,
        new_handle_name: String,
    },

    /// The new handle's `lua_name` doesn't match the displaced handle's.
    /// Replacing with a different `lua_name` would silently change the
    /// downstream Lua global's identity — rejected to keep the agent-
    /// visible binding stable.
    #[error(
        "replace on {name:?}: new handle's lua_name {new_lua_name:?} differs from existing {old_lua_name:?}"
    )]
    LuaNameMismatch {
        name: String,
        old_lua_name: String,
        new_lua_name: String,
    },
}

/// Errors surfaced by the upstream subsystem.
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error("upstream not found: {0}")]
    NotFound(String),

    #[error("failed to spawn stdio upstream {name}: {source}")]
    StdioSpawn {
        name: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to connect upstream {name}: {source}")]
    Connect {
        name: String,
        #[source]
        source: anyhow::Error,
    },

    /// The upstream didn't respond to a tool call within the configured
    /// per-call deadline. This is *our* deadline, not rmcp's: we dropped
    /// the in-flight future after `deadline_ms` elapsed.
    ///
    /// Named carefully so it doesn't collide with
    /// `rmcp::ServiceError::Timeout`, which is rmcp's own request-level
    /// timeout (not wired in our call sites today). The supervisor
    /// increments its consecutive-failure counter on this variant the
    /// same way it does on [`UpstreamError::ServiceCall`] with a
    /// `ServiceError::TransportClosed` inside — both indicate the
    /// transport layer is sick, as opposed to a peer-returned
    /// `McpError` which typically reflects tool-shape problems on the
    /// caller side.
    #[error("upstream {name:?} did not return within {deadline_ms}ms")]
    CallTimeout { name: String, deadline_ms: u64 },

    /// A tool call returned a rmcp `ServiceError`. Wrapped with the
    /// upstream name so downstream error handlers can tell *which*
    /// upstream produced the failure without the caller having to
    /// thread the name through separately.
    ///
    /// This covers every non-timeout failure mode: `TransportClosed`
    /// (the child exited / the HTTP stream died), `McpError` (the peer
    /// rejected our request with a JSON-RPC error), rmcp's internal
    /// `Timeout`, and the residual `UnexpectedResponse` case.
    #[error("upstream {name:?} call failed: {source}")]
    ServiceCall {
        name: String,
        #[source]
        source: rmcp::ServiceError,
    },

    /// The supervisor has classified this upstream as unhealthy
    /// (Unhealthy / Respawning / Dead); `call_tool` short-circuited
    /// without dispatching the request.
    ///
    /// This is a *fast-fail* signal — as opposed to `CallTimeout`,
    /// which blocks for `call_timeout` before deciding. The agent
    /// should see this as "try again in a moment" rather than "retry
    /// this specific tool call" — the right response is typically to
    /// surface a clear "upstream X is currently unavailable"
    /// message, not to spin on retries against a dead pipe.
    ///
    /// The `state` payload lets callers distinguish the sub-states
    /// for logging: Unhealthy ("supervisor is about to respawn"),
    /// Respawning ("in flight now"), Dead ("gave up; will re-arm
    /// periodically"). Downstream agents typically don't care which
    /// — they just need to know the upstream isn't taking calls.
    #[error("upstream {name:?} is currently unavailable (state: {state:?})")]
    TransientUnavailable {
        name: String,
        state: crate::health::HealthState,
    },

    /// An OAuth upstream has no usable persisted credentials, so the
    /// gateway cannot connect headlessly. This is a *configuration
    /// precondition* failure, not a transport sickness: the operator must
    /// run the interactive `login` subcommand once (which opens a browser
    /// and writes credentials to disk) before the gateway can reach this
    /// upstream.
    ///
    /// Surfaced fast — *before* any network I/O — so a not-yet-logged-in
    /// OAuth upstream never wedges concurrent boot and the supervisor can
    /// classify it as "waiting for human action" rather than spinning
    /// respawn attempts against it. See
    /// [`Self::is_actionable_precondition`].
    #[error(
        "upstream {name:?} requires a one-time INTERACTIVE OAuth login by a human operator before \
         the gateway can reach it. You (the agent) cannot do this yourself — it opens a browser. \
         Ask the user to run `rato login {name}` in their terminal; the upstream \
         connects automatically once they do."
    )]
    OAuthLoginRequired { name: String },

    /// An OAuth upstream that *was* authenticated can no longer
    /// authenticate: its refresh token is dead or absent, so rmcp's
    /// silent refresh can't recover it. Distinct from
    /// [`Self::OAuthLoginRequired`] (which means "never logged in") so the
    /// operator-facing message can say "re-authenticate" rather than
    /// "log in," but it shares the same fix and the same classification:
    /// it's an [actionable precondition](Self::is_actionable_precondition)
    /// — only a human re-running `login` clears it, no amount of retrying
    /// will.
    ///
    /// Reached two ways:
    ///   - at connect/handshake time (a restart after the refresh token
    ///     already died — see `connect_oauth_headless`), and
    ///   - mid-life, when a `call_tool` against a previously-healthy
    ///     upstream comes back with an auth failure (see
    ///     [`UpstreamHandle::call_tool`], which also flips the health
    ///     record to [`crate::health::HealthState::NeedsLogin`]).
    #[error(
        "upstream {name:?} can no longer authenticate (OAuth token expired/revoked) and requires \
         INTERACTIVE re-authentication by a human operator. You (the agent) cannot fix this \
         yourself — it opens a browser. Ask the user to run `rato login {name}` in \
         their terminal; the upstream reconnects automatically once they do."
    )]
    OAuthReauthRequired { name: String },
}

impl UpstreamError {
    /// The upstream name this error refers to, if any. `NotFound` carries
    /// the name inline; the other variants capture it as a struct field.
    /// Used by the supervisor to route failure signals to the right
    /// health-state record.
    pub fn upstream_name(&self) -> Option<&str> {
        match self {
            UpstreamError::NotFound(name) => Some(name.as_str()),
            UpstreamError::StdioSpawn { name, .. }
            | UpstreamError::Connect { name, .. }
            | UpstreamError::CallTimeout { name, .. }
            | UpstreamError::ServiceCall { name, .. }
            | UpstreamError::TransientUnavailable { name, .. }
            | UpstreamError::OAuthLoginRequired { name }
            | UpstreamError::OAuthReauthRequired { name } => Some(name.as_str()),
        }
    }

    /// True if this error indicates a *transport-level* sickness — the
    /// upstream is wedged or dead, not just that a particular call had
    /// bad arguments. The supervisor's consecutive-failure counter
    /// increments only on these; peer-returned `McpError`s don't, to
    /// avoid respawning an upstream because an agent sent it garbage.
    ///
    /// `TransientUnavailable` is notably **not** transport-level: it's
    /// the supervisor's own fast-fail signal after it already decided
    /// the upstream is sick. The supervisor must not double-count it
    /// by incrementing the health counter on an error it produced.
    pub fn is_transport_level(&self) -> bool {
        match self {
            UpstreamError::CallTimeout { .. } => true,
            UpstreamError::ServiceCall { source, .. } => matches!(
                source,
                rmcp::ServiceError::TransportClosed
                    | rmcp::ServiceError::Timeout { .. }
                    | rmcp::ServiceError::TransportSend(_)
            ),
            UpstreamError::NotFound(_)
            | UpstreamError::StdioSpawn { .. }
            | UpstreamError::Connect { .. }
            | UpstreamError::TransientUnavailable { .. }
            | UpstreamError::OAuthLoginRequired { .. }
            | UpstreamError::OAuthReauthRequired { .. } => false,
        }
    }

    /// True when the caller should fast-fail / defer rather than
    /// retry the same call. `TransientUnavailable` is the canonical
    /// "upstream is gated, don't waste cycles" signal; the gateway's
    /// agent-facing error formatter can use this to produce a crisp
    /// "try again in a moment" message instead of a verbose
    /// transport-level stack.
    pub fn is_supervisor_gated(&self) -> bool {
        matches!(self, UpstreamError::TransientUnavailable { .. })
    }

    /// True when this error reflects a precondition the *operator* must
    /// satisfy out-of-band — not something retrying will ever fix on its
    /// own. Today the sole case is [`Self::OAuthLoginRequired`]: until a
    /// human runs `login`, every reconnect attempt will fail identically.
    ///
    /// The supervisor uses this to avoid burning its respawn attempt
    /// budget on a doomed reconnect loop: an actionable-precondition
    /// failure parks the upstream in `Dead` (which still re-arms slowly,
    /// so it recovers automatically once `login` runs) rather than
    /// hammering `connect()` at the fast respawn cadence. See
    /// `supervisor::UpstreamSupervisor::tick_respawning`.
    pub fn is_actionable_precondition(&self) -> bool {
        matches!(
            self,
            UpstreamError::OAuthLoginRequired { .. } | UpstreamError::OAuthReauthRequired { .. }
        )
    }
}

#[cfg(test)]
#[path = "upstream_tests.rs"]
mod tests;
