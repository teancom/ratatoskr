// pattern: Imperative Shell
//
// ServerHandler for the gateway's downstream-facing MCP server.
//
// Submodule layout:
//   errors         — pure error-conversion helpers (lua_error_to_tool_outcome, etc.)
//   session        — session-resolution helpers (SessionResolution, require_session, etc.)
//   catalog        — read-only metadata tools (server-details, tool-details, …)
//   execute        — the `execute` tool (Luau VM, inspection guard, offload config)
//   resources      — resources/list + resources/templates/list + resources/read
//   subscriptions  — resources/subscribe + resources/unsubscribe (ref-counted)
//
// This file keeps `SessionState`, `GatewayHandler::new`, `Drop`, the
// `ServerHandler` trait impl (which dispatches to family methods), the
// cross-family `require_upstream_by_lua_name` helper, and
// `build_server_instructions`.
//
// Constructing rmcp model types — house rule
// ---------------------------------------------------------------------
// Many rmcp::model types carry `#[non_exhaustive]`, which means a bare
// `Type { a: 1, b: 2 }` struct-literal breaks on the next rmcp minor
// bump the moment the upstream adds a field. Every construction site
// for an rmcp non_exhaustive type must use one of:
//
//   - a builder (`Tool::new(...)`, `CallToolResult::success(...)`,
//     `ServerInfo::new(...).with_server_info(...)`, `Implementation::new(...)`,
//     `ServerCapabilities::builder()...build()`), or
//   - `let mut x = Type::default(); x.field = ...; x.other = ...;` — see
//     `crates/gateway/src/serve.rs::build_session_manager` for the
//     canonical example and `tool_details.rs` tests for another, or
//   - `Type { ..Default::default() }` struct-update syntax.
//
// Struct-literal syntax (`Type { a, b }`) is fine on rmcp types marked
// `#[expect(clippy::exhaustive_structs, reason = "intentionally exhaustive")]`
// — `ListToolsResult` / `ListResourcesResult` / `ListResourceTemplatesResult`
// (via the `paginated_result!` macro), `RawTextContent`, `ResourceContents`,
// `Content`, `Resource`, `ResourceTemplate`, `ResourceUpdatedNotificationParam`.
// Those are by-contract stable; the rmcp authors promise new fields won't
// land there.
//
// Adding a new rmcp-typed construction? Pick the right idiom for the
// non_exhaustive bit. `rg '\b(Tool|ServerCapabilities|Implementation|
// CallToolResult|CallToolRequestParams|ReadResourceRequestParams|
// ReadResourceResult|SubscribeRequestParams|UnsubscribeRequestParams|
// PaginatedRequestParams|InitializeResult|ServerInfo|ClientCapabilities) \{'
// should find zero struct-literal hits across `crates/`.

mod errors;
mod session;

mod admin;
mod catalog;
mod execute;
mod resources;
mod subscriptions;

// Re-export `EXECUTE_TOOL_DESCRIPTION` so `tools_schema` can import it
// via the stable `crate::handler::EXECUTE_TOOL_DESCRIPTION` path
// regardless of which submodule owns the constant. The prose itself
// lives in `execute.rs` alongside the cross-reference tests.
pub(crate) use execute::EXECUTE_TOOL_DESCRIPTION;

use std::sync::{Arc, OnceLock};

use lua_runtime::{ScriptInputError, parse_script_argument};
use mcp_client::{PeerId, UpstreamConfig, UpstreamHandle, UpstreamKind, resource_uri};
use rmcp::{
    ErrorData, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation,
        ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
        ReadResourceRequestParams, ReadResourceResult, Resource, ResourceTemplate,
        ServerCapabilities, ServerInfo, SubscribeRequestParams, Tool, UnsubscribeRequestParams,
    },
    service::{NotificationContext, RequestContext, RoleServer},
};
use tracing::Instrument;
use uuid::Uuid;

use crate::app_context::AppContext;
use crate::hint;
use crate::list_servers;
use crate::offload_message;
use crate::response_shape;
use crate::response_shape_cache::ShapeKey;
use crate::server_details;
use crate::session_forwarder::SessionForwarder;
use crate::tool_details;
use crate::tool_inspection::{InspectedTool, ToolInspectionStore};
use crate::tools_schema;

use errors::{build_offload_config, execution_outcome_to_content, lua_error_to_tool_outcome};
use session::{
    SessionResolution, parse_string_arg, require_session, resolve_session_from_parts,
    session_id_from_context,
};

/// Header name rmcp's streamable-http-server sets on every request
/// carrying a session. We read it out of the `http::request::Parts`
/// that rmcp stashes in `RequestContext::extensions`. Hardcoded
/// because rmcp's `HEADER_SESSION_ID` constant is `pub(crate)`; this
/// matches the MCP spec and rmcp's internal literal.
const MCP_SESSION_ID_HEADER: &str = "Mcp-Session-Id";

/// Per-downstream-session handler state.
///
/// One instance is created per downstream session by the factory closure
/// in `serve.rs` and torn down (via `Drop`) when the session ends. This
/// means `Drop` fires on the correct lifecycle boundary — at the moment
/// the downstream connection closes, not at process shutdown.
///
/// Shape: `ctx` carries the process-wide shared state (upstream registry,
/// caches, event bus, inspection store, the `LuaRuntime`, etc.); the
/// remaining fields are genuinely per-session.
///
/// `forwarder` and `registered_peer` are `OnceLock<T>` because they are
/// set exactly once during `on_initialized` (after the downstream client
/// announces its capabilities) and must be accessible for the lifetime
/// of the session. `Drop` uses `registered_peer` to deregister from the
/// sampling broker; `forwarder` is dropped implicitly when the
/// `SessionState` goes away, which shuts down the notification-forwarding
/// task.
struct SessionState {
    /// Process-wide shared state. See `app_context.rs` for the
    /// per-field sharing rationale.
    ctx: AppContext,

    /// Notification forwarder for this session's downstream client.
    /// Set once during `on_initialized`; dropped on session close,
    /// which shuts down the forwarding task.
    forwarder: OnceLock<SessionForwarder>,

    /// PeerId we registered with the broker on `on_initialized`.
    /// `Some(_)` only if the downstream client advertised the
    /// `sampling` capability.
    registered_peer: OnceLock<PeerId>,
}

impl Drop for SessionState {
    fn drop(&mut self) {
        // If we registered a peer, deregister it on session drop so the
        // broker doesn't hold a dangling peer that will fail on dispatch.
        // Drop runs synchronously, so we spawn a task for the async call.
        if let Some(peer_id) = self.registered_peer.get().copied() {
            let broker = self.ctx.broker.clone();
            tokio::spawn(async move {
                broker.deregister(peer_id).await;
            });
        }
    }
}

#[derive(Clone)]
pub struct GatewayHandler {
    state: Arc<SessionState>,
}

impl GatewayHandler {
    /// Construct a handler for one downstream session from the
    /// process-wide `AppContext`. The factory closure in `serve::serve_on`
    /// calls this once per session.
    pub fn new(ctx: AppContext) -> Self {
        Self {
            state: Arc::new(SessionState {
                ctx,
                forwarder: OnceLock::new(),
                registered_peer: OnceLock::new(),
            }),
        }
    }
}

impl GatewayHandler {
    /// Look up an upstream by its Lua-sanitized name and wrap the
    /// "not registered" case as a JSON-RPC `invalid_params`.
    ///
    /// Thin adapter over `UpstreamManager::handle_by_lua_name`: the
    /// manager is the single source of truth for lua_name → handle
    /// resolution. The handler only adds the "missing upstream is a
    /// client-visible `invalid_params` error" policy, which is
    /// specifically a handler-level concern (the manager doesn't know
    /// about JSON-RPC error shapes).
    ///
    /// Used by every tool that takes a `luaServerName` argument plus
    /// `resources/read`, `resources/subscribe`, and
    /// `resources/unsubscribe` — seven call sites across the catalog,
    /// resources, and subscriptions families. Lives in `mod.rs` rather
    /// than any single family because every family needs it.
    pub(super) async fn require_upstream_by_lua_name(
        &self,
        lua_server_name: &str,
    ) -> Result<Arc<UpstreamHandle>, ErrorData> {
        self.state
            .ctx
            .upstreams
            .handle_by_lua_name(lua_server_name)
            .await
            .ok_or_else(|| {
                ErrorData::invalid_params(
                    format!("unknown upstream server: {lua_server_name}"),
                    None,
                )
            })
    }

    /// Internal dispatch body for the `ServerHandler::call_tool` router.
    /// Split out so the public trait method can wrap it in an
    /// instrumentation span and duration measurement without the span
    /// scope swallowing the match arms' own layering.
    ///
    /// Not public — rmcp's ServerHandler trait is the only caller.
    #[allow(clippy::too_many_lines)]
    async fn dispatch_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        match request.name.as_ref() {
            "ping" => Ok(CallToolResult::success(vec![Content::text("pong")])),

            "list-servers" => {
                let catalog = self.build_server_catalog().await;
                let entries: Vec<list_servers::CatalogEntry<'_>> = catalog
                    .iter()
                    .map(|(cfg, hint_opt, health_opt)| {
                        list_servers::CatalogEntry::new(cfg, hint_opt.as_deref())
                            .with_health(*health_opt)
                    })
                    .collect();
                // SystemTime::now() is the only impure bit — passed
                // into the (pure) renderer rather than called inside.
                let rendered = list_servers::render(&entries, std::time::SystemTime::now());
                Ok(CallToolResult::success(vec![Content::text(rendered)]))
            }

            "server-details" => self.call_server_details(&request).await,

            "list-server-tools" => self.call_list_server_tools(&request).await,

            "tool-details" => {
                let session_id = require_session(session_id_from_context(&context))?;
                self.call_tool_details(&request, session_id.as_deref())
                    .await
            }

            "inspect-tool-response" => {
                let session_id = require_session(session_id_from_context(&context))?;
                self.call_inspect_tool_response(&request, session_id.as_deref())
                    .await
            }

            "execute" => self.call_execute(request, &context).await,

            "reload-config" => self.call_reload_config().await,

            "reconnect-upstream" => self.call_reconnect_upstream(&request).await,

            "restart-gateway" => self.call_restart_gateway(),

            other => Err(ErrorData::invalid_params(
                format!("unknown tool: {other}"),
                None,
            )),
        }
    }
}

/// Render the downstream-facing `instructions` string.
///
/// A fixed preamble describing the tool surface and the shape of
/// resource URIs. This is the historical "server instructions"
/// string; it's load-bearing because agents parse it once at
/// session init and rely on it throughout a session.
///
/// Extracted so it's reachable from unit tests without a full handler.
fn build_server_instructions() -> String {
    let mut out = String::new();
    // Lead with the catalog-first directive, before the tool enumeration.
    // Agents boot with a flat tool list that includes same-named, often
    // UNauthenticated connectors (e.g. account-level `claude.ai *` MCP
    // integrations whose `authenticate` tool invites an OAuth flow). When a
    // user names a capability, an agent tends to pattern-match it onto that
    // same-named tool and trigger its auth flow — never calling list-servers
    // to discover that this gateway already fronts the same capability,
    // authenticated and ready. This init-time nudge is intentionally
    // category-level (no service names): naming individual upstreams here
    // doesn't scale and only helps the one literal string mentioned. The
    // load-bearing fix lives at the point of action — the `list-servers`
    // tool *description* (tools_schema.rs), which the agent reads at the
    // moment it's choosing between `authenticate` and listing. Init-time
    // prose can't beat a point-of-action tool description; this just primes.
    out.push_str(
        "This gateway proxies many upstream MCP servers behind a small, fixed tool \
         surface. The upstreams are NOT in your flat tool list — they are reached \
         through these tools, and several may already be authenticated and ready. \
         A same-named tool in your flat list (especially one offering to \
         authenticate or set itself up) is often a redundant, unauthenticated \
         duplicate of an upstream already available here. So before triggering any \
         authentication, setup, or OAuth flow, call list-servers: the capability you \
         want is often already here as a ready upstream. When in doubt, list first.\n\n",
    );
    out.push_str("Rust rewrite. Tools: ");
    // Read tool names from the canonical lists in `tools_schema` so renames
    // propagate here automatically. The tests in this module pin the
    // `_gateway.*` builtin enumeration and the response-shape tag set,
    // which aren't deduplicated because they live in separate crates.
    out.push_str(&tools_schema::BASE_TOOL_NAMES.join(", "));
    out.push_str(
        ". Start with list-servers to see the catalog (includes a one-line hint per \
         upstream). Use server-details to read an upstream's full advertised instructions \
         when the catalog hint isn't enough. To call an upstream tool from an execute \
         script, first fetch its schema via tool-details (or probe via \
         inspect-tool-response). tool-details output may include `responseShape` + \
         `responseShapeHint` (populated from prior probes or the upstream's output schema) \
         pointing at the right `_gateway.*` builtin — honor it rather than rediscovering \
         the envelope shape by hand. Resources are exposed with \
         `mcp-proxy://<lua_server>/<percent-encoded-uri>` URIs; pass the namespaced URI \
         verbatim to `resources/read`, `resources/subscribe`, and `resources/unsubscribe`. \
         Inside `execute` scripts, the `_gateway` global carries JSON, time, \
         content-envelope, template, and concurrent-dispatch helpers \
         (`json_decode`, `json_encode`, `unwrap_content`, `rfc3339`, `now_minus`, \
         `expand_template`, `parallel`, `get_result`) — read the execute tool's \
         own description for the per-builtin contract rather than reinventing them.",
    );

    out
}

impl ServerHandler for GatewayHandler {
    fn get_info(&self) -> ServerInfo {
        // Resources are aggregated across upstreams with namespaced URIs
        // (see `resource_uri.rs` for the scheme). We advertise:
        //   - `resources/list_changed`: `SessionForwarder` fans upstream
        //     `ResourceListChanged` events out to downstream clients.
        //   - `resources/subscribe`: `GatewayHandler::subscribe` routes
        //     through `SubscriptionRegistry` with ref-counting so a
        //     single upstream subscription serves N downstream sessions,
        //     and `SessionForwarder` delivers `resources/updated`
        //     selectively based on the registry.
        //
        // We advertise subscribe unconditionally. If the downstream
        // session subscribes to a URI on an upstream that doesn't
        // support subscriptions, the upstream error propagates with
        // an `upstream '<name>'` prefix — matches our posture for
        // `resources/read`.
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_resources_list_changed()
                .enable_resources_subscribe()
                .build(),
        )
        .with_server_info(
            Implementation::new("rato", env!("CARGO_PKG_VERSION"))
                .with_title("Ratatoskr MCP Gateway"),
        )
        .with_instructions(build_server_instructions())
    }

    async fn on_initialized(&self, context: NotificationContext<RoleServer>) {
        let state = self.state.clone();

        // Resolve the session ID so the forwarder can filter
        // `resources/updated` to only the session that subscribed.
        // stdio and malformed HTTP contexts produce `None`; the
        // forwarder drops ResourceUpdated events in that case.
        let session_id =
            resolve_session_from_parts(context.extensions.get::<http::request::Parts>());
        let session_id_for_forwarder = match session_id {
            SessionResolution::HttpSession(id) => Some(id),
            SessionResolution::NoTransportContext | SessionResolution::MissingHeader => None,
        };

        // Attach the notification forwarder regardless of sampling.
        let _ = state.forwarder.set(SessionForwarder::spawn(
            context.peer.clone(),
            state.ctx.bus.clone(),
            state.ctx.subscriptions.clone(),
            session_id_for_forwarder,
        ));

        // Register for sampling only if the downstream client actually
        // advertised the capability. Capability-gating downstream-side
        // matters because peer.create_message() will fail with a protocol
        // error if the peer hasn't declared sampling support — we'd rather
        // not route to peers that can't handle it.
        let has_sampling = context
            .peer
            .peer_info()
            .and_then(|info| info.capabilities.sampling.as_ref().map(|_| ()))
            .is_some();

        if has_sampling {
            let peer_id = state.ctx.broker.register(context.peer.clone()).await;
            let _ = state.registered_peer.set(peer_id);
            tracing::info!(?peer_id, "downstream registered for sampling");
        } else {
            tracing::debug!("downstream did not advertise sampling; not registered with broker");
        }

        tracing::info!("downstream client initialized; forwarder attached");
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let tools = vec![
            tools_schema::ping_tool(),
            tools_schema::list_servers_tool(),
            tools_schema::server_details_tool(),
            tools_schema::list_server_tools_tool(),
            tools_schema::tool_details_tool(),
            tools_schema::inspect_tool_response_tool(),
            tools_schema::execute_tool(),
            tools_schema::reload_config_tool(),
            tools_schema::reconnect_upstream_tool(),
            tools_schema::restart_gateway_tool(),
        ];

        Ok(ListToolsResult {
            tools,
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        // Per-request correlation ID. UUID v4 is local-only — we never
        // expect the downstream agent to pass correlation tokens through
        // the MCP envelope. This is for *operators* reading logs; agents
        // never see it. Everything under this span inherits `req_id`,
        // so a single log line ties the handler dispatch, the Lua
        // execution span, and every upstream call_tool into one thread
        // of story.
        let req_id = Uuid::new_v4();
        let tool_name = request.name.to_string();
        let session_id_peek = match crate::handler::session::session_id_from_context(&context) {
            crate::handler::session::SessionResolution::HttpSession(id) => Some(id),
            _ => None,
        };

        let span = tracing::info_span!(
            "call_tool",
            req_id = %req_id,
            tool = %tool_name,
            session = ?session_id_peek,
        );

        // Manual span + timing rather than `#[instrument]` because
        // rmcp's ServerHandler trait uses `async fn` and the attribute
        // macro doesn't compose cleanly with the trait-method form
        // without a proc-macro rewrite. The shape below is what
        // `#[instrument(skip_all)]` would expand to anyway.
        let start = std::time::Instant::now();
        let result = self
            .dispatch_tool(request, context)
            .instrument(span.clone())
            .await;
        let duration_ms = start.elapsed().as_millis() as u64;

        let outcome = match &result {
            Ok(r) if r.is_error.unwrap_or(false) => "tool_error",
            Ok(_) => "ok",
            Err(_) => "protocol_error",
        };

        // One line per call, at exit, at info: it carries the tool name
        // (via the span), duration, and outcome. This is the gateway's
        // only per-tool usage record — the launchd daemon runs at info,
        // so anything quieter is invisible in production and questions
        // like "does anyone ever call the admin tools?" become
        // unanswerable. Volume is modest by construction: the gateway
        // exposes ~a dozen meta-tools, not the flattened upstream
        // catalog. Entry-level logging would just be noise given the
        // span already carries the tool name.
        span.in_scope(|| {
            tracing::info!(duration_ms, outcome, "call_tool complete");
        });

        result
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        // No pagination: the resource cache returns every upstream's
        // full listing at once, and no user-config upstream today ships
        // enough resources to need pages. If that changes, pagination
        // lands here, not in the cache.
        let resources = self.aggregate_resources().await;
        Ok(ListResourcesResult {
            resources,
            next_cursor: None,
            meta: None,
        })
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        let resource_templates = self.aggregate_resource_templates().await;
        Ok(ListResourceTemplatesResult {
            resource_templates,
            next_cursor: None,
            meta: None,
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        self.call_read_resource(request).await
    }

    /// Subscribe a downstream session to updates on a namespaced
    /// resource URI or template.
    ///
    /// Branches on the bare upstream string shape:
    ///   - Contains `{` → RFC 6570 template, compiled via the Level
    ///     1-2 matcher and stored in the template index. Level 3-4
    ///     constructs surface as `invalid_params` from
    ///     `CompiledTemplate::compile`.
    ///   - Otherwise → concrete URI, stored in the concrete index.
    ///
    /// In both cases the ref-counted delta from the registry decides
    /// whether an upstream RPC is required for this subscription.
    async fn subscribe(
        &self,
        request: SubscribeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.call_subscribe(request, &context).await
    }

    /// Unsubscribe a downstream session from a namespaced URI or template.
    ///
    /// Branch structure mirrors `subscribe`. Concrete-vs-template
    /// dispatch is required because the registry indexes them
    /// separately and the upstream RPC needs to match the original
    /// subscribe shape.
    async fn unsubscribe(
        &self,
        request: UnsubscribeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.call_unsubscribe(request, &context).await
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
