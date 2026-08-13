// pattern: Imperative Shell
//
// Catalog family: read-only metadata tools. These surface what upstreams
// exist, what tools they advertise, and what the gateway has observed
// about their response shapes. No side-effects on upstream state; the
// only writes are bookkeeping on the inspection store and the
// response-shape cache.
//
// Tools covered here: `list-server-tools`, `server-details`,
// `tool-details`,
// `inspect-tool-response`, plus the `list-servers` catalog builder
// (dispatched from `mod.rs`). Also hosts `mark_inspected_if_session`,
// which is shared with the execute family via the inspection store
// contract.
//
// Mirror-changes rule: `EXECUTE_TOOL_DESCRIPTION` lives in `execute.rs`;
// keep the response-shape tag set in this file's `resolve_response_shape`
// in sync with the enum in `response_shape.rs`.

use super::*;

impl GatewayHandler {
    pub(super) async fn call_list_server_tools(
        &self,
        request: &CallToolRequestParams,
    ) -> Result<CallToolResult, ErrorData> {
        let lua_server_name = parse_string_arg(request, "luaServerName")?;
        let handle = self.require_upstream_by_lua_name(&lua_server_name).await?;
        let tools = self.fetch_cached_tools(&handle).await?;
        let rendered = tool_details::render_tool_summary(&tools);
        Ok(CallToolResult::success(vec![Content::text(rendered)]))
    }

    /// Handle a `server-details` call: look up the named upstream,
    /// ask it for its advertised identity + instructions, and render
    /// the JSON response. No side effects (no inspection marking —
    /// this is a metadata read, not a tool dispatch).
    pub(super) async fn call_server_details(
        &self,
        request: &CallToolRequestParams,
    ) -> Result<CallToolResult, ErrorData> {
        let lua_server_name = parse_string_arg(request, "luaServerName")?;
        let handle = self.require_upstream_by_lua_name(&lua_server_name).await?;

        let upstream_instructions = handle.server_instructions();
        let server_impl = handle.server_implementation();
        let resolved_hint = hint::resolve_hint(
            handle.config.hint.as_deref(),
            upstream_instructions.as_deref(),
        );
        let kind_label = match &handle.config.kind {
            UpstreamKind::Stdio { .. } => "stdio",
            UpstreamKind::Http { .. } => "http",
            UpstreamKind::HttpOAuth { .. } => "http-oauth",
        };

        let (server_name, server_version) = match &server_impl {
            Some((n, v)) => (Some(n.as_str()), Some(v.as_str())),
            None => (None, None),
        };

        let health = server_details_health(handle.health_snapshot().state, &handle.config.name);

        let input = server_details::ServerDetailsInput {
            name: &handle.config.name,
            lua_name: &handle.config.lua_name,
            kind: kind_label,
            server_name,
            server_version,
            hint: resolved_hint.as_deref(),
            upstream_instructions: upstream_instructions.as_deref(),
            health,
        };

        Ok(CallToolResult::success(vec![Content::text(
            server_details::render(&input),
        )]))
    }

    /// Build the `list-servers` catalog by pairing each registered
    /// upstream's config with its resolved hint.
    ///
    /// Hint resolution happens here rather than inside `list_servers`
    /// so the Functional Core module stays pure: it doesn't need to
    /// know that a hint can come from a config override OR from an
    /// upstream's live `InitializeResult.instructions`. The shell
    /// owns that policy; the core just renders.
    ///
    /// Returns owned configs + owned resolved hints, then converts to
    /// borrowed `CatalogEntry`s on the way into `render`. The two-step
    /// ownership dance is load-bearing because `CatalogEntry` holds
    /// `&str`s — we need a stable allocation to point into.
    pub(super) async fn build_server_catalog(
        &self,
    ) -> Vec<(
        UpstreamConfig,
        Option<String>,
        Option<mcp_client::HealthSnapshot>,
    )> {
        // Snapshot the config list — cheap `Vec<UpstreamConfig>` clone.
        let configs = self.state.ctx.upstreams.list().await;
        let mut catalog = Vec::with_capacity(configs.len());
        for config in configs {
            let (hint_string, health) =
                match self.state.ctx.upstreams.handle_by_name(&config.name).await {
                    Some(handle) => (
                        hint::resolve_hint(
                            config.hint.as_deref(),
                            handle.server_instructions().as_deref(),
                        ),
                        Some(handle.health_snapshot()),
                    ),
                    // Upstream wasn't registered (e.g. bootstrap dropped
                    // it). Fall back to the config-only path so an
                    // authored override still shows — the user wrote that
                    // hint for a reason, and absence of a live handle
                    // shouldn't silence it. Health stays None; the
                    // renderer prints no health tag in that case.
                    None => (hint::resolve_hint(config.hint.as_deref(), None), None),
                };
            catalog.push((config, hint_string, health));
        }
        catalog
    }

    /// Fetch the tool list for `handle` via the shared cache.
    ///
    /// Centralized so `list-server-tools`, `tool-details`, and
    /// `inspect-tool-response` all get the same (cached) view, and a
    /// future change to caching/RPC strategy lands in exactly one place.
    /// The cache returns `Arc<Vec<Tool>>`; the Arc is cheap to clone
    /// per call, and callers only need `&[Tool]` so we dereference
    /// through the Arc at the use site.
    async fn fetch_cached_tools(
        &self,
        handle: &UpstreamHandle,
    ) -> Result<Arc<Vec<Tool>>, ErrorData> {
        self.state.ctx.tool_cache.get(handle).await.map_err(|e| {
            ErrorData::internal_error(format!("failed to list tools on upstream: {e}"), None)
        })
    }

    pub(super) async fn call_tool_details(
        &self,
        request: &CallToolRequestParams,
        session_id: Option<&str>,
    ) -> Result<CallToolResult, ErrorData> {
        let lua_server_name = parse_string_arg(request, "luaServerName")?;
        let lua_tool_name = parse_string_arg(request, "luaToolName")?;

        let handle = self.require_upstream_by_lua_name(&lua_server_name).await?;
        let tools = self.fetch_cached_tools(&handle).await?;

        let tool = match tool_details::find_by_lua_name(&tools, &lua_tool_name) {
            Ok(t) => t,
            Err(tool_details::LookupError::NotFound) => {
                let available = tool_details::sanitized_tool_names(&tools).join(", ");
                // Return as CallToolResult::error so the agent can react,
                // rather than JSON-RPC error which implies a gateway bug.
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "tool '{lua_tool_name}' not found on server '{lua_server_name}'. \
                     available tools: {available}"
                ))]));
            }
            Err(tool_details::LookupError::Ambiguous { originals }) => {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "tool '{lua_tool_name}' is ambiguous on server '{lua_server_name}': \
                     upstream tools {} all sanitize to the same identifier and are therefore \
                     not callable. rename one of them upstream.",
                    originals.join(", ")
                ))]));
            }
        };

        let shape = self.resolve_response_shape(&lua_server_name, &lua_tool_name, tool);
        let rendered = tool_details::render_tool_details(tool, shape);

        // Mark inspected only on success. Missing session ID means the
        // client skipped the `Mcp-Session-Id` header (e.g. stdio
        // transport, or a pre-initialize request) — render the details
        // without failing but don't mark, because there's no session
        // to mark against.
        self.mark_inspected_if_session(session_id, &lua_server_name, &lua_tool_name);

        Ok(CallToolResult::success(vec![Content::text(rendered)]))
    }

    /// Resolve the best-known `ResponseShape` for this tool. Prefers a
    /// cached observation (populated by prior `inspect-tool-response`
    /// calls in any session), falls back to the tool's declared
    /// `outputSchema`, returns `None` when neither says anything useful.
    ///
    /// Probe beats schema because probe has seen the actual reply —
    /// schema only tells us what the upstream claims it *could* return.
    fn resolve_response_shape(
        &self,
        lua_server: &str,
        lua_tool: &str,
        tool: &Tool,
    ) -> Option<response_shape::ResponseShape> {
        let key = ShapeKey::new(lua_server, lua_tool);
        if let Some(observed) = self.state.ctx.response_shapes.lookup(&key) {
            return Some(observed);
        }
        let out_schema = tool
            .output_schema
            .as_ref()
            .map(|s| serde_json::Value::Object((**s).clone()));
        response_shape::classify_from_schema(out_schema.as_ref())
    }

    pub(super) async fn call_inspect_tool_response(
        &self,
        request: &CallToolRequestParams,
        session_id: Option<&str>,
    ) -> Result<CallToolResult, ErrorData> {
        let lua_server_name = parse_string_arg(request, "luaServerName")?;
        let lua_tool_name = parse_string_arg(request, "luaToolName")?;
        let arguments = request
            .arguments
            .as_ref()
            .cloned()
            .unwrap_or_default()
            .remove("arguments")
            .and_then(|v| match v {
                serde_json::Value::Object(m) => Some(m),
                _ => None,
            });

        let handle = self.require_upstream_by_lua_name(&lua_server_name).await?;
        let tools = self.fetch_cached_tools(&handle).await?;

        let tool = match tool_details::find_by_lua_name(&tools, &lua_tool_name) {
            Ok(t) => t,
            Err(tool_details::LookupError::NotFound) => {
                let available = tool_details::sanitized_tool_names(&tools).join(", ");
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "tool '{lua_tool_name}' not found on server '{lua_server_name}'. \
                     available tools: {available}"
                ))]));
            }
            Err(tool_details::LookupError::Ambiguous { originals }) => {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "tool '{lua_tool_name}' is ambiguous on server '{lua_server_name}': \
                     upstream tools {} all sanitize to the same identifier and are therefore \
                     not callable. rename one of them upstream.",
                    originals.join(", ")
                ))]));
            }
        };

        // Call the tool with the real (unsanitized) name the upstream
        // knows it by. Builder chain because the struct is
        // #[non_exhaustive] (see the rmcp non_exhaustive house rule at
        // the top of handler/mod.rs).
        let mut call_request = rmcp::model::CallToolRequestParams::new(tool.name.clone());
        if let Some(args) = arguments {
            call_request = call_request.with_arguments(args);
        }
        let response = handle.call_tool(call_request).await.map_err(|e| {
            ErrorData::internal_error(format!("upstream tool call failed: {e}"), None)
        })?;

        self.mark_inspected_if_session(session_id, &lua_server_name, &lua_tool_name);

        // Classify the response shape BEFORE offload consumes the
        // CallToolResult. We only record on non-error replies — an
        // error response tells us the shape of this error, not the
        // shape of the tool's successful payloads, and caching the
        // error shape would make the `tool-details` hint actively
        // misleading.
        //
        // Writes are monotonic (`observe` merges with any existing
        // entry), so this is safe to call unconditionally on success:
        // a prior richer observation (e.g. TextJson from a different
        // session) won't be regressed by a later plainer one.
        if response.is_error != Some(true)
            && let Some(shape) = response_shape::classify_from_response(&response)
        {
            self.state
                .ctx
                .response_shapes
                .observe(ShapeKey::new(&lua_server_name, &lua_tool_name), shape);
        }

        // Bound the response size before it reaches the agent's context.
        //
        // The upstream is a black box from the agent's perspective: a
        // `search_datadog_logs` or `list_projects` call can return
        // anywhere from a few bytes to several megabytes, and the agent
        // has no way to predict which until after the call. We enforce
        // a structural guarantee here — no inspect-tool-response reply
        // exceeds the configured threshold on the wire — by routing
        // oversized responses through the same `ExecutionResultStore`
        // the `execute` path uses. The agent gets an execution ID it
        // can feed into `_gateway.get_result` from a follow-up execute
        // script to filter the stored payload server-side.
        //
        // `threshold == 0` disables the feature entirely (symmetric with
        // the execute path) and stdio sessions have no session ID to
        // key storage against, so both skip the bounding and return the
        // upstream reply verbatim.
        Ok(self.maybe_offload_inspect_result(response, session_id))
    }

    /// If the serialized `CallToolResult` exceeds the configured size
    /// threshold and we have a session to store against, stash the full
    /// payload in the execution result store and return a summary
    /// response carrying the execution ID. Otherwise return the upstream
    /// reply unchanged.
    ///
    /// Failures during serialization or storage degrade gracefully: we
    /// fall back to returning an error `CallToolResult` with a diagnostic
    /// message rather than leaking the oversized payload or crashing.
    /// The inspection-mark side-effect has already fired by the time
    /// we reach here, so the agent can still unblock follow-up scripts
    /// even if offload fails.
    fn maybe_offload_inspect_result(
        &self,
        response: CallToolResult,
        session_id: Option<&str>,
    ) -> CallToolResult {
        let threshold = self.state.ctx.settings.result_size_threshold;
        if threshold == 0 {
            return response;
        }
        let Some(session_id) = session_id else {
            return response;
        };

        let serialized = match serde_json::to_string(&response) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to serialize upstream response for offload sizing; passing through verbatim"
                );
                return response;
            }
        };
        if serialized.len() <= threshold {
            return response;
        }

        let byte_size = serialized.len();
        let parsed: serde_json::Value = match serde_json::from_str(&serialized) {
            Ok(v) => v,
            Err(e) => {
                // Shouldn't happen — we just produced this JSON — but
                // we don't want a silent panic on the wire. Fall back
                // to a plain-shape summary so the byte cap still holds.
                tracing::warn!(
                    error = %e,
                    "failed to re-parse serialized response for shape inference; using opaque shape"
                );
                serde_json::Value::Null
            }
        };

        match self.state.ctx.result_store.save(session_id, serialized) {
            Ok(execution_id) => {
                let shape = lua_runtime::infer_shape(&parsed, lua_runtime::DEFAULT_MAX_DEPTH);
                let summary = offload_message::render(&execution_id, byte_size, &shape);
                CallToolResult::success(vec![Content::text(summary)])
            }
            Err(save_err) => {
                tracing::warn!(
                    error = %save_err,
                    byte_size,
                    "refused to offload oversized upstream response; returning error to agent"
                );
                CallToolResult::error(vec![Content::text(save_err.to_string())])
            }
        }
    }

    /// Record that `(lua_server_name, lua_tool_name)` was inspected in
    /// this session, provided we have a session to mark against.
    ///
    /// Shared by `tool-details` and `inspect-tool-response` (both
    /// canonical inspection surfaces). Missing session IDs are
    /// expected in the stdio case and on pre-initialize calls; we log
    /// at debug to keep the audit trail without producing noise.
    pub(super) fn mark_inspected_if_session(
        &self,
        session_id: Option<&str>,
        lua_server_name: &str,
        lua_tool_name: &str,
    ) {
        let Some(session_id) = session_id else {
            tracing::debug!(
                lua_server_name,
                lua_tool_name,
                "no session id; inspection not recorded (stdio or pre-initialize)"
            );
            return;
        };
        self.state.ctx.inspection.mark(
            session_id,
            InspectedTool::new(lua_server_name, lua_tool_name),
        );
        tracing::debug!(
            session_id,
            lua_server_name,
            lua_tool_name,
            "tool marked inspected for session"
        );
    }
}

/// Build the `server-details` health block for a given live state.
///
/// Returns `None` for `Healthy` (absence is the all-clear, matching
/// `list-servers`). For `NeedsLogin` it attaches the exact re-login
/// command keyed off the upstream's display `name`; the transient states
/// carry their name only, since no single command fixes a transient
/// outage. Kept as a free function (not a method) so it's trivially
/// unit-testable without a `GatewayHandler`.
fn server_details_health<'a>(
    state: mcp_client::HealthState,
    name: &str,
) -> Option<server_details::HealthInfo<'a>> {
    use mcp_client::HealthState;
    let state_name = match state {
        HealthState::Healthy => return None,
        HealthState::Suspect => "Suspect",
        HealthState::Unhealthy => "Unhealthy",
        HealthState::Respawning => "Respawning",
        HealthState::Dead => "Dead",
        HealthState::NeedsLogin => "NeedsLogin",
    };
    let relogin_command =
        matches!(state, HealthState::NeedsLogin).then(|| format!("rato login {name}"));
    Some(server_details::HealthInfo {
        state: state_name,
        relogin_command,
    })
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
