// pattern: Imperative Shell
//
// Execute family: the single `execute` tool that runs a Luau script
// inside a sandboxed per-call VM. This is the hot path for agent-driven
// workflows — every interesting upstream call flows through here.
//
// Lives alongside (rather than inside) `lua-runtime` because the
// handler-side pieces are thin but numerous: argument parsing, the
// `ToolCallGuard` that enforces the inspection gate per session, the
// offload config that bounds oversized results, and the mapping from
// runtime errors back to MCP `CallToolResult`s.
//
// `EXECUTE_TOOL_DESCRIPTION` is the ONLY place agents reliably see the
// `_gateway.*` builtin surface. It deliberately does NOT mirror the
// rustdoc on `install_gateway_builtins` in `lua-runtime/src/runtime.rs`:
// the two texts share a builtin roster but serve different audiences.
// The rustdoc is developer documentation — precise and complete. This
// description is agent-tuned prompt text, curated against observed
// failure modes (e.g. the `parallel` worked example exists because
// agents confabulated closures/keyed-maps/phantom-retrieval-APIs;
// other builtins get terse prose because they already land correctly)
// and edited under a context-budget economy. Do not "fix" the
// duplication by flattening one text into the other.
//
// What MUST agree is the roster: both texts enumerate exactly
// `lua_runtime::GATEWAY_BUILTIN_NAMES`. The tests in this file pin
// roster agreement in both directions against that shared constant;
// lua-runtime pins the constant against what's actually installed.

use super::*;

/// Description shown for the `execute` tool. Load-bearing because it's
/// the only place agents reliably see the `_gateway.*` builtin
/// surface — they read it once when deciding how to call the tool and
/// rely on it throughout a session. The `_gateway.*` builtins exist so
/// agents don't have to reinvent JSON parsing, content-envelope
/// unwrapping, and RFC3339 time formatting per-upstream; that only pays
/// off if this description keeps the enumeration up to date as builtins
/// come and go.
pub(crate) const EXECUTE_TOOL_DESCRIPTION: &str = "\
Execute a Luau script inside a sandboxed per-call VM. The script can call upstream \
MCP tools synchronously (no `:await()`) and yields a final value to the caller \
via `result(value)`. Do not write `return result(value)` — `result` is a \
side-effect that stores the value, not a return.\n\
\n\
Upstreams are injected as globals named by their `lua_name` (see `list-servers`); \
each tool is a method on that global, e.g. `datadog.search_logs({query=\"...\"})`. \
You MUST call `tool-details` for any upstream tool before invoking it in a \
script — scripts that call un-inspected tools fail with `NotInspected`.\n\
\n\
An upstream whose `tools/list` is currently failing (e.g. transient outage, \
sick transport) is silently skipped at session-init: its global will be `nil`, \
and a script that references it gets a Lua-level `attempt to index a nil value` \
error rather than the whole session failing. If you see that, retry — the \
upstream may have recovered, and the next `execute` will re-fetch.\n\
\n\
Builtins on the `_gateway` global (always available inside `execute` scripts):\n\
\n\
- `_gateway.json_decode(text)` — parse a JSON string into a Lua value. Returns \
tables for objects/arrays, scalars for primitives, nil for JSON null. Errors \
clearly on malformed input. Use this instead of hand-rolling regex over JSON \
strings.\n\
\n\
- `_gateway.json_encode(value)` — serialize any `result(...)`-compatible Lua \
value to a JSON string. Inverse of `json_decode`. Rejects non-serializable \
values (functions, threads) with a clear error.\n\
\n\
- `_gateway.unwrap_content(value)` — if `value` matches the MCP content-envelope \
shape `{content={{type=\"text\", text=\"...\"}, ...}, ...}`, return the first \
block's text. Otherwise return `value` unchanged. Safe to wrap any upstream \
response in `unwrap_content(...)` without first branching on shape. Composes \
naturally with `json_decode`: `_gateway.json_decode(_gateway.unwrap_content(res))`.\n\
\n\
- `_gateway.rfc3339(unix_seconds)` — format a unix epoch seconds value as \
`YYYY-MM-DDTHH:MM:SSZ`. Accepts integer or float (truncated toward zero). The \
shape agents building time-windowed Loki/Datadog/Grafana queries reach for.\n\
\n\
- `_gateway.now_minus(seconds)` — RFC3339-UTC string for `now - seconds`. \
Negative `seconds` is permitted (for future timestamps). Combine with \
`rfc3339` / literal epoch values when assembling `{from, to}` pairs.\n\
\n\
- `_gateway.expand_template({server, template, params})` — instantiate an RFC \
6570 resource template (any level, 1-4) against the named upstream and return \
the namespaced `mcp-proxy://...` URI ready for `resources/read`. Use when an \
upstream advertises resource templates in `resources/templates/list`.\n\
\n\
- `_gateway.parallel(tasks)` — dispatch a batch of upstream tool calls \
concurrently. `tasks` is a positional array of descriptor tables (not \
functions, not a keyed map):\n\
\n\
    local results = _gateway.parallel({\n\
      { server = \"rootly\",     tool = \"getIncident\",            args = { incident_id = \"abc\" } },\n\
      { server = \"datadog_us\", tool = \"search_datadog_services\", args = { query = \"\" } },\n\
    })\n\
    for i, r in ipairs(results) do\n\
      if r.ok then handle(r.value) else log_error(i, r.error) end\n\
    end\n\
\n\
Results come back in the same order as the input. A failing task produces \
`{ ok = false, error = \"...\" }` rather than aborting the batch, so iterate \
and branch on `r.ok`. Inspection still applies: every `(server, tool)` pair \
must be inspected via `tool-details` this session, or the whole call aborts \
with NotInspected. Use when you have N independent upstream calls and don't \
want to pay N * round-trip-latency.\n\
\n\
- `_gateway.get_result({id})` — retrieve a previously-offloaded execution \
result by ID. Only present when a large prior result was offloaded (the \
gateway tells you the ID in the offload snippet). Returns nil for unknown IDs.\n\
\n\
Errors from builtins include the builtin name in the message so `pcall(...)` \
callers can route by failure site. Large results (>~256 KiB by default) are \
offloaded automatically; the script sees a shape summary and an execution ID \
it can feed back into `_gateway.get_result`.";

/// Build a `ToolCallGuard` closure bound to the given session ID. The
/// closure returns `true` (call allowed) when the store has a record
/// of the agent having inspected `(lua_server, lua_tool)` in this
/// session, `false` otherwise.
///
/// We clone the store Arc and own the session ID string so the
/// closure is `'static + Send + Sync`, which `ToolCallGuard` requires.
fn build_inspection_guard(
    store: ToolInspectionStore,
    session_id: String,
) -> lua_runtime::ToolCallGuard {
    Arc::new(move |lua_server: &str, lua_tool: &str| {
        store.is_inspected(&session_id, &InspectedTool::new(lua_server, lua_tool))
    })
}

impl GatewayHandler {
    /// Handle an `execute` call: parse the script argument, build the
    /// per-session inspection guard and offload backend, then hand the
    /// script to the process-wide `LuaRuntime`. Cancellation flows
    /// through `context.ct`; the runtime's 60 s backstop deadline
    /// bounds a missing-cancellation case.
    pub(super) async fn call_execute(
        &self,
        request: CallToolRequestParams,
        context: &RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let script = match parse_script_argument(request.arguments.as_ref()) {
            Ok(s) => s,
            Err(ScriptInputError::Missing | ScriptInputError::WrongType) => {
                return Err(ErrorData::invalid_params(
                    // Collapse the two into one message; both mean
                    // "the arguments didn't match the schema".
                    "'script' argument is required and must be a string".to_string(),
                    None,
                ));
            }
            Err(ScriptInputError::Empty) => {
                return Err(ErrorData::invalid_params(
                    "'script' argument must not be empty".to_string(),
                    None,
                ));
            }
            Err(e @ ScriptInputError::TooLarge(_)) => {
                // The Display impl already includes both the
                // limit and the provided size, so use it verbatim.
                return Err(ErrorData::invalid_params(e.to_string(), None));
            }
        };

        // Build the inspection guard AND the offload backend
        // from the same session ID. Stdio transport (no
        // transport context in extensions) has no session ID
        // and no header to gate against, so we pass no guard
        // and no offload — unit tests that drive the bridge
        // directly exercise this path.
        //
        // In HTTP, `require_session` already rejected a
        // missing `Mcp-Session-Id` header before we got here,
        // so the only `Ok(None)` we ever see is the stdio case.
        let session_id = require_session(session_id_from_context(context))?;
        let guard = session_id
            .as_deref()
            .map(|sid| build_inspection_guard(self.state.ctx.inspection.clone(), sid.to_string()));
        let offload = session_id.as_deref().and_then(|sid| {
            build_offload_config(
                &self.state.ctx.result_store,
                self.state.ctx.settings.result_size_threshold,
                sid,
            )
        });

        // Per-execute cancellation token.
        //
        // rmcp populates `context.ct` with a `CancellationToken`
        // that fires when the downstream client sends
        // `notifications/cancelled` for this request (see rmcp's
        // service.rs: the token is cancelled inside the service
        // loop when a matching CancelledNotification arrives).
        // Passing it straight through means a cancelled MCP
        // request unwinds the in-flight upstream future
        // promptly instead of pinning a blocking worker on a
        // oneshot that will never fire.
        //
        // The 60 s default execute deadline inside
        // `LuaRuntime` backstops this: even when a client
        // doesn't send a cancellation notification, the
        // blocking thread is bounded.
        //
        // Per-execute timing lands in a debug line at exit. The
        // enclosing call_tool span already carries req_id + session, so
        // grep on those to correlate an execute with the upstream
        // call_tool lines that happened inside it.
        let script_bytes = script.len();
        tracing::debug!(script_bytes, "execute starting");
        let start = std::time::Instant::now();

        let result = self
            .state
            .ctx
            .lua
            .execute(
                script,
                Arc::clone(&self.state.ctx.upstreams),
                guard,
                Some(Arc::clone(&self.state.ctx.tool_cache)),
                offload,
                context.ct.clone(),
            )
            .await;

        let duration_ms = start.elapsed().as_millis() as u64;
        let outcome_label = match &result {
            Ok(lua_runtime::ExecutionOutcome::Direct(_)) => "ok_direct",
            Ok(lua_runtime::ExecutionOutcome::Offloaded { .. }) => "ok_offloaded",
            Err(e) => match e {
                lua_runtime::LuaExecutionError::Cancelled => "cancelled",
                lua_runtime::LuaExecutionError::Timeout { .. } => "timeout",
                lua_runtime::LuaExecutionError::NotInspected { .. } => "not_inspected",
                lua_runtime::LuaExecutionError::UpstreamCall { .. } => "upstream_error",
                lua_runtime::LuaExecutionError::OffloadFailed(_) => "offload_failed",
                lua_runtime::LuaExecutionError::MissingResult => "missing_result",
                _ => "other_error",
            },
        };
        tracing::debug!(duration_ms, outcome = outcome_label, "execute complete");

        match result {
            Ok(outcome) => Ok(CallToolResult::success(execution_outcome_to_content(
                outcome,
            ))),
            Err(err) => lua_error_to_tool_outcome(err),
        }
    }
}

#[cfg(test)]
#[path = "execute_tests.rs"]
mod tests;
