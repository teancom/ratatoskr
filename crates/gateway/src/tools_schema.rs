// pattern: Functional Core
//
// Static tool-schema constructors for the gateway's downstream-facing
// tool surface. Each function returns an rmcp `Tool` describing one
// gateway-owned tool. They carry no handler state and produce the same
// value on every call — pure functions of zero arguments.
//
// Extracted from `impl GatewayHandler` (where they lived purely by
// historical placement) so the `handler/` modules stay focused on dispatch
// logic. These become the left half of a future tool registry once the
// call_tool dispatch match is refactored into a `(ToolDefinition,
// handler-fn)` slice.
//
// The `EXECUTE_TOOL_DESCRIPTION` constant lives in `handler/execute.rs`
// (not here) because it is also read by the tests that pin the
// `_gateway.*` builtin roster. Moving it here would be straightforward but
// is deferred to the same registry refactor so the description constant
// ends up adjacent to the execute handler fn.

use std::sync::Arc;

use rmcp::model::Tool;

use crate::handler::EXECUTE_TOOL_DESCRIPTION;

/// Canonical tool names advertised in every `list_tools` response. Single
/// source of truth — `list_tools`, the `call_tool` dispatch match, and the
/// server-instructions prose all read from here (or are tested against it),
/// so adding a tool in one place without the others fails at test time
/// rather than silently drifting.
pub const BASE_TOOL_NAMES: &[&str] = &[
    "ping",
    "list-servers",
    "server-details",
    "list-server-tools",
    "tool-details",
    "inspect-tool-response",
    "execute",
    "reload-config",
    "reconnect-upstream",
    "restart-gateway",
];

fn empty_object_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false,
        }))
        .expect("static schema is valid"),
    )
}

pub fn ping_tool() -> Tool {
    Tool::new(
        "ping",
        "Returns 'pong'. Used to verify the gateway is alive.",
        empty_object_schema(),
    )
}

pub fn list_servers_tool() -> Tool {
    Tool::new(
        "list-servers",
        "List the upstream MCP servers this gateway fronts — each with a one-line hint \
         describing what it's for. These upstreams are NOT in your flat tool list; this \
         is the only way to discover them. Call this BEFORE authenticating, setting up, \
         or starting an OAuth flow on any same-named tool in your flat list: such a tool \
         is frequently a redundant, unauthenticated duplicate of an upstream already \
         fronted here and ready to use. Listing first is cheap and read-only; \
         authenticating a duplicate you didn't need is not.",
        empty_object_schema(),
    )
}

pub fn execute_tool() -> Tool {
    let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {
                "script": {
                    "type": "string",
                    "description": "Luau script. Call result(value) to yield output. Each upstream is available as a global named by its lua_name (see list-servers); each upstream tool is a method on that global, e.g. my_server.some_tool({arg=1}).",
                }
            },
            "required": ["script"],
            "additionalProperties": false,
        }))
        .expect("static schema is valid"),
    );
    Tool::new("execute", EXECUTE_TOOL_DESCRIPTION, schema)
}

pub fn list_server_tools_tool() -> Tool {
    let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {
                "luaServerName": {
                    "type": "string",
                    "description": "The sanitized Lua name of the upstream server (see list-servers for the catalog)."
                }
            },
            "required": ["luaServerName"],
            "additionalProperties": false,
        }))
        .expect("static schema is valid"),
    );
    Tool::new(
        "list-server-tools",
        "List the tools exposed by one upstream MCP server. Returns sanitized tool names and short descriptions. Use tool-details to read the full schema before calling a tool in an execute script.",
        schema,
    )
}

pub fn tool_details_tool() -> Tool {
    let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {
                "luaServerName": {
                    "type": "string",
                    "description": "The sanitized Lua name of the upstream server."
                },
                "luaToolName": {
                    "type": "string",
                    "description": "The sanitized Lua name of the tool, as shown by list-server-tools."
                }
            },
            "required": ["luaServerName", "luaToolName"],
            "additionalProperties": false,
        }))
        .expect("static schema is valid"),
    );
    Tool::new(
        "tool-details",
        "Get comprehensive information about a specific upstream tool: full description, input schema, and output schema if the server exposes one. Also returns a scannable `parameters` summary array (name, required, type, enum, description per top-level input param) alongside the raw `inputSchema` — scan `parameters` first for the right field name before reaching for the full schema. When the response shape has been observed (by a prior inspect-tool-response call in this or a previous session) or declared by the upstream's output schema, the result also carries `responseShape` (one of `text-prose`, `text-json`, `multi-block`, `structured`, `mixed`) and `responseShapeHint` pointing at the right `_gateway.*` builtin to reach for. YOU MUST call this for each tool you plan to invoke from an execute script — the summary from list-server-tools is not sufficient. A successful call also records that this session has inspected the tool; execute scripts will refuse to call tools this session hasn't inspected.",
        schema,
    )
}

pub fn server_details_tool() -> Tool {
    let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {
                "luaServerName": {
                    "type": "string",
                    "description": "The sanitized Lua name of the upstream server (see list-servers for the catalog)."
                }
            },
            "required": ["luaServerName"],
            "additionalProperties": false,
        }))
        .expect("static schema is valid"),
    );
    Tool::new(
        "server-details",
        "Fetch detailed information about one upstream MCP server: its reported implementation identity, its catalog hint, and the full `instructions` string it advertises (useful when the one-line hint in list-servers isn't enough to decide whether to drill in). Does NOT list tools — use list-server-tools for that.",
        schema,
    )
}

pub fn inspect_tool_response_tool() -> Tool {
    let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {
                "luaServerName": {
                    "type": "string",
                    "description": "The sanitized Lua name of the upstream server."
                },
                "luaToolName": {
                    "type": "string",
                    "description": "The sanitized Lua name of the tool."
                },
                "arguments": {
                    "type": "object",
                    "description": "Arguments to pass to the tool. Subject to the same shape the tool's input schema demands."
                }
            },
            "required": ["luaServerName", "luaToolName"],
            "additionalProperties": false,
        }))
        .expect("static schema is valid"),
    );
    Tool::new(
        "inspect-tool-response",
        "Call an upstream tool once and return its response verbatim. Use this for safe/read-only tools when you want to see the actual output shape, not just the schema. Like tool-details, a successful call marks this tool as inspected for this session.",
        schema,
    )
}

pub fn reload_config_tool() -> Tool {
    Tool::new(
        "reload-config",
        "Re-read the gateway's config file and apply the differences to the live upstream set WITHOUT restarting: newly-added upstreams are connected, removed ones are disconnected, and changed ones are reconnected on their new definition. Unchanged upstreams keep their existing connections (no disruption to in-flight work). Returns a summary of what changed and any per-upstream connect failures. If the config file is missing or invalid, NOTHING changes and the parse error is returned. Gateway-wide SETTINGS (execute deadline, result-size threshold, supervision) are NOT hot-reloaded; if they changed, this reports that a restart-gateway is required to apply them. Use this after you edit the config file to add or remove an MCP server.",
        empty_object_schema(),
    )
}

pub fn reconnect_upstream_tool() -> Tool {
    let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {
                "luaServerName": {
                    "type": "string",
                    "description": "The sanitized Lua name of the upstream server to reconnect (see list-servers for the catalog)."
                }
            },
            "required": ["luaServerName"],
            "additionalProperties": false,
        }))
        .expect("static schema is valid"),
    );
    Tool::new(
        "reconnect-upstream",
        "Tear down and re-establish ONE upstream's connection, even though its config is \
         unchanged. For stdio upstreams this re-runs the configured command from scratch — \
         which matters for launchers that mint short-lived credentials at process start \
         (session tokens, SSO-derived API tokens): the running process keeps its stale token \
         forever, and reload-config deliberately skips unchanged upstreams, so this tool is \
         the ONLY way to refresh such credentials without a full restart-gateway. Call it \
         when an upstream's tool calls start failing with authentication/token-expired \
         errors; the reconnect is self-healing when the upstream's launcher can renew \
         non-interactively. Other upstreams are untouched. Also applies any pending config \
         file edits, exactly like reload-config. If the reconnect fails, the previous \
         (possibly degraded) connection stays registered and the failure is reported.",
        schema,
    )
}

pub fn restart_gateway_tool() -> Tool {
    Tool::new(
        "restart-gateway",
        "Restart the gateway process cleanly: it drains upstreams and exits, and the process supervisor (launchd) respawns it, re-reading the config and ALL settings at boot. This is the only way gateway-wide settings changes take effect. WARNING: this drops the current MCP connection — the session will need to reconnect after a moment. Prefer reload-config for routine add/remove of upstreams; use restart-gateway only when reload-config tells you a setting change needs it (or when you explicitly want a full restart).",
        empty_object_schema(),
    )
}

#[cfg(test)]
#[path = "tools_schema_tests.rs"]
mod tests;
