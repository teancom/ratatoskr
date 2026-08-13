// pattern: Functional Core
//
// Render the `server-details` tool's output. Pure: takes the
// upstream's advertised identity + any instructions string + the
// user-authored config override, returns a JSON-string response.
// No I/O, no upstream calls — the shell gathers those before
// invoking us.
//
// The shape mirrors `tool_details::render_tool_details`: JSON so
// agents can parse it without regex-ing a human-readable blob, with
// fields omitted when absent so a reader's mental model is "present
// keys always have meaningful values."
//
// Two complementary data sources flow in:
//   - The `hint` resolved by `hint::resolve_hint`: short, bounded,
//     priority-ordered (config override → upstream first sentence).
//     This is what `list-servers` also shows, surfaced here so the
//     agent doesn't have to round-trip back to catalog to see the
//     short form.
//   - The upstream's raw `instructions` string: full, uncapped,
//     whatever the upstream shipped. This is the "long form" you
//     pay the context cost for by calling `server-details`
//     explicitly.

use serde_json::{Map, Value};

/// Input data for rendering a `server-details` response.
///
/// Borrowed strings because the caller already owns them — avoids
/// cloning strings we'll immediately serialize. The lifetime is
/// tied to the caller's frame, which holds everything for the
/// duration of the handler call.
pub struct ServerDetailsInput<'a> {
    /// Display name as configured (e.g. "datadog-us").
    pub name: &'a str,
    /// Sanitized Lua identifier (e.g. "datadog_us"). The agent
    /// addresses the server by this name in `execute` scripts and
    /// tool-targeting calls, so surfacing it alongside the display
    /// name is load-bearing.
    pub lua_name: &'a str,
    /// Transport kind — "stdio" or "http". Minor but useful for
    /// debugging ("is this the upstream I expect?") and for the
    /// agent to understand whether it's talking to a local
    /// subprocess or a remote HTTPS endpoint.
    pub kind: &'a str,
    /// Server's self-reported implementation name, if the upstream
    /// completed its initialize handshake. Example: "datadog-mcp",
    /// "Atlassian MCP".
    pub server_name: Option<&'a str>,
    /// Server's self-reported version string, if any.
    pub server_version: Option<&'a str>,
    /// Resolved short-form hint as shown in `list-servers`. Present
    /// iff either the config override or the upstream's
    /// `instructions` yielded content.
    pub hint: Option<&'a str>,
    /// The upstream's raw `instructions` string, verbatim. Present
    /// iff the upstream advertised one.
    pub upstream_instructions: Option<&'a str>,
    /// Operational health, surfaced so `server-details` can explain why
    /// an upstream isn't taking calls (and, for OAuth re-auth, exactly
    /// how to fix it). `None` when no snapshot was available; a `Healthy`
    /// snapshot renders nothing (absence is the all-clear, mirroring
    /// `list-servers`).
    pub health: Option<HealthInfo<'a>>,
}

/// The bit of health a `server-details` reader needs: the state name and,
/// for the `NeedsLogin` case, the exact remediation command. Kept as a
/// pre-resolved carrier so this module stays a pure renderer — the
/// handler reads the live `HealthSnapshot` and decides what to surface.
pub struct HealthInfo<'a> {
    /// Short state name: `"Suspect"`, `"Unhealthy"`, `"Respawning"`,
    /// `"Dead"`, `"NeedsLogin"`. (`"Healthy"` is never passed — the
    /// handler omits the whole block.)
    pub state: &'a str,
    /// For `NeedsLogin`, the literal command an operator runs to
    /// re-authenticate (`rato login <name>`). `None` for the
    /// transient states, where there's no single command that fixes it.
    pub relogin_command: Option<String>,
}

/// Render the details response as a pretty-printed JSON string.
///
/// The JSON shape is:
/// ```json
/// {
///   "name": "datadog-us",
///   "luaServerName": "datadog_us",
///   "transport": "http",
///   "serverImplementation": { "name": "datadog-mcp", "version": "1.0.0" },
///   "hint": "This is an MCP server for ...",
///   "instructions": "This is an MCP server for Datadog ..."
/// }
/// ```
///
/// `serverImplementation`, `hint`, and `instructions` are omitted
/// when absent, not serialized as `null`. An agent reading this
/// output can assume every present key has meaningful content.
pub fn render(input: &ServerDetailsInput<'_>) -> String {
    let mut obj = Map::new();
    obj.insert("name".to_string(), Value::String(input.name.to_string()));
    obj.insert(
        "luaServerName".to_string(),
        Value::String(input.lua_name.to_string()),
    );
    obj.insert(
        "transport".to_string(),
        Value::String(input.kind.to_string()),
    );

    // `serverImplementation` is gated on `server_name`. A version
    // without a name is semantically useless (the reader has no
    // entity to attach the version to), and emitting a block with
    // only `version` would violate the "present keys always have
    // meaningful content" invariant stated above. The real caller
    // always pairs name + version as a unit (see
    // `upstream.server_implementation()`), so this tightening
    // doesn't lose any reachable case.
    if let Some(n) = input.server_name {
        let mut impl_obj = Map::new();
        impl_obj.insert("name".to_string(), Value::String(n.to_string()));
        if let Some(v) = input.server_version {
            impl_obj.insert("version".to_string(), Value::String(v.to_string()));
        }
        obj.insert("serverImplementation".to_string(), Value::Object(impl_obj));
    }

    if let Some(h) = input.hint {
        obj.insert("hint".to_string(), Value::String(h.to_string()));
    }

    if let Some(instructions) = input.upstream_instructions {
        obj.insert(
            "instructions".to_string(),
            Value::String(instructions.to_string()),
        );
    }

    // Health block: present only for non-Healthy upstreams (the handler
    // omits it otherwise). For NeedsLogin it carries the exact re-login
    // command so an agent reading server-details sees the remediation
    // inline rather than having to know the subcommand.
    if let Some(h) = &input.health {
        let mut health_obj = Map::new();
        health_obj.insert("state".to_string(), Value::String(h.state.to_string()));
        if let Some(cmd) = &h.relogin_command {
            health_obj.insert("reLoginCommand".to_string(), Value::String(cmd.clone()));
        }
        obj.insert("health".to_string(), Value::Object(health_obj));
    }

    // Infallible: the object is built entirely from `Value::String`
    // and `Value::Object` leaves above, neither of which can fail to
    // serialize. Using `expect` makes a future change that introduces
    // a non-serialisable type (e.g. a raw float NaN) fail loudly in
    // tests rather than silently returning a non-JSON error sentence
    // that downstream parsers would then reject as a corrupt response.
    serde_json::to_string_pretty(&Value::Object(obj))
        .expect("infallible: object contains only string and object values")
}

#[cfg(test)]
#[path = "server_details_tests.rs"]
mod tests;
