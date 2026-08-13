// pattern: Imperative Shell
//
// Per-session record of which upstream tools the client has inspected.
//
// The gateway's contract with agents is: you don't get to call
// `upstream.foo_tool(...)` from a Lua `execute` script until you've
// asked the gateway for that tool's full schema via `tool-details`
// (or probed a response via `inspect-tool-response`). This store is
// what answers "has this session inspected (upstream, tool)?"
//
// Why in-memory and not SQLite: session TTL is 24h (see
// `SESSION_KEEP_ALIVE` in serve.rs), so one long-lived session carries
// its inspection state for its entire life. Persistence would only pay
// for itself if sessions were short-lived enough that clients
// re-inspect on every fresh connection anyway — they aren't, and
// cross-restart persistence isn't needed for a single-dev-laptop
// deployment.
//
// Keying: we use (lua_server_name, lua_tool_name) because that's what
// the Lua runtime sees when dispatching, and what the agent references
// in its `tool-details` call. Sanitization has already happened — both
// sides speak the same identifier.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// Identifier pair for a single inspected tool, in the Lua-sanitized
/// form both Lua scripts and agent-facing tool args use.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct InspectedTool {
    pub lua_server: String,
    pub lua_tool: String,
}

impl InspectedTool {
    pub fn new(lua_server: impl Into<String>, lua_tool: impl Into<String>) -> Self {
        Self {
            lua_server: lua_server.into(),
            lua_tool: lua_tool.into(),
        }
    }
}

/// Shared handle. Cheap to clone (wraps an `Arc`). Pass one of these
/// into `GatewayHandler` construction and into `serve::run` via
/// whatever the entry point needs.
#[derive(Debug, Clone, Default)]
pub struct ToolInspectionStore {
    inner: Arc<RwLock<HashMap<String, HashSet<InspectedTool>>>>,
}

impl ToolInspectionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `session_id` has now inspected `(lua_server, lua_tool)`.
    /// Idempotent — calling twice is a no-op.
    ///
    /// Sync API on purpose: the Lua enforcement path runs inside
    /// `spawn_blocking` and can't easily `.await`. A `std::sync::RwLock`
    /// over a `HashMap` is fast enough for this use case (inspection
    /// is rare relative to actual tool invocations, and the critical
    /// section is just a hash lookup).
    pub fn mark(&self, session_id: &str, tool: InspectedTool) {
        let mut guard = self
            .inner
            .write()
            .expect("tool inspection store lock poisoned");
        guard
            .entry(session_id.to_string())
            .or_default()
            .insert(tool);
    }

    /// Has this session inspected this tool? Returns `false` for
    /// unknown sessions (safest default: enforcement layer fails
    /// closed).
    pub fn is_inspected(&self, session_id: &str, tool: &InspectedTool) -> bool {
        let guard = self
            .inner
            .read()
            .expect("tool inspection store lock poisoned");
        guard
            .get(session_id)
            .is_some_and(|tools| tools.contains(tool))
    }

    /// Drop every inspection record for `session_id`. Called when rmcp
    /// reports the session ended; prevents unbounded memory growth over
    /// a long-running gateway that services many ephemeral sessions.
    pub fn forget_session(&self, session_id: &str) {
        let mut guard = self
            .inner
            .write()
            .expect("tool inspection store lock poisoned");
        guard.remove(session_id);
    }

    /// Number of sessions currently tracked. Test-only helper; not on
    /// a hot path.
    #[cfg(test)]
    pub fn session_count(&self) -> usize {
        self.inner
            .read()
            .expect("tool inspection store lock poisoned")
            .len()
    }
}

#[cfg(test)]
#[path = "tool_inspection_tests.rs"]
mod tests;
