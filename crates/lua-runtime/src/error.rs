// pattern: Functional Core
//
// Error type for Lua script execution. Pure data — no I/O, no VM state.
// Kept in its own file so tests and callers can match on variants without
// depending on the rest of the runtime.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LuaExecutionError {
    /// Script failed to compile (syntax error, etc.). The inner message is
    /// Luau's compile diagnostic, which includes line/column information.
    #[error("failed to compile lua script: {0}")]
    Compile(String),

    /// Script compiled but raised a Lua runtime error during execution.
    #[error("lua script raised an error: {0}")]
    Runtime(String),

    /// Script executed to completion without calling `result(...)`. Scripts
    /// MUST explicitly yield their output — silent omission is a bug, not
    /// an empty-string success.
    #[error("lua script finished without calling result(...)")]
    MissingResult,

    /// Script exceeded the per-execution instruction budget. Guards against
    /// runaway `while true do end` style loops.
    #[error("lua script exceeded instruction budget of {budget}")]
    InstructionBudgetExceeded { budget: u64 },

    /// A host function that bridges into an upstream tool call failed.
    /// The upstream name and underlying cause are preserved.
    #[error("upstream tool call failed on {upstream}.{tool}: {source}")]
    UpstreamCall {
        upstream: String,
        tool: String,
        #[source]
        source: anyhow::Error,
    },

    /// The value passed to `result(...)` couldn't be converted into JSON
    /// (e.g. a function, a table with non-string keys that isn't an array,
    /// a cycle).
    #[error("result(...) value is not JSON-serializable: {0}")]
    NonSerializableResult(String),

    /// Internal wiring failure (e.g. couldn't create the VM, couldn't set
    /// a global). Unexpected in the happy path; kept distinct so we don't
    /// misreport it as a user script error.
    #[error("lua runtime internal failure: {0}")]
    Internal(String),

    /// The script attempted to call an upstream tool the current session
    /// hasn't inspected via `tool-details` or `inspect-tool-response`. The
    /// gateway enforces this to make sure agents actually read a tool's
    /// schema before invoking it.
    #[error(
        "tool '{lua_server}.{lua_tool}' cannot be called without prior inspection; \
         call tool-details {{ luaServerName = \"{lua_server}\", luaToolName = \"{lua_tool}\" }} first"
    )]
    NotInspected {
        lua_server: String,
        lua_tool: String,
    },

    /// The script's result was large enough to trigger the offload path,
    /// but the offload backend refused it (typically because the single
    /// payload exceeded the per-result ceiling). The agent sees a clear
    /// message pointing at source-side filtering as the fix.
    #[error("failed to offload large result: {0}")]
    OffloadFailed(String),

    /// The per-execute wall-clock deadline elapsed before the script
    /// finished. Usually means an upstream tool call stalled; the
    /// default deadline is generous (60 s) so a real timeout here
    /// points at a misbehaving upstream, not at a legitimate slow
    /// script. Carries the deadline value so the agent/operator can
    /// tell which budget was in force.
    #[error("lua script exceeded per-execute deadline of {deadline_ms} ms")]
    Timeout { deadline_ms: u64 },

    /// The caller cancelled the execute request (e.g. the downstream
    /// MCP client dropped the connection or sent a cancellation
    /// notification). The runtime stops cooperating promptly — no
    /// zombie blocking threads, no in-flight upstream futures left
    /// waiting on a oneshot that will never fire.
    #[error("lua script cancelled by caller")]
    Cancelled,
}
