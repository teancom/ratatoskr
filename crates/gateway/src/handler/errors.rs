// Error-conversion helpers for the gateway handler.
//
// All functions here are pure: they take a typed error value and return an
// rmcp wire shape. No handler state is accessed. Kept in their own module so
// they're trivially unit-testable and the dispatch logic in mod.rs stays
// focused on routing, not error-shaping.

use std::sync::Arc;

use lua_runtime::{ExecutionOutcome, LuaExecutionError, OffloadConfig};
use rmcp::{
    ErrorData,
    model::{CallToolResult, Content},
};

use crate::execution_result_store::ExecutionResultStore;
use crate::offload_message;

/// Convert a LuaExecutionError into the right rmcp error shape. User-facing
/// script errors (compile, runtime, missing result, budget) are returned as
/// `CallToolResult` with `is_error=true` so the agent can see the diagnostic
/// and react; gateway-internal errors (failed to set globals, etc.) are
/// returned as JSON-RPC errors because they mean the gateway itself is
/// broken, not the script.
pub(super) fn lua_error_to_tool_outcome(
    err: LuaExecutionError,
) -> Result<CallToolResult, ErrorData> {
    match &err {
        LuaExecutionError::Internal(_) => Err(ErrorData::internal_error(err.to_string(), None)),
        _ => {
            let payload = serde_json::json!({
                "error": err.to_string(),
            });
            Ok(CallToolResult::error(vec![Content::text(
                serde_json::to_string(&payload)
                    .unwrap_or_else(|_| format!("{{\"error\":\"{err}\"}}")),
            )]))
        }
    }
}

/// Render the outcome of a successful `execute` as tool content.
///
/// For `Direct` results, a plain string round-trips as a `Content::text`
/// so the agent sees the script's exact output; anything structural
/// becomes a JSON string. For `Offloaded` results we don't render the
/// payload at all — we render the shape summary the agent needs to
/// decide whether and how to retrieve the stored data.
pub(super) fn execution_outcome_to_content(outcome: ExecutionOutcome) -> Vec<Content> {
    match outcome {
        ExecutionOutcome::Direct(value) => match value {
            serde_json::Value::String(s) => vec![Content::text(s)],
            other => vec![Content::text(
                serde_json::to_string(&other)
                    .unwrap_or_else(|e| format!("(failed to serialize result: {e})")),
            )],
        },
        ExecutionOutcome::Offloaded {
            execution_id,
            byte_size,
            shape,
        } => vec![Content::text(offload_message::render(
            &execution_id,
            byte_size,
            &shape,
        ))],
    }
}

/// Build an `OffloadConfig` bound to `session_id` if offloading is
/// enabled. Returns `None` when the configured threshold is zero
/// (feature disabled) so the runtime skips the size check and the
/// `_gateway` builtin install entirely.
pub(super) fn build_offload_config(
    store: &Arc<ExecutionResultStore>,
    threshold: usize,
    session_id: &str,
) -> Option<OffloadConfig> {
    if threshold == 0 {
        return None;
    }
    Some(OffloadConfig {
        threshold_bytes: threshold,
        backend: store.backend_for_session(session_id),
    })
}
