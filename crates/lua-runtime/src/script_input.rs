// pattern: Functional Core
//
// Parse the `{ script: String }` shape out of an rmcp tool-call arguments
// map. Pure: in = optional JSON map, out = validated script string or a
// descriptive error. Lives here (rather than in the gateway crate) because
// the concept "what does the execute tool take as input" belongs next to
// the Lua runtime, not alongside the server wiring.

use thiserror::Error;

/// Maximum byte length accepted for a script. The Luau compiler runs
/// synchronously before the instruction-count budget kicks in, so compiling
/// a very large source string is effectively unbounded. No upstream HTTP layer
/// enforces a body-size limit on this codebase's request path, so we enforce
/// the cap here as defence in depth.
pub const MAX_SCRIPT_BYTES: usize = 256 * 1024; // 256 KiB

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ScriptInputError {
    #[error("missing required 'script' argument")]
    Missing,
    #[error("'script' argument must be a string")]
    WrongType,
    #[error("'script' argument must not be empty")]
    Empty,
    #[error("'script' argument exceeds the {MAX_SCRIPT_BYTES}-byte limit ({0} bytes provided)")]
    TooLarge(usize),
}

/// Extract and validate the `script` field from a rmcp tool-call arguments
/// map. The rmcp type is `Option<serde_json::Map<String, serde_json::Value>>`;
/// we accept the inner shape to keep this pure and trivially testable.
pub fn parse_script_argument(
    arguments: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<String, ScriptInputError> {
    let map = arguments.ok_or(ScriptInputError::Missing)?;
    let value = map.get("script").ok_or(ScriptInputError::Missing)?;
    let s = value.as_str().ok_or(ScriptInputError::WrongType)?;
    if s.is_empty() {
        return Err(ScriptInputError::Empty);
    }
    if s.len() > MAX_SCRIPT_BYTES {
        return Err(ScriptInputError::TooLarge(s.len()));
    }
    Ok(s.to_string())
}

#[cfg(test)]
#[path = "script_input_tests.rs"]
mod tests;
