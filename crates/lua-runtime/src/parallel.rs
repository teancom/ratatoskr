// pattern: Functional Core
//
// Input parsing for `_gateway.parallel`. Given a Lua table that the user
// claims is an array of `{ server, tool, args }` descriptors, walk it
// into a `Vec<ParallelTaskDescriptor>` with every field validated. All
// the decisions that can be made without I/O live here:
//
// - Is the outer table a proper array?
// - Is each entry a table?
// - Are `server` and `tool` non-empty strings?
// - Is `args` a table or absent (we default absent to empty)?
//
// The imperative shell in `runtime.rs` still handles the actual
// conversion from `mlua::Value` → JSON for `args` (that needs the
// `Lua` handle) and the dispatch itself. But the descriptor struct is
// the clean intermediate representation between "Lua said this" and
// "Rust will do that with it", and it's pure data, hence here.

use crate::json_shape::describe_json_shape;
use thiserror::Error;

/// A single task inside a `_gateway.parallel` call, fully validated.
///
/// `args` is stored as the already-converted JSON object the upstream
/// call expects. Converting at parse time keeps the dispatcher simple
/// and centralizes the "non-object args" error in one place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelTaskDescriptor {
    pub server: String,
    pub tool: String,
    pub args: serde_json::Map<String, serde_json::Value>,
}

/// Errors produced by the task-list validator. Each variant carries
/// enough context for the agent to correct the call — most notably the
/// 1-based index of the offending task, since agents typically paste
/// an array and need to know which entry tripped the error.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParallelInputError {
    #[error("_gateway.parallel: expected an array table (task list), got {got}")]
    NotAnArray { got: String },

    #[error("_gateway.parallel: task #{index} must be a table, got {got}")]
    TaskNotATable { index: usize, got: String },

    #[error("_gateway.parallel: task #{index} missing required '{field}' field")]
    MissingField { index: usize, field: &'static str },

    #[error("_gateway.parallel: task #{index} '{field}' must be a non-empty string")]
    EmptyField { index: usize, field: &'static str },

    #[error("_gateway.parallel: task #{index} 'args' must be a table/object, got {got}")]
    ArgsNotAnObject { index: usize, got: String },

    #[error("_gateway.parallel: task #{index} '{field}' must be a non-empty string, got {got}")]
    WrongFieldType {
        index: usize,
        field: &'static str,
        got: String,
    },
}

/// Validate a `serde_json::Value` claiming to be a `_gateway.parallel`
/// argument. The shape test is strict: the outer value must be a JSON
/// array (which is how mlua serializes a dense-integer-keyed table).
///
/// Returns the validated descriptor list in input order. An empty array
/// is valid — the runtime will produce an empty result array, no
/// upstream dispatch.
pub fn parse_task_list(
    raw: serde_json::Value,
) -> Result<Vec<ParallelTaskDescriptor>, ParallelInputError> {
    let tasks = match raw {
        serde_json::Value::Array(a) => a,
        other => {
            return Err(ParallelInputError::NotAnArray {
                got: describe_json_shape(&other).to_string(),
            });
        }
    };

    let mut out = Vec::with_capacity(tasks.len());
    for (i, task) in tasks.into_iter().enumerate() {
        let index = i + 1;
        let descriptor = parse_task(index, task)?;
        out.push(descriptor);
    }
    Ok(out)
}

fn parse_task(
    index: usize,
    task: serde_json::Value,
) -> Result<ParallelTaskDescriptor, ParallelInputError> {
    let mut task_obj = match task {
        serde_json::Value::Object(o) => o,
        other => {
            return Err(ParallelInputError::TaskNotATable {
                index,
                got: describe_json_shape(&other).to_string(),
            });
        }
    };

    let server = take_non_empty_string(&mut task_obj, index, "server")?;
    let tool = take_non_empty_string(&mut task_obj, index, "tool")?;

    // `args` is optional — default to an empty object so the downstream
    // call uses `{}`. When present, it must be an object; an array or
    // scalar in the `args` slot is always a user mistake because MCP
    // tool arguments are a JSON object by spec.
    let args = match task_obj.remove("args") {
        None | Some(serde_json::Value::Null) => serde_json::Map::new(),
        Some(serde_json::Value::Object(o)) => o,
        Some(other) => {
            return Err(ParallelInputError::ArgsNotAnObject {
                index,
                got: describe_json_shape(&other).to_string(),
            });
        }
    };

    Ok(ParallelTaskDescriptor { server, tool, args })
}

fn take_non_empty_string(
    task: &mut serde_json::Map<String, serde_json::Value>,
    index: usize,
    field: &'static str,
) -> Result<String, ParallelInputError> {
    match task.remove(field) {
        None | Some(serde_json::Value::Null) => {
            Err(ParallelInputError::MissingField { index, field })
        }
        Some(serde_json::Value::String(s)) => {
            if s.is_empty() {
                Err(ParallelInputError::EmptyField { index, field })
            } else {
                Ok(s)
            }
        }
        // Anything non-string is a type mistake — the agent wrote a
        // number, boolean, or array where a string was required. Report
        // the actual type so the agent can correct the call without
        // guessing what "must be a non-empty string" refers to.
        Some(other) => Err(ParallelInputError::WrongFieldType {
            index,
            field,
            got: describe_json_shape(&other).to_string(),
        }),
    }
}

#[cfg(test)]
#[path = "parallel_tests.rs"]
mod tests;
