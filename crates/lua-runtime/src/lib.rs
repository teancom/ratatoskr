//! pattern: Mixed (unavoidable)
//!
//! Reason: Lua scripts are fundamentally an I/O-coordinating execution
//! environment. Host functions (tool calls into async Rust code) perform I/O;
//! the Lua runtime itself is a stateful VM.
//!
//! Where we CAN separate:
//! - `identifier.rs` is pure (Functional Core) — Lua identifier sanitization.
//! - `error.rs` is pure (Functional Core) — error type, no I/O.
//! - `script_input.rs` is pure (Functional Core) — tool-call arg parsing.
//! - `expand_template.rs` is pure (Functional Core) — RFC 6570 expansion
//!   + namespacing, used by the `_gateway.expand_template` Lua builtin.
//! - `parallel.rs` is pure (Functional Core) — task-list validation for
//!   the `_gateway.parallel` Lua builtin; returns typed descriptors.
//! - `runtime.rs` is the Imperative Shell — owns mlua::Lua and calls async
//!   host code.

pub mod error;
pub mod expand_template;
pub mod identifier;
mod json_shape;
pub mod parallel;
pub mod runtime;
pub mod schema_inferrer;
pub mod script_input;

pub use error::LuaExecutionError;
pub use identifier::{partition_unique_by_sanitized, sanitize_lua_identifier};
pub use runtime::{
    DEFAULT_EXECUTE_DEADLINE, DEFAULT_INSTRUCTION_BUDGET, ExecutionOutcome, ExecutionResultBackend,
    GATEWAY_BUILTIN_NAMES, LuaRuntime, OffloadConfig, ToolCallGuard,
};
pub use schema_inferrer::{DEFAULT_MAX_DEPTH, Shape, format_shape, infer_shape};
pub use script_input::{ScriptInputError, parse_script_argument};
