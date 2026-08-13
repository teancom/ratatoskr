//! pattern: Imperative Shell
//!
//! Library façade for the gateway crate. The binary at src/main.rs uses
//! these same modules via `crate::...`; integration tests use the library
//! via `gateway::...`. Having both a `[[bin]]` and a `[lib]` target on the
//! same crate is idiomatic for Rust CLIs that want testable internals.

pub mod app_context;
pub mod config;
pub mod execution_result_store;
pub(crate) mod handler;
pub(crate) mod hint;
pub(crate) mod inspecting_session_manager;
pub(crate) mod list_servers;
pub mod login;
pub(crate) mod offload_message;
pub mod pidfile;
pub mod reconcile;
pub(crate) mod response_shape;
pub mod response_shape_cache;
pub mod serve;
pub(crate) mod server_details;
pub(crate) mod session_forwarder;
pub mod subscription_registry;
pub mod subscription_service;
pub(crate) mod tool_details;
pub mod tool_inspection;
pub(crate) mod tools_schema;

pub use app_context::AppContext;
pub use serve::{run, serve_on};
