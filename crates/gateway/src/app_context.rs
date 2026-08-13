// pattern: Imperative Shell
//
// Process-wide shared state for the gateway, aggregated into one
// cheap-to-clone handle.
//
// Every field here is "process"-lifetime: constructed once in
// `serve::run`, shared across every downstream session. Examples:
// `upstreams`, `bus`, `tool_cache`.
//
// Nothing per-session lives on `AppContext`. Per-session state (e.g.
// `SessionForwarder`, the peer registered with the sampling broker)
// belongs inside the handler's `SessionState`.
//
// Clone is cheap by construction: every field is either `Arc`-backed
// (registries, caches, stores, broker, inspection store, response-shape
// cache) or `Copy` (`GatewaySettings`). Cloning the struct clones the
// same ~10 `Arc`s the pre-refactor code cloned by hand at every factory
// call site — just named and localized.
//
// Why a single struct rather than threading 10+ `Arc` parameters:
//
//   * `serve_on` previously carried 13 arguments with an
//     `#[allow(clippy::too_many_arguments)]` lint silencer and a comment
//     explicitly advising "add another line when the next arg lands".
//     That scales badly. Adding a new cross-cutting resource was an
//     eight-edit change (serve_on signature, call site, factory closure
//     capture, inner closure Arc::clone, handler constructor, handler
//     SessionState field, tests). With `AppContext`, adding a resource
//     is a one-line field addition plus a construction-site assignment.
//
//   * Test harnesses lose a long wall of by-hand `Arc::new(...)`
//     ceremony in favor of `test_app_context(...)` helpers.
//
//   * Documents intent: "these things are always shared, always
//     together" is now the type system's job, not a convention.

use std::collections::HashMap;
use std::sync::Arc;

use lua_runtime::LuaRuntime;
use mcp_client::{
    ResourceListCache, SharedSamplingBroker, ToolListCache, UpstreamEventBus, UpstreamManager,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::config::GatewaySettings;
use crate::execution_result_store::ExecutionResultStore;
use crate::response_shape_cache::ResponseShapeCache;
use crate::subscription_registry::SubscriptionRegistry;
use crate::subscription_service::SubscriptionService;
use crate::tool_inspection::ToolInspectionStore;

/// Process-wide shared state for the gateway.
///
/// See the module header for the sharing model and rationale. Construct
/// once in `serve::run`; clone freely everywhere else.
#[derive(Clone)]
pub struct AppContext {
    /// Registry of connected upstream MCP servers. Single source of
    /// truth for `handle_by_name` / `handle_by_lua_name` lookups.
    pub upstreams: Arc<UpstreamManager>,

    /// Event bus for upstream-originated notifications (tool-list
    /// changed, resource-list changed, resource updated). Each
    /// subscriber attaches via `bus.subscribe()`; the invalidator
    /// tasks and per-session forwarders both do this.
    pub bus: Arc<UpstreamEventBus>,

    /// Sampling broker for `sampling/createMessage` round-trips.
    /// Downstream sessions register with it on `on_initialized` (if the
    /// client advertises sampling) and deregister on session close.
    pub broker: SharedSamplingBroker,

    /// Tool-inspection store. Keyed by session ID internally; every
    /// handler instance reads/writes through the shared handle.
    pub inspection: ToolInspectionStore,

    /// Per-upstream `tools/list` cache. Invalidated on
    /// `tools/list_changed` via `spawn_invalidator`.
    pub tool_cache: Arc<ToolListCache>,

    /// Per-upstream `resources/list` + `resources/templates/list`
    /// cache. Invalidated on `resources/list_changed` via
    /// `spawn_resource_invalidator`.
    pub resource_cache: Arc<ResourceListCache>,

    /// Offload store for oversized `execute` / `inspect-tool-response`
    /// results. Keyed by session ID; cleared on session close by
    /// `InspectingSessionManager`.
    pub result_store: Arc<ExecutionResultStore>,

    /// Ref-counted subscription registry: tracks which downstream
    /// sessions subscribe to which upstream resources, so one upstream
    /// `resources/subscribe` can serve N downstream sessions.
    ///
    /// This is the pure state structure; `subscription_service` below
    /// wraps it with the upstream fan-out and rollback policy. The
    /// handler never touches the registry directly — every read/write
    /// goes through the service. `InspectingSessionManager` reaches
    /// into the registry in its tests only; production code uses the
    /// service.
    pub subscriptions: Arc<SubscriptionRegistry>,

    /// Service that owns the ref-counted subscription state machine
    /// plus upstream `resources/subscribe` / `resources/unsubscribe`
    /// fan-out. Built from `subscriptions` + `upstreams`; constructed
    /// once in `serve::run` and cloned by `Arc` into every handler.
    ///
    /// Single source of truth for subscription-related RPCs: the
    /// `SubscriptionRegistry` never issues RPCs directly; this
    /// service is the only producer of `resources/subscribe` /
    /// `resources/unsubscribe` on the upstream side (except inside
    /// the service itself).
    pub subscription_service: Arc<SubscriptionService>,

    /// Observed/declared response-shape cache, populated on successful
    /// `inspect-tool-response` calls and read at `tool-details` render
    /// time. Monotonic-merge semantics; see `response_shape_cache.rs`.
    pub response_shapes: ResponseShapeCache,

    /// Gateway-wide settings parsed from the config file. `Copy`, so
    /// field access on the clone is free.
    pub settings: GatewaySettings,

    /// Shared runtime for the `execute` tool. Zero-state today (a
    /// fresh `mlua::Lua` is created per call inside `execute`), so one
    /// runtime instance per process is fine; the handle is here to
    /// give future per-call overrides (instruction budget, memory
    /// limit) a home without refactoring call sites.
    pub lua: Arc<LuaRuntime>,

    /// Root shutdown token. Cancelling it drives the same clean-shutdown
    /// path `serve_on` runs on a SIGINT/SIGTERM (axum graceful drain →
    /// supervisor cancel+join → `close_all_upstreams` → invalidator
    /// join). The `restart-gateway` tool cancels this so the process
    /// exits 0 and launchd (`KeepAlive=true`) respawns it, picking up
    /// config + settings changes at boot. Held here so the
    /// otherwise-stateless per-session handler can reach it.
    pub shutdown: CancellationToken,

    /// Parent token for per-upstream supervisor cancellation. A child of
    /// `shutdown`, so the existing shutdown cancel still cascades to
    /// every supervisor. The `reload-config` tool mints children of this
    /// for supervisors it spawns on newly-added upstreams; both boot and
    /// reload share this one token so their supervisors die together at
    /// shutdown.
    pub supervisor_cancel: CancellationToken,

    /// Per-upstream supervisor cancellation tokens, keyed by display
    /// `name`. Populated at boot by `spawn_stdio_supervisors` /
    /// `spawn_bootstrap_retry_supervisors` and at reload by the diff
    /// apply step. The reload "removed" path cancels an upstream's token
    /// (stopping its respawn loop) before removing the handle, closing
    /// the "removed stdio upstream keeps getting respawned" race.
    pub supervisors: Arc<Mutex<HashMap<String, CancellationToken>>>,

    /// Serializes `reload-config` invocations. Two downstream sessions
    /// could call reload concurrently; without this lock they would race
    /// on `manager.list()` → diff → mutate. Acquired for the whole
    /// duration of one reload.
    pub reload_lock: Arc<Mutex<()>>,
}
