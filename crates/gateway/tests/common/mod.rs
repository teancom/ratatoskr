// pattern: Imperative Shell
//
// Shared integration-test helpers for the gateway crate.
//
// Integration tests live under `crates/gateway/tests/` and each file is
// compiled as its own crate by Cargo. To share helpers, Cargo uses the
// convention `tests/common/mod.rs`: a test file adds `mod common;` and
// can then call `common::*` helpers. Because Cargo compiles each test
// binary independently, unreferenced helpers here will produce
// `dead_code` warnings per-binary — hence the blanket `#![allow]`
// below. This is the documented cost of the shared-helpers layout.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use gateway::AppContext;
use gateway::config::GatewaySettings;
use gateway::execution_result_store::ExecutionResultStore;
use gateway::response_shape_cache::ResponseShapeCache;
use gateway::subscription_registry::SubscriptionRegistry;
use gateway::subscription_service::SubscriptionService;
use gateway::tool_inspection::ToolInspectionStore;
use lua_runtime::LuaRuntime;
use mcp_client::{
    ResourceListCache, SamplingBroker, SharedSamplingBroker, ToolListCache, UpstreamConfig,
    UpstreamEventBus, UpstreamHandle, UpstreamKind, UpstreamManager, connect_with_transport,
};
use rmcp::{ServerHandler, ServiceExt};
use tokio_util::sync::CancellationToken;

/// Build an `AppContext` around a caller-supplied upstream manager,
/// event bus, and sampling broker.
///
/// The three "live" pieces — `upstreams`, `bus`, `broker` — must be the
/// same instances the test harness uses to register fake upstreams and
/// observe dispatch, so we accept them as parameters. Everything else
/// (caches, stores, registry, shape cache, settings) is ephemeral test
/// scaffolding and gets fresh instances.
///
/// Use the `_with_settings` variant when a test needs to set a
/// non-default `GatewaySettings` (most commonly a non-default
/// `result_size_threshold`).
pub async fn test_app_context(
    upstreams: Arc<UpstreamManager>,
    bus: Arc<UpstreamEventBus>,
    broker: SharedSamplingBroker,
) -> AppContext {
    test_app_context_with_settings(upstreams, bus, broker, GatewaySettings::default()).await
}

/// Variant of [`test_app_context`] that accepts a pre-built
/// `GatewaySettings`. Tests exercising offload thresholds, future
/// per-test execute deadlines, etc. use this.
pub async fn test_app_context_with_settings(
    upstreams: Arc<UpstreamManager>,
    bus: Arc<UpstreamEventBus>,
    broker: SharedSamplingBroker,
    settings: GatewaySettings,
) -> AppContext {
    let subscriptions = Arc::new(SubscriptionRegistry::new());
    let subscription_service = Arc::new(SubscriptionService::new(
        Arc::clone(&subscriptions),
        Arc::clone(&upstreams),
    ));
    // Mirror the production `serve::run` wiring: the per-execute
    // deadline on `settings` is baked into the LuaRuntime at
    // construction. Tests that want a non-default deadline pass a
    // tweaked `GatewaySettings` to `_with_settings` and get a runtime
    // honouring it for free.
    let lua = Arc::new(
        LuaRuntime::new().with_execute_deadline(std::time::Duration::from_millis(
            settings.execute_deadline_ms,
        )),
    );
    AppContext {
        upstreams,
        bus,
        broker,
        inspection: ToolInspectionStore::new(),
        tool_cache: Arc::new(ToolListCache::new()),
        resource_cache: Arc::new(ResourceListCache::new()),
        result_store: Arc::new(ExecutionResultStore::new()),
        subscriptions,
        subscription_service,
        response_shapes: ResponseShapeCache::new(),
        settings,
        lua,
        shutdown: tokio_util::sync::CancellationToken::new(),
        supervisor_cancel: tokio_util::sync::CancellationToken::new(),
        supervisors: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        reload_lock: Arc::new(tokio::sync::Mutex::new(())),
    }
}

/// Convenience: assemble a default `SharedSamplingBroker` so test
/// harnesses that don't exercise sampling don't have to import
/// `SamplingBroker` themselves.
pub fn default_broker() -> SharedSamplingBroker {
    Arc::new(SamplingBroker::new())
}

// ---------------------------------------------------------------------
// Shared harness scaffolding: fake-upstream spawning and gateway
// shutdown. See individual doc comments below for the invariants each
// piece pins down; both are deliberately low-level (return raw pieces
// rather than a do-everything struct) so harnesses that need bespoke
// ordering — e.g. registering several handles before starting the
// gateway, or never joining the fake task at all — stay expressible.
// ---------------------------------------------------------------------

/// Upper bound for how long a `serve_on` task gets, after cancellation,
/// to shut down gracefully before a test treats the wait as exhausted.
/// Shared by every harness so the budget is changed in exactly one
/// place; see `cancel_and_join_server`.
pub const SERVER_SHUTDOWN_BUDGET: Duration = Duration::from_secs(3);

/// Outcome of racing a spawned `serve_on` task against
/// [`SERVER_SHUTDOWN_BUDGET`] after signalling cancellation.
///
/// Harnesses disagree on what to do when the budget is exhausted: most
/// treat it as a soft warning (the process is shutting down anyway,
/// and a slow-but-eventual exit isn't the property under test), a few
/// treat it as a hard test failure. Rather than bake one policy into
/// the helper, `cancel_and_join_server` returns this enum and leaves
/// the `match` — and its policy choice — at the call site.
pub enum ServerShutdown {
    /// `serve_on` returned `Ok(())` within budget.
    Clean,
    /// `serve_on` returned `Err(_)` within budget.
    ServeErr(anyhow::Error),
    /// The spawned task panicked (join error) within budget.
    Panicked(tokio::task::JoinError),
    /// Neither happened before the budget elapsed.
    TimedOut,
}

/// Cancel `ct` and await `server_handle` up to [`SERVER_SHUTDOWN_BUDGET`],
/// returning which of the four outcomes occurred.
///
/// This is the "cancel + timeout" mechanics every harness duplicated;
/// it does NOT decide whether a timeout is a warning or a panic —
/// callers `match` on [`ServerShutdown`] and pick their own policy,
/// same as before this helper existed.
pub async fn cancel_and_join_server(
    ct: &CancellationToken,
    server_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
) -> ServerShutdown {
    ct.cancel();
    match tokio::time::timeout(SERVER_SHUTDOWN_BUDGET, server_handle).await {
        Ok(Ok(Ok(()))) => ServerShutdown::Clean,
        Ok(Ok(Err(e))) => ServerShutdown::ServeErr(e),
        Ok(Err(join_err)) => ServerShutdown::Panicked(join_err),
        Err(_elapsed) => ServerShutdown::TimedOut,
    }
}

/// Spawn one in-process fake upstream behind a `tokio::io::duplex`-paired
/// transport and return the connected (but NOT registered)
/// `Arc<UpstreamHandle>` plus the `JoinHandle` for the fake server's
/// driver task.
///
/// Generic over the `ServerHandler` so callers can plug in any fixture
/// (echo tool, resource-listing fake, subscribable fake, ...) without
/// `dyn` erasure or per-file copies of the duplex/transport/connect
/// wiring. Registration into an `UpstreamManager` is deliberately left
/// to the caller: some harnesses register immediately, others collect
/// several handles first (heterogeneous multi-upstream tests) before
/// building the manager.
///
/// Uses a 64 KiB duplex buffer, sized for ordinary tool-call payloads.
/// Use [`spawn_fake_upstream_with_buf`] when a test moves payloads at
/// or above that size (e.g. large-result-offload tests).
///
/// # Critical ordering invariant
///
/// The driver task's `service.waiting()` returns only once its duplex
/// peer — the gateway-side half, owned by the returned
/// `Arc<UpstreamHandle>` (and by any `UpstreamManager` it gets
/// registered into) — disconnects. If a harness awaits this
/// `JoinHandle` (e.g. during shutdown) while still holding a live
/// `Arc<UpstreamHandle>` anywhere (the harness struct, the manager,
/// etc.), the peer never sees EOF and the await deadlocks. Callers that
/// join the driver task MUST drop every `Arc<UpstreamHandle>` clone
/// first — typically via `Option::take()` or `Vec::clear()` on the
/// harness's own fields — before awaiting the task.
pub async fn spawn_fake_upstream<S>(
    display_name: &str,
    lua_name: &str,
    server: S,
    bus: &Arc<UpstreamEventBus>,
    broker: &SharedSamplingBroker,
) -> (Arc<UpstreamHandle>, tokio::task::JoinHandle<()>)
where
    S: ServerHandler + Send + 'static,
{
    spawn_fake_upstream_with_buf(display_name, lua_name, server, bus, broker, 64 * 1024).await
}

/// Variant of [`spawn_fake_upstream`] with an explicit duplex buffer
/// size, for harnesses that push payloads close to (or past) the
/// default 64 KiB. See [`spawn_fake_upstream`] for the full contract,
/// including the drop-before-join ordering invariant.
pub async fn spawn_fake_upstream_with_buf<S>(
    display_name: &str,
    lua_name: &str,
    server: S,
    bus: &Arc<UpstreamEventBus>,
    broker: &SharedSamplingBroker,
    duplex_buf: usize,
) -> (Arc<UpstreamHandle>, tokio::task::JoinHandle<()>)
where
    S: ServerHandler + Send + 'static,
{
    let config = UpstreamConfig::new(
        display_name.to_string(),
        lua_name.to_string(),
        UpstreamKind::Stdio {
            command: "unused-in-test".to_string(),
            args: vec![],
            env: Default::default(),
        },
    );
    spawn_fake_upstream_with_config(config, server, bus, broker, duplex_buf).await
}

/// Lowest-level variant: takes a fully-built `UpstreamConfig` instead of
/// assembling a bare `Stdio` one from `(display_name, lua_name)`. Reach
/// for this when a test needs to stamp something onto the config before
/// connecting — e.g. `.with_sampling_enabled()` or `.with_hint(..)` —
/// that the `(display_name, lua_name)`-only variants can't express. See
/// [`spawn_fake_upstream`] for the full contract, including the
/// drop-before-join ordering invariant.
pub async fn spawn_fake_upstream_with_config<S>(
    config: UpstreamConfig,
    server: S,
    bus: &Arc<UpstreamEventBus>,
    broker: &SharedSamplingBroker,
    duplex_buf: usize,
) -> (Arc<UpstreamHandle>, tokio::task::JoinHandle<()>)
where
    S: ServerHandler + Send + 'static,
{
    let (gateway_side, upstream_side) = tokio::io::duplex(duplex_buf);
    let (gateway_read, gateway_write) = tokio::io::split(gateway_side);
    let (upstream_read, upstream_write) = tokio::io::split(upstream_side);

    let task = tokio::spawn(async move {
        let transport =
            rmcp::transport::async_rw::AsyncRwTransport::<rmcp::RoleServer, _, _>::new_server(
                upstream_read,
                upstream_write,
            );
        let service = server
            .serve(transport)
            .await
            .expect("fake upstream serve should not fail");
        service
            .waiting()
            .await
            .expect("fake upstream serve loop should not error");
    });

    let live = connect_with_transport(&config, bus, broker, gateway_read, gateway_write)
        .await
        .expect("in-memory upstream connect");
    let handle = Arc::new(UpstreamHandle::new(config, live));

    (handle, task)
}
