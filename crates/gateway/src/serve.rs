// pattern: Imperative Shell

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use lua_runtime::LuaRuntime;
use mcp_client::{
    HealthState, LiveUpstream, RealConnector, ResourceListCache, SamplingBroker,
    SharedSamplingBroker, ToolListCache, UpstreamConfig, UpstreamError, UpstreamEventBus,
    UpstreamHandle, UpstreamHealth, UpstreamKind, UpstreamManager, UpstreamSupervisor,
    spawn_invalidator, spawn_resource_invalidator,
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::local::{LocalSessionManager, SessionConfig},
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::app_context::AppContext;
use crate::config;
use crate::execution_result_store::ExecutionResultStore;
use crate::handler::GatewayHandler;
use crate::inspecting_session_manager::InspectingSessionManager;
use crate::response_shape_cache::ResponseShapeCache;
use crate::subscription_registry::SubscriptionRegistry;
use crate::subscription_service::SubscriptionService;
use crate::tool_inspection::ToolInspectionStore;

// The gateway always binds loopback: it fronts credentialed upstreams
// with no auth layer of its own, so exposing it beyond localhost would
// hand every configured token to the LAN. The port comes from the
// config file's `port` (default 8910); the host is deliberately NOT
// configurable.
const BIND_HOST: &str = "127.0.0.1";

/// How long axum's graceful shutdown is allowed to drain in-flight
/// connections after the shutdown signal fires, before we stop waiting
/// and force the deterministic upstream cleanup + process exit.
///
/// rmcp's streamable-HTTP transport keeps a client connection open
/// across requests, so a still-attached client blocks `with_graceful_
/// shutdown` from returning. Without this bound, `restart-gateway`
/// (whose caller is itself an open connection) — and SIGTERM, and the
/// deploy script's bootout — would hang indefinitely whenever a client
/// is connected. 3s is comfortably longer than a real in-flight request
/// needs to finish, and short enough that a restart still feels prompt.
const SHUTDOWN_DRAIN_GRACE: Duration = Duration::from_secs(3);

/// Max upstream connects allowed in flight at once during boot and
/// reload. Connecting is the slow part of startup — stdio upstreams shell
/// out to `npx`/`mcp-remote` cold-starts that take seconds — and they are
/// fully independent, so we fan them out concurrently rather than summing
/// their latencies in a serial loop. The fan-out is *bounded* (not an
/// unbounded `join_all`) so a large roster can't trigger a stampede of
/// simultaneous `npx` cold-starts that spikes CPU/IO and starves the rest
/// of boot. 8 is comfortably above a typical roster's slow-upstream count
/// while staying gentle on a dev laptop.
const BOOTSTRAP_CONNECT_CONCURRENCY: usize = 8;

/// Env var name for an explicit config path override. Namespaced with
/// the `RATO_` prefix so it can't collide with generic `CONFIG_PATH`
/// env vars that some upstream MCP servers (e.g. `unifi-network-mcp`)
/// inspect on their own — when the gateway inherited `CONFIG_PATH`
/// into spawned children, those upstreams read the gateway's JSON
/// config as if it were their own and crashed at bootstrap.
const CONFIG_PATH_ENV: &str = "RATO_CONFIG_PATH";

/// Max config file size we'll even try to read. A real config is a few KB;
/// anything past 1 MiB is either a mistake (pointed at the wrong file) or
/// a deliberate resource-exhaustion attempt. Fail-fast is kinder than
/// OOMing.
const CONFIG_MAX_BYTES: u64 = 1024 * 1024;

/// How long an idle MCP session lives before rmcp reclaims it.
///
/// rmcp defaults to 5 minutes, which is a pathology for this workload:
/// a 5-minute ceiling flushes the session (and the tool-inspection state
/// the client has built up in its context)
/// any time the user steps away for a coffee. On a dev
/// laptop behind no load balancer, there's no reason to be stingy —
/// 24 hours gives Claude a full workday of continuous context without
/// re-initializing.
///
/// `SessionConfig`'s own doc flags a real caveat here: on hosts sitting
/// behind a proxy that silently drops idle HTTP/2 streams, long TTLs can
/// accumulate zombie session workers. That's not our deployment
/// profile (localhost on a single-dev machine), so 24h is safe. Revisit
/// if we ever ship server-side.
const SESSION_KEEP_ALIVE: Duration = Duration::from_secs(60 * 60 * 24);

/// Where an upstream config list came from, for logging.
#[derive(Debug, Clone, Copy)]
pub enum ConfigSource {
    /// `$RATO_CONFIG_PATH` was set and that file was loaded.
    EnvVar,
    /// Default platform path was loaded.
    PlatformDefault,
}

impl ConfigSource {
    pub fn as_log_str(&self) -> &'static str {
        match self {
            ConfigSource::EnvVar => "env:RATO_CONFIG_PATH",
            ConfigSource::PlatformDefault => "platform-default",
        }
    }
}

pub async fn run() -> Result<()> {
    let ct = CancellationToken::new();
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    // Load config and bind the listener BEFORE connecting any upstream.
    // Binding is the cheap, fail-fast step; upstream bootstrap forks one
    // child process per stdio upstream, with user-visible side effects
    // (Xcode raises a permission prompt for every fresh mcpbridge PID).
    // With bootstrap-then-bind ordering, a taken port turns launchd's
    // KeepAlive crash loop into a fork storm: each doomed instance
    // spawns the full stdio fleet and only THEN exits on EADDRINUSE.
    // Bind-first means a port conflict exits before a single child is
    // forked.
    let (configs, settings, source) = load_upstream_configs()?;
    tracing::info!(
        source = source.as_log_str(),
        count = configs.len(),
        result_size_threshold = settings.result_size_threshold,
        "loaded upstream configs"
    );
    // Layer-1 validation: fail fast at boot if any containerized
    // upstream's display name would produce an invalid container name
    // (e.g. contains spaces or non-ASCII). The failure is visible in
    // the startup log instead of being buried in a per-upstream
    // respawn failure later.
    validate_container_upstream_keys(&configs)?;

    let bind_address = format!("{BIND_HOST}:{}", settings.port);
    let listener = TcpListener::bind(&bind_address)
        .await
        .with_context(|| format!("failed to bind {bind_address}"))?;
    tracing::info!(bind = %bind_address, "gateway listening");

    let (manager, failed_bootstrap_configs) =
        bootstrap_upstreams(&bus, &broker, configs, &settings).await;
    let upstreams = Arc::new(manager);

    // Pull `subscriptions` and `subscription_service` out of the struct
    // literal: the service is built from the registry plus the upstream
    // manager, both of which already live on the context. Hoisting the
    // two lines keeps the field init order readable without nesting a
    // two-step construction inside the struct literal.
    let subscriptions = Arc::new(SubscriptionRegistry::new());
    let subscription_service = Arc::new(SubscriptionService::new(
        Arc::clone(&subscriptions),
        Arc::clone(&upstreams),
    ));

    // Per-upstream supervisor tokens hang off this one parent, itself a
    // child of the root `ct`. Created here (not inside `serve_on`) so it
    // can live on the `AppContext` — the `reload-config` tool needs it to
    // mint supervisor tokens for newly-added upstreams at runtime, and
    // boot + reload must share the same token tree so the existing
    // shutdown cancel reaches every supervisor regardless of when it was
    // spawned. `serve_on` receives this same token rather than minting
    // its own.
    let supervisor_cancel = ct.child_token();

    let ctx =
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
            // One LuaRuntime per process — the runtime is zero-state
            // (fresh `mlua::Lua` per call inside `execute`), so sharing
            // the handle across sessions is safe and avoids the
            // per-session construction cost.
            //
            // The per-execute deadline is baked in at construction so
            // every `execute` call against this `AppContext` sees the
            // same bound. Config validation (`gateway::config`) already
            // guarantees `execute_deadline_ms` is > 0; the builder takes
            // a `Duration`, so we convert here.
            lua: Arc::new(LuaRuntime::new().with_execute_deadline(
                std::time::Duration::from_millis(settings.execute_deadline_ms),
            )),
            shutdown: ct.clone(),
            supervisor_cancel: supervisor_cancel.clone(),
            supervisors: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            reload_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

    // Publish our PID so the `login` subcommand can SIGHUP us to hot-reload
    // after a successful OAuth login. Best-effort: a write failure only
    // costs that convenience. Removed after a clean shutdown below.
    let pid_file = crate::pidfile::write();

    let shutdown_ct = ct.clone();
    let shutdown = async move {
        let signal = wait_for_shutdown_signal().await;
        tracing::info!(signal = signal, "shutdown signal received");
        shutdown_ct.cancel();
    };

    let result = serve_on(
        listener,
        ctx,
        failed_bootstrap_configs,
        ct,
        supervisor_cancel,
        shutdown,
    )
    .await;

    // Clean up our PID file on exit. A leftover file is harmless (readers
    // liveness-check the PID), but removing it keeps the state dir tidy.
    if let Some(path) = pid_file {
        crate::pidfile::remove(&path);
    }

    result
}

/// Wait for the first of SIGINT or SIGTERM and return a label describing
/// which one fired. Returns even if installing one of the handlers fails —
/// a working SIGINT path alone is still useful, just logged as a warning.
///
/// We handle SIGTERM because it's the signal launchd/systemd/Docker/CI
/// teardown/`pkill` all send by default. Without an explicit handler,
/// tokio falls through to the OS default (exit 143) and Drop on the
/// upstream manager never runs — which means no `service.cancel().await`,
/// no axum graceful drain, and no "gateway stopped cleanly" log line.
async fn wait_for_shutdown_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "failed to install SIGTERM handler; SIGTERM will exit abruptly");
            // Fall through to a SIGINT-only wait.
            let _ = tokio::signal::ctrl_c().await;
            return "SIGINT";
        }
    };
    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "failed to install SIGINT handler; SIGINT will exit abruptly");
            // Wait on SIGTERM only.
            sigterm.recv().await;
            return "SIGTERM";
        }
    };
    tokio::select! {
        _ = sigint.recv() => "SIGINT",
        _ = sigterm.recv() => "SIGTERM",
    }
}

/// Spawn the SIGHUP-driven config-reload task.
///
/// On each SIGHUP it runs [`reconcile_from_disk`] against `ctx` — the same
/// path the `reload-config` MCP tool uses — and logs a one-line summary of
/// what changed. The loop runs until `cancel` fires (at shutdown), so a
/// stream of SIGHUPs is handled (each reconcile is serialized by the
/// reload lock inside `reconcile_from_disk`).
///
/// On non-Unix platforms there is no SIGHUP; the task parks on `cancel` and
/// does nothing, keeping the join-at-shutdown code path uniform.
fn spawn_sighup_reloader(
    ctx: AppContext,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_on_sighup(cancel, move || {
        let ctx = ctx.clone();
        async move {
            tracing::info!("SIGHUP received; reloading config from disk");
            match reconcile_from_disk(&ctx).await {
                ReconcileResult::ParseError(e) => {
                    tracing::warn!(
                        error = %e,
                        "SIGHUP reload: config invalid; nothing changed (running config untouched)"
                    );
                }
                ReconcileResult::Applied {
                    outcome,
                    settings_lines,
                } => {
                    tracing::info!(
                        connected = outcome.connected.len(),
                        removed = outcome.removed.len(),
                        failed = outcome.failed.len(),
                        settings_changed = settings_lines.len(),
                        "SIGHUP reload applied"
                    );
                    if !settings_lines.is_empty() {
                        tracing::warn!(
                            "SIGHUP reload: gateway-wide settings changed but were NOT \
                             applied (a restart is required): {}",
                            settings_lines.join("; ")
                        );
                    }
                }
            }
        }
    }))
}

/// Run `action` once per received SIGHUP until `cancel` fires. Factored out
/// of [`spawn_sighup_reloader`] so the signal-loop wiring (install handler,
/// fire on signal, stop on cancel) is testable without a config file or
/// process env — a test passes an `action` that records invocations and
/// sends itself real SIGHUPs.
///
/// On non-Unix there is no SIGHUP: the loop parks on `cancel` and `action`
/// never fires.
async fn run_on_sighup<F, Fut>(cancel: CancellationToken, mut action: F)
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = ()> + Send,
{
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sighup = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to install SIGHUP handler; config hot-reload via signal is disabled \
                     (the reload-config MCP tool still works)"
                );
                cancel.cancelled().await;
                return;
            }
        };
        tracing::debug!("SIGHUP config-reload handler installed");
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    tracing::debug!("SIGHUP reloader cancelled at shutdown");
                    return;
                }
                recv = sighup.recv() => {
                    if recv.is_none() {
                        // Signal stream closed — nothing more to do.
                        return;
                    }
                    action().await;
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = &mut action;
        cancel.cancelled().await;
    }
}

/// Serve downstream MCP traffic on `listener`, backed by the
/// process-wide state in `ctx`. Returns when the `shutdown` future
/// resolves and the HTTP server has drained.
///
/// `failed_bootstrap_configs` is the list of upstreams whose first
/// connect failed in `bootstrap_upstreams`. `serve_on` spawns a
/// bootstrap-retry task for each (born-Respawning supervisor for
/// stdio, simple retry loop for HTTP), wired to the same
/// `supervisor_cancel` token as the regular stdio supervisors so all
/// retry-class tasks are cancelled and joined together BEFORE
/// `close_all_upstreams` drains the manager — the only ordering that
/// rules out the "respawn registers a handle into a drained manager"
/// race. Tests that don't exercise bootstrap-retry pass
/// `Vec::new()`.
///
/// `ctx` is moved in and cloned into the handler factory closure (cheap:
/// every `AppContext` field is `Arc`-backed or `Copy`). The caller —
/// `run()` in production, test harnesses in tests — owns every resource
/// that will outlive this call via their own `ctx` clone.
pub async fn serve_on(
    listener: TcpListener,
    ctx: AppContext,
    failed_bootstrap_configs: Vec<UpstreamConfig>,
    ct: CancellationToken,
    supervisor_cancel: CancellationToken,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    // Spin up the tool-list cache invalidator before any downstream
    // session connects. The task subscribes to `bus` and clears cache
    // entries on `UpstreamEvent::ToolListChanged`; without it a stale
    // tool list would persist for the lifetime of the process.
    //
    // We cancel via a child of `ct` so a shutdown signal reaches the
    // invalidator at the same moment it reaches the HTTP server, and
    // we hold the JoinHandle so shutdown can await its exit rather
    // than leaking a background task.
    let invalidator_cancel = ct.child_token();
    let tool_invalidator_handle = spawn_invalidator(
        (*ctx.tool_cache).clone(),
        Arc::clone(&ctx.bus),
        invalidator_cancel.clone(),
    );

    // Resource-list cache invalidator: same pattern, different event
    // variant. Runs on its own task; a stale resource listing would
    // otherwise persist across `resources/list_changed` notifications
    // (which `SessionForwarder` already fans to downstream) until the
    // gateway restarts.
    let resource_invalidator_handle = spawn_resource_invalidator(
        (*ctx.resource_cache).clone(),
        Arc::clone(&ctx.bus),
        invalidator_cancel.clone(),
    );

    // SIGHUP → hot config reload. SIGHUP is the conventional "re-read your
    // config" signal for long-lived daemons, and wiring it here gives the
    // gateway the same reconcile the `reload-config` MCP tool performs —
    // without an attached MCP client. The `login` subcommand sends it on a
    // successful OAuth login so freshly-authorized upstreams connect
    // immediately, but it's equally useful after any hand-edit
    // (`kill -HUP $(cat <pidfile>)`).
    //
    // The task holds a `ctx` clone (it needs the full context to
    // reconcile), so its lifetime MUST be tied to `serve_on`'s: a leaked
    // SIGHUP task would pin the `Arc<UpstreamManager>` alive and defeat the
    // Drop-based child reaping on an aborted `serve_on` (the abnormal-exit
    // cleanup invariant). We give it its OWN child token and hold that
    // token's `DropGuard` in this function's scope — so BOTH a normal
    // shutdown (token cancelled via the parent `ct`) AND an abort of
    // `serve_on` (locals dropped → guard dropped → token cancelled) stop
    // the task and release its `ctx` clone.
    let sighup_cancel = ct.child_token();
    let sighup_handle = spawn_sighup_reloader(ctx.clone(), sighup_cancel.clone());
    let _sighup_drop_guard = sighup_cancel.drop_guard();

    // One supervisor task per stdio upstream (HTTP transports have
    // built-in reconnect in rmcp's `StreamableHttpClientTransport`, so
    // they don't get one). Each supervisor watches its upstream's
    // health record and respawns the child process if it wedges or
    // dies. See `mcp_client::supervisor` for the state machine.
    //
    // Supervisors cancel via `supervisor_cancel`, a child of `ct`
    // created in `run()` and shared onto `ctx.supervisor_cancel` (so the
    // reload tool can mint sibling tokens for upstreams added at
    // runtime). Separate from the invalidators' token so the
    // shutdown-orderings stay clean: supervisors stop before
    // `close_all_upstreams` drains the manager so no respawn races with
    // shutdown. Each spawned supervisor gets its OWN child of this token,
    // registered in `ctx.supervisors` keyed by upstream name, so the
    // reload "removed" path can cancel exactly one upstream's supervisor.
    let mut supervisor_handles = spawn_stdio_supervisors(&ctx, supervisor_cancel.clone()).await;

    // Bootstrap-retry tasks share `supervisor_cancel` with the regular
    // stdio supervisors so they're cancelled and joined together
    // BEFORE `close_all_upstreams` drains the manager. Without
    // sharing this token, a respawn whose `connect` lands between
    // drain and the supervisor noticing cancellation would `register`
    // a phantom handle into a drained manager and leak the child
    // process. Appended into `supervisor_handles` so the existing
    // join loop awaits them too.
    supervisor_handles.extend(
        spawn_bootstrap_retry_supervisors(
            &ctx,
            failed_bootstrap_configs,
            supervisor_cancel.clone(),
        )
        .await,
    );

    // One clone captured by the factory closure; another retained for
    // the InspectingSessionManager and for post-shutdown cleanup.
    let ctx_for_factory = ctx.clone();
    let service = StreamableHttpService::new(
        move || Ok(GatewayHandler::new(ctx_for_factory.clone())),
        InspectingSessionManager::new(build_session_manager(), ctx.clone()).into(),
        StreamableHttpServerConfig::default().with_cancellation_token(ct.child_token()),
    );

    let router = axum::Router::new().nest_service("/mcp", service);

    // Bound the graceful drain. axum's `with_graceful_shutdown` stops
    // accepting new connections once `shutdown` fires, then waits for
    // EXISTING connections to close before returning. rmcp's
    // streamable-HTTP transport keeps a client's connection open across
    // requests, so a connected client (including the very one that
    // invoked `restart-gateway`) would otherwise block the drain — and
    // thus the process exit — indefinitely. The same wedge hits SIGTERM
    // and the deploy script's bootout whenever any client is attached.
    //
    // So: drive the bounded drain via a watchdog that starts counting
    // only AFTER shutdown is signalled. A healthy, un-signalled server
    // runs forever (the watchdog branch never fires); once the token is
    // cancelled, axum gets `SHUTDOWN_DRAIN_GRACE` to drain politely,
    // after which we stop waiting and fall through to the deterministic
    // upstream/invalidator cleanup regardless. Any still-open client
    // connections are dropped when the process exits.
    let drain_token = ct.clone();
    // `with_graceful_shutdown` returns a `WithGracefulShutdown` that is
    // `IntoFuture`, not `Future`, so wrap the `.await` in an async block
    // to get a pollable future for `select!`.
    let graceful = async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
    };
    let serve_result = {
        tokio::pin!(graceful);
        // Watchdog: resolves `SHUTDOWN_DRAIN_GRACE` after the token is
        // cancelled. Until cancellation it parks on `cancelled()`, so it
        // never fires during normal operation.
        let watchdog = async move {
            drain_token.cancelled().await;
            tokio::time::sleep(SHUTDOWN_DRAIN_GRACE).await;
        };
        tokio::select! {
            biased;
            res = &mut graceful => res,
            () = watchdog => {
                tracing::warn!(
                    grace_secs = SHUTDOWN_DRAIN_GRACE.as_secs(),
                    "graceful shutdown exceeded drain grace (a client likely held its \
                     connection open); proceeding with forced upstream cleanup and exit"
                );
                Ok(())
            }
        }
    };

    // Stop the supervisors BEFORE draining the upstream manager. If a
    // supervisor were still running when `close_all_upstreams` ran, it
    // could start a respawn against a manager that's about to be
    // drained — the replace would succeed but the fresh handle would
    // never be registered into any living manager view, leaking a
    // child process. Cancelling first avoids the race.
    supervisor_cancel.cancel();
    for handle in supervisor_handles {
        if let Err(e) = handle.await {
            tracing::warn!(error = %e, "upstream supervisor join error");
        }
    }

    // Deterministic upstream shutdown: drain the manager and call
    // `close()` on each handle. Order matters — axum has already drained,
    // so no in-flight handlers should be holding upstream Arcs; the
    // `try_unwrap` should succeed. If one doesn't (rare race), we log
    // and fall through to Drop-based cleanup, which still reaps the
    // child because stdin-EOF propagates on drop.
    close_all_upstreams(&ctx.upstreams).await;

    // Stop the invalidators before dropping the bus so their exit path
    // is "cancel token fired" (clean), not "last bus clone dropped"
    // (also clean, but racy with other shutdown logging). Both share
    // `invalidator_cancel` because they have the same lifecycle.
    invalidator_cancel.cancel();
    if let Err(e) = tool_invalidator_handle.await {
        tracing::warn!(error = %e, "tool list cache invalidator join error");
    }
    if let Err(e) = resource_invalidator_handle.await {
        tracing::warn!(error = %e, "resource list cache invalidator join error");
    }
    if let Err(e) = sighup_handle.await {
        tracing::warn!(error = %e, "sighup reloader join error");
    }

    // Drop the shell's `AppContext` last: this releases the final Arcs
    // to the registry, caches, bus, broker, etc. With axum drained and
    // both invalidators joined, no one else should be holding clones.
    drop(ctx);

    serve_result.context("axum serve failed")?;
    tracing::info!("gateway stopped cleanly");
    Ok(())
}

/// Mint a per-upstream supervisor cancellation token as a child of
/// `parent`, record it in `ctx.supervisors` keyed by `name`, and return
/// it. Any pre-existing token for `name` is cancelled and replaced — the
/// reload "changed" path relies on this so a respawned supervisor for an
/// edited upstream doesn't leave the old one's token orphaned in the map.
///
/// The returned token is what gets handed to the supervisor; cancelling
/// the map entry (or the `parent`) stops that supervisor's loop.
async fn register_supervisor_token(
    ctx: &AppContext,
    parent: &CancellationToken,
    name: &str,
) -> CancellationToken {
    let token = parent.child_token();
    let mut guard = ctx.supervisors.lock().await;
    if let Some(previous) = guard.insert(name.to_string(), token.clone()) {
        // Defensive: a stale token for this name means a prior supervisor
        // for it is still live. Cancel it so we never run two supervisors
        // for one upstream. At boot this never fires (names are unique);
        // on a reload "changed" respawn it cleans up the old supervisor.
        previous.cancel();
    }
    token
}

/// Spawn one [`UpstreamSupervisor`] per registered stdio upstream.
/// HTTP upstreams are skipped — rmcp's `StreamableHttpClientTransport`
/// handles its own reconnect. Returns the join handles so `serve_on`
/// can await their exit at shutdown.
///
/// Each spawned supervisor gets its own child of `parent` (registered in
/// `ctx.supervisors` by name) so the reload "removed" path can cancel a
/// single upstream's supervisor without touching the others.
///
/// If `effective_supervision_for` surfaces a cross-field validation
/// error (which parse-time validation should have caught for real
/// config files), this function logs at warn and skips spawning the
/// supervisor for that one upstream — a misconfigured supervisor knob
/// shouldn't take the whole gateway down.
pub(crate) async fn spawn_stdio_supervisors(
    ctx: &AppContext,
    parent: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    let configs = ctx.upstreams.list().await;
    spawn_supervisors_for(ctx, &parent, configs).await
}

/// Spawn a stdio supervisor for each config in `configs` that is (a)
/// stdio and (b) currently registered in the manager. Shared by boot
/// (`spawn_stdio_supervisors`, all upstreams) and reload (only the
/// freshly-`connected` upstreams from an `apply_diff`). Each gets its own
/// child of `parent`, registered in `ctx.supervisors` by name.
///
/// Passing only the connected subset at reload time is what keeps a
/// reload from needlessly cancelling+respawning supervisors for the
/// unchanged upstreams (whose tokens already live in the map).
pub(crate) async fn spawn_supervisors_for(
    ctx: &AppContext,
    parent: &CancellationToken,
    configs: Vec<UpstreamConfig>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();

    for config in configs {
        // HTTP upstreams: rmcp reconnects internally; we don't run a
        // supervisor. OAuth-HTTP is treated the same: rmcp's AuthClient
        // transparently refreshes the
        // access token, and the one failure it *can't* self-heal — a dead
        // refresh token — requires a human `login` regardless of whether a
        // supervisor is watching, so a supervisor would add churn without
        // adding recovery.
        if matches!(
            config.kind,
            UpstreamKind::Http { .. } | UpstreamKind::HttpOAuth { .. }
        ) {
            tracing::debug!(
                upstream = %config.name,
                "skipping supervisor for http/http-oauth upstream (rmcp handles reconnect)"
            );
            continue;
        }

        let settings = match ctx.settings.effective_supervision_for(&config) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    upstream = %config.name,
                    error = %e,
                    "skipping supervisor: invalid supervision settings"
                );
                continue;
            }
        };

        // Grab the handle's current health record once at spawn time.
        // If the handle is absent (extreme race: the upstream bootstrap
        // registered it and then something unregistered it between
        // `list()` and here — shouldn't happen at boot but be defensive),
        // skip spawning.
        let Some(handle) = ctx.upstreams.handle_by_name(&config.name).await else {
            tracing::warn!(
                upstream = %config.name,
                "skipping supervisor: handle disappeared between list and lookup"
            );
            continue;
        };
        let health = handle.health();
        drop(handle); // Structural invariant: the supervisor never holds Arc<UpstreamHandle>.

        let token = register_supervisor_token(ctx, parent, &config.name).await;
        let supervisor = UpstreamSupervisor::new(
            config.clone(),
            settings,
            Arc::downgrade(&ctx.upstreams),
            health,
            Arc::clone(&ctx.bus),
            ctx.broker.clone(),
            token,
            Box::new(RealConnector),
        );

        tracing::info!(
            upstream = %config.name,
            call_timeout_ms = settings.call_timeout.as_millis() as u64,
            unhealthy_threshold = settings.unhealthy_threshold,
            "spawning upstream supervisor"
        );
        handles.push(tokio::spawn(supervisor.run()));
    }

    handles
}

/// Spawn one retry task per config that failed its first connect at
/// bootstrap. Handles both transports:
///
/// - **stdio**: born-Respawning [`UpstreamSupervisor`]. First tick
///   runs `tick_respawning` → `try_respawn` immediately (no delay on
///   attempt 1, see `supervisor::tick_respawning`). On success, the
///   supervisor's atomic `register_or_replace` inserts the handle
///   into the manager (no prior handle to swap with); on failure,
///   the usual exponential-backoff-then-Dead-then-rearm cycle
///   applies.
///
/// - **HTTP**: a simple retry loop (the full supervisor state machine
///   is stdio-specific via the [`Connector`] trait, and HTTP doesn't
///   need state transitions because rmcp's
///   `StreamableHttpClientTransport` handles its own reconnect once a
///   handle exists). The loop calls `mcp_client::connect` on an
///   exponential backoff starting at
///   [`HTTP_BOOTSTRAP_RETRY_INITIAL_DELAY`] and capped at the
///   gateway-wide `dead_rearm_interval`, and registers the resulting
///   handle on success. Honors the cancellation token at every await
///   point.
///
/// Returns one [`tokio::task::JoinHandle`] per spawned retry task so
/// the caller can `await` them in shutdown alongside the regular
/// stdio supervisors.
pub(crate) async fn spawn_bootstrap_retry_supervisors(
    ctx: &AppContext,
    failed: Vec<UpstreamConfig>,
    parent: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();

    for config in failed {
        let settings = match ctx.settings.effective_supervision_for(&config) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    upstream = %config.name,
                    error = %e,
                    "skipping bootstrap-retry task: invalid supervision settings"
                );
                continue;
            }
        };

        let token = register_supervisor_token(ctx, &parent, &config.name).await;
        match config.kind {
            UpstreamKind::Stdio { .. } => {
                // Born in Respawning so the very first tick attempts a
                // fresh connect rather than waiting one
                // dead_rearm_interval.
                let health = Arc::new(UpstreamHealth::new());
                health.set_state(HealthState::Respawning);

                let supervisor = UpstreamSupervisor::new(
                    config.clone(),
                    settings,
                    Arc::downgrade(&ctx.upstreams),
                    health,
                    Arc::clone(&ctx.bus),
                    ctx.broker.clone(),
                    token,
                    Box::new(RealConnector),
                );

                tracing::info!(
                    upstream = %config.name,
                    "spawning stdio bootstrap-retry supervisor (born Respawning; \
                     will register on first successful connect)"
                );
                handles.push(tokio::spawn(supervisor.run()));
            }
            // HTTP and OAuth-HTTP share the same slow retry-until-success
            // loop. For a not-yet-logged-in OAuth upstream this is exactly
            // right: each attempt's `connect()` fast-fails with
            // OAuthLoginRequired until the operator runs `login`, after
            // which the next attempt finds persisted credentials and
            // succeeds. No fast respawn spinning, no special-casing.
            UpstreamKind::Http { .. } | UpstreamKind::HttpOAuth { .. } => {
                let upstreams = Arc::downgrade(&ctx.upstreams);
                let bus = Arc::clone(&ctx.bus);
                let broker = ctx.broker.clone();
                let retry_interval = settings.dead_rearm_interval;

                tracing::info!(
                    upstream = %config.name,
                    max_retry_interval_ms = retry_interval.as_millis() as u64,
                    "spawning http bootstrap-retry task (fast first retry, \
                     backoff doubling to dead_rearm_interval)"
                );
                handles.push(tokio::spawn(http_bootstrap_retry_loop(
                    config,
                    upstreams,
                    bus,
                    broker,
                    retry_interval,
                    settings.call_timeout,
                    token,
                )));
            }
        }
    }

    handles
}

/// First retry delay for an HTTP upstream that failed its boot connect.
/// Short on purpose: the listener binds before upstream bootstrap (see
/// the bind-first comment in `run()`), so downstream clients reconnect
/// and take their first `resources/list` snapshot within seconds of a
/// gateway restart. A transient boot-time blip (observed live: mDNS
/// resolution of a `.local` upstream hostname failing right at daemon start)
/// must heal inside that window or every reconnecting client snapshots
/// an empty catalog. Unlike stdio, an HTTP connect attempt has no side
/// effects (no forked child, no per-PID authorization prompt), so fast
/// early retries are safe; the backoff doubles to `dead_rearm_interval`
/// so a genuinely-down upstream still settles at the slow cadence.
const HTTP_BOOTSTRAP_RETRY_INITIAL_DELAY: Duration = Duration::from_secs(2);

/// Retry-until-success-or-cancel loop for an HTTP upstream whose
/// initial connect failed at bootstrap. Sleeps on an exponential
/// backoff from [`HTTP_BOOTSTRAP_RETRY_INITIAL_DELAY`] capped at
/// `retry_interval` between attempts; on success registers the handle
/// into the manager and exits. On manager-drop or token-cancellation,
/// exits without registering.
async fn http_bootstrap_retry_loop(
    config: UpstreamConfig,
    upstreams: std::sync::Weak<UpstreamManager>,
    bus: Arc<UpstreamEventBus>,
    broker: SharedSamplingBroker,
    retry_interval: Duration,
    call_timeout: Duration,
    cancel: CancellationToken,
) {
    tracing::debug!(upstream = %config.name, "http bootstrap-retry task starting");
    let mut attempt: u32 = 0;
    // One-shot guard for the human-facing re-auth notification. The loop
    // re-attempts forever (backoff settling at `retry_interval`) while an
    // OAuth upstream needs login, but we must alert the operator only ONCE
    // per loop lifetime — not on every retry tick. (The mid-life `call_tool` path
    // dedups via the health edge; this loop has no persistent health record
    // across attempts, so the flag lives here instead.)
    let mut reauth_notified = false;
    let mut delay = HTTP_BOOTSTRAP_RETRY_INITIAL_DELAY.min(retry_interval);
    loop {
        attempt = attempt.saturating_add(1);

        // Cooperative cancellation check before each attempt. Even the
        // first attempt waits (briefly): bootstrap just failed
        // milliseconds ago, and an immediate retry would almost always
        // hit the same still-broken-network window. The delay doubles
        // per failure up to `retry_interval`, so a transient boot blip
        // heals in seconds while a genuinely-down upstream settles at
        // the slow `dead_rearm_interval` cadence.
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(
                    upstream = %config.name,
                    attempt,
                    "http bootstrap-retry cancelled before next attempt"
                );
                return;
            }
            _ = tokio::time::sleep(delay) => {}
        }
        delay = delay.saturating_mul(2).min(retry_interval);

        // If the manager has been dropped (process shutdown), give up
        // — there's nothing to register into.
        let Some(manager) = upstreams.upgrade() else {
            tracing::debug!(
                upstream = %config.name,
                "http bootstrap-retry: manager dropped; exiting"
            );
            return;
        };

        match mcp_client::connect(&config, &bus, &broker).await {
            Ok(live) => {
                // Defense in depth: if cancellation fired while we
                // were inside `connect`, drop the live upstream
                // rather than registering into a manager that's
                // about to be (or already has been) drained.
                if cancel.is_cancelled() {
                    tracing::debug!(
                        upstream = %config.name,
                        "http bootstrap-retry connect succeeded but task was \
                         cancelled; dropping fresh upstream without registering"
                    );
                    drop(live);
                    return;
                }
                let handle = Arc::new(UpstreamHandle::new_with_call_timeout(
                    config.clone(),
                    live,
                    call_timeout,
                ));
                manager.register(handle).await;
                // The upstream became reachable after downstream sessions may
                // already have listed resources. Tell those sessions to
                // refresh so a first empty snapshot cannot stick forever.
                bus.publish(mcp_client::UpstreamEvent::ResourceListChanged {
                    upstream: config.name.clone(),
                });
                tracing::info!(
                    upstream = %config.name,
                    attempt,
                    "http bootstrap-retry: connect succeeded, upstream registered"
                );
                return;
            }
            Err(e) => {
                if let Some(manager) = upstreams.upgrade() {
                    manager
                        .record_connection_error(&config, &e.to_string())
                        .await;
                }
                // An actionable-precondition failure (OAuth not logged in)
                // is operator-fixable, not a transient network blip — flag
                // it distinctly so the log reads as "do this" rather than
                // "something's flapping." The retry cadence is identical;
                // only the message differs.
                if e.is_actionable_precondition() {
                    tracing::warn!(
                        upstream = %config.name,
                        attempt,
                        error = %e,
                        next_retry_ms = delay.as_millis() as u64,
                        "http bootstrap-retry: upstream needs operator action; \
                         will keep retrying until resolved"
                    );
                    // Alert the human once: this is the at-restart twin of
                    // the mid-life NeedsLogin notification. The upstream
                    // output never reaches the operator, so without this a
                    // dead-token-at-boot upstream would retry silently
                    // forever. One-shot via `reauth_notified` so the
                    // forever-retry loop doesn't spam.
                    if !reauth_notified {
                        reauth_notified = true;
                        mcp_client::reauth_notify::notify(&config.name);
                    }
                } else {
                    tracing::warn!(
                        upstream = %config.name,
                        attempt,
                        error = %e,
                        next_retry_ms = delay.as_millis() as u64,
                        "http bootstrap-retry: connect failed; will retry"
                    );
                }
            }
        }
    }
}

/// Drain the upstream manager and close each handle explicitly:
/// `UpstreamHandle::close` calls `RunningService::cancel().await`, which
/// closes the transport and
/// awaits the child's exit. Without this, shutdown relied on Drop's
/// async-fire-and-forget path — which usually works because children
/// exit on stdin EOF, but gives no guarantee and no log feedback.
async fn close_all_upstreams(manager: &UpstreamManager) {
    let handles = manager.drain().await;
    if handles.is_empty() {
        return;
    }
    tracing::info!(count = handles.len(), "closing upstreams");

    let mut closed = 0usize;
    let mut still_shared = 0usize;
    let mut failed = 0usize;

    for arc in handles {
        let name = arc.config.name.clone();
        match Arc::try_unwrap(arc) {
            Ok(handle) => match handle.close().await {
                Ok(()) => closed += 1,
                Err(e) => {
                    // A failing explicit close is specifically the
                    // "child process might be leaking" story the user
                    // wants flagged — error! so it stands out from the
                    // routine shutdown chatter.
                    failed += 1;
                    tracing::error!(
                        name = %name,
                        error = %e,
                        "upstream close failed; child process may not have reaped cleanly"
                    );
                }
            },
            Err(_still_shared) => {
                still_shared += 1;
                tracing::warn!(
                    name = %name,
                    "upstream handle still shared at shutdown; falling back to Drop-based cleanup"
                );
            }
        }
    }

    // Final summary: info when everything went clean, error when any
    // close failed (so a post-run grep for `level=error` surfaces the
    // "something leaked at shutdown" episode).
    if failed > 0 {
        tracing::error!(
            closed,
            still_shared,
            failed,
            "upstream shutdown complete with failures"
        );
    } else {
        tracing::info!(closed, still_shared, failed, "upstream shutdown complete");
    }
}

/// Connect every configured upstream and return the populated manager
/// plus the list of configs that failed to connect at boot (`run()`
/// hands those to `serve_on`, which spawns bootstrap-retry supervisors
/// for them).
///
/// Deliberately does NOT load config or bind anything: `run()` loads
/// config and binds the listener first, so that a bind failure exits
/// before this function forks any stdio child (see the bind-first
/// comment in `run()`).
async fn bootstrap_upstreams(
    bus: &UpstreamEventBus,
    broker: &SharedSamplingBroker,
    configs: Vec<UpstreamConfig>,
    settings: &config::GatewaySettings,
) -> (UpstreamManager, Vec<UpstreamConfig>) {
    let (manager, failed_configs) = connect_configs(configs, settings, |cfg| async move {
        mcp_client::connect(&cfg, bus, broker).await
    })
    .await;
    // A downstream session can initialize while the concurrent bootstrap is
    // still in flight. Publish once for every handle that is already live so
    // a client that listed too early gets a catalog refresh as soon as the
    // gateway's notification forwarder is attached.
    for config in manager.list().await {
        bus.publish(mcp_client::UpstreamEvent::ResourceListChanged {
            upstream: config.name,
        });
    }
    (manager, failed_configs)
}

/// Reject boot if any containerized upstream's key produces an
/// invalid container name. Pure validation: no side effects. See
/// `mcp_client::container_runtime::container_name_for` for the
/// character-class rules.
fn validate_container_upstream_keys(configs: &[UpstreamConfig]) -> Result<()> {
    use mcp_client::container_runtime::{container_name_for, detect_runtime_command};
    for cfg in configs {
        if detect_runtime_command(cfg).is_some() {
            container_name_for(&cfg.name).with_context(|| {
                format!(
                    "containerized upstream key {:?} cannot be used as a container name",
                    cfg.name
                )
            })?;
        }
    }
    Ok(())
}

/// Resolve the per-call deadline for one upstream's [`UpstreamHandle`].
///
/// Every handle-construction site must route through this (rather than
/// `UpstreamHandle::new`, which hardcodes the 20 s default) so the
/// `callTimeoutMs` knob — gateway-wide or per-upstream — actually
/// applies. HTTP upstreams have no supervisor, so this is the ONLY
/// place their configured timeout can take effect; stdio upstreams get
/// it here at first connect and again via the supervisor respawn path.
///
/// An invalid per-upstream override falls back to the gateway-wide
/// value with a warning instead of failing the connect: parse-time
/// validation catches real config files, and losing a working upstream
/// over a supervision knob would be a worse failure mode.
fn handle_call_timeout(
    settings: &config::GatewaySettings,
    config: &UpstreamConfig,
) -> std::time::Duration {
    match settings.effective_supervision_for(config) {
        Ok(s) => s.call_timeout,
        Err(e) => {
            tracing::warn!(
                upstream = %config.name,
                error = %e,
                "invalid supervision overrides; using gateway-wide call timeout for handle"
            );
            settings.supervision.call_timeout
        }
    }
}

/// Connect each config via `connector`, registering successes and logging
/// failures. Returns the populated manager AND the list of configs whose
/// connect failed — callers spawn a bootstrap-failed supervisor for each
/// so the gateway keeps retrying instead of silently dropping them.
///
/// One bad upstream must not take down the whole gateway: the TS reference
/// gateway is best-effort at startup, and a real config (mix of stdio
/// containers, HTTP services, auth tokens) will routinely have one or two
/// temporarily-failing entries on any given boot. The operator wants the
/// rest of the fleet up and the failing one logged, not a hard exit.
///
/// Returning failed configs alongside the manager is what enables the
/// "registered Dead via supervisor's first respawn" recovery path: at
/// boot, no `UpstreamHandle` exists for these — only after a successful
/// respawn does the supervisor register one (see
/// `UpstreamSupervisor::try_respawn`'s `UnknownName → register`
/// fallback).
///
/// The connector is injected so tests can simulate a mix of successes and
/// failures without needing real child processes or HTTP endpoints.
pub async fn connect_configs<F, Fut>(
    configs: Vec<UpstreamConfig>,
    settings: &config::GatewaySettings,
    connector: F,
) -> (UpstreamManager, Vec<UpstreamConfig>)
where
    F: Fn(UpstreamConfig) -> Fut,
    Fut: Future<Output = Result<LiveUpstream, UpstreamError>>,
{
    let manager = UpstreamManager::new();
    manager.set_configured(configs.clone()).await;
    let mut succeeded = 0usize;
    let mut failed_configs: Vec<UpstreamConfig> = Vec::new();

    // Fan out the (slow) connects concurrently, then drain the results.
    // Registration is cheap and order-independent, so we apply it
    // sequentially as each connect resolves — preserving the exact
    // best-effort success/failure bookkeeping the serial loop had.
    for (config, result) in connect_all(configs, &connector).await {
        let name = config.name.clone();
        match result {
            Ok(live) => {
                let call_timeout = handle_call_timeout(settings, &config);
                manager
                    .register(Arc::new(UpstreamHandle::new_with_call_timeout(
                        config,
                        live,
                        call_timeout,
                    )))
                    .await;
                succeeded += 1;
            }
            Err(e) => {
                // Log at warn, not error: a failed upstream is expected
                // operational noise, not a gateway-level problem.
                // `serve_on` will spawn a bootstrap-retry task for
                // each entry returned here (subject to supervision-
                // settings validation in the spawn path; a misconfigured
                // upstream may be skipped with its own log line).
                tracing::warn!(
                    name = %name,
                    error = %e,
                    "upstream failed to connect; will retry asynchronously"
                );
                manager
                    .record_connection_error(&config, &e.to_string())
                    .await;
                failed_configs.push(config);
            }
        }
    }

    tracing::info!(
        succeeded,
        failed = failed_configs.len(),
        "upstream bootstrap complete"
    );
    (manager, failed_configs)
}

/// Connect every config through `connector` with a BOUNDED concurrent
/// fan-out, returning each config paired with its connect result. This is
/// the shared connect engine for both boot ([`connect_configs`]) and
/// reload ([`apply_diff`]): connecting is the slow, independent, I/O-bound
/// part of startup, so we run up to [`BOOTSTRAP_CONNECT_CONCURRENCY`] of
/// them at once instead of summing their latencies in a serial loop.
///
/// The connector is taken by shared reference and must be `Fn` (not
/// `FnMut`) precisely because multiple of its futures are alive at once —
/// the production closure only captures `&bus`/`&broker`, so this is no
/// real restriction. Result order is unspecified (completion order); all
/// callers re-key by `config.name`, so ordering never matters.
async fn connect_all<F, Fut>(
    configs: Vec<UpstreamConfig>,
    connector: &F,
) -> Vec<(UpstreamConfig, Result<LiveUpstream, UpstreamError>)>
where
    F: Fn(UpstreamConfig) -> Fut,
    Fut: Future<Output = Result<LiveUpstream, UpstreamError>>,
{
    use futures::stream::StreamExt;

    futures::stream::iter(configs.into_iter().map(|config| {
        let name = config.name.clone();
        tracing::info!(name = %name, "bootstrapping upstream");
        let fut = connector(config.clone());
        async move { (config, fut.await) }
    }))
    .buffer_unordered(BOOTSTRAP_CONNECT_CONCURRENCY)
    .collect()
    .await
}

/// What an `apply_diff` actually did to the live upstream set. Returned
/// so the caller (the `reload-config` handler) can both report to the
/// operator and spawn supervisors for the upstreams that came online.
///
/// `connected` is the set of configs that are now freshly registered
/// (added upstreams plus the new side of changed ones) and therefore
/// need a supervisor spawned. `removed` upstreams already had their
/// supervisor cancelled and their handle closed inside `apply_diff`.
/// `failed` carries the connect errors for added/changed upstreams that
/// could not be brought up — for a "changed" failure, the OLD upstream
/// was left intact (see `apply_diff`).
#[derive(Debug, Default)]
pub struct DiffOutcome {
    /// Configs now freshly registered in the manager (added + changed
    /// successes). The caller spawns one supervisor per stdio entry.
    pub connected: Vec<UpstreamConfig>,
    /// Display names removed from the manager (handle closed, supervisor
    /// cancelled).
    pub removed: Vec<String>,
    /// `(name, error string)` for added/changed upstreams whose connect
    /// failed. Changed failures leave the prior upstream running.
    pub failed: Vec<(String, String)>,
    /// Display names of "changed" upstreams that failed to reconnect and
    /// were therefore left on their OLD config — surfaced separately so
    /// the operator understands the edit did not take effect.
    pub kept_old_on_failure: Vec<String>,
}

/// Apply a reconcile [`UpstreamDiff`] to the live upstream set, mutating
/// `manager` in place. Pure-ish imperative shell: all the I/O (connect)
/// is funnelled through the injected `connector`, so tests drive
/// success/failure without real children — exactly the pattern
/// [`connect_configs`] uses.
///
/// Ordering guarantees the operator cares about:
///   - **added**: connect, then register on success. A failed connect is
///     recorded in `failed` and the upstream is simply absent (the
///     caller may spawn a bootstrap-retry supervisor for it).
///   - **removed**: cancel the upstream's supervisor token FIRST (so an
///     in-flight respawn drops its fresh handle via the supervisor's
///     post-connect cancellation guard), then remove + close the handle.
///   - **changed**: connect the NEW config FIRST; only on success do we
///     swap it in (`replace`), close the old handle, and cancel the old
///     supervisor token. If the new connect FAILS, the old upstream is
///     left fully intact — a bad edit never costs a working upstream.
///
/// Supervisor SPAWNING for `connected` upstreams is the caller's job
/// (via [`spawn_stdio_supervisors`]-style helpers) — kept out of here so
/// this function's contract is "mutate the manager" and stays unit-
/// testable without pulling in `RealConnector`.
pub async fn apply_diff<F, Fut>(
    ctx: &AppContext,
    diff: crate::reconcile::UpstreamDiff,
    connector: F,
) -> DiffOutcome
where
    F: Fn(UpstreamConfig) -> Fut,
    Fut: Future<Output = Result<LiveUpstream, UpstreamError>>,
{
    let mut outcome = DiffOutcome::default();

    // Connect added and changed upstreams concurrently (the slow part),
    // tagging each result with whether it's an add or a swap. The manager
    // mutations below stay sequential and order-independent — they only
    // touch fast in-memory locks — so fanning out just the connects keeps
    // the connect-new-first / register-on-success guarantees intact while
    // collapsing reload latency to ~the slowest single connect.
    let added_names: std::collections::HashSet<String> =
        diff.added.iter().map(|c| c.name.clone()).collect();
    let to_connect: Vec<UpstreamConfig> = diff.added.into_iter().chain(diff.changed).collect();
    let connected = connect_all(to_connect, &connector).await;

    for (config, result) in connected {
        let name = config.name.clone();
        let is_add = added_names.contains(&name);

        match (result, is_add) {
            // ── added: register on success ──────────────────────────
            (Ok(live), true) => {
                let call_timeout = handle_call_timeout(&ctx.settings, &config);
                ctx.upstreams
                    .register(Arc::new(UpstreamHandle::new_with_call_timeout(
                        config.clone(),
                        live,
                        call_timeout,
                    )))
                    .await;
                invalidate_list_caches(ctx, &name).await;
                // A newly reachable upstream may complete after downstream
                // sessions have already listed an empty catalog. Broadcast the
                // lifecycle change so those sessions refresh their resources.
                ctx.bus
                    .publish(mcp_client::UpstreamEvent::ResourceListChanged {
                        upstream: config.name.clone(),
                    });
                outcome.connected.push(config);
            }
            (Err(e), true) => {
                ctx.upstreams
                    .record_connection_error(&config, &e.to_string())
                    .await;
                tracing::warn!(name = %name, error = %e, "reload: added upstream failed to connect");
                outcome.failed.push((name, e.to_string()));
            }

            // ── changed (connect-new-first) ─────────────────────────
            (Ok(live), false) => {
                let call_timeout = handle_call_timeout(&ctx.settings, &config);
                let new_handle = Arc::new(UpstreamHandle::new_with_call_timeout(
                    config.clone(),
                    live,
                    call_timeout,
                ));
                match ctx.upstreams.replace(&name, new_handle).await {
                    Ok(old) => {
                        // Stop the old supervisor; the caller will spawn a
                        // fresh one for the new config from `connected`.
                        cancel_supervisor_token(ctx, &name).await;
                        close_one_upstream(old).await;
                        // The caches are keyed by display name, so the swap
                        // above left them pointing at the OLD handle's tool /
                        // resource lists. Wipe them so the next caller re-
                        // fetches from the freshly connected upstream.
                        invalidate_list_caches(ctx, &name).await;
                        outcome.connected.push(config);
                    }
                    Err(e) => {
                        // The new handle couldn't be installed (e.g. the
                        // upstream vanished from the manager mid-reload).
                        // Drop it; leave whatever is there.
                        tracing::warn!(name = %name, error = %e, "reload: changed upstream swap failed");
                        outcome.failed.push((name, e.to_string()));
                    }
                }
            }
            (Err(e), false) => {
                ctx.upstreams
                    .record_connection_error(&config, &e.to_string())
                    .await;
                // Connect-new-first paid off: the old upstream is still
                // registered and supervised. Report and move on.
                tracing::warn!(
                    name = %name,
                    error = %e,
                    "reload: changed upstream failed to reconnect; keeping previous config running"
                );
                outcome.failed.push((name.clone(), e.to_string()));
                outcome.kept_old_on_failure.push(name);
            }
        }
    }

    // ── removed (cancel supervisor first, then remove + close) ──────
    for name in diff.removed {
        cancel_supervisor_token(ctx, &name).await;
        invalidate_list_caches(ctx, &name).await;
        if let Some(handle) = ctx.upstreams.remove(&name).await {
            close_one_upstream(handle).await;
            ctx.bus
                .publish(mcp_client::UpstreamEvent::ResourceListChanged {
                    upstream: name.clone(),
                });
            outcome.removed.push(name);
        } else {
            // Already gone (raced with a drain or a prior remove). Not an
            // error — the desired end state (absent) holds.
            tracing::debug!(name = %name, "reload: removed upstream already absent from manager");
            outcome.removed.push(name);
        }
    }

    outcome
}

/// Invalidate the per-upstream `tools/list` and `resources/list` caches
/// for `name`.
///
/// Both caches are keyed by upstream display name (see `ToolListCache` /
/// `ResourceListCache`) and are normally invalidated only when an upstream
/// emits a `*_list_changed` notification. The reload path swaps a changed
/// upstream's handle in place under the SAME name, so the cached lists
/// would otherwise keep serving the OLD handle's tools/resources until a
/// full gateway restart — a freshly connected child has no reason to emit
/// a change notification. Calling this on add/change/remove makes the next
/// `get` re-fetch from the live handle, so newly enabled (or removed)
/// upstream tools surface immediately after `reload-config`.
async fn invalidate_list_caches(ctx: &AppContext, name: &str) {
    ctx.tool_cache.invalidate(name).await;
    ctx.resource_cache.invalidate(name).await;
}

/// Cancel and forget the supervisor token registered for `name`, if any.
/// A no-op for HTTP upstreams (which have no supervisor entry) and for
/// names that never had one.
async fn cancel_supervisor_token(ctx: &AppContext, name: &str) {
    if let Some(token) = ctx.supervisors.lock().await.remove(name) {
        token.cancel();
    }
}

/// Close one upstream handle, logging a failed close at error (the
/// "child may have leaked" story) — the single-handle counterpart to
/// [`close_all_upstreams`]'s per-entry arm.
async fn close_one_upstream(handle: Arc<UpstreamHandle>) {
    let name = handle.config.name.clone();
    match Arc::try_unwrap(handle) {
        Ok(h) => {
            if let Err(e) = h.close().await {
                tracing::error!(
                    name = %name,
                    error = %e,
                    "reload: upstream close failed; child process may not have reaped cleanly"
                );
            }
        }
        Err(_still_shared) => {
            // A tool call is still in flight on this handle. Drop our
            // Arc; the last holder's Drop reaps the child (stdin EOF).
            tracing::warn!(
                name = %name,
                "reload: upstream handle still shared at removal; falling back to Drop-based cleanup"
            );
        }
    }
}

/// Resolve the upstream config list AND gateway settings by trying, in order:
/// 1. `$RATO_CONFIG_PATH` — if set, the file MUST exist and parse. Silent
///    fallback when the user explicitly pointed at a path would mask typos.
/// 2. Platform default path (macOS: `~/Library/Preferences/ratatoskr/config.json`).
///
/// No config file anywhere is a hard error: a gateway whose entire job
/// is fronting configured upstreams has nothing useful to do without a
/// config, and failing loudly beats booting an empty impostor.
pub fn load_upstream_configs()
-> Result<(Vec<UpstreamConfig>, config::GatewaySettings, ConfigSource)> {
    load_upstream_configs_with_env(|k| std::env::var_os(k))
}

/// Re-resolve and parse the config from the same source the gateway
/// booted from (`$RATO_CONFIG_PATH` → platform default). Used by the
/// `reload-config` tool: it needs the freshly-
/// parsed upstream list + settings to diff against the running state.
///
/// Returns the parse error as an `anyhow::Error` so the handler can
/// surface it verbatim and change NOTHING (the "invalid config leaves
/// the running config untouched" guarantee lives in the handler, which
/// only computes the diff on the `Ok` path).
pub fn reload_parse_config() -> Result<(Vec<UpstreamConfig>, config::GatewaySettings)> {
    let (upstreams, settings, _source) = load_upstream_configs()?;
    Ok((upstreams, settings))
}

/// Result of a [`reconcile_from_disk`] call: either the config didn't parse
/// (nothing changed) or it did and the diff was applied.
pub enum ReconcileResult {
    /// The config file was missing or invalid. NOTHING was changed — the
    /// running configuration is untouched. Carries the parse error string.
    ParseError(String),
    /// The config parsed and the upstream diff was applied. Carries the
    /// apply outcome plus the gateway-wide settings-change lines (which are
    /// reported but NOT hot-applied — they still require a restart).
    Applied {
        outcome: DiffOutcome,
        settings_lines: Vec<String>,
    },
}

/// Re-read the config file and reconcile the live upstream set to it,
/// applying the diff via [`apply_diff`] and spawning supervisors for the
/// freshly-connected upstreams. This is the single shared reconcile path
/// behind BOTH the `reload-config` MCP tool and the SIGHUP signal handler,
/// so the two can never drift.
///
/// Serializes against concurrent reloads via `ctx.reload_lock`: a SIGHUP
/// arriving while an MCP `reload-config` is mid-flight (or vice versa)
/// waits rather than racing the list → diff → mutate sequence.
///
/// Invalid config changes nothing (the "a typo never disturbs the running
/// gateway" guarantee). Gateway-wide settings changes are detected and
/// returned but not applied — those still require a restart.
pub async fn reconcile_from_disk(ctx: &AppContext) -> ReconcileResult {
    reconcile_from_disk_forcing(ctx, std::collections::HashSet::new()).await
}

/// [`reconcile_from_disk`] with an explicit set of upstream names to
/// force-reconnect even though their config is unchanged.
///
/// This is the mechanism behind the `reconnect-upstream` tool: some
/// upstreams capture credentials at process start (a stdio launcher
/// script that mints a session token, say) and the only way to refresh
/// them is to re-run the launcher — but their *declarative config* never
/// changes, so a plain reload classifies them `unchanged` and skips
/// them. Callers pass the display names to promote; unknown names are
/// silently ignored (see `promote_for_reconnect`), so callers should
/// validate existence first if they want a "no such upstream" error.
pub async fn reconcile_from_disk_forcing(
    ctx: &AppContext,
    force_reconnect: std::collections::HashSet<String>,
) -> ReconcileResult {
    reconcile_from_disk_inner(ctx, force_reconnect, None, reload_parse_config)
        .await
        .expect("unnamed reconciliation cannot fail name resolution")
        .1
}

/// Resolve a reconnect target from the same disk snapshot that is reconciled.
/// Validation and parsing finish before any upstream mutation, under the reload lock.
pub async fn reconnect_from_disk(
    ctx: &AppContext,
    lua_name: &str,
) -> Result<(Option<String>, ReconcileResult), String> {
    reconcile_from_disk_inner(
        ctx,
        std::collections::HashSet::new(),
        Some(lua_name),
        reload_parse_config,
    )
    .await
}

async fn reconcile_from_disk_inner(
    ctx: &AppContext,
    mut force_reconnect: std::collections::HashSet<String>,
    lua_name: Option<&str>,
    load_config: impl FnOnce() -> Result<(Vec<UpstreamConfig>, config::GatewaySettings)>,
) -> Result<(Option<String>, ReconcileResult), String> {
    // Serialize reloads: two callers (two MCP sessions, or an MCP session
    // and a SIGHUP) would otherwise race on list → diff → mutate.
    let _guard = ctx.reload_lock.lock().await;

    // Parse first. On ANY error, change nothing and report verbatim.
    let (next_upstreams, parsed_settings) = match load_config() {
        Ok(parsed) => parsed,
        Err(e) => return Ok((None, ReconcileResult::ParseError(format!("{e:#}")))),
    };

    let current = ctx.upstreams.list().await;
    // Include disconnected configured entries in removals so their retry tasks
    // are cancelled even though no live handle exists.
    let previous_configured = ctx.upstreams.configured().await;
    let target = lua_name
        .map(|name| crate::reconcile::resolve_upstream_name(name, &current, &next_upstreams))
        .transpose()?;
    if let Some(name) = &target {
        force_reconnect.insert(name.clone());
    }
    let mut diff = crate::reconcile::diff_upstreams(&current, &next_upstreams);
    for config in previous_configured {
        if !next_upstreams.iter().any(|c| c.name == config.name)
            && !diff.removed.contains(&config.name)
        {
            diff.removed.push(config.name);
        }
    }
    ctx.upstreams.set_configured(next_upstreams).await;

    // Force-reconnect any upstream sitting in NeedsLogin. Its config is
    // unchanged (login only rewrote credentials on disk, not the config),
    // so diff_upstreams classified it as `unchanged` — but the whole
    // point of the SIGHUP that `login` just sent is to reconnect it now
    // that fresh credentials exist. `promote_for_reconnect` moves it into
    // `changed` so apply_diff re-attempts it connect-new-first (safe even
    // if no login actually happened: the reconnect just fast-fails and
    // the old handle stays).
    let mut force = needs_login_upstreams(ctx).await;
    if !force.is_empty() {
        tracing::info!(
            count = force.len(),
            upstreams = ?force,
            "reload: force-reconnecting upstreams in NeedsLogin (credentials may have been refreshed)"
        );
    }
    // Union in the caller's explicit force set (`reconnect-upstream`).
    if !force_reconnect.is_empty() {
        tracing::info!(
            upstreams = ?force_reconnect,
            "reload: force-reconnecting upstreams at caller's explicit request"
        );
        force.extend(force_reconnect);
    }
    crate::reconcile::promote_for_reconnect(&mut diff, &current, &force);

    let settings_lines = crate::reconcile::settings_changes(&ctx.settings, &parsed_settings);

    let bus = Arc::clone(&ctx.bus);
    let broker = ctx.broker.clone();
    let outcome = apply_diff(ctx, diff, |cfg| {
        let bus = Arc::clone(&bus);
        let broker = broker.clone();
        async move { mcp_client::connect(&cfg, &bus, &broker).await }
    })
    .await;

    // Spawn supervisors for the freshly-connected upstreams (added +
    // changed successes). HTTP/OAuth upstreams are filtered out inside.
    let supervisor_parent = ctx.supervisor_cancel.clone();
    let _handles = spawn_supervisors_for(ctx, &supervisor_parent, outcome.connected.clone()).await;

    Ok((
        target,
        ReconcileResult::Applied {
            outcome,
            settings_lines,
        },
    ))
}

/// Collect the display names of all currently-registered upstreams whose
/// live health state is [`HealthState::NeedsLogin`].
///
/// Used by [`reconcile_from_disk`] to force-reconnect OAuth upstreams
/// whose credentials may have just been refreshed by a `login` run (the
/// trigger for the SIGHUP that drove the reload). Reads each handle's
/// `health_snapshot()` — a cheap atomic load — under no lock beyond the
/// manager's own.
async fn needs_login_upstreams(ctx: &AppContext) -> std::collections::HashSet<String> {
    let mut names = std::collections::HashSet::new();
    for config in ctx.upstreams.list().await {
        if let Some(handle) = ctx.upstreams.handle_by_name(&config.name).await
            && handle.health_snapshot().state == HealthState::NeedsLogin
        {
            names.insert(config.name);
        }
    }
    names
}

/// Testable core of `load_upstream_configs`. The `env_fn` closure replaces
/// `std::env::var_os` so tests can supply a controlled environment without
/// mutating the process's env (which is `unsafe` in Rust 2024 and
/// non-deterministic across parallel test threads).
///
/// This mirrors the injected-connector pattern that `connect_configs` already
/// uses: production calls with the real env, tests call with a closure that
/// returns controlled values.
fn load_upstream_configs_with_env(
    env_fn: impl for<'k> Fn(&'k str) -> Option<std::ffi::OsString>,
) -> Result<(Vec<UpstreamConfig>, config::GatewaySettings, ConfigSource)> {
    if let Some(path) = env_fn(CONFIG_PATH_ENV).map(PathBuf::from) {
        let parsed = read_and_parse_config(&path).with_context(|| {
            format!(
                "failed to load config from ${}={}",
                CONFIG_PATH_ENV,
                path.display()
            )
        })?;
        return Ok((parsed.upstreams, parsed.settings, ConfigSource::EnvVar));
    }

    if let Some(default_path) = platform_default_config_path_with_env(|k| env_fn(k))
        && default_path.exists()
    {
        let parsed = read_and_parse_config(&default_path)
            .with_context(|| format!("failed to load config from {}", default_path.display()))?;
        return Ok((
            parsed.upstreams,
            parsed.settings,
            ConfigSource::PlatformDefault,
        ));
    }

    anyhow::bail!(
        "no config file found: set $RATO_CONFIG_PATH, or create one at the \
         platform default (~/Library/Preferences/ratatoskr/config.json)"
    );
}

/// Read a config file from disk and parse it. Defense-in-depth: size check
/// before slurp, explicit existence error (not serde's JSON error), parse
/// errors surface as context-wrapped.
fn read_and_parse_config(path: &Path) -> Result<config::ParsedConfig> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("failed to stat config file {}", path.display()))?;
    if metadata.len() > CONFIG_MAX_BYTES {
        anyhow::bail!(
            "config file at {} is {} bytes, which exceeds the {}-byte safety limit",
            path.display(),
            metadata.len(),
            CONFIG_MAX_BYTES
        );
    }
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    let parsed = config::parse_full(&contents).context("config file contents are invalid")?;
    Ok(parsed)
}

/// Platform-default config path. macOS-only on purpose — this gateway
/// runs on exactly one developer machine and cross-platform support is
/// an explicit non-goal. Returns `None` if `$HOME` isn't set (e.g.
/// non-interactive environments); the caller then errors out with the
/// no-config-found message.
///
/// Accepts an env-fn (rather than reading `std::env` directly) so tests
/// can exercise the "`$HOME` unset" branch deterministically.
fn platform_default_config_path_with_env(
    env_fn: impl for<'k> Fn(&'k str) -> Option<std::ffi::OsString>,
) -> Option<PathBuf> {
    let home = env_fn("HOME")?;
    Some(
        PathBuf::from(home)
            .join("Library")
            .join("Preferences")
            .join("ratatoskr")
            .join("config.json"),
    )
}

/// Build the `LocalSessionManager` we hand to rmcp's `StreamableHttpService`.
///
/// Uses field-assignment rather than a struct literal because
/// `SessionConfig` and `LocalSessionManager` are both `#[non_exhaustive]`
/// (a recurring rmcp pitfall). Both structs expose their fields as
/// `pub`, so direct mutation after `Default::default()` is the documented
/// extension path.
///
/// Wider house rule for all rmcp non_exhaustive types (audit date 2026-04-23)
/// lives at the top of `crates/gateway/src/handler/mod.rs`; keep the two in
/// sync if rmcp introduces new non_exhaustive shapes we need to construct.
fn build_session_manager() -> LocalSessionManager {
    let mut session_config = SessionConfig::default();
    session_config.keep_alive = Some(SESSION_KEEP_ALIVE);

    let mut manager = LocalSessionManager::default();
    manager.session_config = session_config;
    manager
}

#[cfg(test)]
#[path = "serve_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "connection_catalog_tests.rs"]
mod connection_catalog_tests;
