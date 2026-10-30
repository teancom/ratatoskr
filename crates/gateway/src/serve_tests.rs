use super::*;
use std::ffi::OsString;

#[test]
fn session_manager_uses_extended_keep_alive() {
    // Regression guard: rmcp's built-in default is a 5-minute keep-alive,
    // which would flush a client's inspected-tools state every time the
    // user stepped away. If this test ever starts failing, the change
    // was probably reverted by a `Default::default()` creeping back in.
    let manager = build_session_manager();
    assert_eq!(
        manager.session_config.keep_alive,
        Some(SESSION_KEEP_ALIVE),
        "session keep-alive must be our extended value, not rmcp's 5-min default"
    );
}

// ── load_upstream_configs / read_and_parse_config / platform_default_config_path ──
//
// Each test drives the env-injected variants so that no test touches the
// real process environment. This avoids `unsafe` env mutation and the
// parallel-test data-race it would introduce. The pattern mirrors the
// connector injection in `connect_configs`.

/// A minimal but valid config that parses without error.
const MINIMAL_CONFIG: &str = r#"{"mcpClients": {}}"#;

#[test]
fn load_upstream_configs_env_var_points_to_nonexistent_file_is_hard_error() {
    // The env-var branch has must-exist semantics: a typo in $RATO_CONFIG_PATH
    // must surface as an error, not a silent fallback.
    let result = load_upstream_configs_with_env(|k| {
        if k == CONFIG_PATH_ENV {
            Some(OsString::from("/this/path/does/not/exist/config.json"))
        } else {
            None
        }
    });
    assert!(
        result.is_err(),
        "a nonexistent $RATO_CONFIG_PATH must produce Err, not a silent fallback"
    );
}

#[test]
fn load_upstream_configs_env_var_points_to_valid_file_yields_env_var_source() {
    // When $RATO_CONFIG_PATH is set and the file exists and parses, the
    // function must return ConfigSource::EnvVar. Any other source would
    // mean the env-var branch was silently bypassed.
    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "ratatoskr-test-valid-{}.json",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
    ));
    std::fs::write(&path, MINIMAL_CONFIG).expect("write temp config");

    let result = load_upstream_configs_with_env(|k| {
        if k == CONFIG_PATH_ENV {
            Some(OsString::from(path.as_os_str()))
        } else {
            None
        }
    });
    let _ = std::fs::remove_file(&path); // best-effort cleanup

    let (_upstreams, _settings, source) = result.expect("valid file should load");
    assert!(
        matches!(source, ConfigSource::EnvVar),
        "expected ConfigSource::EnvVar, got {source:?}"
    );
}

#[test]
fn load_upstream_configs_no_env_and_no_default_file_is_a_hard_error() {
    // When $RATO_CONFIG_PATH is unset AND $HOME points to a tempdir that
    // does not contain the platform-default config, boot must fail with
    // an actionable error — not silently start a gateway with no
    // upstreams (or, as in the spike era, a hardcoded demo upstream).
    let dir = std::env::temp_dir();
    // Inject an empty HOME so the platform-default path won't exist.
    let result = load_upstream_configs_with_env(|k| {
        if k == "HOME" {
            Some(OsString::from(dir.as_os_str()))
        } else {
            None
        }
    });

    let err = result.expect_err("missing config everywhere must be an error");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("no config file found"),
        "error must say what's missing and how to fix it; got: {msg}"
    );
}

#[test]
fn read_and_parse_config_rejects_file_exceeding_size_limit() {
    // The 1 MiB guard prevents accidental or malicious OOM when $RATO_CONFIG_PATH
    // points at a huge file. Verify the exact error path fires.
    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "ratatoskr-test-huge-{}.json",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
    ));
    // Write CONFIG_MAX_BYTES + 1 bytes. Content doesn't need to be valid
    // JSON — the size check fires before the read.
    let oversized = vec![b'x'; (CONFIG_MAX_BYTES + 1) as usize];
    std::fs::write(&path, &oversized).expect("write oversized file");

    let result = read_and_parse_config(&path);
    let _ = std::fs::remove_file(&path); // best-effort cleanup

    assert!(
        result.is_err(),
        "a file exceeding CONFIG_MAX_BYTES must be rejected"
    );
    let msg = format!(
        "{:#}",
        result.expect_err("is_err check above guarantees Err")
    );
    assert!(
        msg.contains("safety limit"),
        "error message must mention the safety limit; got: {msg}"
    );
}

#[test]
fn platform_default_config_path_returns_none_when_home_is_unset() {
    // Non-interactive environments (CI containers, launchd without HOME)
    // must not panic or return a path pointing at "/Library/..." — instead
    // the function must return None so the caller reports the
    // no-config-found error.
    let path = platform_default_config_path_with_env(|_k| None);
    assert!(
        path.is_none(),
        "expected None when HOME is unset, got {path:?}"
    );
}

// ── spawn_stdio_supervisors ──
//
// Pin the filter that skips HTTP upstreams. The supervisor is a
// stdio-only feature: HTTP transports
// handle their own reconnect inside rmcp's
// `StreamableHttpClientTransport`, so spawning a supervisor for
// them would duplicate work and risk double-respawn races.

use mcp_client::{
    SamplingBroker, SharedSamplingBroker, UpstreamConfig, UpstreamEventBus, UpstreamHandle,
    UpstreamKind, UpstreamManager, connect_with_transport,
};
use rmcp::ServerHandler;
use rmcp::ServiceExt;
use rmcp::model::{ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer};

struct FakeServer;
impl ServerHandler for FakeServer {}

/// Build a handle whose underlying transport is a duplex pair (not
/// a real subprocess / HTTP socket). We use this to construct
/// registered handles for both `Stdio` and `Http` `UpstreamKind`
/// variants without depending on npx or a network listener. The
/// concrete transport behind the `LiveUpstream` is irrelevant for
/// the spawn-filter test — only the stored `config.kind` matters.
async fn make_handle_with_kind(
    name: &str,
    kind: UpstreamKind,
) -> (
    rmcp::service::RunningService<rmcp::RoleServer, FakeServer>,
    Arc<UpstreamHandle>,
) {
    let (server_side, client_side) = tokio::io::duplex(64 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_side);
    let (cli_r, cli_w) = tokio::io::split(client_side);

    let bus = UpstreamEventBus::new();
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());

    let config = UpstreamConfig::new(name, name, kind);

    let (server_svc, live) = tokio::join!(
        FakeServer.serve((srv_r, srv_w)),
        connect_with_transport(&config, &bus, &broker, cli_r, cli_w),
    );
    let server_svc = server_svc.expect("fake server serve");
    let live = live.expect("connect_with_transport");
    let handle = Arc::new(UpstreamHandle::new(config, live));
    (server_svc, handle)
}

#[tokio::test]
async fn spawn_stdio_supervisors_skips_http_upstreams() {
    // Property: given a manager registered with one Stdio and one
    // Http upstream, `spawn_stdio_supervisors` returns exactly one
    // JoinHandle — the one for the Stdio. This pins the
    // structural filter that keeps HTTP upstreams supervisor-free.
    let (_srv_stdio, handle_stdio) = make_handle_with_kind(
        "a-stdio",
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    )
    .await;
    let (_srv_http, handle_http) = make_handle_with_kind(
        "a-http",
        UpstreamKind::Http {
            url: "http://localhost:0/mcp".into(),
            headers: Default::default(),
        },
    )
    .await;

    let manager = Arc::new(UpstreamManager::new());
    manager.register(handle_stdio).await;
    manager.register(handle_http).await;

    // Assemble a minimal AppContext. Most fields are irrelevant for
    // this test — `spawn_stdio_supervisors` only reads
    // `ctx.upstreams`, `ctx.settings`, `ctx.bus`, `ctx.broker`.
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let ctx = AppContext {
        upstreams: Arc::clone(&manager),
        bus,
        broker,
        inspection: crate::tool_inspection::ToolInspectionStore::new(),
        tool_cache: Arc::new(mcp_client::ToolListCache::new()),
        resource_cache: Arc::new(mcp_client::ResourceListCache::new()),
        result_store: Arc::new(crate::execution_result_store::ExecutionResultStore::new()),
        subscriptions: Arc::new(crate::subscription_registry::SubscriptionRegistry::new()),
        subscription_service: Arc::new(crate::subscription_service::SubscriptionService::new(
            Arc::new(crate::subscription_registry::SubscriptionRegistry::new()),
            Arc::clone(&manager),
        )),
        response_shapes: crate::response_shape_cache::ResponseShapeCache::new(),
        settings: crate::config::GatewaySettings::default(),
        lua: Arc::new(lua_runtime::LuaRuntime::new()),
        shutdown: CancellationToken::new(),
        supervisor_cancel: CancellationToken::new(),
        supervisors: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        reload_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let ct = CancellationToken::new();
    let handles = spawn_stdio_supervisors(&ctx, ct.clone()).await;

    assert_eq!(
        handles.len(),
        1,
        "exactly one supervisor must spawn — the stdio upstream's. \
         HTTP upstreams are skipped by design."
    );

    // Cancel and drain so the supervised stdio handle is released.
    ct.cancel();
    for h in handles {
        let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
    }
}

/// Build an `AppContext` wrapping `manager`, with every other field a
/// throwaway default. Only safe for tests that read `ctx.upstreams`.
fn app_context_for(manager: Arc<UpstreamManager>) -> AppContext {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    AppContext {
        upstreams: Arc::clone(&manager),
        bus,
        broker,
        inspection: crate::tool_inspection::ToolInspectionStore::new(),
        tool_cache: Arc::new(mcp_client::ToolListCache::new()),
        resource_cache: Arc::new(mcp_client::ResourceListCache::new()),
        result_store: Arc::new(crate::execution_result_store::ExecutionResultStore::new()),
        subscriptions: Arc::new(crate::subscription_registry::SubscriptionRegistry::new()),
        subscription_service: Arc::new(crate::subscription_service::SubscriptionService::new(
            Arc::new(crate::subscription_registry::SubscriptionRegistry::new()),
            Arc::clone(&manager),
        )),
        response_shapes: crate::response_shape_cache::ResponseShapeCache::new(),
        settings: crate::config::GatewaySettings::default(),
        lua: Arc::new(lua_runtime::LuaRuntime::new()),
        shutdown: CancellationToken::new(),
        supervisor_cancel: CancellationToken::new(),
        supervisors: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        reload_lock: Arc::new(tokio::sync::Mutex::new(())),
    }
}

#[tokio::test]
async fn needs_login_upstreams_collects_only_needs_login_state() {
    // Property: `needs_login_upstreams` returns exactly the registered
    // upstreams whose live health is NeedsLogin — the input to the
    // SIGHUP-reload force-reconnect. A Healthy upstream is excluded.
    let (_srv_oauth, oauth_handle) = make_handle_with_kind(
        "Datadog-Acme",
        UpstreamKind::HttpOAuth {
            url: "https://mcp.datadoghq.com/api/unstable/mcp-server/mcp".into(),
            scopes: vec![],
            client_name: None,
            callback_port: 0,
            client_id: None,
        },
    )
    .await;
    // Flip it to NeedsLogin, as `call_tool` would on an auth failure.
    oauth_handle.health().set_state(HealthState::NeedsLogin);

    let (_srv_healthy, healthy_handle) = make_handle_with_kind(
        "datadog-us",
        UpstreamKind::Http {
            url: "http://localhost:0/mcp".into(),
            headers: Default::default(),
        },
    )
    .await;

    let manager = Arc::new(UpstreamManager::new());
    manager.register(oauth_handle).await;
    manager.register(healthy_handle).await;

    let ctx = app_context_for(Arc::clone(&manager));
    let needs = needs_login_upstreams(&ctx).await;

    assert!(
        needs.contains("Datadog-Acme"),
        "the NeedsLogin OAuth upstream must be collected: {needs:?}"
    );
    assert!(
        !needs.contains("datadog-us"),
        "a Healthy upstream must not be collected: {needs:?}"
    );
    assert_eq!(needs.len(), 1);
}

// ── apply_diff ──────────────────────────────────────────────────
//
// These pin the imperative-shell contract of `apply_diff` using the
// injected-connector pattern: no real children, just controlled
// success/failure responses. The four cases the operator depends on:
// added connects and registers; added connect-failure is recorded;
// removed closes + cancels its supervisor; a changed upstream whose
// new connect fails keeps the OLD one running (connect-new-first).

fn stdio_config(name: &str, command: &str) -> UpstreamConfig {
    UpstreamConfig::new(
        name,
        name,
        UpstreamKind::Stdio {
            command: command.into(),
            args: vec![],
            env: Default::default(),
        },
    )
}

/// Build a real-but-in-process `LiveUpstream` over a duplex pair,
/// returning it alongside the running fake server (which the caller
/// must keep alive). This is the success path of a fake connector.
async fn fake_live(
    config: &UpstreamConfig,
) -> (
    rmcp::service::RunningService<rmcp::RoleServer, FakeServer>,
    LiveUpstream,
) {
    let (server_side, client_side) = tokio::io::duplex(64 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_side);
    let (cli_r, cli_w) = tokio::io::split(client_side);
    let bus = UpstreamEventBus::new();
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let (server_svc, live) = tokio::join!(
        FakeServer.serve((srv_r, srv_w)),
        connect_with_transport(config, &bus, &broker, cli_r, cli_w),
    );
    (
        server_svc.expect("fake server serve"),
        live.expect("connect_with_transport"),
    )
}

/// A fake upstream server that advertises exactly one tool, whose
/// name is fixed at construction. Used to tell an OLD handle apart
/// from a NEW one through the tool cache: warm the cache against a
/// server reporting `old_tool`, swap in one reporting `new_tool`, and
/// the next `get` reveals whether the cache was invalidated.
#[derive(Clone)]
struct OneToolServer {
    tool_name: String,
}

impl ServerHandler for OneToolServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let schema: Arc<serde_json::Map<String, serde_json::Value>> =
            Arc::new(serde_json::Map::new());
        Ok(ListToolsResult {
            tools: vec![Tool::new(self.tool_name.clone(), "test tool", schema)],
            next_cursor: None,
            meta: None,
        })
    }
}

/// Build an in-process `LiveUpstream` backed by a `OneToolServer`
/// advertising `tool_name`. Returns the running server (the caller
/// must keep it alive) alongside the live upstream. The tool-list
/// counterpart to [`fake_live`].
async fn live_with_one_tool(
    config: &UpstreamConfig,
    tool_name: &str,
) -> (
    rmcp::service::RunningService<rmcp::RoleServer, OneToolServer>,
    LiveUpstream,
) {
    let (server_side, client_side) = tokio::io::duplex(64 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_side);
    let (cli_r, cli_w) = tokio::io::split(client_side);
    let bus = UpstreamEventBus::new();
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let server = OneToolServer {
        tool_name: tool_name.to_string(),
    };
    let (server_svc, live) = tokio::join!(
        server.serve((srv_r, srv_w)),
        connect_with_transport(config, &bus, &broker, cli_r, cli_w),
    );
    (
        server_svc.expect("fake server serve"),
        live.expect("connect_with_transport"),
    )
}

/// Minimal `AppContext` for apply_diff tests: only `upstreams`,
/// `bus`, `broker`, `supervisors`, and the tokens are exercised.
fn minimal_ctx(manager: Arc<UpstreamManager>) -> AppContext {
    let subscriptions = Arc::new(crate::subscription_registry::SubscriptionRegistry::new());
    let subscription_service = Arc::new(crate::subscription_service::SubscriptionService::new(
        Arc::clone(&subscriptions),
        Arc::clone(&manager),
    ));
    AppContext {
        upstreams: manager,
        bus: Arc::new(UpstreamEventBus::new()),
        broker: Arc::new(SamplingBroker::new()),
        inspection: crate::tool_inspection::ToolInspectionStore::new(),
        tool_cache: Arc::new(mcp_client::ToolListCache::new()),
        resource_cache: Arc::new(mcp_client::ResourceListCache::new()),
        result_store: Arc::new(crate::execution_result_store::ExecutionResultStore::new()),
        subscriptions,
        subscription_service,
        response_shapes: crate::response_shape_cache::ResponseShapeCache::new(),
        settings: crate::config::GatewaySettings::default(),
        lua: Arc::new(lua_runtime::LuaRuntime::new()),
        shutdown: CancellationToken::new(),
        supervisor_cancel: CancellationToken::new(),
        supervisors: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        reload_lock: Arc::new(tokio::sync::Mutex::new(())),
    }
}

#[tokio::test]
async fn reconnect_parse_and_name_errors_do_not_mutate_registered_upstreams() {
    // Property: disk errors and invalid targets cannot apply pending removals.
    let manager = Arc::new(UpstreamManager::new());
    let (_srv, handle) = make_handle_with_kind(
        "retained",
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    )
    .await;
    manager.register(handle).await;
    let ctx = minimal_ctx(Arc::clone(&manager));
    let configured_before = manager.configured().await;
    let before = manager
        .handle_by_name("retained")
        .await
        .expect("registered");
    let (_, result) = reconcile_from_disk_inner(&ctx, Default::default(), Some("retained"), || {
        Err(anyhow::anyhow!("invalid disk config fixture"))
    })
    .await
    .expect("parse errors are reported");
    match result {
        ReconcileResult::ParseError(message) => assert_eq!(message, "invalid disk config fixture"),
        ReconcileResult::Applied { .. } => panic!("must not apply invalid disk config"),
    }
    assert_eq!(manager.configured().await, configured_before);
    let error = reconcile_from_disk_inner(&ctx, Default::default(), Some("unknown"), || {
        Ok((vec![], config::GatewaySettings::default()))
    })
    .await
    .err()
    .expect("unknown target rejected");
    assert_eq!(
        error,
        "unknown upstream server: unknown; available Lua names: retained"
    );
    assert!(Arc::ptr_eq(
        &before,
        &manager
            .handle_by_name("retained")
            .await
            .expect("still registered")
    ));
}

#[tokio::test]
async fn reconnect_configured_only_target_returns_connection_diagnostic() {
    // Property: a failed initial upstream reaches the connection path without a live handle.
    let manager = Arc::new(UpstreamManager::new());
    let ctx = minimal_ctx(Arc::clone(&manager));
    let cfg = stdio_config("failed-startup", "/nonexistent/ratatoskr-test-command");
    let (target, result) =
        reconcile_from_disk_inner(&ctx, Default::default(), Some("failed_startup"), || {
            Ok((vec![cfg], config::GatewaySettings::default()))
        })
        .await
        .expect("configured target resolves");
    assert_eq!(target.as_deref(), Some("failed-startup"));
    match result {
        ReconcileResult::Applied { outcome, .. } => {
            assert!(outcome.connected.is_empty());
            assert_eq!(outcome.failed.len(), 1);
            assert_eq!(outcome.failed[0].0, "failed-startup");
            assert!(
                !outcome.failed[0].1.is_empty(),
                "connection error is surfaced"
            );
        }
        ReconcileResult::ParseError(message) => panic!("unexpected parse error: {message}"),
    }
    assert!(manager.list().await.is_empty());
}

#[tokio::test]
async fn apply_diff_added_upstream_connects_and_registers() {
    let manager = Arc::new(UpstreamManager::new());
    let ctx = minimal_ctx(Arc::clone(&manager));

    let new_cfg = stdio_config("fresh", "/bin/fresh");
    let diff = crate::reconcile::UpstreamDiff {
        added: vec![new_cfg.clone()],
        ..Default::default()
    };

    // Keep the fake servers alive for the duration of the test via a
    // shared, owned collector the closure can clone into each call.
    let servers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let outcome = apply_diff(&ctx, diff, |cfg| {
        let servers = Arc::clone(&servers);
        async move {
            let (srv, live) = fake_live(&cfg).await;
            servers.lock().await.push(srv);
            Ok(live)
        }
    })
    .await;

    assert_eq!(
        outcome
            .connected
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["fresh"],
        "a successfully-connected addition must be reported as connected"
    );
    assert!(outcome.failed.is_empty());
    assert!(
        manager.handle_by_name("fresh").await.is_some(),
        "the added upstream must be registered in the manager"
    );
}

#[tokio::test]
async fn apply_diff_added_upstream_connect_failure_is_recorded() {
    let manager = Arc::new(UpstreamManager::new());
    let ctx = minimal_ctx(Arc::clone(&manager));

    let diff = crate::reconcile::UpstreamDiff {
        added: vec![stdio_config("broken", "/bin/broken")],
        ..Default::default()
    };

    let outcome = apply_diff(&ctx, diff, |cfg| async move {
        Err(UpstreamError::NotFound(cfg.name))
    })
    .await;

    assert!(outcome.connected.is_empty());
    assert_eq!(outcome.failed.len(), 1);
    assert_eq!(outcome.failed[0].0, "broken");
    assert!(
        manager.handle_by_name("broken").await.is_none(),
        "a failed connect must not leave a phantom registration"
    );
}

#[tokio::test]
async fn apply_diff_removed_upstream_is_closed_and_supervisor_cancelled() {
    let manager = Arc::new(UpstreamManager::new());
    let (_srv, handle) = make_handle_with_kind(
        "gone",
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    )
    .await;
    manager.register(handle).await;
    let ctx = minimal_ctx(Arc::clone(&manager));

    // Simulate a live supervisor for "gone" by registering a token.
    let sup_token = ctx.supervisor_cancel.child_token();
    ctx.supervisors
        .lock()
        .await
        .insert("gone".to_string(), sup_token.clone());

    let diff = crate::reconcile::UpstreamDiff {
        removed: vec!["gone".to_string()],
        ..Default::default()
    };
    let outcome = apply_diff(&ctx, diff, |cfg| async move {
        // The connector must NOT be called for a pure removal.
        panic!("connector must not run for removed upstream {}", cfg.name);
    })
    .await;

    assert_eq!(outcome.removed, vec!["gone".to_string()]);
    assert!(
        manager.handle_by_name("gone").await.is_none(),
        "removed upstream must be gone from the manager"
    );
    assert!(
        sup_token.is_cancelled(),
        "the removed upstream's supervisor token must be cancelled so its respawn loop stops"
    );
    assert!(
        !ctx.supervisors.lock().await.contains_key("gone"),
        "the supervisor map entry must be dropped on removal"
    );
}

#[tokio::test]
async fn apply_diff_changed_upstream_keeps_old_when_new_connect_fails() {
    // Connect-new-first: a bad edit must not cost a working upstream.
    let manager = Arc::new(UpstreamManager::new());
    let (_srv, handle) = make_handle_with_kind(
        "svc",
        UpstreamKind::Stdio {
            command: "/bin/old".into(),
            args: vec![],
            env: Default::default(),
        },
    )
    .await;
    manager.register(handle).await;
    let ctx = minimal_ctx(Arc::clone(&manager));

    // The edited config that we'll fail to connect.
    let changed = stdio_config("svc", "/bin/new");
    let diff = crate::reconcile::UpstreamDiff {
        changed: vec![changed],
        ..Default::default()
    };

    let outcome = apply_diff(&ctx, diff, |cfg| async move {
        Err(UpstreamError::NotFound(cfg.name))
    })
    .await;

    assert!(
        outcome.connected.is_empty(),
        "the failed reconnect connects nothing"
    );
    assert_eq!(outcome.failed.len(), 1);
    assert_eq!(outcome.kept_old_on_failure, vec!["svc".to_string()]);
    assert!(
        manager.handle_by_name("svc").await.is_some(),
        "the OLD upstream must still be registered after a failed reconnect"
    );
}

#[tokio::test]
async fn apply_diff_invalidates_tool_cache_for_changed_upstream() {
    // Regression: reload swaps a CHANGED upstream's handle in place
    // under the same display name. The tool cache is keyed by that
    // name, so without explicit invalidation `list-server-tools` keeps
    // serving the OLD handle's tools until a full restart (a freshly
    // connected child emits no `tools/list_changed`). This pins that
    // apply_diff wipes the cache so the new upstream's tools surface
    // right after `reload-config` — the exact bug a real reload hit.
    let manager = Arc::new(UpstreamManager::new());

    // Register an OLD upstream advertising `old_tool` and warm the cache.
    let old_cfg = stdio_config("svc", "/bin/old");
    let (_srv_old, old_live) = live_with_one_tool(&old_cfg, "old_tool").await;
    let old_handle = Arc::new(UpstreamHandle::new(old_cfg, old_live));
    manager.register(Arc::clone(&old_handle)).await;
    let ctx = minimal_ctx(Arc::clone(&manager));

    let before = ctx.tool_cache.get(&old_handle).await.expect("list old");
    assert_eq!(before.len(), 1);
    assert_eq!(
        before[0].name.as_ref(),
        "old_tool",
        "cache should be warmed with the old upstream's tool"
    );

    // Reload: the same name now resolves to an upstream advertising
    // `new_tool`. Keep the connected fake servers alive past the call.
    let changed = stdio_config("svc", "/bin/new");
    let diff = crate::reconcile::UpstreamDiff {
        changed: vec![changed],
        ..Default::default()
    };
    let servers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let outcome = apply_diff(&ctx, diff, |cfg| {
        let servers = Arc::clone(&servers);
        async move {
            let (srv, live) = live_with_one_tool(&cfg, "new_tool").await;
            servers.lock().await.push(srv);
            Ok(live)
        }
    })
    .await;
    assert_eq!(
        outcome.connected.len(),
        1,
        "the changed upstream reconnected"
    );

    // The cache must now reflect the NEW upstream. A stale entry would
    // still report `old_tool`.
    let new_handle = manager
        .handle_by_name("svc")
        .await
        .expect("svc still registered after reload");
    let after = ctx.tool_cache.get(&new_handle).await.expect("list new");
    assert_eq!(after.len(), 1);
    assert_eq!(
        after[0].name.as_ref(),
        "new_tool",
        "tool cache must be invalidated on reload; a stale entry would say old_tool"
    );
}

// ── parallel connect ────────────────────────────────────────────
//
// Boot and reload used to connect upstreams strictly serially: each
// `connect().await` had to complete before the next began, so boot
// latency was the SUM of every upstream's connect time (dominated by
// slow npx cold-starts). These pin the new behaviour: connects run
// concurrently with a bounded fan-out, so latency is ~the slowest
// single connect, while the success/failure bookkeeping is unchanged.

/// A connector that records concurrency: it bumps a shared in-flight
/// counter on entry, tracks the high-water mark, holds for a beat so
/// overlapping calls actually coexist, then decrements. `peak` lets a
/// test assert that more than one connect was in flight at once.
struct ConcurrencyProbe {
    in_flight: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}
impl ConcurrencyProbe {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            peak: std::sync::atomic::AtomicUsize::new(0),
        })
    }
    async fn enter(&self) {
        use std::sync::atomic::Ordering;
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        // Hold long enough that serial execution would never overlap,
        // but short enough to keep the test fast. Condition-based
        // waiting isn't applicable here — we are deliberately probing
        // *timing* overlap, which is exactly the "testing timing
        // behaviour" exception to the no-arbitrary-sleep rule.
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
    fn peak(&self) -> usize {
        self.peak.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_configs_runs_connects_concurrently() {
    // Property: given N configs, `connect_configs` has more than one
    // connect future in flight at the same time. A serial loop would
    // peak at exactly 1. This is the regression guard for the slow
    // sequential boot.
    let probe = ConcurrencyProbe::new();
    let configs: Vec<UpstreamConfig> = (0..4)
        .map(|i| stdio_config(&format!("u{i}"), "/bin/x"))
        .collect();

    let servers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let settings = crate::config::GatewaySettings::default();
    let (manager, failed) = connect_configs(configs, &settings, |cfg| {
        let probe = Arc::clone(&probe);
        let servers = Arc::clone(&servers);
        async move {
            probe.enter().await;
            let (srv, live) = fake_live(&cfg).await;
            servers.lock().await.push(srv);
            Ok(live)
        }
    })
    .await;

    assert!(failed.is_empty(), "all fake connects succeed");
    assert_eq!(manager.list().await.len(), 4, "every success is registered");
    assert!(
        probe.peak() > 1,
        "connects must overlap (peak in-flight {} should exceed 1); a serial loop peaks at 1",
        probe.peak()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_configs_caps_concurrency_at_the_bound() {
    // Property: the fan-out is BOUNDED. With far more configs than the
    // concurrency limit, in-flight connects never exceed the cap —
    // protecting a large roster from launching an unbounded stampede
    // of npx cold-starts at once.
    let probe = ConcurrencyProbe::new();
    let n = BOOTSTRAP_CONNECT_CONCURRENCY * 3;
    let configs: Vec<UpstreamConfig> = (0..n)
        .map(|i| stdio_config(&format!("u{i}"), "/bin/x"))
        .collect();

    let servers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let settings = crate::config::GatewaySettings::default();
    let (manager, failed) = connect_configs(configs, &settings, |cfg| {
        let probe = Arc::clone(&probe);
        let servers = Arc::clone(&servers);
        async move {
            probe.enter().await;
            let (srv, live) = fake_live(&cfg).await;
            servers.lock().await.push(srv);
            Ok(live)
        }
    })
    .await;

    assert!(failed.is_empty());
    assert_eq!(manager.list().await.len(), n);
    assert!(
        probe.peak() <= BOOTSTRAP_CONNECT_CONCURRENCY,
        "in-flight connects ({}) must never exceed the bound ({})",
        probe.peak(),
        BOOTSTRAP_CONNECT_CONCURRENCY
    );
}

#[tokio::test]
async fn connect_configs_partitions_successes_and_failures_under_concurrency() {
    // Property: regardless of completion order, every success lands in
    // the manager and every failure lands in `failed_configs`, with no
    // double-counting. Half the configs fail by name parity.
    let configs: Vec<UpstreamConfig> = (0..6)
        .map(|i| stdio_config(&format!("u{i}"), "/bin/x"))
        .collect();

    let servers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let settings = crate::config::GatewaySettings::default();
    let (manager, mut failed) = connect_configs(configs, &settings, |cfg| {
        let servers = Arc::clone(&servers);
        async move {
            // Even-indexed upstreams fail, odd succeed.
            let idx: usize = cfg
                .name
                .trim_start_matches('u')
                .parse()
                .expect("test config names are u<index>");
            if idx.is_multiple_of(2) {
                Err(UpstreamError::NotFound(cfg.name))
            } else {
                let (srv, live) = fake_live(&cfg).await;
                servers.lock().await.push(srv);
                Ok(live)
            }
        }
    })
    .await;

    let mut registered: Vec<String> = manager
        .list()
        .await
        .iter()
        .map(|c| c.name.clone())
        .collect();
    registered.sort();
    assert_eq!(registered, vec!["u1", "u3", "u5"]);

    failed.sort_by(|a, b| a.name.cmp(&b.name));
    let failed_names: Vec<&str> = failed.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(failed_names, vec!["u0", "u2", "u4"]);
}

#[tokio::test]
async fn connect_configs_applies_per_upstream_call_timeout_override() {
    // Regression: `callTimeoutMs` (gateway-wide or per-upstream) was
    // parsed and merged but never reached the handle on the boot path —
    // `connect_configs` built handles via `UpstreamHandle::new`, which
    // hardcodes the 20 s default. For HTTP upstreams (no supervisor,
    // hence no respawn path to re-apply settings) the knob was dead
    // config. This pins that the boot path threads the effective
    // supervision `call_timeout` into every registered handle.
    let slow =
        stdio_config("slow", "/bin/x").with_supervision(mcp_client::PartialSupervisionSettings {
            call_timeout_ms: Some(90_000),
            ..Default::default()
        });
    let plain = stdio_config("plain", "/bin/x");

    let servers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let settings = crate::config::GatewaySettings::default();
    let (manager, failed) = connect_configs(vec![slow, plain], &settings, |cfg| {
        let servers = Arc::clone(&servers);
        async move {
            let (srv, live) = fake_live(&cfg).await;
            servers.lock().await.push(srv);
            Ok(live)
        }
    })
    .await;
    assert!(failed.is_empty());

    let slow_handle = manager.handle_by_name("slow").await.expect("registered");
    assert_eq!(
        slow_handle.call_timeout(),
        Duration::from_secs(90),
        "per-upstream callTimeoutMs must reach the handle at boot"
    );
    let plain_handle = manager.handle_by_name("plain").await.expect("registered");
    assert_eq!(
        plain_handle.call_timeout(),
        mcp_client::DEFAULT_CALL_TIMEOUT,
        "upstreams without an override keep the default"
    );
}

#[tokio::test]
async fn apply_diff_applies_per_upstream_call_timeout_override() {
    // Same regression as the boot-path test, on the reload surface: an
    // upstream ADDED via reload-config must get its configured
    // call_timeout, not the hardcoded default. (This is exactly the
    // sequence an operator uses to give one slow HTTP upstream a longer
    // deadline without restarting the gateway.)
    let manager = Arc::new(UpstreamManager::new());
    let ctx = minimal_ctx(Arc::clone(&manager));

    let new_cfg =
        stdio_config("slow", "/bin/x").with_supervision(mcp_client::PartialSupervisionSettings {
            call_timeout_ms: Some(120_000),
            ..Default::default()
        });
    let diff = crate::reconcile::UpstreamDiff {
        added: vec![new_cfg],
        ..Default::default()
    };

    let servers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let outcome = apply_diff(&ctx, diff, |cfg| {
        let servers = Arc::clone(&servers);
        async move {
            let (srv, live) = fake_live(&cfg).await;
            servers.lock().await.push(srv);
            Ok(live)
        }
    })
    .await;
    assert_eq!(outcome.connected.len(), 1);

    let handle = manager.handle_by_name("slow").await.expect("registered");
    assert_eq!(
        handle.call_timeout(),
        Duration::from_secs(120),
        "per-upstream callTimeoutMs must reach the handle on the reload added path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_diff_connects_added_and_changed_concurrently() {
    // Property: the reload path fans out its connects too — added and
    // changed upstreams connect concurrently rather than one-at-a-time.
    // Same regression guard as boot, on the reload surface.
    let manager = Arc::new(UpstreamManager::new());

    // Pre-register two upstreams that the diff will mark "changed".
    for name in ["c0", "c1"] {
        let (_srv, handle) = make_handle_with_kind(
            name,
            UpstreamKind::Stdio {
                command: "/bin/old".into(),
                args: vec![],
                env: Default::default(),
            },
        )
        .await;
        std::mem::forget(_srv); // keep the fake server task alive for the test
        manager.register(handle).await;
    }
    let ctx = minimal_ctx(Arc::clone(&manager));

    let probe = ConcurrencyProbe::new();
    let diff = crate::reconcile::UpstreamDiff {
        added: vec![stdio_config("a0", "/bin/x"), stdio_config("a1", "/bin/x")],
        changed: vec![
            stdio_config("c0", "/bin/new"),
            stdio_config("c1", "/bin/new"),
        ],
        ..Default::default()
    };

    let servers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let outcome = apply_diff(&ctx, diff, |cfg| {
        let probe = Arc::clone(&probe);
        let servers = Arc::clone(&servers);
        async move {
            probe.enter().await;
            let (srv, live) = fake_live(&cfg).await;
            servers.lock().await.push(srv);
            Ok(live)
        }
    })
    .await;

    assert_eq!(
        outcome.connected.len(),
        4,
        "2 added + 2 changed all connected"
    );
    assert!(outcome.failed.is_empty());
    assert!(
        probe.peak() > 1,
        "reload connects must overlap (peak {} should exceed 1)",
        probe.peak()
    );
}

/// The SIGHUP loop fires its action once per signal and stops on
/// cancel. This pins the wiring `spawn_sighup_reloader` depends on
/// (install handler → run action on signal → exit on cancel) without
/// needing a config file or process env — the production action just
/// substitutes `reconcile_from_disk` for the counter bump here.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_on_sighup_fires_action_per_signal_and_stops_on_cancel() {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let fired = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();

    let fired_task = Arc::clone(&fired);
    let cancel_task = cancel.clone();
    let handle = tokio::spawn(async move {
        run_on_sighup(cancel_task, move || {
            let fired = Arc::clone(&fired_task);
            async move {
                fired.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await;
    });

    // Give the task time to install the SIGHUP handler before we raise
    // the signal — otherwise the default disposition (terminate) could
    // race the handler installation and kill the test runner.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let me = Pid::from_raw(std::process::id() as i32);
    kill(me, Signal::SIGHUP).expect("send SIGHUP to self");

    // Poll until the action observes the signal (bounded).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while fired.load(Ordering::SeqCst) < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "SIGHUP action did not fire within the budget"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Cancelling makes the loop exit; the task joins.
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("run_on_sighup task must exit promptly after cancel")
        .expect("task join");

    assert_eq!(
        fired.load(Ordering::SeqCst),
        1,
        "exactly one SIGHUP should have fired exactly one action"
    );
}
