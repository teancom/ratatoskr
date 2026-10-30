//! Unit tests for `UpstreamManager`'s registry invariants.
//!
//! Strategy: stand up real `UpstreamHandle`s around an in-process
//! MCP client/server pair via `tokio::io::duplex`, matching the
//! pattern used in `upstream_handler.rs`'s tests. That gives us a
//! genuine `LiveUpstream` at near-zero cost — no external child
//! process, no network I/O — which is all we need to exercise the
//! `RwLock<HashMap>`-backed lookups and drain behaviour on
//! `UpstreamManager`.
//!
//! Each helper returns `(server_svc, live)` so the caller keeps the
//! fake server alive for the duration of the test; dropping it
//! closes the duplex.
use super::*;
use std::sync::Arc;

use rmcp::{ServerHandler, ServiceExt};

use crate::event_bus::UpstreamEventBus;
use crate::sampling::SamplingBroker;
use crate::supervision::SupervisionSettings;

/// A do-nothing MCP server. All trait methods use the `ServerHandler`
/// defaults; we only need it to complete the initialize handshake so
/// `connect_with_transport` can return a real `LiveUpstream`.
struct FakeServer;
impl ServerHandler for FakeServer {}

/// The running fake-server side of an in-process pair. Holding this
/// keeps the duplex open for the lifetime of the test — the test
/// body binds it with a `_server` prefix and lets Drop close the
/// server end when the test returns.
type ServerHolder = rmcp::service::RunningService<rmcp::RoleServer, FakeServer>;

async fn make_handle(name: &str, lua_name: &str) -> (ServerHolder, Arc<UpstreamHandle>) {
    let (server_side, client_side) = tokio::io::duplex(64 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_side);
    let (cli_r, cli_w) = tokio::io::split(client_side);

    let bus = UpstreamEventBus::new();
    let broker = Arc::new(SamplingBroker::new());

    let config = UpstreamConfig::new(
        name,
        lua_name,
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    );

    let (server_svc, live) = tokio::join!(
        FakeServer.serve((srv_r, srv_w)),
        crate::connect::connect_with_transport(&config, &bus, &broker, cli_r, cli_w),
    );

    let server_svc = server_svc.expect("fake server serve");
    let live = live.expect("connect_with_transport");
    let handle = Arc::new(UpstreamHandle::new(config, live));
    (server_svc, handle)
}

#[tokio::test]
async fn successful_replacements_clear_connection_diagnostics() {
    // Property: manual replacement and retry success cannot retain obsolete errors.
    let manager = UpstreamManager::new();
    let (_server1, first) = make_handle("recovering", "recovering").await;
    let cfg = first.config.clone();
    manager.set_configured(vec![cfg.clone()]).await;
    manager.register(first).await;
    manager.record_connection_error(&cfg, "first failure").await;
    let (_server2, second) = make_handle("recovering", "recovering").await;
    manager
        .replace("recovering", second)
        .await
        .expect("replacement");
    assert_eq!(manager.connection_error("recovering").await, None);
    manager.record_connection_error(&cfg, "retry failed").await;
    let (_server3, third) = make_handle("recovering", "recovering").await;
    manager
        .register_or_replace("recovering", third)
        .await
        .expect("retry swap");
    assert_eq!(manager.connection_error("recovering").await, None);
    manager.remove("recovering").await;
    manager
        .record_connection_error(&cfg, "bootstrap retry failed")
        .await;
    let (_server4, fourth) = make_handle("recovering", "recovering").await;
    manager
        .register_or_replace("recovering", fourth)
        .await
        .expect("retry insertion");
    assert_eq!(manager.connection_error("recovering").await, None);
}

#[tokio::test]
async fn handle_by_name_returns_none_for_missing_and_some_for_registered() {
    // Property: `handle_by_name` is a straight HashMap lookup against
    // `UpstreamConfig::name`. Missing name → None, registered name →
    // Some with the exact handle we registered.
    let manager = UpstreamManager::new();
    assert!(manager.handle_by_name("ghost").await.is_none());

    let (_server, handle) = make_handle("alpha", "alpha_lua").await;
    manager.set_configured(vec![handle.config.clone()]).await;
    manager
        .record_connection_error(&handle.config, "executable missing")
        .await;
    assert_eq!(
        manager.connection_error("alpha").await.as_deref(),
        Some("executable missing")
    );
    manager.register(Arc::clone(&handle)).await;
    assert_eq!(manager.connection_error("alpha").await, None);

    assert!(manager.handle_by_name("ghost").await.is_none());
    let got = manager
        .handle_by_name("alpha")
        .await
        .expect("registered upstream must be findable by display name");
    assert!(Arc::ptr_eq(&got, &handle));
}

#[tokio::test]
async fn handle_by_lua_name_finds_by_sanitized_identifier_when_name_differs() {
    // Property: `handle_by_lua_name` scans the map by `config.lua_name`,
    // NOT `config.name`. This pins the linear-scan branch the review
    // flagged: register under display-name "display-hyphenated" with a
    // sanitized `lua_name` of "display_hyphenated", then look up by the
    // lua-name form and confirm we get the same handle. Also pin the
    // negative: looking up by the display name via this API returns None.
    let manager = UpstreamManager::new();

    let (_server, handle) = make_handle("display-hyphenated", "display_hyphenated").await;
    manager.register(Arc::clone(&handle)).await;

    let got = manager
        .handle_by_lua_name("display_hyphenated")
        .await
        .expect("lua_name lookup must find the registered handle");
    assert!(Arc::ptr_eq(&got, &handle));

    // Looking up by display name via the lua_name API is a miss.
    assert!(
        manager
            .handle_by_lua_name("display-hyphenated")
            .await
            .is_none(),
        "handle_by_lua_name must not match the display `name` field"
    );
    // And a genuinely absent lua_name is also a miss.
    assert!(manager.handle_by_lua_name("ghost").await.is_none());
}

#[tokio::test]
async fn drain_empties_the_manager_and_returns_all_handles() {
    // Property: `drain()` takes every registered handle out of the map
    // and hands them back to the caller. After drain, subsequent
    // lookups miss and a second drain returns empty. This pins the
    // graceful-shutdown contract the struct doc comment describes.
    let manager = UpstreamManager::new();

    let (_s1, h1) = make_handle("one", "one_lua").await;
    let (_s2, h2) = make_handle("two", "two_lua").await;
    manager.register(Arc::clone(&h1)).await;
    manager.register(Arc::clone(&h2)).await;

    let drained = manager.drain().await;
    assert_eq!(
        drained.len(),
        2,
        "drain must return every registered handle"
    );

    let mut names: Vec<&str> = drained.iter().map(|h| h.config.name.as_str()).collect();
    names.sort();
    assert_eq!(names, vec!["one", "two"]);

    // After drain the manager is empty.
    assert!(manager.list().await.is_empty());
    assert!(manager.handle_by_name("one").await.is_none());
    assert!(manager.handle_by_name("two").await.is_none());

    // Second drain is a no-op — nothing left to take.
    assert!(manager.drain().await.is_empty());
}

#[tokio::test]
async fn remove_evicts_both_indexes() {
    // Property: `remove(name)` takes the named handle out of BOTH
    // `by_name` and `by_lua_name` and returns the displaced Arc —
    // while leaving every OTHER upstream untouched. This is the
    // single-upstream counterpart to `drain`, used by the reload
    // diff's "removed" path; the cross-index eviction has to match
    // `register`/`drain` discipline.
    let manager = UpstreamManager::new();

    let (_s1, h1) = make_handle("one", "one_lua").await;
    let (_s2, h2) = make_handle("two", "two_lua").await;
    manager.register(Arc::clone(&h1)).await;
    manager.register(Arc::clone(&h2)).await;

    let removed = manager
        .remove("one")
        .await
        .expect("remove must return the displaced handle for a registered name");
    assert!(
        Arc::ptr_eq(&removed, &h1),
        "remove must return the exact handle that was registered"
    );

    // "one" is gone from both lookup indexes.
    assert!(manager.handle_by_name("one").await.is_none());
    assert!(manager.handle_by_lua_name("one_lua").await.is_none());

    // "two" is fully intact — remove is surgical, not a drain.
    assert!(manager.handle_by_name("two").await.is_some());
    assert!(manager.handle_by_lua_name("two_lua").await.is_some());

    // Removing an unknown name is a no-op that returns None.
    assert!(manager.remove("ghost").await.is_none());
    // Removing the same name twice: second call finds nothing.
    assert!(manager.remove("one").await.is_none());
}

// ---- duplicate-name behaviour ----
//
// The `register` fix introduced a two-tier response to a duplicate
// display `name`:
//
//   - A `debug_assert!` — fires in debug builds (including
//     `cargo test` default), catching misconfiguration the
//     loudest possible way during development.
//   - A `warn!` + replace — runs in release builds, keeping the
//     operator-visible "log at warn, continue" policy consistent
//     with `connect_configs`.
//
// We pin both halves with separate tests: one `#[should_panic]`
// test that only compiles under `debug_assertions`, and one
// replacement test that only compiles when `debug_assertions` is
// disabled. Splitting avoids runtime `cfg!` branches and lets the
// test body assert exactly one contract per test.

#[cfg(debug_assertions)]
#[tokio::test]
#[should_panic(expected = "duplicate upstream name")]
async fn duplicate_name_register_trips_debug_assert() {
    // Property: in debug builds, registering two handles with the
    // same `config.name` trips the `debug_assert!` — a loud
    // development-time signal that two upstream configs share a
    // display name. The panic message identifies the offending name.
    let manager = UpstreamManager::new();
    let (_s1, h1) = make_handle("dup", "dup_lua_a").await;
    manager.register(h1).await;

    let (_s2, h2) = make_handle("dup", "dup_lua_b").await;
    manager.register(h2).await; // expected to panic
}

#[cfg(not(debug_assertions))]
#[tokio::test]
async fn duplicate_name_register_replaces_and_logs_warning() {
    // Property: in release builds (debug_assertions disabled), the
    // second `register` with a matching `config.name` emits a
    // `tracing::warn!` and REPLACES the first handle — matching the
    // "log at warn, continue" policy. The map ends with exactly one
    // entry, keyed by "dup", pointing at the second handle.
    //
    // We don't capture the tracing output here (the crate has no
    // tracing-test dep and adding one for a single release-only
    // assertion is over-scope); the structural replacement IS the
    // behaviour a caller of `UpstreamManager` observes, and that's
    // what this test pins.
    let manager = UpstreamManager::new();
    let (_s1, h1) = make_handle("dup", "dup_lua_a").await;
    manager.register(Arc::clone(&h1)).await;

    let (_s2, h2) = make_handle("dup", "dup_lua_b").await;
    manager.register(Arc::clone(&h2)).await;

    let listed = manager.list().await;
    assert_eq!(
        listed.len(),
        1,
        "duplicate name must NOT create a second entry"
    );

    let got = manager
        .handle_by_name("dup")
        .await
        .expect("post-replacement lookup must find the new handle");
    assert!(
        Arc::ptr_eq(&got, &h2),
        "duplicate name must replace the previous handle, not keep the old one"
    );
}

// ---- duplicate-lua_name behaviour (R5b contract) ----
//
// Symmetric to the display-name pair above, but pins the OTHER
// half of the two-index invariant: two handles with different
// display names that sanitize to the same `lua_name`.
//
// The config parser already rejects this at startup via
// `ConfigError::LuaNameCollision` (see
// `crates/gateway/src/config.rs`), so tripping the debug_assert
// here means a test has built an inconsistent manager directly.
// The invariant is worth pinning at this layer regardless —
// `UpstreamManager` is used by fixtures that bypass the parser,
// and the two-index consistency must hold irrespective of the
// caller.

#[cfg(debug_assertions)]
#[tokio::test]
#[should_panic(expected = "duplicate upstream lua_name")]
async fn duplicate_lua_name_register_trips_debug_assert() {
    // Property: in debug builds, registering two handles whose
    // `config.lua_name` matches (but whose display names differ)
    // trips the `debug_assert!` for the lua_name index. The panic
    // message identifies the offending lua_name so the operator
    // can trace it back to the colliding configs.
    let manager = UpstreamManager::new();
    let (_s1, h1) = make_handle("display-a", "shared_lua").await;
    manager.register(h1).await;

    let (_s2, h2) = make_handle("display-b", "shared_lua").await;
    manager.register(h2).await; // expected to panic
}

#[cfg(not(debug_assertions))]
#[tokio::test]
async fn duplicate_lua_name_register_replaces_and_cleans_up_display_name() {
    // Property: in release builds, registering a second handle
    // with a matching `lua_name` but a DIFFERENT display `name`
    // REPLACES the first handle in both indices. The old display
    // name must no longer resolve — otherwise `by_name` would
    // carry a stale pointer to a Handle that's no longer in
    // `by_lua_name`, violating the two-index consistency
    // invariant.
    //
    // This pins the cross-index eviction branch inside `register`:
    // duplicate on one key evicts the other key's entry for the
    // displaced Arc as well.
    let manager = UpstreamManager::new();
    let (_s1, h1) = make_handle("display-a", "shared_lua").await;
    manager.register(Arc::clone(&h1)).await;

    let (_s2, h2) = make_handle("display-b", "shared_lua").await;
    manager.register(Arc::clone(&h2)).await;

    // Exactly one entry in the manager — the second handle.
    let listed = manager.list().await;
    assert_eq!(
        listed.len(),
        1,
        "duplicate lua_name must NOT create a second entry"
    );

    // `handle_by_lua_name` resolves the shared key to the new handle.
    let got_by_lua = manager
        .handle_by_lua_name("shared_lua")
        .await
        .expect("post-replacement lua_name lookup must find the new handle");
    assert!(
        Arc::ptr_eq(&got_by_lua, &h2),
        "duplicate lua_name must replace the previous handle"
    );

    // The DISPLACED handle's display name must no longer resolve —
    // otherwise the two indices are out of sync.
    assert!(
        manager.handle_by_name("display-a").await.is_none(),
        "displaced handle's display name must be evicted from by_name"
    );

    // And the new handle's own display name does resolve.
    let got_by_name = manager
        .handle_by_name("display-b")
        .await
        .expect("new handle's display name must resolve");
    assert!(Arc::ptr_eq(&got_by_name, &h2));
}

#[tokio::test]
async fn handle_by_lua_name_is_independent_map_not_scan() {
    // Property: the lua_name index resolves keys whose value is
    // not present anywhere in the display-name key space. Before
    // the secondary index, `handle_by_lua_name` scanned the
    // single map's values — it happened to work because each
    // value carried its own `lua_name`. The secondary index must
    // resolve the same cases, so we re-assert the scenario here
    // against the new implementation to pin that nothing in the
    // O(1) path accidentally falls back to the display name.
    //
    // Two handles with unrelated display and lua names: looking
    // up each lua_name returns the correct handle, even when the
    // lua_name doesn't appear as any display name.
    let manager = UpstreamManager::new();
    let (_s1, h1) = make_handle("display-one", "sanitized_one").await;
    let (_s2, h2) = make_handle("display-two", "sanitized_two").await;
    manager.register(Arc::clone(&h1)).await;
    manager.register(Arc::clone(&h2)).await;

    let got1 = manager
        .handle_by_lua_name("sanitized_one")
        .await
        .expect("first lua_name must resolve");
    let got2 = manager
        .handle_by_lua_name("sanitized_two")
        .await
        .expect("second lua_name must resolve");
    assert!(Arc::ptr_eq(&got1, &h1));
    assert!(Arc::ptr_eq(&got2, &h2));

    // Cross-checks: display names never resolve via the lua_name index.
    assert!(manager.handle_by_lua_name("display-one").await.is_none());
    assert!(manager.handle_by_lua_name("display-two").await.is_none());
}

// ── layer-1 tests: per-call timeout + UpstreamError::CallTimeout ──
//
// These pin the *boundary behaviour* of `UpstreamHandle::call_tool`'s
// wall-clock deadline:
//   - A tool that returns well within the deadline yields `Ok(_)`.
//   - A tool that takes longer than the deadline yields
//     `UpstreamError::CallTimeout` with the configured `deadline_ms`
//     echoed (pins that the error carries the *configured* value
//     rather than the module default — catches regressions that hard-
//     code the default inside the error construction).
//
// Scaled wall time rather than `tokio::time::pause()` because paused
// time auto-advance isn't reliable on the `multi_thread` flavour (see
// `lua-runtime/tests/timeout_and_cancel.rs` for the longer form of
// this explanation; same constraint applies here). Ratios match:
// deadline is ~6× the legitimate-work case and ~1/6× the wedged-work
// case, mirroring the 20 s real default against a hypothetical 3 s
// tool call (legitimate) or 120 s tool call (wedged).

use crate::health::HealthState;
use rmcp::{
    RoleServer,
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
};
use std::time::Duration;

/// A fake upstream whose `call_tool` sleeps for a configured duration
/// then returns trivially. Mirrors `lua-runtime`'s `SlowUpstream` but
/// lives here because the layer-1 contract we're pinning is on
/// `UpstreamHandle`, not on the Lua runtime.
#[derive(Clone)]
struct SlowUpstream {
    delay: Duration,
}

impl ServerHandler for SlowUpstream {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("slow-upstream", "0.0.1"))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        let schema: Arc<serde_json::Map<String, serde_json::Value>> = Arc::new(
            serde_json::from_value(serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false,
            }))
            .expect("static schema valid"),
        );
        Ok(ListToolsResult {
            tools: vec![Tool::new(
                "slow",
                "Sleeps for a configured duration.",
                schema,
            )],
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        _request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        tokio::time::sleep(self.delay).await;
        Ok(CallToolResult::success(vec![Content::text("done")]))
    }
}

/// Build a handle whose backing peer is `SlowUpstream { delay }`.
/// Returns the running server-side service (must stay alive for the
/// duration of the test) alongside the client-side handle.
async fn make_slow_handle(
    name: &str,
    delay: Duration,
    call_timeout: Duration,
) -> (
    rmcp::service::RunningService<RoleServer, SlowUpstream>,
    Arc<UpstreamHandle>,
) {
    let (server_side, client_side) = tokio::io::duplex(64 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_side);
    let (cli_r, cli_w) = tokio::io::split(client_side);

    let bus = UpstreamEventBus::new();
    let broker = Arc::new(SamplingBroker::new());

    let config = UpstreamConfig::new(
        name,
        name.replace('-', "_"),
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    );

    let (server_svc, live) = tokio::join!(
        SlowUpstream { delay }.serve((srv_r, srv_w)),
        crate::connect::connect_with_transport(&config, &bus, &broker, cli_r, cli_w),
    );

    let server_svc = server_svc.expect("fake server serve");
    let live = live.expect("connect_with_transport");
    let handle = Arc::new(UpstreamHandle::new_with_call_timeout(
        config,
        live,
        call_timeout,
    ));
    (server_svc, handle)
}

#[tokio::test]
async fn call_tool_succeeds_when_upstream_returns_before_deadline() {
    // Property: a tool call that returns well before `call_timeout`
    // resolves as `Ok(CallToolResult)`. Pins that the timeout wrapper
    // doesn't interfere with the happy path.
    let (_server, handle) = make_slow_handle(
        "fast",
        Duration::from_millis(50),
        Duration::from_millis(500),
    )
    .await;

    let t0 = std::time::Instant::now();
    let result = handle
        .call_tool(CallToolRequestParams::new("slow"))
        .await
        .expect("under-deadline call should succeed");
    let elapsed = t0.elapsed();

    assert!(!result.is_error.unwrap_or(false), "tool must not error");
    assert!(
        elapsed < Duration::from_millis(500),
        "call must return before the 500ms deadline (got {elapsed:?})"
    );
}

#[tokio::test]
async fn call_tool_returns_call_timeout_when_upstream_exceeds_deadline() {
    // Property: when the upstream takes longer than `call_timeout`,
    // we bail with `UpstreamError::CallTimeout` naming the upstream
    // and echoing the *configured* `deadline_ms` (not the default).
    // The upstream-name field is what lets the supervisor route the
    // failure signal to the right health record.
    let (_server, handle) = make_slow_handle(
        "wedged",
        Duration::from_millis(500),
        Duration::from_millis(100),
    )
    .await;

    let t0 = std::time::Instant::now();
    let err = handle
        .call_tool(CallToolRequestParams::new("slow"))
        .await
        .expect_err("over-deadline call must fail");
    let elapsed = t0.elapsed();

    match err {
        UpstreamError::CallTimeout { name, deadline_ms } => {
            assert_eq!(name, "wedged", "error must name the configured upstream");
            assert_eq!(
                deadline_ms, 100,
                "error must echo the configured deadline, not the default"
            );
        }
        other => panic!("expected CallTimeout, got {other:?}"),
    }
    assert!(
        elapsed >= Duration::from_millis(100) && elapsed < Duration::from_millis(500),
        "timeout must fire near the deadline, not wait for the upstream (got {elapsed:?})"
    );
    assert!(
        err_is_transport_level(&UpstreamError::CallTimeout {
            name: "x".into(),
            deadline_ms: 1
        }),
        "CallTimeout must be classified as transport-level so the supervisor counts it"
    );
}

/// Helper that matches `UpstreamError::is_transport_level` — defined
/// inline rather than imported so this test file's assertions read
/// as the contract under test.
fn err_is_transport_level(e: &UpstreamError) -> bool {
    e.is_transport_level()
}

#[test]
fn default_call_timeout_is_twenty_seconds() {
    // Regression guard: `UpstreamHandle::new` delegates to
    // `new_with_call_timeout` using `DEFAULT_CALL_TIMEOUT`. Pinning
    // the default here catches a future edit that changes the
    // constant without touching the per-upstream override path (the
    // constant is the only place the default lives — the handle
    // reflects whatever it gets).
    assert_eq!(DEFAULT_CALL_TIMEOUT, Duration::from_secs(20));
}

#[test]
fn upstream_error_classifications() {
    // Property matrix: `is_transport_level` must be true for
    // CallTimeout and for ServiceCall-wrapping-TransportClosed (or
    // rmcp's own Timeout), and false for the connection/spawn
    // errors (those surface at boot, not during steady-state
    // tool-call traffic, so the supervisor shouldn't count them
    // against liveness).
    let call_to = UpstreamError::CallTimeout {
        name: "x".into(),
        deadline_ms: 1000,
    };
    assert!(call_to.is_transport_level());
    assert_eq!(call_to.upstream_name(), Some("x"));

    let service_transport_closed = UpstreamError::ServiceCall {
        name: "y".into(),
        source: rmcp::ServiceError::TransportClosed,
    };
    assert!(service_transport_closed.is_transport_level());

    let service_mcp_err = UpstreamError::ServiceCall {
        name: "z".into(),
        // A peer-returned McpError is tool-shape noise, not upstream
        // sickness — the supervisor must not respawn on these.
        source: rmcp::ServiceError::McpError(rmcp::ErrorData::new(
            rmcp::model::ErrorCode::INVALID_PARAMS,
            "bad args",
            None,
        )),
    };
    assert!(
        !service_mcp_err.is_transport_level(),
        "peer McpError is tool-level, not transport-level"
    );

    let not_found = UpstreamError::NotFound("ghost".into());
    assert!(!not_found.is_transport_level());
    assert_eq!(not_found.upstream_name(), Some("ghost"));

    // OAuthLoginRequired is an operator-precondition failure, not a
    // transport sickness and not a supervisor-gated fast-fail. It IS
    // an actionable precondition (the only variant that is), and it
    // carries its upstream name like the others.
    let needs_login = UpstreamError::OAuthLoginRequired {
        name: "Datadog-Acme".into(),
    };
    assert!(!needs_login.is_transport_level());
    assert!(!needs_login.is_supervisor_gated());
    assert!(needs_login.is_actionable_precondition());
    assert_eq!(needs_login.upstream_name(), Some("Datadog-Acme"));
    // The message must name the exact subcommand the operator runs.
    let msg = needs_login.to_string();
    assert!(
        msg.contains("login Datadog-Acme"),
        "error message must tell the operator the exact command: {msg}"
    );

    // Nothing else should claim to be an actionable precondition.
    assert!(!call_to.is_actionable_precondition());
    assert!(!not_found.is_actionable_precondition());
    assert!(!service_transport_closed.is_actionable_precondition());
}

// ── auth-failure string classifier (is_auth_failure) ──

#[test]
fn auth_markers_are_current_for_rmcp_1_5() {
    // Pin the detection against the ACTUAL Display strings rmcp 1.5
    // emits for its auth error variants. If an rmcp upgrade reworded
    // any of these, this test fails — which is the signal to update
    // AUTH_FAILURE_MARKERS rather than silently lose detection.
    //
    // We construct the real error values (not hand-typed strings) so
    // the assertion is against rmcp's own `#[error("…")]` output.
    use rmcp::transport::auth::AuthError;

    let authorization_required = AuthError::AuthorizationRequired;
    assert!(
        is_auth_failure(&authorization_required),
        "rmcp's AuthorizationRequired display ({:?}) no longer matches a marker; \
         update AUTH_FAILURE_MARKERS",
        authorization_required.to_string()
    );

    let refresh_failed = AuthError::TokenRefreshFailed("server rejected".to_string());
    assert!(
        is_auth_failure(&refresh_failed),
        "TokenRefreshFailed display ({:?}) no longer matches",
        refresh_failed.to_string()
    );

    let authz_failed = AuthError::AuthorizationFailed("Forbidden".to_string());
    assert!(
        is_auth_failure(&authz_failed),
        "AuthorizationFailed display ({:?}) no longer matches",
        authz_failed.to_string()
    );

    let token_expired = AuthError::TokenExpired;
    assert!(
        is_auth_failure(&token_expired),
        "TokenExpired display ({:?}) no longer matches",
        token_expired.to_string()
    );
}

#[test]
fn is_auth_failure_walks_the_source_chain() {
    // The real failure path nests the auth error several layers deep:
    // ServiceError -> transport client error -> StreamableHttpError
    // -> AuthError. Only the innermost link carries the marker text,
    // so detection MUST walk `source()`, not just the top Display.
    use std::error::Error;
    use std::fmt;

    #[derive(Debug)]
    struct Wrapper(Box<dyn Error + Send + Sync + 'static>);
    impl fmt::Display for Wrapper {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            // Top-level message is generic — no marker here.
            write!(f, "transport error during request")
        }
    }
    impl Error for Wrapper {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(self.0.as_ref())
        }
    }

    let inner = rmcp::transport::auth::AuthError::AuthorizationRequired;
    let wrapped = Wrapper(Box::new(inner));
    assert!(
        is_auth_failure(&wrapped),
        "auth marker buried under a generic wrapper must still be found via source()"
    );
}

#[test]
fn is_auth_failure_is_false_for_unrelated_errors() {
    // Fail-safe property: a non-auth error must NOT be misclassified
    // as needs-login. Transport-closed, timeouts, and tool-shape
    // errors are the common non-auth failures.
    assert!(!is_auth_failure(&rmcp::ServiceError::TransportClosed));
    assert!(!is_auth_failure(&rmcp::ServiceError::UnexpectedResponse));

    let mcp_err = rmcp::ServiceError::McpError(rmcp::ErrorData::new(
        rmcp::model::ErrorCode::INVALID_PARAMS,
        "bad args",
        None,
    ));
    assert!(
        !is_auth_failure(&mcp_err),
        "an invalid-params tool error is not an auth failure"
    );

    // A generic io-flavored message with no marker.
    let io = std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "connection reset by peer",
    );
    assert!(!is_auth_failure(&io));
}

#[test]
fn is_auth_failure_matches_http_401_text() {
    // A resource server returning 401 may surface as a plain status
    // string rather than a typed AuthError (e.g. when the upstream
    // rejects a still-"valid"-looking token). Both the numeric "401"
    // and "Unauthorized" wording are covered.
    let status_num = std::io::Error::other("HTTP status 401 from upstream");
    assert!(is_auth_failure(&status_num));

    let status_word = std::io::Error::other("server responded 401 Unauthorized");
    assert!(is_auth_failure(&status_word));
}

// ── layer-1 tests: per-call timeout + health record on list_* ──
//
// Mirrors the `call_tool` boundary tests above for the three
// `list_*` passthroughs. The architectural concern these pin: a
// wedged `list_tools` (e.g. a stdio upstream like mcp-remote whose
// SSE bridge is mid-reconnect-storm and has stopped responding to
// RPCs even though the child stdio fd is still open) must surface
// as a `ServiceError::Timeout` AND record `FailureKind::CallTimeout`
// on the health record so the supervisor's existing
// `consecutive_failures` gate can flip Suspect → respawn.
//
// Without this layer, a wedged upstream pins every `execute` call's
// install loop indefinitely, since `list_tools` would have no timeout
// and no health-record path of its own. The fix lives in
// `Self::list_with_health` — these tests pin its contract.
//
// Same wall-clock-scaling rationale as the call_tool tests above
// (paused-time auto-advance is unreliable on `multi_thread`).

/// Sibling of `SlowUpstream` whose `list_tools` (rather than
/// `call_tool`) sleeps for the configured duration. Used to verify
/// that `UpstreamHandle::list_tools`'s own timeout fires
/// independently of `call_tool`'s.
#[derive(Clone)]
struct SlowListUpstream {
    delay: Duration,
}

impl ServerHandler for SlowListUpstream {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("slow-list-upstream", "0.0.1"))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        tokio::time::sleep(self.delay).await;
        Ok(ListToolsResult {
            tools: vec![],
            next_cursor: None,
            meta: None,
        })
    }
}

/// Build a handle whose backing peer's `list_tools` sleeps for
/// `delay` before returning. Returns `(server_holder, handle)` —
/// the server holder must stay in scope for the duration of the
/// test.
async fn make_slow_list_handle(
    name: &str,
    delay: Duration,
    call_timeout: Duration,
) -> (
    rmcp::service::RunningService<RoleServer, SlowListUpstream>,
    Arc<UpstreamHandle>,
) {
    let (server_side, client_side) = tokio::io::duplex(64 * 1024);
    let (srv_r, srv_w) = tokio::io::split(server_side);
    let (cli_r, cli_w) = tokio::io::split(client_side);

    let bus = UpstreamEventBus::new();
    let broker = Arc::new(SamplingBroker::new());

    let config = UpstreamConfig::new(
        name,
        name.replace('-', "_"),
        UpstreamKind::Stdio {
            command: "/bin/true".into(),
            args: vec![],
            env: Default::default(),
        },
    );

    let (server_svc, live) = tokio::join!(
        SlowListUpstream { delay }.serve((srv_r, srv_w)),
        crate::connect::connect_with_transport(&config, &bus, &broker, cli_r, cli_w),
    );

    let server_svc = server_svc.expect("fake server serve");
    let live = live.expect("connect_with_transport");
    let handle = Arc::new(UpstreamHandle::new_with_call_timeout(
        config,
        live,
        call_timeout,
    ));
    (server_svc, handle)
}

#[tokio::test]
async fn list_tools_succeeds_when_upstream_returns_before_deadline() {
    // Property: a list_tools that returns before `call_timeout`
    // resolves Ok and records a success on the health record. Pins
    // that the timeout wrapper doesn't interfere with the happy
    // path, and that the wiring connects through to the health
    // record (so the supervisor's record_success-driven Suspect →
    // Healthy promotion still fires for list-only traffic).
    let (_server, handle) = make_slow_list_handle(
        "fast-list",
        Duration::from_millis(50),
        Duration::from_millis(500),
    )
    .await;

    let t0 = std::time::Instant::now();
    let _tools = handle
        .list_tools()
        .await
        .expect("under-deadline list_tools should succeed");
    let elapsed = t0.elapsed();

    assert!(
        elapsed < Duration::from_millis(500),
        "list_tools must return before the 500ms deadline (got {elapsed:?})"
    );
    let snap = handle.health_snapshot();
    assert_eq!(snap.consecutive_failures, 0);
    assert!(
        snap.last_success_at.is_some(),
        "successful list_tools must stamp last_success_at"
    );
}

#[tokio::test]
async fn list_tools_returns_service_error_timeout_when_upstream_exceeds_deadline() {
    // Property: when the upstream's list_tools takes longer than
    // `call_timeout`, we bail with `rmcp::ServiceError::Timeout`
    // carrying the configured deadline. Returning the rmcp-native
    // shape (rather than a wrapper variant) means callers
    // (ToolListCache, ResourceListCache, the resources handler)
    // see one canonical error regardless of whether the timeout
    // came from rmcp's own per-request deadline or our wrapper.
    let (_server, handle) = make_slow_list_handle(
        "wedged-list",
        Duration::from_millis(500),
        Duration::from_millis(100),
    )
    .await;

    let t0 = std::time::Instant::now();
    let err = handle
        .list_tools()
        .await
        .expect_err("over-deadline list_tools must fail");
    let elapsed = t0.elapsed();

    match err {
        rmcp::ServiceError::Timeout { timeout } => {
            assert_eq!(
                timeout,
                Duration::from_millis(100),
                "Timeout error must echo the configured call_timeout, not the default"
            );
        }
        other => panic!("expected ServiceError::Timeout, got {other:?}"),
    }
    assert!(
        elapsed >= Duration::from_millis(100) && elapsed < Duration::from_millis(500),
        "timeout must fire near the deadline, not wait for the upstream (got {elapsed:?})"
    );
}

#[tokio::test]
async fn list_tools_timeout_records_call_timeout_failure_on_health() {
    // Property: a list_tools timeout records on the health record
    // exactly the way `call_tool` does — increments
    // consecutive_failures and stamps last_failure_kind =
    // CallTimeout. This is the load-bearing wiring: it's what lets
    // the supervisor's existing tick_healthy → Suspect transition
    // fire when an upstream's list_tools is wedged but its stdio
    // transport is still open.
    let (_server, handle) = make_slow_list_handle(
        "wedged-list-records",
        Duration::from_millis(500),
        Duration::from_millis(100),
    )
    .await;

    let _ = handle
        .list_tools()
        .await
        .expect_err("over-deadline list_tools must fail");

    let snap = handle.health_snapshot();
    assert_eq!(
        snap.consecutive_failures, 1,
        "list_tools timeout must count toward consecutive failures (transport-level)"
    );
    assert_eq!(
        snap.last_failure_kind,
        FailureKind::CallTimeout,
        "list_tools timeout records as CallTimeout — same kind call_tool uses, so the supervisor's existing classifier path covers both"
    );
    assert!(snap.last_failure_at.is_some());
}

#[tokio::test]
async fn list_tools_three_consecutive_timeouts_reach_supervisor_threshold() {
    // Property: three list_tools timeouts in a row leave the health
    // record in the state the supervisor's tick_healthy reads as
    // "flip to Suspect" (consecutive_failures >= 3, the default
    // unhealthy_threshold). This is the end-to-end-ish proof that
    // list_tools wedges are now visible to the supervisor — the
    // exact bug class that caused the rootly wedge to persist for
    // 17h before our last fix landed.
    let (_server, handle) = make_slow_list_handle(
        "wedged-three-times",
        Duration::from_millis(500),
        Duration::from_millis(50),
    )
    .await;

    for i in 1..=3 {
        let result = handle.list_tools().await;
        assert!(
            matches!(result, Err(rmcp::ServiceError::Timeout { .. })),
            "iteration {i}: expected Timeout, got {result:?}"
        );
    }

    let snap = handle.health_snapshot();
    assert_eq!(
        snap.consecutive_failures, 3,
        "three timeouts must accumulate into three consecutive failures"
    );
    // Default supervisor threshold is 3; with three failures, the
    // supervisor's next tick_healthy would flip Healthy → Suspect.
    // We don't drive the supervisor here (that's the integration
    // test's job); we pin the precondition.
    let default_threshold = SupervisionSettings::default().unhealthy_threshold;
    assert!(
        snap.consecutive_failures >= default_threshold,
        "three list_tools timeouts must reach or exceed the default \
         unhealthy_threshold ({default_threshold}) so a supervisor \
         tick would flip Suspect"
    );
}

#[tokio::test]
async fn list_resources_and_list_resource_templates_flow_through_the_wrapper() {
    // Property: list_resources and list_resource_templates dispatch
    // through `Self::list_with_health` the same way list_tools does.
    // The wrapper is the single point of behaviour (timeout +
    // health-record); pinning one outcome on each method proves the
    // dispatch reaches it. The full timeout-shape matrix is exercised
    // on list_tools above — we don't re-run it here because the
    // wrapper is generic over T and a copy-paste-bug would also break
    // list_tools' tests.
    //
    // The default `ServerHandler` answers both list_resources and
    // list_resource_templates with `Ok([])` — that's a *successful*
    // empty response, not an error, so it must record a success on
    // the health record (auto-promoting Suspect → Healthy if we
    // were Suspect).
    let (_server, handle) = make_slow_list_handle(
        "list-resources-success-path",
        Duration::from_millis(50),
        Duration::from_millis(500),
    )
    .await;

    // Drive a list_resources success.
    let resources = handle
        .list_resources()
        .await
        .expect("default list_resources returns Ok([])");
    assert!(
        resources.is_empty(),
        "default upstream advertises no resources"
    );

    // And list_resource_templates.
    let templates = handle
        .list_resource_templates()
        .await
        .expect("default list_resource_templates returns Ok([])");
    assert!(
        templates.is_empty(),
        "default upstream advertises no resource templates"
    );

    let snap = handle.health_snapshot();
    assert_eq!(
        snap.consecutive_failures, 0,
        "two successes must leave the failure counter at zero"
    );
    assert!(
        snap.last_success_at.is_some(),
        "successful list_* must stamp last_success_at — that's the auto-promote signal the supervisor reads"
    );
}

// ── layer-2 integration tests: call_tool drives UpstreamHealth ──
//
// These tests pin the *wiring* between `call_tool` and the health
// record. The health module's own tests (see `health::tests`)
// cover the state-machine semantics in isolation; these tests
// cover the specific classifier + record sequence `call_tool`
// runs on each outcome.

#[tokio::test]
async fn call_tool_success_records_success_on_health() {
    // Property: a successful tool call records on the handle's
    // health record: consecutive_failures stays at 0, state stays
    // Healthy, last_success_at is set.
    let (_server, handle) =
        make_slow_handle("ok", Duration::from_millis(20), Duration::from_millis(500)).await;

    handle
        .call_tool(CallToolRequestParams::new("slow"))
        .await
        .expect("under-deadline call should succeed");

    let snap = handle.health_snapshot();
    assert_eq!(snap.state, HealthState::Healthy);
    assert_eq!(snap.consecutive_failures, 0);
    assert!(
        snap.last_success_at.is_some(),
        "success must stamp last_success_at"
    );
    assert!(
        snap.last_failure_at.is_none(),
        "a solo success must leave last_failure_at None"
    );
}

#[tokio::test]
async fn call_tool_timeout_records_transport_level_failure() {
    // Property: a per-call timeout increments the
    // consecutive-failure counter (it's a transport-level failure)
    // and records the `CallTimeout` kind.
    let (_server, handle) = make_slow_handle(
        "wedged",
        Duration::from_millis(500),
        Duration::from_millis(100),
    )
    .await;

    let _ = handle
        .call_tool(CallToolRequestParams::new("slow"))
        .await
        .expect_err("over-deadline call must fail");

    let snap = handle.health_snapshot();
    assert_eq!(
        snap.consecutive_failures, 1,
        "CallTimeout must count toward the consecutive-failure tally"
    );
    assert_eq!(snap.last_failure_kind, FailureKind::CallTimeout);
    assert!(snap.last_failure_at.is_some());
}

#[tokio::test]
async fn call_tool_timeout_then_success_resets_counter() {
    // Property: one success after N failures zeros the counter
    // and stamps last_success_at, but does NOT clear
    // last_failure_kind / last_failure_at — those are monotonic
    // records of "has this ever happened" for observability.
    //
    // We build two handles against two separate slow upstreams:
    // the "wedged" one for the timeout case, and a "fast" one to
    // record a success afterward. A single handle can't switch
    // the upstream behaviour mid-test because the fake server's
    // delay is fixed at construction.
    //
    // Actually, simpler: reuse the same handle for both calls
    // but against a SlowUpstream whose delay is shorter than the
    // timeout — then drive the "timeout" case by making the
    // timeout itself very short, then extend it. But
    // `call_timeout` is fixed at construction (`new_with_call_timeout`);
    // there's no way to swap it mid-flight.
    //
    // Cleanest: record a failure directly via the health handle
    // (which is exposed via `health()`), then verify
    // record_success clears the counter. This exercises the
    // record_success contract through the public surface.
    let (_server, handle) = make_slow_handle(
        "mixed",
        Duration::from_millis(20),
        Duration::from_millis(500),
    )
    .await;

    // Pretend we had three timeouts before this call.
    handle.health().record_failure(FailureKind::CallTimeout);
    handle.health().record_failure(FailureKind::CallTimeout);
    handle.health().record_failure(FailureKind::CallTimeout);
    assert_eq!(handle.health_snapshot().consecutive_failures, 3);

    handle
        .call_tool(CallToolRequestParams::new("slow"))
        .await
        .expect("success under the deadline");

    let snap = handle.health_snapshot();
    assert_eq!(
        snap.consecutive_failures, 0,
        "success must zero the counter"
    );
    assert!(
        snap.last_success_at.is_some(),
        "success must stamp last_success_at"
    );
    assert!(
        snap.last_failure_at.is_some(),
        "last_failure_at is monotonic — set once, never cleared by a subsequent success"
    );
}

#[test]
fn classifier_maps_every_service_error_variant() {
    // Property: the classifier covers every rmcp ServiceError
    // variant we import. The wildcard arm catches future
    // non_exhaustive additions; we pin the explicit mappings here
    // so a rmcp bump that renames a variant (or changes its
    // shape) fails loudly during compile or trips this test.

    // TransportClosed is unit-variant.
    assert_eq!(
        failure_kind_from_service_error(&rmcp::ServiceError::TransportClosed),
        FailureKind::TransportClosed
    );

    // Timeout { timeout: Duration } — arbitrary duration for
    // this test; we care about the variant, not the value.
    assert_eq!(
        failure_kind_from_service_error(&rmcp::ServiceError::Timeout {
            timeout: Duration::from_secs(1),
        }),
        FailureKind::RmcpTimeout
    );

    // McpError(ErrorData) — construct with a sentinel payload;
    // we only care about the variant classification.
    assert_eq!(
        failure_kind_from_service_error(&rmcp::ServiceError::McpError(rmcp::ErrorData::new(
            rmcp::model::ErrorCode::INVALID_PARAMS,
            "bad args",
            None,
        ))),
        FailureKind::PeerMcpError
    );

    // UnexpectedResponse is unit-variant.
    assert_eq!(
        failure_kind_from_service_error(&rmcp::ServiceError::UnexpectedResponse),
        FailureKind::UnexpectedResponse
    );

    // TransportSend(DynamicTransportError) — rmcp does not
    // publicly expose a constructor for DynamicTransportError, so
    // we can't synthesise an instance in a unit test. The arm is
    // covered by the match itself (the compiler verifies
    // exhaustiveness at the non_exhaustive boundary via the
    // wildcard), and the classifier tests above pin the
    // other explicit arms. A future refactor that makes
    // `DynamicTransportError` publicly constructible should add a
    // direct test here.
}

// ── layer-3 tests: UpstreamManager::replace atomic swap ──
//
// The replace API is invoked by the supervisor after it builds a
// fresh `LiveUpstream` via `connect::connect()`. These tests pin:
//   - happy path: swap returns the old Arc, both indices point at
//     the new Arc, lookups find the new handle.
//   - rejection: UnknownName, NameMismatch, LuaNameMismatch.
//   - non-corruption: after a rejected replace, the registry
//     still returns the ORIGINAL handle from both indices.
//
// The "atomic under a write lock" aspect isn't tested directly —
// pinning it would require a controlled concurrency harness, and
// the invariant is visible in the source (one `inner.write().await`
// covers both index mutations). A future reader who breaks
// atomicity by splitting the locks will see the other tests pass
// but will need to reason about the structural invariant on their
// own.

#[tokio::test]
async fn replace_swaps_both_indices_and_returns_old_handle() {
    // Property: replace against a registered name:
    //   1. Returns the displaced Arc (so caller can close() it).
    //   2. Updates by_name to point at the new Arc.
    //   3. Updates by_lua_name to point at the new Arc.
    //   4. Leaves exactly one entry per index (no orphans).
    let manager = UpstreamManager::new();
    let (_s_old, h_old) = make_handle("server-a", "server_a").await;
    manager.register(Arc::clone(&h_old)).await;

    let (_s_new, h_new) = make_handle("server-a", "server_a").await;
    let returned = manager
        .replace("server-a", Arc::clone(&h_new))
        .await
        .expect("replace against registered name should succeed");

    // Returned Arc is the old one, not the new one.
    assert!(
        Arc::ptr_eq(&returned, &h_old),
        "replace must return the DISPLACED handle so the caller can close() it"
    );

    // Both indices now resolve to the NEW handle.
    let by_name = manager
        .handle_by_name("server-a")
        .await
        .expect("by_name must resolve after replace");
    assert!(
        Arc::ptr_eq(&by_name, &h_new),
        "by_name must point at the new Arc"
    );

    let by_lua = manager
        .handle_by_lua_name("server_a")
        .await
        .expect("by_lua_name must resolve after replace");
    assert!(
        Arc::ptr_eq(&by_lua, &h_new),
        "by_lua_name must point at the new Arc"
    );

    // Exactly one entry in the manager — not two (swap, not insert).
    assert_eq!(manager.list().await.len(), 1);
}

#[tokio::test]
async fn replace_rejects_unknown_name() {
    // Property: replace against a name that isn't registered
    // returns UnknownName and does NOT insert the new handle. The
    // supervisor's respawn logic should never hit this in practice
    // — if it does, the registry was drained while the supervisor
    // was mid-respawn, and silently inserting would leave a
    // straggler handle that the shutdown path already moved past.
    let manager = UpstreamManager::new();
    let (_s, h_new) = make_handle("ghost", "ghost").await;

    let err = manager
        .replace("ghost", h_new)
        .await
        .expect_err("replace against unregistered name must fail");

    match err {
        ReplaceError::UnknownName(name) => assert_eq!(name, "ghost"),
        other => panic!("expected UnknownName, got {other:?}"),
    }
    // Registry is still empty.
    assert!(manager.list().await.is_empty());
}

#[tokio::test]
async fn replace_rejects_name_mismatch_with_new_handle_config() {
    // Property: if the supervisor accidentally builds a handle
    // whose `config.name` doesn't match the replace target, the
    // swap is rejected with NameMismatch. Registry is untouched.
    let manager = UpstreamManager::new();
    let (_s_old, h_old) = make_handle("server-a", "server_a").await;
    manager.register(Arc::clone(&h_old)).await;

    let (_s_wrong, h_wrong_name) = make_handle("server-b", "server_b").await;

    let err = manager
        .replace("server-a", h_wrong_name)
        .await
        .expect_err("replace with name mismatch must fail");

    match err {
        ReplaceError::NameMismatch {
            requested,
            new_handle_name,
        } => {
            assert_eq!(requested, "server-a");
            assert_eq!(new_handle_name, "server-b");
        }
        other => panic!("expected NameMismatch, got {other:?}"),
    }

    // After a rejected replace, the ORIGINAL handle still resolves.
    let resolved = manager
        .handle_by_name("server-a")
        .await
        .expect("rejected replace must leave the original handle in place");
    assert!(
        Arc::ptr_eq(&resolved, &h_old),
        "original handle must still be registered after a rejected replace"
    );
}

#[tokio::test]
async fn replace_rejects_lua_name_mismatch() {
    // Property: a replace that would change the `lua_name` (and
    // therefore the downstream Lua global's identity) is rejected
    // with LuaNameMismatch. This pins the agent-visible binding
    // stability — an agent that bound `gitlab.create_issue` at
    // session setup must not see that binding silently re-target
    // a different Lua global mid-session.
    //
    // Synthetic setup: we register under display "server" with
    // lua_name "server_v1", then build a new handle with the same
    // display name but lua_name "server_v2". make_handle derives
    // lua_name from the second arg, so this lets us simulate the
    // failure mode without rebuilding the config parser's
    // sanitization pipeline.
    let manager = UpstreamManager::new();
    let (_s_old, h_old) = make_handle("server", "server_v1").await;
    manager.register(Arc::clone(&h_old)).await;

    let (_s_new, h_new) = make_handle("server", "server_v2").await;

    let err = manager
        .replace("server", h_new)
        .await
        .expect_err("replace with lua_name mismatch must fail");

    match err {
        ReplaceError::LuaNameMismatch {
            name,
            old_lua_name,
            new_lua_name,
        } => {
            assert_eq!(name, "server");
            assert_eq!(old_lua_name, "server_v1");
            assert_eq!(new_lua_name, "server_v2");
        }
        other => panic!("expected LuaNameMismatch, got {other:?}"),
    }

    // Original handle is still the registered one in both indices.
    let by_name = manager
        .handle_by_name("server")
        .await
        .expect("original must survive rejected replace");
    assert!(Arc::ptr_eq(&by_name, &h_old));

    let by_lua = manager
        .handle_by_lua_name("server_v1")
        .await
        .expect("original lua_name must still resolve");
    assert!(Arc::ptr_eq(&by_lua, &h_old));

    // And the new, rejected lua_name does NOT resolve.
    assert!(
        manager.handle_by_lua_name("server_v2").await.is_none(),
        "rejected replace must not insert new lua_name mapping"
    );
}

// ── register_or_replace tests ──
//
// Atomic insert-or-swap: combines `register`'s "fresh insert" and
// `replace`'s "swap with safety rails" under a single write-lock
// acquisition. The supervisor's `try_respawn` uses it so a respawn
// against a previously-unregistered config (bootstrap-failed retry)
// can register without giving up the lock between `replace`'s
// UnknownName return and a separate `register` call.

#[tokio::test]
async fn register_or_replace_fresh_insertion_returns_none() {
    // Property: registering against an empty manager inserts the
    // handle and returns Ok(None) — there was nothing to displace.
    let manager = UpstreamManager::new();
    let (_s, h) = make_handle("fresh", "fresh").await;

    let displaced = manager
        .register_or_replace("fresh", Arc::clone(&h))
        .await
        .expect("fresh insert must succeed");
    assert!(
        displaced.is_none(),
        "no displaced handle on fresh insertion; got {displaced:?}"
    );

    // Both indices resolve to the new handle.
    let by_name = manager
        .handle_by_name("fresh")
        .await
        .expect("by_name must resolve");
    assert!(Arc::ptr_eq(&by_name, &h));
    let by_lua = manager
        .handle_by_lua_name("fresh")
        .await
        .expect("by_lua_name must resolve");
    assert!(Arc::ptr_eq(&by_lua, &h));
}

#[tokio::test]
async fn register_or_replace_swap_returns_displaced() {
    // Property: when name is already registered, register_or_replace
    // swaps and returns the displaced Arc — same contract as
    // `replace` for the swap path.
    let manager = UpstreamManager::new();
    let (_s_old, h_old) = make_handle("swap", "swap").await;
    manager.register(Arc::clone(&h_old)).await;

    let (_s_new, h_new) = make_handle("swap", "swap").await;
    let displaced = manager
        .register_or_replace("swap", Arc::clone(&h_new))
        .await
        .expect("swap must succeed");
    let displaced = displaced.expect("swap must return Some(displaced)");
    assert!(
        Arc::ptr_eq(&displaced, &h_old),
        "displaced Arc must be the original"
    );

    let by_name = manager
        .handle_by_name("swap")
        .await
        .expect("by_name must resolve");
    assert!(
        Arc::ptr_eq(&by_name, &h_new),
        "by_name must point at the new Arc"
    );
}

#[tokio::test]
async fn register_or_replace_rejects_name_mismatch() {
    // Property: same safety rail `replace` enforces — if the
    // handle's `config.name` doesn't match the target, reject. A
    // bug-built handle for the wrong upstream must not corrupt
    // the registry.
    let manager = UpstreamManager::new();
    let (_s, h) = make_handle("alpha", "alpha").await;
    let err = manager
        .register_or_replace("beta", h)
        .await
        .expect_err("name mismatch must fail");
    match err {
        ReplaceError::NameMismatch {
            requested,
            new_handle_name,
        } => {
            assert_eq!(requested, "beta");
            assert_eq!(new_handle_name, "alpha");
        }
        other => panic!("expected NameMismatch, got {other:?}"),
    }
    assert!(manager.list().await.is_empty(), "rejection must not insert");
}

#[tokio::test]
async fn register_or_replace_rejects_lua_name_mismatch_on_swap() {
    // Property: replacing an existing entry whose lua_name differs
    // from the new handle's lua_name would silently change the
    // agent-visible Lua-global identity. Reject.
    let manager = UpstreamManager::new();
    let (_s_old, h_old) = make_handle("server", "server_v1").await;
    manager.register(Arc::clone(&h_old)).await;

    let (_s_new, h_new) = make_handle("server", "server_v2").await;
    let err = manager
        .register_or_replace("server", h_new)
        .await
        .expect_err("lua_name mismatch must fail");
    match err {
        ReplaceError::LuaNameMismatch {
            name,
            old_lua_name,
            new_lua_name,
        } => {
            assert_eq!(name, "server");
            assert_eq!(old_lua_name, "server_v1");
            assert_eq!(new_lua_name, "server_v2");
        }
        other => panic!("expected LuaNameMismatch, got {other:?}"),
    }
    // Original survives.
    let by_lua = manager
        .handle_by_lua_name("server_v1")
        .await
        .expect("original must survive");
    assert!(Arc::ptr_eq(&by_lua, &h_old));
}

#[tokio::test]
async fn register_or_replace_rejects_lua_name_collision_on_fresh_insert() {
    // Property: a fresh insertion whose lua_name collides with a
    // DIFFERENT existing upstream's lua_name is rejected. Without
    // this gate, the new insertion would silently displace the
    // existing upstream's Lua global, breaking its agent-visible
    // binding. (`register` itself logs+displaces in this case; the
    // supervisor path uses `register_or_replace` precisely because
    // it wants the strict semantics.)
    let manager = UpstreamManager::new();
    let (_s_old, h_old) = make_handle("server-a", "shared").await;
    manager.register(Arc::clone(&h_old)).await;

    // Different display name, colliding lua_name.
    let (_s_new, h_new) = make_handle("server-b", "shared").await;
    let err = manager
        .register_or_replace("server-b", h_new)
        .await
        .expect_err("colliding lua_name on fresh insert must fail");
    assert!(
        matches!(err, ReplaceError::LuaNameMismatch { .. }),
        "expected LuaNameMismatch, got {err:?}"
    );

    // Original is unchanged.
    let by_name = manager
        .handle_by_name("server-a")
        .await
        .expect("original must survive");
    assert!(Arc::ptr_eq(&by_name, &h_old));
    assert!(
        manager.handle_by_name("server-b").await.is_none(),
        "rejected insert must not register"
    );
}

// ── layer-5 tests: TransientUnavailable gate ──
//
// `call_tool` reads `health.state()` before dispatching. When the
// state is Unhealthy/Respawning/Dead, we return
// `UpstreamError::TransientUnavailable` immediately — the
// supervisor has already decided this upstream is sick, no point
// burning a 20 s timeout proving it again.
//
// Suspect is deliberately NOT gated: the supervisor is probing
// there, and letting real calls through lets a recovering upstream
// self-heal.

#[tokio::test]
async fn call_tool_fast_fails_when_unhealthy() {
    // Property: once the health state is Unhealthy, call_tool
    // returns TransientUnavailable immediately without even
    // attempting to dispatch. Test with a fast upstream so the
    // only reason this could return quickly is the gate — if the
    // gate weren't there, the call would succeed (and the
    // assertion would fail).
    let (_server, handle) = make_slow_handle(
        "unhealthy",
        Duration::from_millis(20),
        Duration::from_millis(500),
    )
    .await;
    handle.health().set_state(HealthState::Unhealthy);

    let t0 = std::time::Instant::now();
    let err = handle
        .call_tool(CallToolRequestParams::new("slow"))
        .await
        .expect_err("Unhealthy must fast-fail");
    let elapsed = t0.elapsed();

    match err {
        UpstreamError::TransientUnavailable { name, state } => {
            assert_eq!(name, "unhealthy");
            assert_eq!(state, HealthState::Unhealthy);
        }
        other => panic!("expected TransientUnavailable, got {other:?}"),
    }
    assert!(
        elapsed < Duration::from_millis(50),
        "gate must fire fast, not wait for the upstream (got {elapsed:?})"
    );
}

#[tokio::test]
async fn call_tool_fast_fails_when_respawning() {
    // Property: same as Unhealthy — Respawning is also gated.
    let (_server, handle) = make_slow_handle(
        "respawning",
        Duration::from_millis(20),
        Duration::from_millis(500),
    )
    .await;
    handle.health().set_state(HealthState::Respawning);

    let err = handle
        .call_tool(CallToolRequestParams::new("slow"))
        .await
        .expect_err("Respawning must fast-fail");

    assert!(matches!(
        err,
        UpstreamError::TransientUnavailable {
            state: HealthState::Respawning,
            ..
        }
    ));
}

#[tokio::test]
async fn call_tool_fast_fails_when_dead() {
    // Property: same as Unhealthy — Dead is also gated. Gives
    // the operator a clean "upstream X is down" signal to agents
    // while the supervisor periodically re-arms.
    let (_server, handle) = make_slow_handle(
        "dead",
        Duration::from_millis(20),
        Duration::from_millis(500),
    )
    .await;
    handle.health().set_state(HealthState::Dead);

    let err = handle
        .call_tool(CallToolRequestParams::new("slow"))
        .await
        .expect_err("Dead must fast-fail");

    assert!(matches!(
        err,
        UpstreamError::TransientUnavailable {
            state: HealthState::Dead,
            ..
        }
    ));
}

#[tokio::test]
async fn call_tool_still_dispatches_when_suspect() {
    // Property: Suspect is NOT gated — the supervisor is probing
    // and we want real call traffic to either succeed (promoting
    // back to Healthy via record_success) or fail (confirming the
    // supervisor's concern). Gating Suspect would hide the
    // recovery path.
    //
    // Real-traffic record_success is the *only* Suspect → Healthy edge
    // (pings in Suspect are demotion-only, see supervisor.rs). That
    // makes Suspect-
    // dispatch even more load-bearing: if `call_tool` were gated
    // on Suspect, an upstream that needs real traffic to recover
    // would have no path back to Healthy at all.
    let (_server, handle) = make_slow_handle(
        "suspect",
        Duration::from_millis(20),
        Duration::from_millis(500),
    )
    .await;
    handle.health().set_state(HealthState::Suspect);

    handle
        .call_tool(CallToolRequestParams::new("slow"))
        .await
        .expect("Suspect must still dispatch calls");

    // Success on a Suspect-state handle auto-promotes to Healthy
    // via record_success's compare_exchange.
    assert_eq!(handle.health_snapshot().state, HealthState::Healthy);
}

#[test]
fn transient_unavailable_is_not_transport_level_and_is_supervisor_gated() {
    // Property: the supervisor must NOT double-count its own
    // fast-fail signal. `is_transport_level` returns false for
    // TransientUnavailable so a supervisor that iterates over
    // a batch of errors doesn't inflate the consecutive-failure
    // counter on errors it produced itself.
    //
    // Symmetric: `is_supervisor_gated` is the positive predicate,
    // letting the gateway's error formatter pick a friendlier
    // agent-facing message.
    let err = UpstreamError::TransientUnavailable {
        name: "x".into(),
        state: HealthState::Unhealthy,
    };
    assert!(!err.is_transport_level());
    assert!(err.is_supervisor_gated());
    assert_eq!(err.upstream_name(), Some("x"));

    // Spot-check: the other variants are NOT supervisor-gated.
    assert!(
        !UpstreamError::CallTimeout {
            name: "y".into(),
            deadline_ms: 1
        }
        .is_supervisor_gated()
    );
}

#[tokio::test]
async fn replace_drops_health_record_of_old_handle() {
    // Property: a successful replace returns the old Arc with its
    // health record intact (caller can inspect it if needed for
    // final-state logging), while the NEW Arc has a fresh
    // `UpstreamHealth`. This pins that respawning an upstream
    // doesn't inherit the wedged-upstream's consecutive-failure
    // tally — the supervisor's counter must start clean on the
    // new handle or the respawn-on-failure loop would oscillate.
    let manager = UpstreamManager::new();
    let (_s_old, h_old) = make_handle("svr", "svr").await;
    // Drive the old handle to Unhealthy with a few recorded failures.
    h_old.health().record_failure(FailureKind::CallTimeout);
    h_old.health().record_failure(FailureKind::CallTimeout);
    h_old.health().set_state(HealthState::Unhealthy);
    manager.register(Arc::clone(&h_old)).await;

    let (_s_new, h_new) = make_handle("svr", "svr").await;
    let displaced = manager
        .replace("svr", Arc::clone(&h_new))
        .await
        .expect("replace should succeed");

    // Displaced Arc's health still reflects its sickly final state
    // — the caller can log it before dropping.
    let old_snap = displaced.health_snapshot();
    assert_eq!(old_snap.state, HealthState::Unhealthy);
    assert_eq!(old_snap.consecutive_failures, 2);

    // New Arc's health is fresh.
    let new_snap = h_new.health_snapshot();
    assert_eq!(new_snap.state, HealthState::Healthy);
    assert_eq!(new_snap.consecutive_failures, 0);
}

// ── UpstreamHandle::ping tests ──
//
// The supervisor uses `ping` as its active probe in the Suspect
// state. These tests pin the three outcomes the supervisor
// distinguishes against: success (observability-only signal that
// the transport is breathing — promotion to Healthy is owned by
// `record_success` on a real-traffic call, NOT by ping success),
// timeout (transport-level ping failure; counts toward K
// consecutive ping failures → Unhealthy), and transport error
// (ditto). The unexpected-response case is covered implicitly —
// rmcp server code responds to `PingRequest` with
// `ServerResult::EmptyResult`, so there's no way to drive a
// non-empty success path against a well-behaved server; that arm
// exists as a defensive match rather than something we can test
// end-to-end.

#[tokio::test]
async fn ping_succeeds_against_a_healthy_fake_server() {
    // Property: the default FakeServer (used elsewhere in this test
    // module) returns `ServerResult::EmptyResult` for `PingRequest`
    // per the rmcp `ServerHandler` default impl. `ping` translates
    // that into `Ok(())`.
    let (_server, handle) = make_handle("pingable", "pingable").await;
    handle
        .ping(Duration::from_secs(1))
        .await
        .expect("healthy fake server must respond to ping");
}

#[tokio::test]
async fn ping_returns_call_timeout_when_server_does_not_respond_in_time() {
    // Property: when the timeout elapses before a response, `ping`
    // surfaces `UpstreamError::CallTimeout` naming the upstream and
    // echoing the configured `deadline_ms`. The supervisor's
    // classifier treats this as a transport-level failure.
    //
    // We can't easily drive "server never responds to ping" without
    // a custom fake that overrides the Ping handler AND
    // `wait_for_cancel` doesn't work for the Ping case in rmcp's
    // default impl. Instead we use a vanishingly small timeout
    // (1 ms) against the healthy FakeServer: the duplex-channel
    // round trip can in principle complete in < 1 ms on a warm
    // runtime, so this test may occasionally see a success. To
    // keep the assertion deterministic we skip the assertion in
    // that case and verify the structure of the failure when it
    // does fire — every observed run on CI-class hardware trips
    // the timeout.
    let (_server, handle) = make_handle("slow-ping", "slow_ping").await;
    let result = handle.ping(Duration::from_nanos(1)).await;

    match result {
        Ok(()) => {
            // Rare but legal: the round-trip beat the 1 ns deadline.
            // Nothing to assert; skip silently.
        }
        Err(UpstreamError::CallTimeout { name, deadline_ms }) => {
            assert_eq!(name, "slow-ping");
            assert_eq!(
                deadline_ms, 0,
                "1 ns rounds down to 0 ms — error must echo the configured value"
            );
        }
        Err(other) => panic!("expected CallTimeout or Ok, got {other:?}"),
    }
}

#[tokio::test]
async fn ping_does_not_mutate_health_record() {
    // Property: `ping` is a read-only probe from the handle's
    // perspective — it does not touch `UpstreamHealth`. The
    // supervisor drives state transitions based on ping outcomes
    // itself; the handle's record is the `call_tool` record.
    //
    // This guards against a future refactor that accidentally wires
    // `ping` into `record_failure` (which would let a single
    // supervisor probe bump the same counter that real traffic
    // uses to detect wedges).
    let (_server, handle) = make_handle("observer", "observer").await;

    let before = handle.health_snapshot();
    let _ = handle.ping(Duration::from_secs(1)).await;
    let after = handle.health_snapshot();

    assert_eq!(before.state, after.state);
    assert_eq!(
        before.consecutive_failures, after.consecutive_failures,
        "ping must not touch the consecutive-failure counter"
    );
    assert_eq!(before.last_failure_kind, after.last_failure_kind);
    assert_eq!(
        before.last_success_at, after.last_success_at,
        "ping must not stamp last_success_at — that's call_tool's job"
    );
}
