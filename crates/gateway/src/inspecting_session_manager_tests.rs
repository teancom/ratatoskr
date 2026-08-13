use super::*;
use crate::execution_result_store::ExecutionResultStore;
use crate::response_shape_cache::ResponseShapeCache;
use crate::subscription_registry::{SubscriptionRegistry, UpstreamSubKey, UpstreamTemplateKey};
use crate::subscription_service::SubscriptionService;
use crate::tool_inspection::{InspectedTool, ToolInspectionStore};
use lua_runtime::LuaRuntime;
use mcp_client::UpstreamManager;
use mcp_client::template_matcher::CompiledTemplate;
use mcp_client::{
    ResourceListCache, SamplingBroker, SharedSamplingBroker, ToolListCache, UpstreamEventBus,
};
use std::sync::Arc;

fn make_manager() -> (
    InspectingSessionManager,
    ToolInspectionStore,
    Arc<ExecutionResultStore>,
    Arc<SubscriptionRegistry>,
) {
    let inspection = ToolInspectionStore::new();
    let result_store = Arc::new(ExecutionResultStore::new());
    let subscriptions = Arc::new(SubscriptionRegistry::new());
    // Empty UpstreamManager: the close-session tests don't exercise
    // real upstream RPCs; teardown falls through cleanly when the
    // manager has no matching lua_name.
    let upstreams = Arc::new(UpstreamManager::new());
    let subscription_service = Arc::new(SubscriptionService::new(
        Arc::clone(&subscriptions),
        Arc::clone(&upstreams),
    ));
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let settings = crate::config::GatewaySettings::default();
    let lua = Arc::new(
        LuaRuntime::new().with_execute_deadline(std::time::Duration::from_millis(
            settings.execute_deadline_ms,
        )),
    );
    let ctx = AppContext {
        upstreams,
        bus: Arc::new(UpstreamEventBus::new()),
        broker,
        inspection: inspection.clone(),
        tool_cache: Arc::new(ToolListCache::new()),
        resource_cache: Arc::new(ResourceListCache::new()),
        result_store: Arc::clone(&result_store),
        subscriptions: Arc::clone(&subscriptions),
        subscription_service,
        response_shapes: ResponseShapeCache::new(),
        settings,
        lua,
        shutdown: tokio_util::sync::CancellationToken::new(),
        supervisor_cancel: tokio_util::sync::CancellationToken::new(),
        supervisors: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        reload_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    let wrapped = InspectingSessionManager::new(LocalSessionManager::default(), ctx);
    (wrapped, inspection, result_store, subscriptions)
}

#[tokio::test]
async fn close_session_clears_inspection_entry() {
    let (mgr, store, _result_store, _subs) = make_manager();
    store.mark(
        "test-session",
        InspectedTool::new("gitlab", "list_pipeline_jobs"),
    );
    assert!(store.is_inspected(
        "test-session",
        &InspectedTool::new("gitlab", "list_pipeline_jobs")
    ));

    // Close a session that never existed in the inner manager's
    // map — rmcp's LocalSessionManager.close_session is defined
    // to be a no-op for unknown IDs (it just calls
    // `sessions.remove(id)` and discards the result). Our wrapper
    // still runs forget_session on the store, so the inspection
    // entry should be gone regardless of the inner call's outcome.
    let _ = mgr.close_session(&"test-session".into()).await;

    assert!(
        !store.is_inspected(
            "test-session",
            &InspectedTool::new("gitlab", "list_pipeline_jobs")
        ),
        "close_session must clear inspection entries even for unknown inner sessions"
    );
}

#[tokio::test]
async fn close_session_clears_result_store_entry() {
    // Load-bearing property for the offload feature: a closed
    // session's stored results go away with the session. Without
    // this, a gateway running long enough would accumulate
    // offloaded payloads from expired sessions forever.
    let (mgr, _store, result_store, _subs) = make_manager();
    let _id = result_store
        .save("test-session", "some-stored-blob".to_string())
        .expect("save must succeed");
    assert_eq!(result_store.entry_count("test-session"), 1);

    let _ = mgr.close_session(&"test-session".into()).await;

    assert_eq!(
        result_store.entry_count("test-session"),
        0,
        "close_session must clear the session's offloaded results"
    );
}

#[tokio::test]
async fn close_session_does_not_affect_other_sessions() {
    let (mgr, store, _result_store, _subs) = make_manager();
    store.mark("keep", InspectedTool::new("gitlab", "list_pipeline_jobs"));
    store.mark("drop", InspectedTool::new("gitlab", "list_pipeline_jobs"));

    let _ = mgr.close_session(&"drop".into()).await;

    assert!(store.is_inspected("keep", &InspectedTool::new("gitlab", "list_pipeline_jobs")));
    assert!(!store.is_inspected("drop", &InspectedTool::new("gitlab", "list_pipeline_jobs")));
}

#[tokio::test]
async fn close_session_drops_subscription_entries_for_that_session() {
    // Property: when a session's rmcp record is torn down, the
    // subscription registry must forget every concrete sub it
    // owned. Otherwise the next time that session ID is reused
    // (rmcp reallocates session IDs over process lifetime) a
    // subscribe call would return AlreadySubscribed and never
    // issue the upstream call the new session needs.
    let (mgr, _store, _result_store, subs) = make_manager();
    let key = UpstreamSubKey::new("rootly", "incident://42");
    subs.add_concrete("closing-session", key.clone());

    assert!(subs.session_is_subscribed("closing-session", &key));

    let _ = mgr.close_session(&"closing-session".into()).await;

    assert!(
        !subs.session_is_subscribed("closing-session", &key),
        "close_session must purge the registry entry for the closed session"
    );
}

#[tokio::test]
async fn close_session_drops_template_subscription_entries_for_that_session() {
    // Property: when a session closes, its template subscriptions are
    // torn down just like concrete subscriptions. If someone accidentally
    // deleted the `drop_session_templates` call in `close_session`, or
    // swapped the order of the two teardown calls, this test would catch
    // it — no test in this file previously exercised the template path.
    let (mgr, _store, _result_store, subs) = make_manager();
    let key = UpstreamTemplateKey::new("rootly", "incident://{id}");
    let compiled =
        CompiledTemplate::compile("incident://{id}").expect("test template must compile");
    subs.add_template("closing-session", key, compiled);

    // Before close: the session is visible via the template fan-out query.
    let before = subs.template_sessions_for("rootly", "incident://42");
    assert!(
        before.contains(&"closing-session".to_string()),
        "session must be in template_sessions_for before close"
    );

    let _ = mgr.close_session(&"closing-session".into()).await;

    // After close: the session must be gone from the template index.
    assert!(
        subs.template_sessions_for("rootly", "incident://42")
            .is_empty(),
        "close_session must purge template registry entries for the closed session"
    );
}

#[tokio::test]
async fn close_session_leaves_other_sessions_subscriptions_alone() {
    // Same key, two sessions. Close one; the other's subscription
    // must survive and the upstream entry must remain live for it.
    let (mgr, _store, _result_store, subs) = make_manager();
    let key = UpstreamSubKey::new("rootly", "incident://42");
    subs.add_concrete("leaving", key.clone());
    subs.add_concrete("staying", key.clone());

    let _ = mgr.close_session(&"leaving".into()).await;

    assert!(!subs.session_is_subscribed("leaving", &key));
    assert!(
        subs.session_is_subscribed("staying", &key),
        "other sessions' subs must not be collateral damage"
    );
}

#[tokio::test]
async fn close_session_without_subscriptions_is_safe_noop() {
    // Regression guard: a session that never subscribed to anything
    // must not blow up in the teardown path. `drop_session` returns
    // an empty Vec and the helper early-exits.
    let (mgr, store, _result_store, _subs) = make_manager();
    store.mark("quiet", InspectedTool::new("gitlab", "list_pipeline_jobs"));

    let _ = mgr.close_session(&"quiet".into()).await;

    assert!(!store.is_inspected("quiet", &InspectedTool::new("gitlab", "list_pipeline_jobs")));
}

#[tokio::test]
async fn delegates_create_then_has_session_round_trip() {
    // Property: the wrapper actually calls through to the inner
    // manager for the methods it doesn't customize. If a future
    // refactor accidentally turned `create_session` into a no-op
    // (e.g. by stubbing it during a mass-rename), HTTP would still
    // start but every initialize handshake would explode opaquely.
    // A direct create-then-has round trip pins the delegation.
    let (mgr, _store, _result_store, _subs) = make_manager();
    let (id, _transport) = mgr
        .create_session()
        .await
        .expect("create_session must delegate successfully");
    assert!(
        mgr.has_session(&id)
            .await
            .expect("has_session must delegate successfully"),
        "session created via the wrapper must be visible via has_session"
    );
}
