//! Pure state-machine tests: the delta transitions, the
//! drop_session fan-out, and the rollback behaviour when the
//! upstream RPC fails. An empty `UpstreamManager` is sufficient —
//! looking up a non-registered lua_name returns `None`, which we
//! exploit to drive the "upstream gone" branches without a fake
//! MCP server.
//!
//! Tests that exercise the handler-level JSON-RPC error shape
//! (invalid_params vs internal_error) stay on the handler side;
//! this file tests domain behaviour.
use super::*;

fn make_service() -> SubscriptionService {
    let registry = Arc::new(SubscriptionRegistry::new());
    let upstreams = Arc::new(UpstreamManager::new());
    SubscriptionService::new(registry, upstreams)
}

#[tokio::test]
async fn subscribe_rejects_decode_failure() {
    // Property: a malformed namespaced URI surfaces as DecodeUri
    // with the original URI preserved for the handler's error
    // message.
    let svc = make_service();
    let err = svc
        .subscribe("s", "not-a-namespaced-uri")
        .await
        .expect_err("malformed URI must fail");
    match err {
        SubscribeError::DecodeUri { uri, .. } => {
            assert_eq!(uri, "not-a-namespaced-uri");
        }
        other => panic!("expected DecodeUri, got {other:?}"),
    }
}

#[tokio::test]
async fn subscribe_rejects_unknown_upstream() {
    // Property: a well-formed namespaced URI whose lua_name isn't
    // registered surfaces as UnknownUpstream. The registry must
    // NOT have been mutated — a retry that registers the upstream
    // first must see a fresh FirstSubscriber delta.
    let svc = make_service();
    let err = svc
        .subscribe("s", "mcp-proxy://ghost/res%3A%2F%2Fa")
        .await
        .expect_err("unknown upstream must fail");
    match err {
        SubscribeError::UnknownUpstream { upstream } => assert_eq!(upstream, "ghost"),
        other => panic!("expected UnknownUpstream, got {other:?}"),
    }
    // Registry was not mutated: session has no concrete key yet.
    assert!(
        !svc.registry
            .session_is_subscribed("s", &UpstreamSubKey::new("ghost", "res%3A%2F%2Fa")),
        "UnknownUpstream must not leave a registry entry behind"
    );
}

#[tokio::test]
async fn unsubscribe_of_unregistered_upstream_rolls_back_registry_removal() {
    // Property: when unsubscribe fails because the upstream is
    // gone, the registry removal is rolled back. A retry after
    // the upstream comes back up must see LastSubscriberGone
    // again and re-attempt the upstream teardown.
    let svc = make_service();

    // Pre-populate the registry directly (bypassing subscribe,
    // since there is no upstream to satisfy the service path).
    // The registry is keyed by the DECODED upstream URI, which is
    // what resource_uri::decode produces from the namespaced form
    // we pass into unsubscribe below. Keep these two in sync.
    let key = UpstreamSubKey::new("ghost", "res://a");
    svc.registry.add_concrete("s", key.clone());

    let err = svc
        .unsubscribe("s", "mcp-proxy://ghost/res%3A%2F%2Fa")
        .await
        .expect_err("unregistered upstream must fail");
    match err {
        UnsubscribeError::UnknownUpstream { upstream } => assert_eq!(upstream, "ghost"),
        other => panic!("expected UnknownUpstream, got {other:?}"),
    }

    // Rollback: the session is still registered against the key,
    // so a retry would see LastSubscriberGone again.
    assert!(
        svc.registry.session_is_subscribed("s", &key),
        "UnknownUpstream on unsubscribe must roll back the registry removal"
    );
}

#[tokio::test]
async fn drop_session_clears_concrete_and_template_entries() {
    // Property: drop_session hands the registry a single entry
    // point for both concrete and template cleanup. After the
    // call, every key the session owned is gone from both
    // indices. The upstream-teardown phase runs against an empty
    // manager and silently skips the "upstream no longer
    // registered" branch — which is the production behaviour at
    // shutdown anyway.
    let svc = make_service();
    let concrete = UpstreamSubKey::new("rootly", "incident://42");
    let template = UpstreamTemplateKey::new("rootly", "incident://{id}");
    svc.registry.add_concrete("s", concrete.clone());
    svc.registry.add_template(
        "s",
        template,
        CompiledTemplate::compile("incident://{id}").expect("test template must compile"),
    );
    // Sanity: the session is visible.
    assert!(svc.registry.session_is_subscribed("s", &concrete));
    assert!(
        svc.registry
            .template_sessions_for("rootly", "incident://42")
            .contains(&"s".to_string())
    );

    svc.drop_session("s").await;

    assert!(
        !svc.registry.session_is_subscribed("s", &concrete),
        "drop_session must remove the concrete entry"
    );
    assert!(
        svc.registry
            .template_sessions_for("rootly", "incident://42")
            .is_empty(),
        "drop_session must remove the template entry"
    );
}

#[tokio::test]
async fn drop_session_on_empty_session_is_safe_noop() {
    // Regression guard: a session that never subscribed must not
    // trip any internal assertion. Both drain calls return empty;
    // teardown helpers early-exit.
    let svc = make_service();
    svc.drop_session("never-subscribed").await;
}
