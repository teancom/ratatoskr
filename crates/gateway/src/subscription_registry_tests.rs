use super::*;

fn key(upstream: &str, uri: &str) -> UpstreamSubKey {
    UpstreamSubKey::new(upstream, uri)
}

#[test]
fn first_subscriber_returns_first_subscriber_delta() {
    let reg = SubscriptionRegistry::new();
    assert_eq!(
        reg.add_concrete("sess-a", key("rootly", "incident://42")),
        SubscribeDelta::FirstSubscriber
    );
}

#[test]
fn second_session_on_same_key_returns_additional_subscriber() {
    let reg = SubscriptionRegistry::new();
    let k = key("rootly", "incident://42");
    reg.add_concrete("sess-a", k.clone());

    assert_eq!(
        reg.add_concrete("sess-b", k),
        SubscribeDelta::AdditionalSubscriber
    );
}

#[test]
fn repeated_subscribe_from_same_session_is_idempotent() {
    // Property: subscribing the same session to the same key twice
    // is a no-op. Otherwise a flaky downstream that retries would
    // trick us into never tearing down the upstream sub.
    let reg = SubscriptionRegistry::new();
    let k = key("rootly", "incident://42");
    reg.add_concrete("sess-a", k.clone());

    assert_eq!(
        reg.add_concrete("sess-a", k),
        SubscribeDelta::AlreadySubscribed
    );
}

#[test]
fn unsubscribe_when_other_sessions_present_returns_other_subscribers_remain() {
    let reg = SubscriptionRegistry::new();
    let k = key("rootly", "incident://42");
    reg.add_concrete("sess-a", k.clone());
    reg.add_concrete("sess-b", k.clone());

    assert_eq!(
        reg.remove_concrete("sess-a", &k),
        UnsubscribeDelta::OtherSubscribersRemain
    );
}

#[test]
fn unsubscribe_by_last_session_returns_last_subscriber_gone() {
    let reg = SubscriptionRegistry::new();
    let k = key("rootly", "incident://42");
    reg.add_concrete("sess-a", k.clone());

    assert_eq!(
        reg.remove_concrete("sess-a", &k),
        UnsubscribeDelta::LastSubscriberGone
    );
}

#[test]
fn unsubscribe_never_subscribed_is_not_subscribed() {
    // Idempotent: the client sending an unsubscribe for a URI we
    // don't track isn't a hard error (could be a retry, could be
    // crossed-wires); silently no-op.
    let reg = SubscriptionRegistry::new();
    assert_eq!(
        reg.remove_concrete("sess-a", &key("rootly", "incident://42")),
        UnsubscribeDelta::NotSubscribed
    );
}

#[test]
fn unsubscribe_different_session_for_existing_key_is_not_subscribed() {
    // Session B never subscribed to this key, so its unsubscribe is
    // a no-op — session A remains registered.
    let reg = SubscriptionRegistry::new();
    let k = key("rootly", "incident://42");
    reg.add_concrete("sess-a", k.clone());

    assert_eq!(
        reg.remove_concrete("sess-b", &k),
        UnsubscribeDelta::NotSubscribed
    );
    assert!(reg.session_is_subscribed("sess-a", &k));
}

#[test]
fn session_is_subscribed_reports_truthfully() {
    let reg = SubscriptionRegistry::new();
    let k = key("rootly", "incident://42");
    assert!(!reg.session_is_subscribed("sess-a", &k));
    reg.add_concrete("sess-a", k.clone());
    assert!(reg.session_is_subscribed("sess-a", &k));
    assert!(!reg.session_is_subscribed("sess-b", &k));
}

#[test]
fn drop_session_returns_only_keys_whose_last_session_left() {
    let reg = SubscriptionRegistry::new();
    let shared = key("rootly", "incident://42");
    let solo_a = key("rootly", "incident://7");
    let solo_b = key("datadog", "mcp/toolsets");

    reg.add_concrete("sess-a", shared.clone());
    reg.add_concrete("sess-a", solo_a.clone());
    reg.add_concrete("sess-b", shared.clone());
    reg.add_concrete("sess-b", solo_b.clone());

    let mut orphaned = reg.drop_session("sess-a");
    orphaned.sort_by(|a, b| a.uri.cmp(&b.uri));

    assert_eq!(orphaned, vec![solo_a.clone()]);

    // Sanity: sess-b's keys are still intact, and the shared key
    // still has sess-b.
    assert!(reg.session_is_subscribed("sess-b", &shared));
    assert!(reg.session_is_subscribed("sess-b", &solo_b));
    assert!(!reg.session_is_subscribed("sess-a", &solo_a));
}

#[test]
fn drop_session_is_idempotent_for_unknown_session() {
    let reg = SubscriptionRegistry::new();
    reg.add_concrete("sess-a", key("rootly", "incident://42"));

    assert_eq!(reg.drop_session("nobody"), Vec::<UpstreamSubKey>::new());
    // State unchanged.
    assert!(reg.session_is_subscribed("sess-a", &key("rootly", "incident://42")));
}

#[test]
fn sessions_for_returns_every_subscribed_session() {
    // Fan-out query: the forwarder uses this to deliver one update
    // to N sessions. We don't guarantee order; callers treat it as
    // a set.
    let reg = SubscriptionRegistry::new();
    let k = key("rootly", "incident://42");
    reg.add_concrete("sess-a", k.clone());
    reg.add_concrete("sess-b", k.clone());
    reg.add_concrete("sess-c", k.clone());

    let mut got = reg.sessions_for(&k);
    got.sort();
    assert_eq!(got, vec!["sess-a", "sess-b", "sess-c"]);
}

#[test]
fn sessions_for_unknown_key_is_empty() {
    let reg = SubscriptionRegistry::new();
    assert!(reg.sessions_for(&key("nobody", "resource://x")).is_empty());
}

#[test]
fn invariant_no_empty_session_sets() {
    // After add-then-remove on the single subscriber, the key
    // should disappear entirely (not linger with an empty set).
    // This guards the ref-count path: a lingering empty set would
    // cause subsequent `add_concrete` to return AdditionalSubscriber
    // instead of FirstSubscriber, missing the upstream subscribe
    // call for a brand-new subscription.
    let reg = SubscriptionRegistry::new();
    let k = key("rootly", "incident://42");

    reg.add_concrete("sess-a", k.clone());
    reg.remove_concrete("sess-a", &k);

    assert_eq!(
        reg.add_concrete("sess-b", k),
        SubscribeDelta::FirstSubscriber,
        "after the last subscriber left, the next subscribe must be treated as first"
    );
}

fn compile(t: &str) -> CompiledTemplate {
    CompiledTemplate::compile(t).expect("test templates are valid")
}

#[test]
fn template_first_subscriber_triggers_first_subscriber_delta() {
    let reg = SubscriptionRegistry::new();
    let key = UpstreamTemplateKey::new("rootly", "incident://{id}");
    assert_eq!(
        reg.add_template("sess-a", key, compile("incident://{id}")),
        SubscribeDelta::FirstSubscriber
    );
}

#[test]
fn template_second_session_is_additional_subscriber() {
    let reg = SubscriptionRegistry::new();
    let key = UpstreamTemplateKey::new("rootly", "incident://{id}");
    reg.add_template("sess-a", key.clone(), compile("incident://{id}"));

    assert_eq!(
        reg.add_template("sess-b", key, compile("incident://{id}")),
        SubscribeDelta::AdditionalSubscriber
    );
}

#[test]
fn template_repeated_subscribe_from_same_session_is_idempotent() {
    // Property: subscribing the same session to the same template
    // twice is a no-op. Mirrors the concrete-side guarantee —
    // otherwise a retrying downstream could prevent us from ever
    // tearing down the upstream template sub.
    let reg = SubscriptionRegistry::new();
    let key = UpstreamTemplateKey::new("rootly", "incident://{id}");
    reg.add_template("sess-a", key.clone(), compile("incident://{id}"));

    assert_eq!(
        reg.add_template("sess-a", key, compile("incident://{id}")),
        SubscribeDelta::AlreadySubscribed
    );
}

#[test]
fn template_sessions_for_matches_concrete_uri() {
    let reg = SubscriptionRegistry::new();
    let key = UpstreamTemplateKey::new("rootly", "incident://{id}");
    reg.add_template("sess-a", key.clone(), compile("incident://{id}"));
    reg.add_template("sess-b", key, compile("incident://{id}"));

    let mut got = reg.template_sessions_for("rootly", "incident://42");
    got.sort();
    assert_eq!(got, vec!["sess-a", "sess-b"]);
}

#[test]
fn template_sessions_for_rejects_non_matching_concrete_uri() {
    let reg = SubscriptionRegistry::new();
    reg.add_template(
        "sess-a",
        UpstreamTemplateKey::new("rootly", "incident://{id}"),
        compile("incident://{id}"),
    );
    assert!(reg.template_sessions_for("rootly", "team://42").is_empty());
}

#[test]
fn template_sessions_for_scopes_to_upstream() {
    // Two upstreams advertising the same template string. A
    // concrete URI from upstream X must not fan out to sessions
    // subscribed to upstream Y's same-shaped template.
    let reg = SubscriptionRegistry::new();
    reg.add_template(
        "sess-rootly",
        UpstreamTemplateKey::new("rootly", "incident://{id}"),
        compile("incident://{id}"),
    );
    reg.add_template(
        "sess-other",
        UpstreamTemplateKey::new("other", "incident://{id}"),
        compile("incident://{id}"),
    );

    assert_eq!(
        reg.template_sessions_for("rootly", "incident://42"),
        vec!["sess-rootly"]
    );
    assert_eq!(
        reg.template_sessions_for("other", "incident://42"),
        vec!["sess-other"]
    );
}

#[test]
fn remove_template_last_session_returns_last_subscriber_gone() {
    let reg = SubscriptionRegistry::new();
    let key = UpstreamTemplateKey::new("rootly", "incident://{id}");
    reg.add_template("sess-a", key.clone(), compile("incident://{id}"));

    assert_eq!(
        reg.remove_template("sess-a", &key),
        UnsubscribeDelta::LastSubscriberGone
    );
    // And the entry is fully gone.
    assert!(
        reg.template_sessions_for("rootly", "incident://42")
            .is_empty()
    );
}

#[test]
fn remove_template_when_key_not_present_returns_not_subscribed() {
    // Idempotent: unsubscribing from a template we don't track is a
    // no-op (could be a retry or crossed wires), never a hard error.
    let reg = SubscriptionRegistry::new();
    assert_eq!(
        reg.remove_template(
            "sess-a",
            &UpstreamTemplateKey::new("rootly", "incident://{id}"),
        ),
        UnsubscribeDelta::NotSubscribed
    );
}

#[test]
fn remove_template_different_session_on_existing_key_returns_not_subscribed() {
    // Session B never subscribed to this template, so its unsubscribe
    // is a no-op — session A remains registered on the key.
    let reg = SubscriptionRegistry::new();
    let key = UpstreamTemplateKey::new("rootly", "incident://{id}");
    reg.add_template("sess-a", key.clone(), compile("incident://{id}"));

    assert_eq!(
        reg.remove_template("sess-b", &key),
        UnsubscribeDelta::NotSubscribed
    );
    assert_eq!(
        reg.template_sessions_for("rootly", "incident://42"),
        vec!["sess-a"]
    );
}

#[test]
fn drop_session_templates_returns_orphaned_template_keys() {
    let reg = SubscriptionRegistry::new();
    let solo = UpstreamTemplateKey::new("rootly", "incident://{id}");
    let shared = UpstreamTemplateKey::new("rootly", "team://{team_id}");

    reg.add_template("sess-a", solo.clone(), compile("incident://{id}"));
    reg.add_template("sess-a", shared.clone(), compile("team://{team_id}"));
    reg.add_template("sess-b", shared.clone(), compile("team://{team_id}"));

    let mut orphans = reg.drop_session_templates("sess-a");
    orphans.sort_by(|a, b| a.template.cmp(&b.template));

    assert_eq!(orphans, vec![solo]);
    // sess-b still has the shared template.
    assert_eq!(
        reg.template_sessions_for("rootly", "team://7"),
        vec!["sess-b"]
    );
}

#[test]
fn drop_session_templates_is_idempotent_for_unknown_session() {
    // Mirror of drop_session_is_idempotent_for_unknown_session:
    // dropping a session we don't know about returns an empty vec
    // and leaves existing subscriptions intact. The session-close
    // path hits this whenever a session closed without any template
    // subs.
    let reg = SubscriptionRegistry::new();
    let key = UpstreamTemplateKey::new("rootly", "incident://{id}");
    reg.add_template("sess-a", key, compile("incident://{id}"));

    assert_eq!(
        reg.drop_session_templates("nobody"),
        Vec::<UpstreamTemplateKey>::new()
    );
    // State unchanged.
    assert_eq!(
        reg.template_sessions_for("rootly", "incident://42"),
        vec!["sess-a"]
    );
}

#[test]
fn add_remove_add_cycle_across_different_sessions() {
    // Regression pin for the full lifecycle: sess-a subscribes,
    // then leaves, then sess-b subscribes. The upstream call is
    // required on both sides of the gap — the registry's job is
    // to surface that, not to memoize.
    let reg = SubscriptionRegistry::new();
    let k = key("rootly", "incident://42");

    assert_eq!(
        reg.add_concrete("sess-a", k.clone()),
        SubscribeDelta::FirstSubscriber
    );
    assert_eq!(
        reg.remove_concrete("sess-a", &k),
        UnsubscribeDelta::LastSubscriberGone
    );
    assert_eq!(
        reg.add_concrete("sess-b", k),
        SubscribeDelta::FirstSubscriber
    );
}

#[test]
fn template_add_remove_add_cycle_across_different_sessions() {
    // Mirrors the concrete-side lifecycle: after the last session
    // leaves, the TemplateEntry is removed from the Vec, so the
    // next add_template must produce a fresh entry and return
    // FirstSubscriber (not AdditionalSubscriber via a tombstoned
    // entry). Guards against a regression that reused stale state.
    let reg = SubscriptionRegistry::new();
    let key = UpstreamTemplateKey::new("rootly", "incident://{id}");

    assert_eq!(
        reg.add_template("sess-a", key.clone(), compile("incident://{id}")),
        SubscribeDelta::FirstSubscriber
    );
    assert_eq!(
        reg.remove_template("sess-a", &key),
        UnsubscribeDelta::LastSubscriberGone
    );
    assert_eq!(
        reg.add_template("sess-b", key, compile("incident://{id}")),
        SubscribeDelta::FirstSubscriber
    );
}
