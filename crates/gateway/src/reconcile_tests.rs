use super::*;
use mcp_client::UpstreamKind;

fn stdio(name: &str, lua_name: &str, command: &str) -> UpstreamConfig {
    UpstreamConfig::new(
        name,
        lua_name,
        UpstreamKind::Stdio {
            command: command.to_string(),
            args: vec![],
            env: Default::default(),
        },
    )
}

fn names(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn changed_names(diff: &UpstreamDiff) -> Vec<String> {
    names(diff.changed.iter().map(|c| c.name.clone()).collect())
}

fn added_names(diff: &UpstreamDiff) -> Vec<String> {
    names(diff.added.iter().map(|c| c.name.clone()).collect())
}

#[test]
fn empty_to_empty_is_no_op() {
    let diff = diff_upstreams(&[], &[]);
    assert!(diff.is_empty());
    assert_eq!(diff, UpstreamDiff::default());
}

#[test]
fn pure_additions_when_running_set_is_empty() {
    let next = vec![stdio("a", "a", "/bin/a"), stdio("b", "b", "/bin/b")];
    let diff = diff_upstreams(&[], &next);
    assert_eq!(added_names(&diff), vec!["a", "b"]);
    assert!(diff.removed.is_empty());
    assert!(diff.changed.is_empty());
    assert!(diff.unchanged.is_empty());
}

#[test]
fn pure_removals_when_next_config_is_empty() {
    let current = vec![stdio("a", "a", "/bin/a"), stdio("b", "b", "/bin/b")];
    let diff = diff_upstreams(&current, &[]);
    assert_eq!(names(diff.removed.clone()), vec!["a", "b"]);
    assert!(diff.added.is_empty());
    assert!(diff.changed.is_empty());
}

#[test]
fn identical_config_is_entirely_unchanged() {
    let cfgs = vec![stdio("a", "a", "/bin/a"), stdio("b", "b", "/bin/b")];
    let diff = diff_upstreams(&cfgs, &cfgs);
    assert!(diff.is_empty(), "no add/remove/change work");
    assert_eq!(names(diff.unchanged.clone()), vec!["a", "b"]);
}

#[test]
fn changed_command_classifies_as_changed_not_add_remove() {
    let current = vec![stdio("a", "a", "/bin/old")];
    let next = vec![stdio("a", "a", "/bin/new")];
    let diff = diff_upstreams(&current, &next);
    assert_eq!(changed_names(&diff), vec!["a"]);
    assert!(diff.added.is_empty());
    assert!(diff.removed.is_empty());
    assert!(diff.unchanged.is_empty());
    // The stored "changed" value is the NEW config.
    match &diff.changed[0].kind {
        UpstreamKind::Stdio { command, .. } => assert_eq!(command, "/bin/new"),
        other => panic!("expected stdio, got {other:?}"),
    }
}

#[test]
fn changed_env_is_detected() {
    let mut env = std::collections::HashMap::new();
    env.insert("TOKEN".to_string(), "secret".to_string());
    let current = vec![stdio("a", "a", "/bin/a")];
    let next = vec![UpstreamConfig::new(
        "a",
        "a",
        UpstreamKind::Stdio {
            command: "/bin/a".to_string(),
            args: vec![],
            env,
        },
    )];
    let diff = diff_upstreams(&current, &next);
    assert_eq!(changed_names(&diff), vec!["a"]);
}

#[test]
fn changed_supervision_override_is_detected() {
    use mcp_client::supervision::PartialSupervisionSettings;
    let current = vec![stdio("a", "a", "/bin/a")];
    let next = vec![
        stdio("a", "a", "/bin/a").with_supervision(PartialSupervisionSettings {
            unhealthy_threshold: Some(7),
            ..Default::default()
        }),
    ];
    let diff = diff_upstreams(&current, &next);
    assert_eq!(
        changed_names(&diff),
        vec!["a"],
        "a per-upstream supervision override is part of the connect contract"
    );
}

#[test]
fn changed_hint_is_detected() {
    let current = vec![stdio("a", "a", "/bin/a")];
    let next = vec![stdio("a", "a", "/bin/a").with_hint("new hint")];
    let diff = diff_upstreams(&current, &next);
    assert_eq!(changed_names(&diff), vec!["a"]);
}

#[test]
fn rename_reads_as_remove_plus_add() {
    // Same command, different display name → the old name is removed
    // and the new name is added. Keyed by name, so this is correct:
    // the Lua global identity changes with the name.
    let current = vec![stdio("old-name", "old_name", "/bin/a")];
    let next = vec![stdio("new-name", "new_name", "/bin/a")];
    let diff = diff_upstreams(&current, &next);
    assert_eq!(added_names(&diff), vec!["new-name"]);
    assert_eq!(names(diff.removed.clone()), vec!["old-name"]);
    assert!(diff.changed.is_empty());
    assert!(diff.unchanged.is_empty());
}

#[test]
fn mixed_diff_partitions_every_name_exactly_once() {
    // Invariant (the core property of a reconcile): the union of all
    // four buckets equals the union of both input name-sets, with no
    // name appearing in two buckets. `keep` unchanged, `edit`
    // changed, `gone` removed, `fresh` added.
    let current = vec![
        stdio("keep", "keep", "/bin/keep"),
        stdio("edit", "edit", "/bin/old"),
        stdio("gone", "gone", "/bin/gone"),
    ];
    let next = vec![
        stdio("keep", "keep", "/bin/keep"),
        stdio("edit", "edit", "/bin/new"),
        stdio("fresh", "fresh", "/bin/fresh"),
    ];
    let diff = diff_upstreams(&current, &next);

    assert_eq!(added_names(&diff), vec!["fresh"]);
    assert_eq!(names(diff.removed.clone()), vec!["gone"]);
    assert_eq!(changed_names(&diff), vec!["edit"]);
    assert_eq!(names(diff.unchanged.clone()), vec!["keep"]);

    // Partition invariant: every input name appears exactly once
    // across the four buckets.
    let mut all_classified: Vec<String> = added_names(&diff);
    all_classified.extend(diff.removed.clone());
    all_classified.extend(changed_names(&diff));
    all_classified.extend(diff.unchanged.clone());
    all_classified.sort();
    all_classified.dedup();
    assert_eq!(
        all_classified,
        vec!["edit", "fresh", "gone", "keep"],
        "every name from either input must land in exactly one bucket, no duplicates"
    );
}

#[test]
fn settings_changes_empty_when_identical() {
    let s = GatewaySettings::default();
    assert!(settings_changes(&s, &s).is_empty());
}

#[test]
fn settings_changes_reports_each_changed_field() {
    let running = GatewaySettings::default();
    let parsed = GatewaySettings {
        result_size_threshold: running.result_size_threshold + 1000,
        execute_deadline_ms: running.execute_deadline_ms + 5000,
        ..running
    };
    let lines = settings_changes(&running, &parsed);
    assert_eq!(lines.len(), 2);
    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("result_size_threshold:"))
    );
    assert!(lines.iter().any(|l| l.starts_with("execute_deadline_ms:")));
    // supervision unchanged → not reported.
    assert!(!lines.iter().any(|l| l.contains("supervision")));
}

#[test]
fn settings_changes_reports_supervision_change() {
    let running = GatewaySettings::default();
    let mut parsed = running;
    parsed.supervision.unhealthy_threshold = running.supervision.unhealthy_threshold + 1;
    let lines = settings_changes(&running, &parsed);
    assert_eq!(lines, vec!["supervision: changed".to_string()]);
}

#[test]
fn settings_changes_reports_port_change() {
    let running = GatewaySettings::default();
    let parsed = GatewaySettings {
        port: running.port + 1,
        ..running
    };
    let lines = settings_changes(&running, &parsed);
    assert_eq!(
        lines,
        vec![format!("port: {} -> {}", running.port, parsed.port)]
    );
}

// ── promote_for_reconnect (NeedsLogin force-reconnect) ──

fn force(names: &[&str]) -> std::collections::HashSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

#[test]
fn promote_moves_named_unchanged_into_changed() {
    // Property: a NeedsLogin upstream that diffed as `unchanged`
    // (config identical, only credentials refreshed) is promoted into
    // `changed` so the reload reconnects it — carrying its current
    // config.
    let current = vec![stdio("a", "a", "/bin/a"), stdio("b", "b", "/bin/b")];
    let mut diff = diff_upstreams(&current, &current);
    assert_eq!(names(diff.unchanged.clone()), vec!["a", "b"]);

    promote_for_reconnect(&mut diff, &current, &force(&["a"]));

    assert_eq!(changed_names(&diff), vec!["a"], "a should be promoted");
    assert_eq!(
        names(diff.unchanged.clone()),
        vec!["b"],
        "b should remain unchanged"
    );
    // The promoted config is the running one (command preserved).
    match &diff.changed[0].kind {
        UpstreamKind::Stdio { command, .. } => assert_eq!(command, "/bin/a"),
        other => panic!("expected stdio, got {other:?}"),
    }
}

#[test]
fn promote_is_noop_for_empty_force_set() {
    let current = vec![stdio("a", "a", "/bin/a")];
    let mut diff = diff_upstreams(&current, &current);
    let before = diff.clone();
    promote_for_reconnect(&mut diff, &current, &force(&[]));
    assert_eq!(diff, before, "empty force set must not touch the diff");
}

#[test]
fn promote_ignores_names_not_in_unchanged() {
    // A name that's already added/changed/removed (or never present)
    // must be ignored — the caller can pass the whole NeedsLogin set
    // without pre-filtering. Here "a" is genuinely changing config, so
    // it's already in `changed`; promoting it again must be a no-op.
    let current = vec![stdio("a", "a", "/bin/old")];
    let next = vec![stdio("a", "a", "/bin/new")];
    let mut diff = diff_upstreams(&current, &next);
    assert_eq!(changed_names(&diff), vec!["a"]);
    let before = diff.clone();

    promote_for_reconnect(&mut diff, &current, &force(&["a", "ghost"]));

    assert_eq!(
        diff, before,
        "names not in `unchanged` must leave the diff untouched"
    );
}
