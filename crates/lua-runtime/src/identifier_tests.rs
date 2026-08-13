use super::*;

#[test]
fn hyphen_becomes_underscore() {
    assert_eq!(sanitize_lua_identifier("data-server"), "data_server");
}

#[test]
fn dot_becomes_underscore() {
    assert_eq!(sanitize_lua_identifier("my.api"), "my_api");
}

#[test]
fn leading_digit_gets_prefix() {
    assert_eq!(sanitize_lua_identifier("123service"), "_123service");
}

#[test]
fn reserved_word_gets_prefix() {
    assert_eq!(sanitize_lua_identifier("while"), "_while");
    assert_eq!(sanitize_lua_identifier("end"), "_end");
}

#[test]
fn empty_string_produces_valid_identifier() {
    assert_eq!(sanitize_lua_identifier(""), "_");
}

#[test]
fn already_valid_is_unchanged() {
    assert_eq!(sanitize_lua_identifier("calculator"), "calculator");
}

#[test]
fn idempotent() {
    // Property: sanitizing twice yields the same result as sanitizing once.
    let inputs = [
        "data-server",
        "my.api",
        "123service",
        "while",
        "",
        "ok",
        "_while",
        "_nil",
    ];
    for s in inputs {
        let once = sanitize_lua_identifier(s);
        let twice = sanitize_lua_identifier(&once);
        assert_eq!(once, twice, "idempotence violated for input {s:?}");
    }
}

#[test]
fn partition_passes_through_distinct_names() {
    let (mut unique, collisions) = partition_unique_by_sanitized(&["alpha", "beta", "gamma_three"]);
    unique.sort();
    assert_eq!(unique, vec!["alpha", "beta", "gamma_three"]);
    assert!(
        collisions.is_empty(),
        "no collisions expected: {collisions:?}"
    );
}

#[test]
fn partition_quarantines_colliding_pairs() {
    // Both colliding originals are excluded from `unique` — picking
    // a winner would silently let an agent inspect one and dispatch
    // the other.
    let (mut unique, collisions) = partition_unique_by_sanitized(&["safe-tool", "safe.tool", "ok"]);
    unique.sort();
    assert_eq!(unique, vec!["ok"]);
    assert_eq!(collisions.len(), 1);
    let originals = collisions.get("safe_tool").expect("safe_tool collision");
    assert_eq!(originals.len(), 2);
    assert!(originals.contains(&"safe-tool".to_string()));
    assert!(originals.contains(&"safe.tool".to_string()));
}

#[test]
fn partition_handles_three_way_collision() {
    // All three originals collide; none survive into `unique`.
    let (unique, collisions) = partition_unique_by_sanitized(&["a-b", "a.b", "a/b"]);
    assert!(unique.is_empty(), "all three collide: {unique:?}");
    let originals = collisions.get("a_b").expect("a_b collision");
    assert_eq!(originals.len(), 3);
}

#[test]
fn partition_preserves_originals_verbatim() {
    // `unique` returns the ORIGINAL name, not the sanitized form —
    // callers need the original to dispatch upstream.
    let (unique, _) = partition_unique_by_sanitized(&["original-name"]);
    assert_eq!(unique, vec!["original-name"]);
}

#[test]
fn reserved_word_and_its_prefixed_form_collide() {
    // "nil" → "_nil" and "_nil" → "_nil": both must be quarantined.
    // A future change to the prefix strategy could break this silently
    // without this pin.
    let (unique, collisions) = partition_unique_by_sanitized(&["nil", "_nil"]);
    assert!(
        unique.is_empty(),
        "both names collide; unique should be empty: {unique:?}"
    );
    let originals = collisions.get("_nil").expect("_nil collision bucket");
    assert_eq!(originals.len(), 2);
    assert!(originals.contains(&"nil".to_string()));
    assert!(originals.contains(&"_nil".to_string()));
}

#[test]
fn digit_prefix_and_its_prefixed_form_collide() {
    // "1foo" → "_1foo" and "_1foo" → "_1foo": both must be quarantined.
    let (unique, collisions) = partition_unique_by_sanitized(&["1foo", "_1foo"]);
    assert!(
        unique.is_empty(),
        "both names collide; unique should be empty: {unique:?}"
    );
    let originals = collisions.get("_1foo").expect("_1foo collision bucket");
    assert_eq!(originals.len(), 2);
    assert!(originals.contains(&"1foo".to_string()));
    assert!(originals.contains(&"_1foo".to_string()));
}
