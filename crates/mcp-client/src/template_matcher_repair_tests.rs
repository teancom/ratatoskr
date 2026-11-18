use super::*;

fn bound(template: &str, uri: &str, name: &str) -> String {
    CompiledTemplate::compile(template)
        .expect("supported template")
        .bind(uri)
        .expect("valid nonempty expansion")
        .values[name]
        .clone()
}

#[test]
fn simple_bindings_decode_percent_encoded_scalars() {
    // Property: bindings re-expand without double encoding.
    use serde::Deserialize as _;
    use serde::de::IntoDeserializer;
    for value in ["/", "é", "a b", "%2F", "%", "a#b"] {
        let scalar = uri_template::Value::deserialize(
            serde_json::Value::String(value.to_owned()).into_deserializer(),
        )
        .expect("scalar")
        .into_static();
        let uri = uri_template::UriTemplate::new("resource:{x}")
            .expect("template")
            .expand(&std::collections::HashMap::from([("x", scalar)]))
            .expect("expand");
        let binding = bound("resource:{x}", &uri, "x");
        assert_eq!(binding, value);
        let rebound = uri_template::Value::deserialize(
            serde_json::Value::String(binding).into_deserializer(),
        )
        .expect("scalar")
        .into_static();
        assert_eq!(
            uri_template::UriTemplate::new("resource:{x}")
                .expect("template")
                .expand(&std::collections::HashMap::from([("x", rebound)]))
                .expect("re-expand"),
            uri
        );
    }
    for (uri, expected) in [
        ("resource:%2F", "/"),
        ("resource:%C3%A9", "é"),
        ("resource:a%20b", "a b"),
        ("resource:%252F", "%2F"),
        ("resource:%25", "%"),
    ] {
        assert_eq!(bound("resource:{x}", uri, "x"), expected);
    }
}

#[test]
fn values_containing_separator_literals_still_match() {
    // Property: separator text inside either value is not necessarily the split.
    for template in ["resource:{x}a{y}", "resource:{+x}a{y}", "resource:{x}a{+y}"] {
        assert_eq!(bound(template, "resource:aaa", "x"), "a");
        assert_eq!(bound(template, "resource:aaa", "y"), "a");
    }
}

#[test]
fn repeated_names_are_consistent_and_search_alternative_splits() {
    // Property: every occurrence denotes the same scalar, including ambiguous splits.
    assert!(
        !CompiledTemplate::compile("{x}/{x}")
            .expect("supported")
            .matches("a/b")
    );
    assert_eq!(bound("{x}a{x}", "aaaaa", "x"), "aa");
    assert_eq!(bound("{x}/{+x}", "%C3%A9/%C3%A9", "x"), "é");
    assert_eq!(bound("{+x}/{x}", "%C3%A9/%C3%A9", "x"), "é");
}

#[test]
fn simple_capture_does_not_split_percent_triplets() {
    // Property: the A inside %0A belongs to the scalar, not the literal separator.
    assert_eq!(bound("{x}A{y}", "%0AAa", "x"), "\n");
}

#[test]
fn reserved_capture_does_not_split_percent_triplets() {
    // Property: reserved captures retain intact triplets for re-expansion.
    assert_eq!(bound("{+x}a{+y}", "aa%aa", "x"), "a");
    assert_eq!(bound("{+x}a{+y}", "aa%aa", "y"), "%aa");
}

#[test]
fn suffix_reachability_requires_decodable_utf8() {
    // Property: an invalid decoded suffix cannot hide the later valid delimiter.
    assert_eq!(bound("{x}C{y}", "%C3%A9Ca", "x"), "é");
    assert_eq!(bound("{x}a{y}", "aa%C3%A9", "y"), "é");
}

#[test]
fn failed_suffix_scan_has_linear_work() {
    // Property: fixed-size templates do not repeatedly scan every remaining URI tail.
    let template = CompiledTemplate::compile("{x}a{y}z").expect("supported");
    for n in [1_000, 2_000, 4_000, 32_000] {
        let mut work = 20 * n + 100;
        let allowance = work;
        assert!(search::bind_with_budget(&template.parts, &"a".repeat(n), &mut work).is_none());
        assert_eq!(allowance - work, 14 * n + 10);
    }
}

#[test]
fn mixed_operator_constraints_preserve_percent_spelling_in_both_orders() {
    // Property: reserved triplets may be the scalar's literal spelling, not a decoded byte.
    for (template, uri) in [
        ("{+x}/{x}", "%41/%2541"),
        ("{x}/{+x}", "%2541/%41"),
        ("{+x}/{+x}/{x}", "%41/%41/%2541"),
    ] {
        assert_eq!(bound(template, uri, "x"), "%41");
    }
}

#[test]
fn unsupported_empty_and_adjacent_captures_stay_rejected() {
    // Property: this repair does not choose a new partition policy.
    for (template, uri) in [
        ("{x}", ""),
        ("{#x}", "#"),
        ("{x}{y}", "ab"),
        ("{x}{x}", "aa"),
    ] {
        assert!(
            !CompiledTemplate::compile(template)
                .expect("supported")
                .matches(uri)
        );
    }
}

#[test]
fn matching_budget_exhaustion_is_explicitly_bounded() {
    // Property: the same valid repeated-name match succeeds only with enough work.
    let template = CompiledTemplate::compile("{x}a{x}").expect("supported");
    assert!(search::bind_with_budget(&template.parts, "aaaaa", &mut 1).is_none());
    assert!(search::bind_with_budget(&template.parts, "aaaaa", &mut 10_000).is_some());
}
