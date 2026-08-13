use super::*;
use rmcp::model::{Content, RawContent, RawTextContent};

// CallToolResult is #[non_exhaustive], so tests build through the
// `success` builder and then mutate fields as needed.
fn text_result(text: &str) -> CallToolResult {
    CallToolResult::success(vec![Content::new(
        RawContent::Text(RawTextContent {
            text: text.to_string(),
            meta: None,
        }),
        None,
    )])
}

fn multi_text_result(texts: &[&str]) -> CallToolResult {
    CallToolResult::success(
        texts
            .iter()
            .map(|t| {
                Content::new(
                    RawContent::Text(RawTextContent {
                        text: t.to_string(),
                        meta: None,
                    }),
                    None,
                )
            })
            .collect(),
    )
}

fn structured_result(value: serde_json::Value) -> CallToolResult {
    let mut r = CallToolResult::success(vec![]);
    r.structured_content = Some(value);
    r
}

// Single non-text content block — image used here as a concrete stand-in
// for any non-text variant (image, resource, resource_link, audio). The
// classifier's (None, _) arms hinge only on `as_text()` returning None,
// so the specific non-text kind doesn't matter for the test.
fn single_image_result() -> CallToolResult {
    CallToolResult::success(vec![Content::image("base64data", "image/png")])
}

// ---- classify_from_response ----

#[test]
fn classifies_single_prose_text() {
    let r = text_result("Hello, this is a human-readable reply.");
    assert_eq!(classify_from_response(&r), Some(ResponseShape::TextProse));
}

#[test]
fn classifies_single_json_object_text() {
    // The exact shape F2 complains about: upstream wraps JSON in a
    // text block. Agent rediscovery would be `unwrap_content` +
    // `json_decode`; the hint needs to say so.
    let r = text_result(r#"{"incident_id":"42","state":"open"}"#);
    assert_eq!(classify_from_response(&r), Some(ResponseShape::TextJson));
}

#[test]
fn classifies_single_json_array_text() {
    let r = text_result(r#"[{"id":1},{"id":2}]"#);
    assert_eq!(classify_from_response(&r), Some(ResponseShape::TextJson));
}

#[test]
fn bare_json_scalar_in_text_is_prose_not_json() {
    // Property: a bare "hello" or 42 parses as JSON but is NOT
    // meaningfully "structured JSON the agent should decode." The
    // classifier should call it prose so the hint doesn't push the
    // agent toward json_decode on a literal string.
    for text in ["\"hello\"", "42", "true", "null"] {
        let r = text_result(text);
        assert_eq!(
            classify_from_response(&r),
            Some(ResponseShape::TextProse),
            "bare JSON scalar {text:?} must not classify as TextJson"
        );
    }
}

#[test]
fn prose_that_starts_with_brace_but_fails_parse_is_prose() {
    // Edge case: a prose text that happens to start with `{` but
    // isn't valid JSON. Must not be TextJson — the `json_decode`
    // call would fail at runtime.
    let r = text_result("{ not really JSON, just happens to start this way }");
    assert_eq!(classify_from_response(&r), Some(ResponseShape::TextProse));
}

#[test]
fn classifies_multi_block_as_multi_with_len() {
    let r = multi_text_result(&["first", "second", "third"]);
    assert_eq!(
        classify_from_response(&r),
        Some(ResponseShape::MultiBlock { len: 3 })
    );
}

#[test]
fn classifies_structured_only() {
    let r = structured_result(serde_json::json!({"foo": "bar"}));
    assert_eq!(classify_from_response(&r), Some(ResponseShape::Structured));
}

#[test]
fn classifies_structured_plus_text_mirror_as_mixed() {
    // Real-world shape: structured_content carries the data, content
    // has a text mirror. The agent should prefer structured_content
    // but the catch-all in the hint must acknowledge the mirror.
    let mut r = text_result("Human-readable mirror");
    r.structured_content = Some(serde_json::json!({"value": 1}));
    assert_eq!(classify_from_response(&r), Some(ResponseShape::Mixed));
}

#[test]
fn empty_response_yields_none() {
    let r = CallToolResult::success(vec![]);
    assert_eq!(classify_from_response(&r), None);
}

#[test]
fn classifies_single_non_text_block_as_multiblock_len_one() {
    // Property: a single non-text content block (image, resource link,
    // etc.) takes the (None, false) path and emits MultiBlock { len: 1 }
    // — the len == 1 specialisation whose hint tells the agent to look
    // at result.content[0] rather than iterate.
    let r = single_image_result();
    assert_eq!(
        classify_from_response(&r),
        Some(ResponseShape::MultiBlock { len: 1 })
    );
}

#[test]
fn classifies_single_non_text_block_with_structured_as_structured() {
    // Property: the (None, true) path — a single non-text block plus
    // structured_content — classifies as Structured outright. No
    // MultiBlock-style structural advice needed because the agent
    // should be drilling into structured_content anyway.
    let mut r = single_image_result();
    r.structured_content = Some(serde_json::json!({"value": 1}));
    assert_eq!(classify_from_response(&r), Some(ResponseShape::Structured));
}

#[test]
fn null_structured_content_does_not_count_as_structured() {
    // Defense in depth against permissive upstreams that send
    // `structured_content: null`. Must fall through to text
    // classification, not claim Structured.
    let mut r = text_result("just prose");
    r.structured_content = Some(serde_json::Value::Null);
    assert_eq!(classify_from_response(&r), Some(ResponseShape::TextProse));
}

// ---- classify_from_schema ----

#[test]
fn schema_with_properties_classifies_structured() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {"id": {"type": "string"}}
    });
    assert_eq!(
        classify_from_schema(Some(&schema)),
        Some(ResponseShape::Structured)
    );
}

#[test]
fn schema_array_with_items_classifies_structured() {
    let schema = serde_json::json!({
        "type": "array",
        "items": {"type": "object"}
    });
    assert_eq!(
        classify_from_schema(Some(&schema)),
        Some(ResponseShape::Structured)
    );
}

#[test]
fn empty_schema_returns_none() {
    // Property: a bare `{}` is non-informative. Upstreams that ship
    // this are declaring "there IS a schema" without meaningfully
    // constraining the shape. Jumping to Structured on that alone
    // would mislabel prose-only tools.
    let schema = serde_json::json!({});
    assert_eq!(classify_from_schema(Some(&schema)), None);
}

#[test]
fn missing_schema_returns_none() {
    assert_eq!(classify_from_schema(None), None);
}

#[test]
fn schema_array_with_null_items_returns_none() {
    // Property: a permissive upstream shipping `"items": null`
    // (technically invalid JSON Schema but not implausible) must not
    // be over-classified as Structured. Mirrors the has_properties
    // posture of requiring a non-degenerate value.
    let schema = serde_json::json!({"type": "array", "items": null});
    assert_eq!(classify_from_schema(Some(&schema)), None);
}

#[test]
fn schema_with_type_but_no_properties_returns_none() {
    // Just `{"type": "object"}` without properties is as uninformative
    // as `{}`. Downstream tools sometimes ship this; it's not
    // evidence of a structured payload.
    let schema = serde_json::json!({"type": "object"});
    assert_eq!(classify_from_schema(Some(&schema)), None);
}

// ---- merge lattice ----

#[test]
fn merge_is_idempotent() {
    use ResponseShape::*;
    for s in [
        TextProse,
        TextJson,
        MultiBlock { len: 3 },
        Structured,
        Mixed,
    ] {
        assert_eq!(merge(s, s), s, "idempotence failed for {s:?}");
    }
}

#[test]
fn merge_is_commutative() {
    use ResponseShape::*;
    let shapes = [
        TextProse,
        TextJson,
        MultiBlock { len: 5 },
        MultiBlock { len: 3 },
        Structured,
        Mixed,
    ];
    for a in shapes {
        for b in shapes {
            assert_eq!(
                merge(a, b),
                merge(b, a),
                "commutativity failed for {a:?} / {b:?}"
            );
        }
    }
}

#[test]
fn merge_is_associative() {
    // Associativity is the third required property (with idempotence
    // and commutativity) for `merge` to define a valid join-semilattice
    // — which the module comment implicitly claims. A future restructure
    // of the match arms could silently break it without this test.
    use ResponseShape::*;
    let shapes = [
        TextProse,
        TextJson,
        MultiBlock { len: 5 },
        MultiBlock { len: 3 },
        Structured,
        Mixed,
    ];
    for a in shapes {
        for b in shapes {
            for c in shapes {
                assert_eq!(
                    merge(merge(a, b), c),
                    merge(a, merge(b, c)),
                    "associativity failed for {a:?} / {b:?} / {c:?}"
                );
            }
        }
    }
}

#[test]
fn merge_never_downgrades_to_unknown() {
    // Property: there is no `Unknown` variant precisely because
    // merging must never produce "less informative than both
    // inputs." A new observation either refines existing knowledge
    // or promotes to `Mixed`.
    use ResponseShape::*;
    for a in [
        TextProse,
        TextJson,
        MultiBlock { len: 2 },
        Structured,
        Mixed,
    ] {
        for b in [
            TextProse,
            TextJson,
            MultiBlock { len: 2 },
            Structured,
            Mixed,
        ] {
            let m = merge(a, b);
            // `Mixed` is the top; everything else must be one of the
            // inputs or a merge-promotion thereof. No "reset to nothing".
            assert!(
                matches!(
                    m,
                    TextProse | TextJson | MultiBlock { .. } | Structured | Mixed
                ),
                "merge({a:?}, {b:?}) produced unclassified shape {m:?}"
            );
        }
    }
}

#[test]
fn merge_text_prose_and_text_json_promotes_to_json() {
    // Once an agent's observed JSON content, don't un-learn it on a
    // subsequent prose call: the upstream might have emitted a prose
    // error message on a 404 but shipped JSON on success. The more
    // actionable hint wins.
    assert_eq!(
        merge(ResponseShape::TextProse, ResponseShape::TextJson),
        ResponseShape::TextJson
    );
}

#[test]
fn merge_text_and_structured_promotes_to_mixed() {
    assert_eq!(
        merge(ResponseShape::TextProse, ResponseShape::Structured),
        ResponseShape::Mixed
    );
    assert_eq!(
        merge(ResponseShape::TextJson, ResponseShape::Structured),
        ResponseShape::Mixed
    );
}

#[test]
fn merge_multi_block_takes_max_length() {
    assert_eq!(
        merge(
            ResponseShape::MultiBlock { len: 3 },
            ResponseShape::MultiBlock { len: 7 }
        ),
        ResponseShape::MultiBlock { len: 7 }
    );
}

#[test]
fn merge_multi_block_and_structured_promotes_to_mixed() {
    assert_eq!(
        merge(
            ResponseShape::MultiBlock { len: 3 },
            ResponseShape::Structured
        ),
        ResponseShape::Mixed
    );
}

#[test]
fn merge_multi_block_and_text_prefers_multi_block() {
    // Structural information wins over per-block classification:
    // once we know there are multiple blocks, telling the agent
    // "it's prose" is less useful than "iterate result.content".
    assert_eq!(
        merge(
            ResponseShape::TextProse,
            ResponseShape::MultiBlock { len: 5 }
        ),
        ResponseShape::MultiBlock { len: 5 }
    );
}

// ---- hint + tag rendering ----

#[test]
fn every_variant_has_a_nonempty_hint_and_tag() {
    use ResponseShape::*;
    for s in [
        TextProse,
        TextJson,
        MultiBlock { len: 3 },
        Structured,
        Mixed,
    ] {
        assert!(!s.hint().is_empty(), "empty hint for {s:?}");
        assert!(!s.tag().is_empty(), "empty tag for {s:?}");
        assert!(
            !s.tag().contains(' '),
            "tag {:?} must be one token (agents pattern-match on it)",
            s.tag()
        );
    }
}

#[test]
fn text_json_hint_names_both_builtins() {
    // Load-bearing: the whole point of `TextJson` over `TextProse`
    // is to point the agent at json_decode in addition to
    // unwrap_content. Without both names in the hint, the agent
    // does the two-step rediscovery anyway.
    let h = ResponseShape::TextJson.hint();
    assert!(
        h.contains("unwrap_content"),
        "hint must name unwrap_content: {h}"
    );
    assert!(h.contains("json_decode"), "hint must name json_decode: {h}");
}
