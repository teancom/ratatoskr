use super::*;
use serde_json::json;

// ---- infer_shape ----

#[test]
fn primitives_map_to_their_variants() {
    assert_eq!(infer_shape(&json!(null), DEFAULT_MAX_DEPTH), Shape::Null);
    assert_eq!(infer_shape(&json!(true), DEFAULT_MAX_DEPTH), Shape::Bool);
    assert_eq!(infer_shape(&json!(42), DEFAULT_MAX_DEPTH), Shape::Number);
    assert_eq!(infer_shape(&json!(1.5), DEFAULT_MAX_DEPTH), Shape::Number);
    assert_eq!(infer_shape(&json!("x"), DEFAULT_MAX_DEPTH), Shape::String);
}

#[test]
fn empty_array_has_null_item_shape_and_zero_len() {
    // Property: empty input is representable. The Shape::Null
    // placeholder is never surfaced in rendered output because
    // write_shape special-cases len == 0 and omits the "of:" line
    // entirely. The internal Shape::Null is an artefact of
    // Option::unwrap_or on an empty iterator; callers should treat
    // it as "no item type known", not "items are null".
    let s = infer_shape(&json!([]), DEFAULT_MAX_DEPTH);
    assert_eq!(
        s,
        Shape::Array {
            items: Box::new(Shape::Null),
            len: 0
        }
    );
    // Verify the renderer does NOT emit "of: null" for empty arrays.
    assert_eq!(
        format_shape(&s, 0),
        vec!["array (0 items)".to_string()],
        "empty array must not render a false item-type assertion"
    );
}

#[test]
fn array_item_shape_mirrors_first_element() {
    let s = infer_shape(&json!([{"a": 1}, {"a": 2}, {"a": 3}]), DEFAULT_MAX_DEPTH);
    let Shape::Array { items, len } = s else {
        panic!("expected Array");
    };
    assert_eq!(len, 3);
    assert_eq!(
        *items,
        Shape::Object {
            fields: vec![("a".to_string(), Shape::Number)]
        }
    );
}

#[test]
fn mixed_primitive_array_becomes_array_mixed() {
    // Property: [1, "a", true] has three different JSON type tags;
    // the inferred shape must be ArrayMixed rather than silently
    // asserting the first element's type (Number) for all items.
    let s = infer_shape(&json!([1, "a", true]), DEFAULT_MAX_DEPTH);
    assert_eq!(s, Shape::ArrayMixed { len: 3 });
    assert_eq!(
        format_shape(&s, 0),
        vec!["array (3 items, mixed types)".to_string()],
    );
}

#[test]
fn null_mixed_into_array_becomes_array_mixed() {
    // Property: [null, 1] is heterogeneous (null vs number type tags).
    // Current behaviour: ArrayMixed, not Array { items: Null }.
    let s = infer_shape(&json!([null, 1]), DEFAULT_MAX_DEPTH);
    assert_eq!(s, Shape::ArrayMixed { len: 2 });
}

#[test]
fn homogeneous_object_array_uses_first_element_shape() {
    // Property: when all elements share the same JSON type tag
    // (object), ArrayMixed is NOT emitted. A future change to the
    // heuristic that accidentally dropped to ArrayMixed for any
    // structural type would be caught here.
    let s = infer_shape(
        &json!([{"a": 1, "b": 2}, {"a": 3, "b": 4}]),
        DEFAULT_MAX_DEPTH,
    );
    let Shape::Array { len, .. } = s else {
        panic!("expected Array for homogeneous object array; got {s:?}");
    };
    assert_eq!(len, 2);
}

#[test]
fn object_fields_appear_in_key_order() {
    // Property: serde_json::Map without preserve_order gives us
    // alphabetical order. Pin the property so a future features
    // flip doesn't silently reorder the agent-facing output.
    let s = infer_shape(&json!({"z": 1, "a": 2, "m": 3}), DEFAULT_MAX_DEPTH);
    let Shape::Object { fields } = s else {
        panic!("expected Object");
    };
    let keys: Vec<_> = fields.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["a", "m", "z"]);
}

#[test]
fn depth_cap_zero_makes_even_primitives_stay_primitive() {
    // Property: primitives at root never hit the opacity path;
    // only containers do.
    assert_eq!(infer_shape(&json!(42), 0), Shape::Number);
    assert_eq!(infer_shape(&json!("x"), 0), Shape::String);
}

#[test]
fn depth_cap_truncates_deep_objects() {
    // depth cap 1 means: root is fine, children of children
    // become opaque.
    let deep = json!({"outer": {"inner": {"deeper": {"a": 1}}}});
    let s = infer_shape(&deep, 1);
    let Shape::Object { fields } = s else {
        panic!("expected Object at root");
    };
    assert_eq!(fields.len(), 1);
    // At depth 1, we still describe the outer field's shape —
    // it's a container, but the recursion-budget check happens
    // at depth == max, so the outer's CHILDREN become opaque.
    let (_, outer_shape) = &fields[0];
    assert!(
        matches!(outer_shape, Shape::ObjectOpaque { .. }),
        "expected outer to be opaque at depth 1; got {outer_shape:?}"
    );
}

#[test]
fn depth_cap_truncates_deep_arrays() {
    let deep = json!([[[[1]]]]);
    let s = infer_shape(&deep, 1);
    let Shape::Array { items, len } = s else {
        panic!("expected Array");
    };
    assert_eq!(len, 1);
    assert!(
        matches!(items.as_ref(), Shape::ArrayOpaque { .. }),
        "inner should be opaque once we cross the depth budget"
    );
}

// ---- format_shape ----

#[test]
fn primitive_renders_as_single_line() {
    assert_eq!(format_shape(&Shape::Number, 0), vec!["number".to_string()]);
    assert_eq!(
        format_shape(&Shape::String, 2),
        vec!["    string".to_string()]
    );
}

#[test]
fn empty_object_says_empty() {
    assert_eq!(
        format_shape(&Shape::Object { fields: vec![] }, 0),
        vec!["object (empty)".to_string()]
    );
}

#[test]
fn object_with_primitive_field_is_compact() {
    let s = Shape::Object {
        fields: vec![("name".to_string(), Shape::String)],
    };
    assert_eq!(format_shape(&s, 0), vec!["name: string".to_string()]);
}

#[test]
fn object_with_null_field_infers_and_renders_correctly() {
    // Property: a null-valued field infers to Shape::Null and renders
    // as "field: null" inline (compact path). Previously untested;
    // the inference path is trivial but the compact rendering of Null
    // inside an object was unverified.
    let s = infer_shape(&json!({"present": null}), DEFAULT_MAX_DEPTH);
    assert_eq!(
        s,
        Shape::Object {
            fields: vec![("present".to_string(), Shape::Null)]
        }
    );
    assert_eq!(
        format_shape(&s, 0),
        vec!["present: null".to_string()],
        "null-valued field must render as compact 'field: null' line"
    );
}

#[test]
fn array_of_objects_renders_with_nested_fields() {
    // Reflects the common MCP pattern: { tools: [ {name, description}, ...] }
    let s = Shape::Array {
        items: Box::new(Shape::Object {
            fields: vec![
                ("name".to_string(), Shape::String),
                ("score".to_string(), Shape::Number),
            ],
        }),
        len: 42,
    };
    let out = format_shape(&s, 0);
    assert_eq!(
        out,
        vec![
            "array (42 items) of:".to_string(),
            "  name: string".to_string(),
            "  score: number".to_string(),
        ]
    );
}

#[test]
fn opaque_container_shows_count_and_truncated_tag() {
    assert_eq!(
        format_shape(&Shape::ArrayOpaque { len: 7 }, 0),
        vec!["array (7 items, truncated)".to_string()]
    );
    assert_eq!(
        format_shape(&Shape::ObjectOpaque { key_count: 12 }, 0),
        vec!["object (12 keys, truncated)".to_string()]
    );
}

#[test]
fn deeply_nested_object_indents_correctly() {
    // Property: each structural level adds exactly two spaces of
    // indentation. No double-indents or missing levels.
    let s = Shape::Object {
        fields: vec![(
            "outer".to_string(),
            Shape::Object {
                fields: vec![("inner".to_string(), Shape::String)],
            },
        )],
    };
    let out = format_shape(&s, 0);
    assert_eq!(
        out,
        vec!["outer: object".to_string(), "  inner: string".to_string(),]
    );
}

// ---- round-trip-ish properties ----

#[test]
fn infer_then_format_is_stable_on_small_inputs() {
    // Property: the same input always produces the same formatted
    // output (no clock, no random, no hashmap ordering randomness
    // leaking through). Run twice, compare.
    let v = json!({
        "results": [
            {"id": "a", "score": 0.9, "tags": ["x", "y"]},
            {"id": "b", "score": 0.8, "tags": ["z"]},
        ],
        "total": 2,
    });
    let a = format_shape(&infer_shape(&v, DEFAULT_MAX_DEPTH), 0);
    let b = format_shape(&infer_shape(&v, DEFAULT_MAX_DEPTH), 0);
    assert_eq!(a, b, "render must be deterministic");
    // Spot-check the output looks reasonable.
    assert!(a.iter().any(|l| l.contains("results:")));
    assert!(a.iter().any(|l| l.contains("total: number")));
}

#[test]
fn format_never_includes_raw_data_only_types_and_counts() {
    // Load-bearing property: the WHOLE POINT of offloading is that
    // the summary doesn't leak the payload. If a future refactor
    // accidentally stringifies a value into the summary, this
    // test catches it.
    let v = json!({
        "secret": "do not leak this",
        "password": "super-secret-12345",
        "count": 999_999,
    });
    let out = format_shape(&infer_shape(&v, DEFAULT_MAX_DEPTH), 0).join("\n");
    assert!(
        !out.contains("do not leak this"),
        "string values must not appear in summary; got: {out}"
    );
    assert!(
        !out.contains("super-secret-12345"),
        "string values must not appear in summary; got: {out}"
    );
    assert!(
        !out.contains("999999") && !out.contains("999_999"),
        "number values must not appear in summary; got: {out}"
    );
}
