use super::*;

#[test]
fn headline_names_size_and_shape() {
    let s = Shape::Array {
        items: Box::new(Shape::Object { fields: vec![] }),
        len: 42,
    };
    let msg = render("exec-1", 60_000, &s);
    assert!(msg.starts_with("Result offloaded:"));
    assert!(msg.contains("60000 bytes"));
    assert!(msg.contains("array of 42 items"));
    assert!(msg.contains("Execution ID: exec-1"));
}

#[test]
fn human_bytes_covers_each_unit() {
    assert_eq!(human_bytes(0), "0 B");
    assert_eq!(human_bytes(1023), "1023 B");
    assert_eq!(human_bytes(1024), "1.00 KiB");
    assert_eq!(human_bytes(60_000), "58.59 KiB");
    assert_eq!(human_bytes(2 * 1024 * 1024), "2.00 MiB");
    assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.00 GiB");
}

#[test]
fn array_snippet_shows_slice_example() {
    let s = Shape::Array {
        items: Box::new(Shape::Object { fields: vec![] }),
        len: 10,
    };
    let msg = render("id-1", 70_000, &s);
    assert!(msg.contains("for i = 1, math.min(10, #data) do"));
    assert!(msg.contains("table.insert(subset, data[i])"));
}

#[test]
fn object_snippet_picks_top_level_fields() {
    let s = Shape::Object {
        fields: vec![
            ("id".to_string(), Shape::String),
            ("name".to_string(), Shape::String),
            ("score".to_string(), Shape::Number),
            ("extra".to_string(), Shape::Bool),
        ],
    };
    let msg = render("id-2", 80_000, &s);
    // Sample up to 3 fields in the example — the fourth is
    // intentionally omitted so the example stays concise.
    assert!(msg.contains("id = data.id"));
    assert!(msg.contains("name = data.name"));
    assert!(msg.contains("score = data.score"));
    assert!(
        !msg.contains("extra = data.extra"),
        "example should cap at 3 fields to avoid leaking the full shape"
    );
}

#[test]
fn string_snippet_shows_substring_example() {
    let msg = render("id-3", 60_000, &Shape::String);
    assert!(msg.contains("string.sub(data, 1, 1000)"));
}

#[test]
fn render_surfaces_structural_metadata_from_shape() {
    // Property: render()'s output is bounded to what the `Shape`
    // tree carries — field names, type tags, counts — plus the
    // execution ID, byte size, and the canned retrieval snippet.
    //
    // The raw-data-leak contract lives UPSTREAM in
    // `schema_inferrer` (see
    // `format_never_includes_raw_data_only_types_and_counts`
    // in crates/lua-runtime/src/schema_inferrer_tests.rs). `Shape`
    // itself cannot carry raw values: its variants hold only
    // type tags, field names, and counts. So there is no path
    // by which raw payload bytes could reach render() through
    // `shape` — which means this test cannot meaningfully assert
    // "raw data is absent"; that property is already pinned at
    // the shape-inference boundary.
    //
    // What render() DOES control is the promotion of structural
    // metadata (field names, item counts) into the rendered
    // text. That's what we pin here.
    let shape = Shape::Object {
        fields: vec![
            ("password".to_string(), Shape::String),
            ("count".to_string(), Shape::Number),
        ],
    };
    let msg = render("id-4", 123_456, &shape);
    // Field names are structural metadata and must appear.
    assert!(
        msg.contains("password"),
        "field name 'password' should surface as structural metadata: {msg}"
    );
    assert!(
        msg.contains("count"),
        "field name 'count' should surface as structural metadata: {msg}"
    );
    // The byte size and execution ID are the other inputs
    // render() controls — both must round-trip.
    assert!(msg.contains("123456"), "byte size must appear: {msg}");
    assert!(msg.contains("id-4"), "execution ID must appear: {msg}");
}

// ---- Envelope detection ----
//
// These tests pin the "drill into CallToolResult" behavior. The
// motivating bug: before envelope detection, a single-text
// CallToolResult whose text field was several MiB would produce a
// snippet like `result({ content = data.content, ... })` — which
// copies the entire envelope to the result, re-shipping the
// megabytes we just offloaded. The snippet must instead drill into
// `data.content[1].text` and take a bounded prefix.

/// Shape of a rmcp `CallToolResult` with a single text content block.
/// Matches what we actually saw from `GitLab.list_projects` (the
/// originating real-world evidence): 6 MiB of TSV inside a single
/// text content item.
fn single_text_envelope() -> Shape {
    Shape::Object {
        fields: vec![(
            "content".to_string(),
            Shape::Array {
                items: Box::new(Shape::Object {
                    fields: vec![
                        ("text".to_string(), Shape::String),
                        ("type".to_string(), Shape::String),
                    ],
                }),
                len: 1,
            },
        )],
    }
}

#[test]
fn single_text_envelope_drills_into_content_text() {
    let msg = render("env-1", 6_500_000, &single_text_envelope());
    assert!(
        msg.contains("string.sub(data.content[1].text, 1, 1000)"),
        "snippet must drill into data.content[1].text; got:\n{msg}"
    );
    // The generic "pick a few top-level fields" template must NOT
    // fire for envelope shapes.
    assert!(
        !msg.contains("content = data.content"),
        "generic object snippet must not re-ship the envelope: {msg}"
    );
}

#[test]
fn multi_content_envelope_iterates_bounded() {
    // Property: when content has multiple items (image + text,
    // repeated text blocks, etc.), the snippet walks them with a
    // bounded loop rather than drilling into index 1.
    let shape = Shape::Object {
        fields: vec![(
            "content".to_string(),
            Shape::Array {
                items: Box::new(Shape::Object {
                    fields: vec![
                        ("text".to_string(), Shape::String),
                        ("type".to_string(), Shape::String),
                    ],
                }),
                len: 25,
            },
        )],
    };
    let msg = render("env-2", 200_000, &shape);
    assert!(
        msg.contains("for i = 1, math.min(10, #data.content) do"),
        "snippet must walk data.content bounded by 10: {msg}"
    );
    assert!(msg.contains("table.insert(subset, data.content[i])"));
    assert!(
        msg.contains("(of 25)"),
        "snippet must surface the true item count so the agent can adjust: {msg}"
    );
}

#[test]
fn structured_content_envelope_drills_to_structured_field() {
    // Property: when structured_content is present, the snippet
    // points there — that's where the machine-parseable payload
    // lives; the `content` block is a human-readable mirror.
    let shape = Shape::Object {
        fields: vec![
            (
                "content".to_string(),
                Shape::Array {
                    items: Box::new(Shape::Object {
                        fields: vec![("text".to_string(), Shape::String)],
                    }),
                    len: 1,
                },
            ),
            (
                "structured_content".to_string(),
                Shape::Object {
                    fields: vec![
                        ("items".to_string(), Shape::ArrayOpaque { len: 42 }),
                        ("total".to_string(), Shape::Number),
                    ],
                },
            ),
        ],
    };
    let msg = render("env-3", 500_000, &shape);
    assert!(
        msg.contains("result(data.structured_content)"),
        "snippet must suggest data.structured_content: {msg}"
    );
    // structured_content's presence must dominate content inspection.
    assert!(
        !msg.contains("string.sub(data.content[1].text"),
        "structured_content must take precedence over content-text: {msg}"
    );
}

#[test]
fn structured_content_null_does_not_trigger_structured_branch() {
    // Defense in depth: if structured_content is present but null
    // (edge case from permissive upstreams), we must not suggest
    // drilling into it — fall back to content-text.
    let shape = Shape::Object {
        fields: vec![
            (
                "content".to_string(),
                Shape::Array {
                    items: Box::new(Shape::Object {
                        fields: vec![("text".to_string(), Shape::String)],
                    }),
                    len: 1,
                },
            ),
            ("structured_content".to_string(), Shape::Null),
        ],
    };
    let msg = render("env-3b", 60_000, &shape);
    assert!(
        msg.contains("string.sub(data.content[1].text, 1, 1000)"),
        "null structured_content must fall through to content text: {msg}"
    );
    assert!(
        !msg.contains("result(data.structured_content)"),
        "null structured_content must not be drilled into: {msg}"
    );
}

#[test]
fn opaque_content_array_len_one_drills_into_single_text() {
    // Property: a single-item opaque content array should route
    // to Envelope::SingleText, matching the non-opaque Array path.
    // This keeps the agent-facing snippet consistent: `len == 1`
    // in both branches produces the direct `data.content[1].text`
    // drill rather than a bounded-iteration loop over a
    // single-item array.
    let shape = Shape::Object {
        fields: vec![("content".to_string(), Shape::ArrayOpaque { len: 1 })],
    };
    let msg = render("env-4a", 100_000, &shape);
    assert!(
        msg.contains("string.sub(data.content[1].text, 1, 1000)"),
        "opaque content array of len 1 must drill into data.content[1].text: {msg}"
    );
    assert!(
        !msg.contains("for i = 1, math.min"),
        "len=1 opaque content must not produce a loop snippet: {msg}"
    );
}

#[test]
fn opaque_content_array_still_treated_as_envelope() {
    // Robustness: if the shape walker truncated `content`'s item
    // type (depth cap), we still get multi-content iteration rather
    // than the generic "pick fields" snippet. Better a bounded
    // iteration than re-shipping the envelope.
    let shape = Shape::Object {
        fields: vec![("content".to_string(), Shape::ArrayOpaque { len: 3 })],
    };
    let msg = render("env-4", 100_000, &shape);
    assert!(
        msg.contains("data.content"),
        "opaque content array must still produce an envelope snippet: {msg}"
    );
    assert!(
        !msg.contains("content = data.content"),
        "opaque content array must not fall through to the generic object snippet: {msg}"
    );
}

#[test]
fn non_envelope_object_uses_generic_snippet() {
    // Regression: non-envelope objects (no `content` field) must
    // keep the existing "pick a few fields" behavior. This pins the
    // back-compat path for anything stored via the execute offload
    // that isn't a CallToolResult envelope — e.g. the agent
    // returning a Lua table directly.
    let shape = Shape::Object {
        fields: vec![
            ("id".to_string(), Shape::String),
            ("name".to_string(), Shape::String),
            ("score".to_string(), Shape::Number),
        ],
    };
    let msg = render("env-5", 80_000, &shape);
    assert!(
        msg.contains("-- Example: pick a few top-level fields"),
        "non-envelope object must use the generic snippet: {msg}"
    );
    assert!(msg.contains("id = data.id"));
    assert!(
        !msg.contains("data.content"),
        "non-envelope object must not produce envelope-drill snippet: {msg}"
    );
}

#[test]
fn envelope_with_empty_content_array_still_drills() {
    // Boundary: content: [] should still produce the single-text
    // snippet (it's the best default for the "got an envelope,
    // don't know what's in it" case). The agent seeing
    // `data.content[1].text` and a length-zero array will get a
    // nil-index error — which is the correct, loud failure mode:
    // the offloaded envelope had no content at all, so there's
    // nothing to filter.
    let shape = Shape::Object {
        fields: vec![(
            "content".to_string(),
            Shape::Array {
                items: Box::new(Shape::Null),
                len: 0,
            },
        )],
    };
    let msg = render("env-6", 1000, &shape);
    assert!(
        msg.contains("data.content"),
        "empty content array must still produce an envelope snippet: {msg}"
    );
}
