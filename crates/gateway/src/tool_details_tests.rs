use std::sync::Arc;

use super::*;

fn tool(name: &str, description: Option<&str>) -> Tool {
    let mut t = Tool::default();
    t.name = name.to_string().into();
    t.description = description.map(|s| s.to_string().into());
    t.input_schema = Arc::new(
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {"x": {"type": "number"}},
            "required": ["x"]
        }))
        .expect("static schema is valid"),
    );
    t
}

#[test]
fn find_by_sanitized_name_matches_exact() {
    let tools = vec![tool("search_docs", None), tool("get-merge-request", None)];
    assert_eq!(
        find_by_lua_name(&tools, "get_merge_request")
            .expect("present")
            .name
            .as_ref(),
        "get-merge-request"
    );
}

#[test]
fn find_by_sanitized_name_returns_not_found_for_missing() {
    let tools = vec![tool("a", None)];
    assert_eq!(
        find_by_lua_name(&tools, "nonexistent"),
        Err(LookupError::NotFound)
    );
}

#[test]
fn find_by_sanitized_name_returns_ambiguous_for_collision() {
    // Two upstream tools sanitize to the same identifier. Returning
    // "the first one" would let the agent inspect tool A and the
    // runtime dispatch to tool B (or vice versa) — silent capability
    // bypass. Refuse the lookup; surface every offender by its
    // original name so the operator knows what to rename upstream.
    let tools = vec![
        tool("get-issue", Some("a")),
        tool("get.issue", Some("b")),
        tool("plain", None),
    ];
    match find_by_lua_name(&tools, "get_issue") {
        Err(LookupError::Ambiguous { originals }) => {
            assert!(
                originals.contains(&"get-issue".to_string()),
                "got {originals:?}"
            );
            assert!(
                originals.contains(&"get.issue".to_string()),
                "got {originals:?}"
            );
            assert_eq!(originals.len(), 2, "got {originals:?}");
        }
        other => panic!("expected Ambiguous; got {other:?}"),
    }
}

#[test]
fn sanitized_tool_names_round_trip_through_sanitizer() {
    let tools = vec![
        tool("data.server", None),
        tool("my-tool", None),
        tool("plain", None),
    ];
    let names = sanitized_tool_names(&tools);
    // Sorted and unique.
    assert_eq!(names, vec!["data_server", "my_tool", "plain"]);
}

#[test]
fn sanitized_tool_names_excludes_collisions() {
    // Colliding tools are not callable, so they must NOT appear in
    // the "available tools: ..." diagnostic — listing them as
    // available would mislead the agent into trying tool-details on
    // a name that returns Ambiguous.
    let tools = vec![
        tool("ok", None),
        tool("get-issue", None),
        tool("get.issue", None),
    ];
    let names = sanitized_tool_names(&tools);
    assert_eq!(names, vec!["ok"], "colliding 'get_issue' must be excluded");
}

#[test]
fn partition_separates_unique_from_collisions() {
    let tools = vec![
        tool("ok", None),
        tool("get-issue", None),
        tool("get.issue", None),
        tool("plain", None),
    ];
    let (unique, collisions) = partition_by_sanitized_name(&tools);
    let unique_names: Vec<&str> = unique.iter().map(|t| t.name.as_ref()).collect();
    // Order isn't guaranteed by the partition itself; sort for compare.
    let mut sorted_unique = unique_names.clone();
    sorted_unique.sort();
    assert_eq!(sorted_unique, vec!["ok", "plain"]);

    assert_eq!(collisions.len(), 1, "exactly one colliding sanitized name");
    let originals = collisions.get("get_issue").expect("get_issue collision");
    assert_eq!(originals.len(), 2);
    assert!(originals.contains(&"get-issue".to_string()));
    assert!(originals.contains(&"get.issue".to_string()));
}

#[test]
fn render_tool_summary_lists_collisions_under_unavailable() {
    // Property: an agent reading list-server-tools must see that
    // colliding tools exist on the upstream (otherwise debugging
    // upstream config is impossible) AND must understand they are
    // not callable. Two distinct sections.
    let tools = vec![
        tool("ok", Some("works")),
        tool("get-issue", Some("first")),
        tool("get.issue", Some("second")),
    ];
    let out = render_tool_summary(&tools);
    assert!(
        out.contains("- ok: works"),
        "unique tool must appear in the available section: {out}"
    );
    assert!(
        out.contains("unavailable"),
        "colliding tools must be flagged in an unavailable section: {out}"
    );
    assert!(
        out.contains("get_issue"),
        "the colliding sanitized name must be shown so the operator can fix it upstream: {out}"
    );
    assert!(
        out.contains("get-issue") && out.contains("get.issue"),
        "both original names must be shown under the collision: {out}"
    );
}

#[test]
fn render_tool_summary_empty_is_friendly() {
    assert_eq!(render_tool_summary(&[]), "(this upstream exposes no tools)");
}

#[test]
fn render_tool_summary_includes_sanitized_names() {
    let tools = vec![tool("my-tool", Some("Does a thing")), tool("quiet", None)];
    let out = render_tool_summary(&tools);
    assert!(out.contains("- my_tool: Does a thing"), "got:\n{out}");
    assert!(out.contains("- quiet\n"), "got:\n{out}");
}

#[test]
fn render_tool_summary_preserves_full_descriptions() {
    // The consumer is an agent that has already narrowed to one
    // upstream by calling list-server-tools; there's no reason to
    // hide content. Real-world trigger: Glean's `search` tool has
    // disambiguating information ("Slack, Jira, Confluence, Drive
    // ...") several sentences into its description. First-sentence
    // truncation used to hide it and force a tool-details round
    // trip. Full descriptions removes that friction.
    let glean_search = "Search the Glean index for content. \
        Use this when looking for internal documentation, Slack \
        messages, Jira tickets, Confluence pages, and Google \
        Drive files. Supports natural-language queries.";
    let tools = vec![tool("search", Some(glean_search))];
    let out = render_tool_summary(&tools);
    assert!(
        out.contains("Slack messages"),
        "middle-of-description content must survive:\n{out}"
    );
    assert!(
        out.contains("natural-language queries"),
        "last-sentence content must survive:\n{out}"
    );
    assert!(
        !out.contains("..."),
        "no truncation marker should appear:\n{out}"
    );
}

#[test]
fn render_tool_summary_flattens_whitespace_to_one_line_per_tool() {
    // One-tool-per-line is load-bearing for downstream parsing: an
    // agent splitting on '\n' must get one entry per tool. Tabs,
    // newlines, and runs of spaces in the upstream description all
    // collapse to single spaces so the per-tool line stays intact.
    let multi_para = "Paragraph one.\n\n\tParagraph two\n  with  \
        indented continuation.";
    let tools = vec![tool("t", Some(multi_para))];
    let out = render_tool_summary(&tools);

    // Exactly one content line for this tool (plus trailing
    // newline from the renderer's format!).
    let content_lines: Vec<&str> = out.lines().filter(|l| l.starts_with("- t:")).collect();
    assert_eq!(
        content_lines.len(),
        1,
        "tool entry must occupy exactly one line:\n{out}"
    );
    assert_eq!(
        content_lines[0], "- t: Paragraph one. Paragraph two with indented continuation.",
        "whitespace must collapse to single spaces:\n{out}"
    );
}

#[test]
fn collapse_whitespace_is_idempotent() {
    // Property: compressing an already-compressed string is a no-op.
    // Catches regressions where the function starts adding or
    // removing characters it shouldn't.
    let inputs = [
        "",
        "already clean",
        "   leading and trailing   ",
        "multi\n\nline\twith\ttabs",
        "emoji 🔍 and non-ascii café",
    ];
    for raw in inputs {
        let once = collapse_whitespace(raw);
        let twice = collapse_whitespace(&once);
        assert_eq!(once, twice, "not idempotent for input {raw:?}");
    }
}

#[test]
fn collapse_whitespace_is_byte_safe_on_multibyte_input() {
    // No panics on arbitrary UTF-8. A byte-slice-based implementation
    // would panic if a truncation point fell in the middle of a
    // multi-byte character; split_whitespace operates on chars, so
    // this is structurally safe, but pin a test so a future
    // "optimization" back to byte slicing is caught.
    let s = "🔍".repeat(50); // 200 bytes, 50 chars, no ASCII
    let out = collapse_whitespace(&s);
    assert_eq!(out, s, "pure non-whitespace input passes through");
}

#[test]
fn render_tool_details_includes_schema_and_sanitized_name() {
    let t = tool("get-issue", Some("Get a Jira issue by key"));
    let rendered = render_tool_details(&t, None);

    // Parse back as JSON to assert structure without getting tangled
    // in whitespace.
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    assert_eq!(value["luaToolName"], "get_issue");
    assert_eq!(value["originalName"], "get-issue");
    assert_eq!(value["description"], "Get a Jira issue by key");
    assert_eq!(value["inputSchema"]["type"], "object");
}

#[test]
fn render_tool_details_omits_original_name_when_unchanged() {
    let t = tool("plain_name", None);
    let rendered = render_tool_details(&t, None);
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    assert!(
        value.get("originalName").is_none(),
        "originalName must only appear when it differs from lua-sanitized form"
    );
}

#[test]
fn render_tool_details_omits_response_shape_when_none() {
    // Property: when no shape info is available (no prior inspect,
    // no schema signal), the fields are ABSENT — not null, not
    // "unknown" — because honest absence is better than confident
    // guessing. An agent that sees the fields missing knows to try
    // the tool and find out.
    let t = tool("plain", None);
    let rendered = render_tool_details(&t, None);
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    assert!(
        value.get("responseShape").is_none(),
        "responseShape must be absent when shape is unknown: {rendered}"
    );
    assert!(
        value.get("responseShapeHint").is_none(),
        "responseShapeHint must be absent when shape is unknown: {rendered}"
    );
}

#[test]
fn render_tool_details_emits_text_json_tag_and_hint() {
    // Load-bearing: the tag is agent-pattern-matchable, the hint is
    // human-readable-by-an-agent. Both must be present for the
    // agent to (a) route on the tag in code and (b) know which
    // builtin to reach for.
    let t = tool("get_incident", Some("Fetch incident details"));
    let rendered = render_tool_details(&t, Some(ResponseShape::TextJson));
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    assert_eq!(value["responseShape"], "text-json");
    let hint = value["responseShapeHint"]
        .as_str()
        .expect("responseShapeHint must be a string");
    assert!(
        hint.contains("unwrap_content") && hint.contains("json_decode"),
        "text-json hint must name both builtins: {hint}"
    );
}

#[test]
fn render_tool_details_emits_structured_tag_and_hint() {
    let t = tool("get_something", None);
    let rendered = render_tool_details(&t, Some(ResponseShape::Structured));
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    assert_eq!(value["responseShape"], "structured");
    let hint = value["responseShapeHint"]
        .as_str()
        .expect("responseShapeHint must be a string");
    assert!(
        hint.contains("structured_content"),
        "structured hint must point at structured_content: {hint}"
    );
}

// ---- summarize_parameters ----

#[test]
fn summarize_parameters_returns_none_for_empty_schema() {
    let schema = serde_json::json!({});
    assert!(summarize_parameters(&schema).is_none());
}

#[test]
fn summarize_parameters_returns_none_for_empty_properties() {
    // Tools that take no arguments ship `{"type":"object", "properties":{}}`.
    // No reason to emit an empty `parameters: []` — the whole point
    // of the summary is to save the agent a schema walk, and an
    // empty list says the same thing as the absent field.
    let schema = serde_json::json!({
        "type": "object",
        "properties": {}
    });
    assert!(summarize_parameters(&schema).is_none());
}

#[test]
fn summarize_parameters_lists_required_fields_first() {
    // F5 is about helping agents find the right param name quickly.
    // Required-first ordering is the single highest-value
    // disambiguation: if a tool has five optional params and one
    // required, the required is what the agent MUST supply, and
    // it should be visually at the top.
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "optional_second": {"type": "string"},
            "optional_first": {"type": "string"},
            "required_field": {"type": "string"},
        },
        "required": ["required_field"]
    });
    let summary = summarize_parameters(&schema).expect("should produce summary");
    let arr = summary.as_array().expect("array");
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["name"], "required_field");
    assert_eq!(arr[0]["required"], true);
    // Optionals alphabetical within their group.
    assert_eq!(arr[1]["name"], "optional_first");
    assert_eq!(arr[2]["name"], "optional_second");
}

#[test]
fn summarize_parameters_preserves_type_description_and_enum() {
    // The three pieces of information an agent uses to pick the
    // right parameter: name, type, and (when present) allowed
    // values. Description is the tie-breaker for ambiguous names.
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "level": {
                "type": "string",
                "enum": ["debug", "info", "warn", "error"],
                "description": "Log level filter."
            }
        }
    });
    let summary = summarize_parameters(&schema).expect("should produce summary");
    let arr = summary.as_array().expect("array");
    let level = &arr[0];
    assert_eq!(level["name"], "level");
    assert_eq!(level["type"], "string");
    assert_eq!(level["description"], "Log level filter.");
    let enum_arr = level["enum"].as_array().expect("enum array");
    assert_eq!(enum_arr.len(), 4);
    assert_eq!(enum_arr[0], "debug");
}

#[test]
fn summarize_parameters_renders_union_types() {
    // JSONSchema allows `type: [...]` for nullable unions. Render
    // as `string|null` so the agent sees at a glance that null is
    // acceptable. Three-way unions are rare but supported.
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "maybe_string": {"type": ["string", "null"]}
        }
    });
    let summary = summarize_parameters(&schema).expect("should produce summary");
    let arr = summary.as_array().expect("array");
    assert_eq!(arr[0]["type"], "string|null");
}

#[test]
fn summarize_parameters_handles_any_of_as_union() {
    // `anyOf` / `oneOf` without a `type` key fall back to a plain
    // "union" label. Agent reads the full inputSchema if they need
    // the variant list — the summary isn't trying to replace the
    // schema, just to save a walk for the common cases.
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "value": {
                "anyOf": [
                    {"type": "string"},
                    {"type": "number"}
                ]
            }
        }
    });
    let summary = summarize_parameters(&schema).expect("should produce summary");
    let arr = summary.as_array().expect("array");
    assert_eq!(arr[0]["type"], "union");
}

#[test]
fn summarize_parameters_omits_description_when_empty() {
    // Don't emit `description: ""` — an empty string is as
    // useful as absent and adds noise. Pinned because a future
    // refactor might treat `Some("")` as "present".
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "x": {"type": "number", "description": ""}
        }
    });
    let summary = summarize_parameters(&schema).expect("should produce summary");
    let arr = summary.as_array().expect("array");
    assert!(
        arr[0].get("description").is_none(),
        "empty description must be omitted: {:?}",
        arr[0]
    );
}

#[test]
fn summarize_parameters_handles_nested_objects_without_flattening() {
    // Parameter whose schema is an object: we show `type: "object"`
    // and stop. Flattening `filter.status` to a top-level
    // `filter_status` entry would risk collision with a real
    // top-level `filter_status` and would also hide the nesting.
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "filter": {
                "type": "object",
                "properties": {
                    "status": {"type": "string"}
                }
            }
        }
    });
    let summary = summarize_parameters(&schema).expect("should produce summary");
    let arr = summary.as_array().expect("array");
    assert_eq!(arr.len(), 1, "only top-level params appear in the summary");
    assert_eq!(arr[0]["name"], "filter");
    assert_eq!(arr[0]["type"], "object");
}

#[test]
fn render_tool_details_includes_parameters_summary() {
    // End-to-end pin: the renderer must ship `parameters` alongside
    // `inputSchema`. Without this, the summary is dead code; with
    // it, agents get the scannable view for free.
    let t = tool("search_docs", Some("Search docs"));
    let rendered = render_tool_details(&t, None);
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    let params = value
        .get("parameters")
        .expect("parameters field must be present for tools with non-empty props");
    let arr = params.as_array().expect("parameters must be an array");
    assert_eq!(arr.len(), 1, "test fixture has one property: x");
    assert_eq!(arr[0]["name"], "x");
    assert_eq!(arr[0]["required"], true);
    assert_eq!(arr[0]["type"], "number");
}

#[test]
fn render_tool_details_includes_output_schema_when_present() {
    // Symmetric to `render_tool_details_includes_parameters_summary`: when
    // the upstream declares an outputSchema, the renderer must surface it
    // verbatim so agents can branch on the declared shape before probing.
    // A future regression that accidentally drops or mangles the field
    // would not be caught by the other render_tool_details tests, since
    // the `tool()` helper leaves output_schema absent.
    let mut t = tool("get_thing", Some("Fetches a thing"));
    t.output_schema = Some(Arc::new(
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {"id": {"type": "string"}}
        }))
        .expect("static schema is valid"),
    ));
    let rendered = render_tool_details(&t, None);
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    assert_eq!(value["outputSchema"]["type"], "object");
    assert_eq!(value["outputSchema"]["properties"]["id"]["type"], "string");
}

#[test]
fn render_tool_details_omits_output_schema_when_absent() {
    // Complement to the "when present" test: honest absence is the
    // contract, same as description / responseShape / parameters.
    let t = tool("plain", None);
    let rendered = render_tool_details(&t, None);
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    assert!(
        value.get("outputSchema").is_none(),
        "outputSchema must be absent when the upstream declares none: {rendered}"
    );
}

#[test]
fn render_tool_details_omits_description_when_absent() {
    // Property: same honest-absence contract as responseShape and
    // outputSchema. A tool with no description must NOT get a
    // `"description": null` or `"description": ""` entry — the field
    // is simply absent so agents that branch on field presence see
    // the correct signal.
    let t = tool("plain", None);
    let rendered = render_tool_details(&t, None);
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    assert!(
        value.get("description").is_none(),
        "description must be absent when the tool has none: {rendered}"
    );
}

#[test]
fn render_tool_details_omits_parameters_when_schema_has_no_properties() {
    // Property: the "absent is better than empty" contract for the
    // parameters summary. When summarize_parameters returns None
    // (schema has no `properties` or empty `properties: {}`), the
    // rendered JSON must not emit `"parameters": null` or
    // `"parameters": []`. Agents check field presence to decide
    // whether they can skip walking the raw inputSchema, so this
    // contract is load-bearing.
    let mut t = tool("no_args", Some("Takes no arguments"));
    t.input_schema = Arc::new(
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {}
        }))
        .expect("static schema is valid"),
    );
    let rendered = render_tool_details(&t, None);
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    assert!(
        value.get("parameters").is_none(),
        "parameters must be absent when the schema has no properties: {rendered}"
    );
}

#[test]
fn render_tool_details_emits_block_count_for_multi_block_shape() {
    // Property: the observed max block count survives from the cache
    // (max-merged in response_shape_cache) all the way to the agent.
    // Without this, an agent receiving `responseShape: "multi-block"`
    // knows to iterate but has no bound and must re-probe the raw
    // response to get one.
    let t = tool("t", None);
    let rendered = render_tool_details(&t, Some(ResponseShape::MultiBlock { len: 5 }));
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    assert_eq!(value["responseShape"], "multi-block");
    assert_eq!(
        value["responseShapeBlockCount"], 5,
        "MultiBlock len must reach the agent: {rendered}"
    );
}

#[test]
fn render_tool_details_omits_block_count_for_non_multi_block_shapes() {
    // Property: responseShapeBlockCount is MultiBlock-specific. For any
    // other shape it must be absent — not zero, not null — so agents
    // that branch on field presence to decide "is this a multi-block
    // iteration job?" get a clean signal.
    for shape in [
        ResponseShape::TextProse,
        ResponseShape::TextJson,
        ResponseShape::Structured,
        ResponseShape::Mixed,
    ] {
        let t = tool("t", None);
        let rendered = render_tool_details(&t, Some(shape));
        let value: serde_json::Value =
            serde_json::from_str(&rendered).expect("details must be valid JSON");
        assert!(
            value.get("responseShapeBlockCount").is_none(),
            "responseShapeBlockCount must be absent for {shape:?}: {rendered}"
        );
    }
}

#[test]
fn render_tool_details_omits_block_count_when_shape_is_none() {
    // Complement to the per-variant test: when we have no shape info
    // at all, the block-count field must also be absent.
    let t = tool("t", None);
    let rendered = render_tool_details(&t, None);
    let value: serde_json::Value =
        serde_json::from_str(&rendered).expect("details must be valid JSON");
    assert!(
        value.get("responseShapeBlockCount").is_none(),
        "responseShapeBlockCount must be absent when no shape is known: {rendered}"
    );
}

#[test]
fn render_tool_details_response_shape_tag_is_single_token() {
    // Agents doing string-equality against known tags must get a
    // whitespace-free token. Pinned defensively so a future "helpful"
    // multi-word tag doesn't break the contract.
    for shape in [
        ResponseShape::TextProse,
        ResponseShape::TextJson,
        ResponseShape::MultiBlock { len: 3 },
        ResponseShape::Structured,
        ResponseShape::Mixed,
    ] {
        let t = tool("t", None);
        let rendered = render_tool_details(&t, Some(shape));
        let value: serde_json::Value =
            serde_json::from_str(&rendered).expect("details must be valid JSON");
        let tag = value["responseShape"]
            .as_str()
            .expect("responseShape must be a string");
        assert!(
            !tag.contains(char::is_whitespace),
            "responseShape tag must be single-token (got {tag:?}) for {shape:?}"
        );
    }
}
