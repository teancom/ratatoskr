//! Tests exercise the Functional Core in isolation (no Lua, no
//! runtime). The Lua-level wiring is smoke-tested separately in
//! `runtime::tests` where a fake `_gateway.expand_template` call
//! exercises the host closure.

use super::*;
use serde_json::json;

// ---------- Level 1: simple variable expansion ----------

#[test]
fn level_1_simple_scalar() {
    let got = expand_and_namespace(
        "rootly",
        "incident://{incident_id}",
        &json!({ "incident_id": "42" }),
    )
    .expect("expand");
    // The expanded form is `incident://42`, which then round-trips
    // through the resource_uri codec. Writing the literal percent-
    // encoded form pins both the expansion AND the namespacing.
    assert_eq!(got, "mcp-proxy://rootly/incident%3A%2F%2F42");
}

#[test]
fn level_1_number_param_is_stringified() {
    // Lua converts integer literals to JSON integers; the template
    // expander wants a Scalar string. Round-tripping via to_string
    // is the contract (handled by uri-template's serde impl).
    let got = expand_and_namespace("u", "x://{id}", &json!({ "id": 42 })).expect("expand");
    let (name, uri) = mcp_client::resource_uri::decode(&got).expect("roundtrip");
    assert_eq!(name, "u");
    assert_eq!(uri, "x://42");
}

#[test]
fn level_1_bool_param_is_stringified() {
    let got = expand_and_namespace("u", "x://{flag}", &json!({ "flag": true })).expect("expand");
    let (_, uri) = mcp_client::resource_uri::decode(&got).expect("roundtrip");
    assert_eq!(uri, "x://true");
}

// ---------- Level 2: reserved expansion ----------

#[test]
fn level_2_reserved_expansion_preserves_reserved_chars() {
    // `{+path}` is Level 2: reserved characters in `path` do NOT
    // get percent-encoded by the expander. The outer codec then
    // encodes the whole thing (we namespace aggressively). Key
    // property: a path containing `/` expands without the `/`
    // being `%2F`'d during the RFC 6570 step.
    let got = expand_and_namespace("u", "file://{+path}", &json!({ "path": "/etc/passwd" }))
        .expect("expand");
    let (_, uri) = mcp_client::resource_uri::decode(&got).expect("roundtrip");
    assert_eq!(uri, "file:///etc/passwd");
}

// ---------- Level 3: multiple variables ----------

#[test]
fn level_3_multiple_scalars_in_one_template() {
    let got = expand_and_namespace(
        "api",
        "https://example.com/{user}/{repo}",
        &json!({ "user": "octocat", "repo": "hello-world" }),
    )
    .expect("expand");
    let (_, uri) = mcp_client::resource_uri::decode(&got).expect("roundtrip");
    assert_eq!(uri, "https://example.com/octocat/hello-world");
}

#[test]
fn level_3_form_style_query_expansion() {
    // `{?q,limit}` is the form-style query expansion. `limit=10`
    // and `q=rust` both appear in the output, separated by `&`.
    let got = expand_and_namespace(
        "api",
        "https://example.com/search{?q,limit}",
        &json!({ "q": "rust", "limit": 10 }),
    )
    .expect("expand");
    let (_, uri) = mcp_client::resource_uri::decode(&got).expect("roundtrip");
    // Order of query vars follows the template's declaration order.
    assert_eq!(uri, "https://example.com/search?q=rust&limit=10");
}

// ---------- Level 4: explode modifier, lists, associative arrays ----------

#[test]
fn level_4_list_with_explode_modifier() {
    // `{?tag*}` explodes an array into `?tag=a&tag=b&tag=c`.
    let got = expand_and_namespace(
        "api",
        "https://example.com/search{?tag*}",
        &json!({ "tag": ["rust", "mcp", "uri"] }),
    )
    .expect("expand");
    let (_, uri) = mcp_client::resource_uri::decode(&got).expect("roundtrip");
    assert_eq!(uri, "https://example.com/search?tag=rust&tag=mcp&tag=uri");
}

#[test]
fn level_4_associative_array_with_explode() {
    // `{?params*}` explodes an object into `?k=v&k2=v2`.
    // The uri-template crate converts associative-array (Keys) values
    // through BTreeValues, so keys are sorted alphabetically at
    // expansion time — limit appears before q regardless of the order
    // they appear in the serde_json literal.
    let got = expand_and_namespace(
        "api",
        "https://example.com/search{?params*}",
        &json!({ "params": { "q": "rust", "limit": "10" } }),
    )
    .expect("expand");
    let (_, uri) = mcp_client::resource_uri::decode(&got).expect("roundtrip");
    assert_eq!(uri, "https://example.com/search?limit=10&q=rust");
}

// ---------- Missing / null params ----------

#[test]
fn missing_variable_expands_to_empty() {
    // Per RFC 6570 §3.2.1, an undefined variable is "null" and
    // expands to nothing. The template `x://{missing}` with no
    // `missing` key becomes `x://` — not an error.
    let got = expand_and_namespace("u", "x://{missing}", &json!({})).expect("expand");
    let (_, uri) = mcp_client::resource_uri::decode(&got).expect("roundtrip");
    assert_eq!(uri, "x://");
}

#[test]
fn null_valued_param_is_treated_as_undefined() {
    // Same as missing: JSON null → skip the variable entirely.
    let got = expand_and_namespace("u", "x://{v}", &json!({ "v": null })).expect("expand");
    let (_, uri) = mcp_client::resource_uri::decode(&got).expect("roundtrip");
    assert_eq!(uri, "x://");
}

#[test]
fn extra_params_are_ignored() {
    // Template has `{a}` but params has {a, unused}. The unused
    // key is silently ignored — matches RFC 6570 §1.2.
    let got = expand_and_namespace("u", "x://{a}", &json!({ "a": "ok", "unused": "whatever" }))
        .expect("expand");
    let (_, uri) = mcp_client::resource_uri::decode(&got).expect("roundtrip");
    assert_eq!(uri, "x://ok");
}

// ---------- Error paths ----------

#[test]
fn empty_server_errors() {
    let err = expand_and_namespace("", "x://{a}", &json!({ "a": "b" }))
        .expect_err("empty server must produce an error");
    assert_eq!(err, ExpandTemplateError::EmptyServer);
}

#[test]
fn empty_template_errors() {
    let err = expand_and_namespace("u", "", &json!({}))
        .expect_err("empty template must produce an error");
    assert_eq!(err, ExpandTemplateError::EmptyTemplate);
}

#[test]
fn malformed_template_returns_parse_failed() {
    // `{` without a closing `}` is not a valid RFC 6570 template.
    // The parser rejects it with a structured error that names the
    // template text.
    let err = expand_and_namespace("u", "x://{unclosed", &json!({ "unclosed": "v" }))
        .expect_err("unclosed template must produce a parse error");
    match err {
        ExpandTemplateError::ParseFailed { template, .. } => {
            assert_eq!(template, "x://{unclosed");
        }
        other => panic!("expected ParseFailed, got {other:?}"),
    }
}

#[test]
fn non_object_params_rejected_at_root() {
    // A plain string can't be `params` — the Lua host normally
    // guarantees an object, but direct FC callers get a clear
    // error rather than a panic.
    let err = expand_and_namespace("u", "x://{a}", &json!("just-a-string"))
        .expect_err("non-object params must produce an error");
    match err {
        ExpandTemplateError::UnsupportedParamValue { key, reason } => {
            assert_eq!(key, "<root>");
            assert_eq!(
                reason, "params must be an object (Lua table with string keys); got string",
                "reason should name the rejected shape"
            );
        }
        other => panic!("expected UnsupportedParamValue, got {other:?}"),
    }
}

#[test]
fn nested_array_in_list_rejected() {
    // `{tag*}` wants a list of scalars, not a list of lists. The
    // crate's ScalarValueVisitor rejects the nested array; we
    // surface that as UnsupportedParamValue keyed by `tag`.
    let err = expand_and_namespace("u", "x{?tag*}", &json!({ "tag": ["a", ["nested"], "c"] }))
        .expect_err("nested array in list must produce an error");
    match err {
        ExpandTemplateError::UnsupportedParamValue { key, .. } => {
            assert_eq!(key, "tag");
        }
        other => panic!("expected UnsupportedParamValue, got {other:?}"),
    }
}

#[test]
fn nested_object_in_assoc_array_rejected() {
    // Associative arrays want scalar values, not nested objects.
    let err = expand_and_namespace(
        "u",
        "x{?params*}",
        &json!({ "params": { "k": { "nested": "bad" } } }),
    )
    .expect_err("nested object in assoc array must produce an error");
    match err {
        ExpandTemplateError::UnsupportedParamValue { key, .. } => {
            assert_eq!(key, "params");
        }
        other => panic!("expected UnsupportedParamValue, got {other:?}"),
    }
}

#[test]
fn prefix_modifier_on_keys_value_returns_expand_failed() {
    // The uri-template-core crate rejects applying a prefix modifier
    // (`:N`) to a Keys (associative-array) value at expansion time.
    // This exercises the ExpandFailed code path through the real
    // parsed.expand(&values) call — not just the direct constructor
    // used in error_messages_are_lowercase_sentence_fragments.
    let err = expand_and_namespace("u", "x://{params:2}", &json!({ "params": { "a": "1" } }))
        .expect_err("prefix modifier on a Keys value must fail at expand time");
    match err {
        ExpandTemplateError::ExpandFailed { template, .. } => {
            assert_eq!(template, "x://{params:2}");
        }
        other => panic!("expected ExpandFailed, got {other:?}"),
    }
}

// ---------- Round-trip with the codec ----------

#[test]
fn expanded_form_always_decodable_without_error() {
    // Property pin: every expansion we produce must decode cleanly
    // back to (server, expanded) without error. This guards the
    // codec <-> expand_and_namespace contract — if either side
    // produces an output the other can't consume, this test fails.
    // Correctness of individual expansions is covered by the level-
    // specific tests above; this test focuses purely on the
    // encode/decode round-trip being lossless for the server name.
    let cases: &[(&str, &str, serde_json::Value)] = &[
        ("rootly", "incident://{id}", json!({"id": "42"})),
        (
            "rootly",
            "team://{team_id}",
            json!({"team_id": "dev-infra"}),
        ),
        (
            "api",
            "https://example.com/{u}/{r}",
            json!({"u": "octocat", "r": "hello-world"}),
        ),
        ("u", "x{?tag*}", json!({"tag": ["a", "b", "c"]})),
        ("u", "x://{+path}", json!({"path": "/etc/passwd"})),
    ];
    for (server, template, params) in cases {
        let namespaced = expand_and_namespace(server, template, params)
            .unwrap_or_else(|e| panic!("expand failed for {server:?}/{template:?}: {e}"));
        let (decoded_server, decoded_uri) = mcp_client::resource_uri::decode(&namespaced)
            .unwrap_or_else(|e| panic!("codec decode failed for {server:?}/{template:?}: {e}"));
        assert_eq!(&decoded_server, server);
        // We don't pin the exact expansion here (that's the
        // individual-level tests' job). What matters is that the
        // expansion is non-empty and the roundtrip is lossless for
        // the <server, expansion> pair.
        assert!(
            !decoded_uri.is_empty(),
            "expansion must be non-empty for {template:?}"
        );
    }
}

// ---------- Error message house style ----------

#[test]
fn error_messages_are_lowercase_sentence_fragments() {
    // Same house rule as `resource_uri::DecodeError`: lowercase
    // first letter, no trailing period. Makes composition as
    // `"_gateway.expand_template: {err}"` read cleanly.
    let samples = [
        ExpandTemplateError::EmptyServer,
        ExpandTemplateError::EmptyTemplate,
        ExpandTemplateError::ParseFailed {
            template: "x".to_string(),
            reason: "y".to_string(),
        },
        ExpandTemplateError::ExpandFailed {
            template: "x".to_string(),
            reason: "y".to_string(),
        },
        ExpandTemplateError::UnsupportedParamValue {
            key: "k".to_string(),
            reason: "r".to_string(),
        },
    ];
    for err in samples {
        let msg = err.to_string();
        let first = msg.chars().next().expect("non-empty");
        assert!(
            first.is_ascii_lowercase() || !first.is_alphabetic(),
            "message must start lowercase: {msg:?}"
        );
        assert!(!msg.ends_with('.'), "message must be a fragment: {msg:?}");
    }
}
