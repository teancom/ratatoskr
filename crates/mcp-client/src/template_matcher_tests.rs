use super::*;

fn compile_ok(t: &str) -> CompiledTemplate {
    CompiledTemplate::compile(t).unwrap_or_else(|e| panic!("should compile '{t}': {e}"))
}

#[test]
fn literal_template_matches_only_itself() {
    let t = compile_ok("rootly://incidents");
    assert!(t.matches("rootly://incidents"));
    assert!(!t.matches("rootly://incident"));
    assert!(!t.matches("rootly://incidents/42"));
    assert!(t.is_literal());
}

#[test]
fn level_1_single_var_captures_value() {
    let t = compile_ok("incident://{id}");
    let b = t.bind("incident://42").expect("matches");
    assert_eq!(b.values.get("id").map(String::as_str), Some("42"));

    // Must match exactly — trailing slash fails.
    assert!(!t.matches("incident://42/"));
    // Empty capture fails: no text where the variable should be.
    assert!(!t.matches("incident://"));
}

#[test]
fn level_1_multi_var_with_separators() {
    // The kind of template a real upstream exposes.
    let t = compile_ok("projects/{project_id}/issues/{issue_iid}");
    let b = t.bind("projects/my-proj/issues/42").expect("matches shape");
    assert_eq!(
        b.values.get("project_id").map(String::as_str),
        Some("my-proj")
    );
    assert_eq!(b.values.get("issue_iid").map(String::as_str), Some("42"));

    // Missing second component: no match.
    assert!(!t.matches("projects/my-proj/issues/"));
    // Extra trailing: no match.
    assert!(!t.matches("projects/my-proj/issues/42/extra"));
}

#[test]
fn level_2_reserved_var_captures_like_level_1_for_matching() {
    // `{+var}` differs from `{var}` only in how expansion
    // encodes reserved chars. Matching-side behaviour uses
    // last-occurrence (maximal) capture to the next literal boundary.
    let t = compile_ok("path://{+any}/tail");
    let b = t.bind("path://a/b/c/tail").expect("matches");
    // Last-occurrence / maximal: `rfind("/tail")` on "a/b/c/tail"
    // locates the one at the end, so `any` captures "a/b/c".
    assert_eq!(b.values.get("any").map(String::as_str), Some("a/b/c"));
}

#[test]
fn level_2_reserved_var_maximal_capture_when_separator_recurs() {
    // RFC 6570 `{+var}` absorbs reserved characters including the
    // separator literal itself. When the separator literal appears
    // more than once, the variable must capture the maximal prefix
    // (last occurrence), not the minimal one (first occurrence).
    // Template `{+path}/v2` over URI `api/v2/endpoint/v2`:
    //   - minimal (wrong): path = "api", leftover = "endpoint/v2" → fails
    //   - maximal (correct): path = "api/v2/endpoint", consumes trailing "/v2"
    let t = compile_ok("{+path}/v2");
    let b = t.bind("api/v2/endpoint/v2").expect("matches");
    assert_eq!(
        b.values.get("path").map(String::as_str),
        Some("api/v2/endpoint"),
    );
}

#[test]
fn level_2_fragment_captures_after_hash() {
    let t = compile_ok("doc://page{#section}");
    let b = t.bind("doc://page#intro").expect("matches");
    assert_eq!(b.values.get("section").map(String::as_str), Some("intro"));

    // No `#` -> no match.
    assert!(!t.matches("doc://page"));
    // `#` but empty after -> no match.
    assert!(!t.matches("doc://page#"));
}

#[test]
fn compile_rejects_level_3_list_expression() {
    match CompiledTemplate::compile("{a,b,c}").expect_err("template should have failed to compile")
    {
        CompileError::ListExpression { expr } => assert_eq!(expr, "a,b,c"),
        other => panic!("expected ListExpression, got {other:?}"),
    }
}

#[test]
fn compile_rejects_form_style_operators() {
    for op in ['?', '&', ';', '/', '.'] {
        let template = format!("base{{{op}v}}");
        let err = CompiledTemplate::compile(&template)
            .expect_err("template should have failed to compile");
        assert!(
            matches!(&err, CompileError::UnsupportedOperator { op: got } if *got == op),
            "template '{template}' expected UnsupportedOperator({op}), got {err:?}"
        );
    }
}

#[test]
fn compile_rejects_explode_modifier() {
    let err =
        CompiledTemplate::compile("{list*}").expect_err("template should have failed to compile");
    assert!(
        matches!(&err, CompileError::UnsupportedModifier { modifier, .. } if modifier == "*"),
        "got {err:?}"
    );
}

#[test]
fn compile_rejects_prefix_modifier() {
    let err = CompiledTemplate::compile("{prefix:3}")
        .expect_err("template should have failed to compile");
    assert!(
        matches!(&err, CompileError::UnsupportedModifier { modifier, .. } if modifier == ":3"),
        "got {err:?}"
    );
}

#[test]
fn compile_rejects_unterminated_expression() {
    let err =
        CompiledTemplate::compile("foo{bar").expect_err("template should have failed to compile");
    assert_eq!(err, CompileError::UnterminatedExpression);
}

#[test]
fn compile_rejects_empty_expression() {
    let err =
        CompiledTemplate::compile("foo{}bar").expect_err("template should have failed to compile");
    assert_eq!(err, CompileError::EmptyExpression);
}

#[test]
fn compile_rejects_second_fragment() {
    // Any content after a fragment expression triggers FragmentNotLast
    // before we can even parse a second fragment — the "multiple
    // fragments" path is subsumed by the "fragment not last" check.
    let err = CompiledTemplate::compile("doc{#a}more{#b}")
        .expect_err("template should have failed to compile");
    assert_eq!(err, CompileError::FragmentNotLast);
}

#[test]
fn compile_rejects_content_after_fragment() {
    // A fragment expression must be the last part of the template.
    // Content after {#var} produces a matcher that can never match
    // any URI — surface this at compile time.
    let err = CompiledTemplate::compile("doc{#section}suffix")
        .expect_err("template should have failed to compile");
    assert_eq!(err, CompileError::FragmentNotLast);
}

#[test]
fn is_literal_returns_true_for_pure_literal() {
    let t = compile_ok("foo://bar/baz");
    assert!(t.is_literal());
    assert_eq!(t.as_literal().as_deref(), Some("foo://bar/baz"));
}

#[test]
fn is_literal_returns_false_when_vars_present() {
    let t = compile_ok("foo://{bar}");
    assert!(!t.is_literal());
    assert!(t.as_literal().is_none());
}

#[test]
fn level_1_var_refuses_to_cross_reserved_characters() {
    // RFC 6570: Level-1 `{var}` expansion percent-encodes reserved
    // chars (including `/`). A concrete URI where the matching
    // position contains `/` was NOT produced by this template —
    // it was either a `{+var}` template, a different template, or
    // the upstream violated its own spec.
    //
    // Contrast with `{+name}/end` over the same URI: that MUST
    // match with `name` = "a/b", pinned by
    // `level_2_reserved_var_captures_like_level_1_for_matching`.
    let t = compile_ok("path://{name}/end");
    assert!(
        !t.matches("path://a/b/end"),
        "Level-1 {{name}} must not capture across '/'"
    );
    // And the well-formed input still matches:
    let b = t.bind("path://plain/end").expect("matches");
    assert_eq!(b.values.get("name").map(String::as_str), Some("plain"));
}

#[test]
fn level_1_var_refuses_to_cross_query_delimiters() {
    // Pin the same rule for `?` — expansion percent-encodes it,
    // so unencoded `?` in a Level-1 position is a mismatch.
    let t = compile_ok("host/{path}");
    assert!(
        !t.matches("host/a?b"),
        "Level-1 {{path}} must not capture across '?'"
    );
}

/// Build a `uri_template::Value` for a scalar string.
///
/// The crate's `Scalar` type is private, so we can't construct
/// `Value::Scalar(...)` directly at runtime. Same trick the gateway
/// uses in production inside `lua-runtime`'s `expand_template`:
/// go through serde's `Deserialize` impl (available behind the
/// `serde` feature we enable workspace-wide), then normalize to
/// `'static` via `Value::into_static`.
fn scalar_value(s: &str) -> uri_template::Value<'static, 'static> {
    use serde::Deserialize as _;
    use serde::de::IntoDeserializer;
    let json = serde_json::Value::String(s.to_string());
    let borrowed: uri_template::Value<'_, '_> =
        uri_template::Value::deserialize(json.into_deserializer())
            .expect("serde_json string -> Value::Scalar never fails");
    borrowed.into_static()
}

/// Expand a template with the oracle expander to a URI string.
fn expand_oracle(template_str: &'static str, params: &[(&str, &str)]) -> String {
    let ut = uri_template::UriTemplate::new(template_str).expect("uri_template parses");
    let mut ctx: std::collections::HashMap<&str, uri_template::Value<'_, '_>> =
        std::collections::HashMap::new();
    for (k, v) in params {
        ctx.insert(*k, scalar_value(v));
    }
    ut.expand(&ctx).expect("expansion succeeds")
}

#[test]
fn roundtrip_level_1_single_var() {
    // Oracle: expanding a Level 1 template with a known value
    // MUST produce a URI the matcher accepts, binding the
    // original value. Uses `uri_template` for expansion — the
    // same crate the gateway uses in `_gateway.expand_template`.
    let template_str = "incident://{id}";
    let t = compile_ok(template_str);

    for val in ["42", "some-id", "a.b.c", "0"] {
        let expanded = expand_oracle(template_str, &[("id", val)]);
        let b = t
            .bind(&expanded)
            .unwrap_or_else(|| panic!("expanded '{expanded}' should match template"));
        assert_eq!(
            b.values.get("id").map(String::as_str),
            Some(val),
            "roundtrip lost the value for '{val}'"
        );
    }
}

#[test]
fn roundtrip_level_1_multi_var() {
    let template_str = "projects/{project_id}/issues/{issue_iid}";
    let t = compile_ok(template_str);
    let expanded = expand_oracle(
        template_str,
        &[("project_id", "my-proj"), ("issue_iid", "42")],
    );

    let b = t.bind(&expanded).expect("matches");
    assert_eq!(
        b.values.get("project_id").map(String::as_str),
        Some("my-proj")
    );
    assert_eq!(b.values.get("issue_iid").map(String::as_str), Some("42"));
}

#[test]
fn template_with_leading_variable_matches() {
    // No literal prefix.
    let t = compile_ok("{scheme}://example.com");
    let b = t.bind("https://example.com").expect("matches");
    assert_eq!(b.values.get("scheme").map(String::as_str), Some("https"));
}

#[test]
fn adjacent_variables_not_supported_cleanly() {
    // Documented current behaviour: `{a}{b}` is ambiguous
    // without a separator. The FIRST variable captures up to
    // the next LITERAL, and since there's no literal between
    // them, `a` swallows everything and `b` gets empty -> no
    // match. This is not "broken" so much as "not a useful
    // template shape for our domain" — separators between
    // variables are how real resource URIs are structured.
    let t = compile_ok("{a}{b}");
    assert!(
        !t.matches("foobar"),
        "adjacent variables without a separator must not silently bind to one-char prefixes"
    );
}

#[test]
fn bindings_map_contains_only_declared_variables() {
    let t = compile_ok("a/{x}/b/{y}");
    let b = t.bind("a/1/b/2").expect("matches");
    assert_eq!(b.values.len(), 2);
    assert!(b.values.contains_key("x"));
    assert!(b.values.contains_key("y"));
}
