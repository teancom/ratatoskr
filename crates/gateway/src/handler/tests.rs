//! Cross-family tests that exercise mod.rs's pieces:
//! `list_tools` / `call_tool` dispatch-symmetry and
//! `build_server_instructions`. Session-resolution tests live
//! alongside `resolve_session_from_parts` in `session.rs`; the
//! execute-description pins live alongside `EXECUTE_TOOL_DESCRIPTION`
//! in `execute.rs`.
use super::*;

// ----- list_tools / call_tool symmetry check -----
//
// list_tools and call_tool maintain two independent representations
// of tool names. Adding a tool in one but not the other produces
// either an undiscoverable-but-invokable tool (missing from list) or
// a discoverable-but-erroring tool (missing dispatch arm).
//
// The canonical tool-name list lives in `tools_schema::BASE_TOOL_NAMES`.
// Tests in `tools_schema` pin that the constant agrees with the
// `Tool::new("<name>", …)` literals in the tool constructors (and
// therefore with what `list_tools` advertises).
// `build_server_instructions` also reads from the constant, so the
// server-instructions prose stays in sync automatically.
//
// The test below is a *partial* self-enforcing check: it mirrors the
// names handled by `call_tool`'s match arms into a literal array and
// asserts that array equals `BASE_TOOL_NAMES`.
// A mismatch catches the case where someone adds a canonical name
// but the mirror list here doesn't follow — not the case where
// someone adds a canonical name and a dispatch arm but the mirror
// wasn't touched. Fully closing that loop requires either a macro
// over the match (out of scope) or a future data-driven dispatch
// refactor that drives dispatch from the name list. Until then,
// `cargo test` will still catch the more common "forgot to update
// tools_schema" path, which is the one drift direction we've actually
// hit in practice.
//
// Maintenance: when you add a tool, update `tools_schema::BASE_TOOL_NAMES`,
// the dispatch arm in `call_tool`, and the mirror array below.

#[test]
fn list_tools_and_call_tool_name_sets_are_identical() {
    use crate::tools_schema::BASE_TOOL_NAMES;

    // Names dispatched by `call_tool`'s match arms. Must mirror the
    // string literals in the match exactly. If you add an arm, add
    // it here; the assertion against the canonical constant will
    // fail if `tools_schema` doesn't know about it.
    let call_tool_base = [
        "ping",
        "list-servers",
        "server-details",
        "list-server-tools",
        "tool-details",
        "inspect-tool-response",
        "execute",
        "reload-config",
        "reconnect-upstream",
        "restart-gateway",
    ];

    let mut canonical_base: Vec<&str> = BASE_TOOL_NAMES.to_vec();
    canonical_base.sort_unstable();
    let mut dispatched_base = call_tool_base.to_vec();
    dispatched_base.sort_unstable();
    assert_eq!(
        dispatched_base, canonical_base,
        "call_tool's base-tool dispatch arms must exactly cover \
         tools_schema::BASE_TOOL_NAMES; add the new name to BOTH"
    );
}

// The execute tool description pins live alongside
// `EXECUTE_TOOL_DESCRIPTION` in `handler/execute.rs`.

// ----- server-instructions cross-reference pins -----
//
// `build_server_instructions` produces the free-text `instructions`
// block that downstream MCP clients (and the agents behind them) see
// on initialize. Tool names are deduplicated against `tools_schema`
// (the prose reads from the same constants), but the `_gateway.*`
// builtin enumeration is prose-only — no deduplication is possible
// today because the Rust handler can't introspect the Lua VM's
// globals. That makes the builtin test a genuine drift catcher.
//
// The tool-name test below is a formatting sanity check rather than
// a drift catcher: it confirms the enumeration reaches the output
// string (catches regressions in the join/push_str plumbing).

#[test]
fn server_instructions_lead_with_catalog_first_directive() {
    // Behavioral pin, not a formatting check. Agents boot with a flat
    // tool list that includes same-named, often-unauthenticated
    // connectors (e.g. account-level `claude.ai *` integrations whose
    // `authenticate` tool invites OAuth). They pattern-match a named
    // capability onto that tool and trigger its auth flow, never
    // calling list-servers to find that this gateway already fronts the
    // same capability. The fix is boundary placement: the "list before
    // authenticating" directive must come BEFORE the tool enumeration
    // so it wins the attention budget at session init. If a refactor
    // moves it back into mid-paragraph prose, this test fails and
    // explains why the order is load-bearing.
    let instructions = build_server_instructions();

    let cta = instructions
        .find("list-servers")
        .expect("instructions must direct agents to list-servers");
    let enumeration = instructions
        .find("Rust rewrite. Tools:")
        .expect("instructions must contain the tool enumeration preamble");
    assert!(
        cta < enumeration,
        "the list-servers call-to-action must precede the tool enumeration \
         so it lands at the high-attention start of the instructions \
         (see writing-claude-directives: boundary placement) — found CTA at \
         {cta}, enumeration at {enumeration}"
    );

    // Motivation must travel with the directive: the WHY (upstreams are
    // not in the flat tool list) is what makes the rule generalize to
    // every same-named connector rather than any one service. Pin a
    // phrase from the rationale, kept category-level — assert NO service
    // name is hardcoded, since instance examples don't scale.
    assert!(
        instructions.contains("NOT in your flat tool list"),
        "the catalog-first directive must explain WHY (upstreams are not in \
         the flat tool list) — motivation is what makes agents generalize \
         the rule to any same-named connector"
    );
    assert!(
        !instructions.to_lowercase().contains("datadog"),
        "the directive must stay category-level: naming a specific service \
         only helps that one literal string and doesn't scale to the rest \
         of the same-named-connector family"
    );
}

#[test]
fn server_instructions_names_every_advertised_tool() {
    use crate::tools_schema::BASE_TOOL_NAMES;

    let instructions = build_server_instructions();

    for name in BASE_TOOL_NAMES {
        assert!(
            instructions.contains(name),
            "server instructions must name base tool '{name}' \
             — regression in build_server_instructions formatting"
        );
    }
}

#[test]
fn server_instructions_names_every_gateway_builtin() {
    // The same builtin list `EXECUTE_TOOL_DESCRIPTION` enumerates also
    // appears in the server-instructions prose (without the `_gateway.`
    // prefix, in a parenthesised "(`json_decode`, `json_encode`, …)"
    // list). Pin it so a new builtin reaches both discovery surfaces,
    // not just one. Builtin set mirrors `install_gateway_builtins`.
    //
    // We match on the backtick-wrapped form to avoid false negatives
    // (`parallel` is a common English word) and false positives (a
    // typo like `get_resultX` wouldn't trip a substring test for
    // `get_result`, but it would trip the `\`get_result\`` needle).
    let instructions = build_server_instructions();
    let builtins = [
        "json_decode",
        "json_encode",
        "unwrap_content",
        "rfc3339",
        "now_minus",
        "expand_template",
        "parallel",
        "get_result",
    ];
    for name in builtins {
        let needle = format!("`{name}`");
        assert!(
            instructions.contains(&needle),
            "server instructions must name _gateway.{name} (as `{name}`) \
             in the builtins enumeration — keep the list in sync with \
             install_gateway_builtins and EXECUTE_TOOL_DESCRIPTION"
        );
    }
}
