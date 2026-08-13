//! Tests for the `execute` tool's description pins.
//!
//! Moved here alongside `EXECUTE_TOOL_DESCRIPTION` so a change to
//! the description and the cross-reference check live in the same
//! file — regressions in either half show up as a test failure in
//! the module that owns the prose.
use super::*;

#[test]
fn execute_description_mentions_every_known_gateway_builtin() {
    // Direction 1 of the roster contract (installed → described):
    // every builtin lua-runtime installs must be named in the
    // description, because the description is the only place agents
    // discover the surface. The roster constant is itself pinned to
    // the actually-installed set by a lua-runtime test, so this can't
    // silently drift on either end.
    for name in lua_runtime::GATEWAY_BUILTIN_NAMES {
        let qualified = format!("_gateway.{name}");
        assert!(
            EXECUTE_TOOL_DESCRIPTION.contains(&qualified),
            "execute tool description is missing builtin {qualified}; \
             update EXECUTE_TOOL_DESCRIPTION when you add/remove builtins"
        );
    }
}

#[test]
fn execute_description_names_no_phantom_builtins() {
    // Direction 2 of the roster contract (described → installed):
    // every `_gateway.<ident>` the description mentions must exist in
    // the roster. Catches a stale description entry surviving a
    // builtin's removal — agents would otherwise call an API that
    // isn't there.
    let mut described: Vec<&str> = Vec::new();
    let mut rest = EXECUTE_TOOL_DESCRIPTION;
    while let Some(pos) = rest.find("_gateway.") {
        let after = &rest[pos + "_gateway.".len()..];
        let end = after
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(after.len());
        if end > 0 {
            described.push(&after[..end]);
        }
        rest = &after[end..];
    }
    assert!(
        !described.is_empty(),
        "scanner must find builtin mentions; if the description dropped \
         the `_gateway.` prefix convention, update this test's scanner"
    );
    for name in described {
        assert!(
            lua_runtime::GATEWAY_BUILTIN_NAMES.contains(&name),
            "execute tool description names `_gateway.{name}`, which is \
             not in lua_runtime::GATEWAY_BUILTIN_NAMES — either a typo \
             or a stale entry for a removed builtin"
        );
    }
}

#[test]
fn execute_description_flags_result_is_side_effect_not_return() {
    // Mining showed agents writing `return result(x)` instead of
    // `result(x)`. Pure doc fix — pin that we call it out.
    assert!(
        EXECUTE_TOOL_DESCRIPTION.contains("Do not write `return result"),
        "description must flag the result-is-side-effect gotcha"
    );
}

#[test]
fn execute_description_reminds_agents_to_inspect_tools_first() {
    // Inspection gate is load-bearing: scripts that skip
    // `tool-details` fail at runtime. The description should
    // make that unambiguous so agents don't waste a retry.
    assert!(
        EXECUTE_TOOL_DESCRIPTION.contains("tool-details"),
        "description must point at tool-details for the inspection gate"
    );
    assert!(
        EXECUTE_TOOL_DESCRIPTION.contains("NotInspected")
            || EXECUTE_TOOL_DESCRIPTION.contains("un-inspected"),
        "description must name the failure mode when agents skip inspection"
    );
}
