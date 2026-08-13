use super::*;

#[test]
fn empty_input_returns_none() {
    assert_eq!(first_sentence(""), None);
}

#[test]
fn whitespace_only_input_returns_none() {
    // Defense-in-depth: a source that contains only tabs/newlines
    // would produce an empty-string hint otherwise. Callers treat
    // `None` as "no hint"; an empty string would render a blank
    // line and confuse the agent.
    assert_eq!(first_sentence("   \n\t  \n "), None);
}

#[test]
fn single_sentence_passes_through() {
    assert_eq!(
        first_sentence("This is a server."),
        Some("This is a server.".to_string())
    );
}

#[test]
fn multi_sentence_stops_at_first_period() {
    // Property: the datadog-us shape in the live probe. The first
    // sentence is self-contained; sentences after it are
    // supplementary (links, setup pointers) that belong in the
    // full instructions, not the catalog hint.
    let raw = "This is an MCP server for Datadog (a software observability platform). \
               Consult Datadog docs if you're unsure. \
               Docs at https://example.com";
    assert_eq!(
        first_sentence(raw),
        Some("This is an MCP server for Datadog (a software observability platform).".to_string())
    );
}

#[test]
fn first_line_wins_when_no_sentence_boundary() {
    // Property: the Glean shape. The upstream's instructions is a
    // markdown document starting with a heading; the first line
    // is a title without terminal punctuation. Using it as the
    // hint is strictly better than emitting nothing.
    let raw = "Glean MCP Server Usage Guidelines\n\n## Tool Selection\n\n1. ...";
    assert_eq!(
        first_sentence(raw),
        Some("Glean MCP Server Usage Guidelines".to_string())
    );
}

#[test]
fn handles_exclaim_and_question_as_sentence_terminators() {
    // Property: sentence detection is permissive of English's three
    // terminal punctuation marks, not just `.`.
    assert_eq!(
        first_sentence("Welcome! This part does not appear."),
        Some("Welcome!".to_string())
    );
    assert_eq!(
        first_sentence("Need help? Ask the docs."),
        Some("Need help?".to_string())
    );
}

#[test]
fn does_not_break_on_abbreviation_with_no_trailing_space() {
    // "etc." at end-of-input still counts as a boundary because
    // peek() returns None; that's correct. But "v1.0" inside the
    // string must NOT terminate — the period is followed by a
    // non-whitespace char.
    let raw = "Server v1.0 for docs. More text.";
    assert_eq!(
        first_sentence(raw),
        Some("Server v1.0 for docs.".to_string())
    );
}

#[test]
fn cap_truncation_fires_at_exact_boundary() {
    // Pins the >= boundary: exactly HINT_CHAR_CAP chars of input gets
    // the ellipsis (it hit the cap, not a sentence boundary).
    let exact = "a".repeat(HINT_CHAR_CAP);
    let out = first_sentence(&exact).expect("non-empty");
    assert_eq!(out.chars().count(), HINT_CHAR_CAP + 1);
    assert!(out.ends_with('…'), "ellipsis at cap: {out}");
}

#[test]
fn no_truncation_one_char_below_cap() {
    // Pins the other side: one char under the cap is returned without
    // ellipsis (natural line-end, not a cap cut).
    let just_under = "a".repeat(HINT_CHAR_CAP - 1);
    let out = first_sentence(&just_under).expect("non-empty");
    assert_eq!(out.chars().count(), HINT_CHAR_CAP - 1);
    assert!(!out.ends_with('…'), "no ellipsis below cap: {out}");
}

#[test]
fn cap_truncation_adds_ellipsis_marker() {
    // Property: a sentence longer than HINT_CHAR_CAP gets cut at
    // the cap and marked. The marker tells the agent this isn't
    // the complete thought; callers can pull full instructions
    // via server-details if they want.
    let long = "a".repeat(HINT_CHAR_CAP + 50);
    let out = first_sentence(&long).expect("non-empty input produces a hint");
    // Should be cap chars plus the ellipsis.
    assert_eq!(out.chars().count(), HINT_CHAR_CAP + 1);
    assert!(out.ends_with('…'), "truncation marker present: {out}");
}

#[test]
fn sentence_shorter_than_cap_has_no_ellipsis() {
    // Negative of the previous test: a natural sentence boundary
    // is not a truncation. The agent has the full thought.
    let raw = "Short sentence. More stuff.";
    let out = first_sentence(raw).expect("non-empty");
    assert!(!out.contains('…'), "no ellipsis on natural boundary: {out}");
}

#[test]
fn whitespace_runs_collapse_before_measurement() {
    // Property: raw indentation doesn't eat the character budget.
    // The consumer of the hint cares about chars of content, not
    // chars of whitespace.
    let raw = "\n\n\t  Actual content goes here.    Trailing stuff.";
    assert_eq!(
        first_sentence(raw),
        Some("Actual content goes here.".to_string())
    );
}

#[test]
fn multibyte_content_is_char_not_byte_truncated() {
    // Property: a 300-char emoji string truncates to HINT_CHAR_CAP
    // chars, not to HINT_CHAR_CAP bytes. Confirms the implementation
    // doesn't accidentally slice mid-codepoint.
    let raw = "🔍".repeat(300); // 300 chars, 1200 bytes
    let out = first_sentence(&raw).expect("non-empty");
    // HINT_CHAR_CAP characters plus the ellipsis.
    assert_eq!(out.chars().count(), HINT_CHAR_CAP + 1);
    // Must be valid UTF-8 — the `String` type guarantees this,
    // but the assertion pins that we didn't panic on a byte
    // boundary during construction.
    assert!(out.ends_with('…'));
}

#[test]
fn idempotent_on_already_compacted_input() {
    // Property: running the extractor on its own output produces
    // the same output (minus any potential re-ellipsis). For
    // non-truncated results, strictly idempotent.
    let raw = "Simple sentence.";
    let once = first_sentence(raw).expect("non-empty");
    let twice = first_sentence(&once).expect("non-empty");
    assert_eq!(once, twice);
}

// ---- resolve_hint -------------------------------------------------

#[test]
fn resolve_prefers_config_override_when_present() {
    // Property: user override wins regardless of what the upstream
    // set. Users who bothered to author a hint in config.json are
    // overriding for a reason.
    assert_eq!(
        resolve_hint(
            Some("User-authored hint"),
            Some("Upstream says different stuff.")
        ),
        Some("User-authored hint".to_string())
    );
}

#[test]
fn resolve_falls_back_to_upstream_when_no_override() {
    assert_eq!(
        resolve_hint(None, Some("First sentence. Second sentence.")),
        Some("First sentence.".to_string())
    );
}

#[test]
fn resolve_returns_none_when_neither_source_set() {
    assert_eq!(resolve_hint(None, None), None);
}

#[test]
fn resolve_empty_override_falls_through_to_upstream() {
    // Property: an empty string in config.json shouldn't *suppress*
    // a useful upstream-advertised hint. The user likely just
    // scaffolded the key and forgot to fill it; degrade to the
    // upstream's content rather than emitting nothing.
    assert_eq!(
        resolve_hint(Some(""), Some("Upstream hint.")),
        Some("Upstream hint.".to_string())
    );
    assert_eq!(
        resolve_hint(Some("   \t  "), Some("Upstream hint.")),
        Some("Upstream hint.".to_string())
    );
}

#[test]
fn resolve_trims_override_whitespace() {
    // Convenience: config.json authors often leave a space at the
    // start/end. Match the "don't punish typos in human-edited
    // config" posture the rest of the parser has.
    assert_eq!(
        resolve_hint(Some("  hint here  "), None),
        Some("hint here".to_string())
    );
}

#[test]
fn resolve_override_bypasses_char_cap() {
    // Property: config overrides are UNCAPPED. If the user writes
    // a 400-char hint, they get all 400 chars. The cap is a
    // defense against upstreams that dump their whole README into
    // `instructions`; users authoring their own override don't
    // need protecting from themselves.
    let long = "x".repeat(400);
    assert_eq!(resolve_hint(Some(&long), None), Some(long));
}
