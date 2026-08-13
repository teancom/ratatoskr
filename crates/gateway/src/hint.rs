// pattern: Functional Core
//
// Catalog-hint helpers used by `list-servers` to render a short,
// bounded one-liner per upstream. Two concerns in one tiny module:
//
//   1. Extract the first sentence from an upstream's
//      `InitializeResult.instructions` string, capped at a fixed
//      character ceiling. Used as a fallback when the upstream
//      advertises instructions but no config-level override exists.
//
//   2. Resolve "the hint we should show for this upstream" given the
//      user's override and the upstream's advertised instructions.
//      Priority: config override wins; upstream instructions second;
//      nothing otherwise.
//
// Both are pure — input in, String/Option out, no I/O. Kept here
// rather than inside `list_servers.rs` so the rules are testable in
// isolation: the rendering tests don't need to set up instructions
// strings, and these tests don't need to know about rendering format.

use crate::tool_details::collapse_whitespace;

/// Upper bound on the number of characters we'll extract from an
/// upstream's `instructions` string when producing a catalog hint.
/// Chosen so ten upstreams × 200 chars stays well under 5 KB total,
/// which matches the proxy's "protect session-start context" posture.
/// Characters — not bytes — so multi-byte scripts (CJK, emoji) aren't
/// truncated disproportionately.
pub const HINT_CHAR_CAP: usize = 200;

/// Extract a compact one-line hint from an upstream's raw
/// `InitializeResult.instructions` string.
///
/// Returns `None` when the input is empty or whitespace-only (no
/// signal); otherwise returns `Some(hint)` capped at `HINT_CHAR_CAP`
/// chars, with whitespace collapsed and trailing punctuation preserved.
///
/// Termination order: the first sentence boundary wins, in whatever
/// order it appears. A sentence boundary is any of:
///
///   - `.`, `!`, `?` followed by whitespace or end-of-input
///   - a newline: the input is pre-split on lines; only the first
///     non-blank line reaches the boundary-detection loop, so content
///     after the first `\n` is always discarded before the walk begins
///   - the `HINT_CHAR_CAP` character limit
///
/// If the cap truncates mid-sentence, a trailing ellipsis `…` marks
/// the truncation so the agent knows to pull full `instructions` via
/// `server-details` if it needs the rest. Sentence-boundary hits do
/// NOT get the ellipsis — they're the natural end of a thought.
///
/// Whitespace is collapsed before measurement: every run of
/// whitespace becomes a single space. Without this, a single
/// indented line in the upstream's source would eat the entire
/// budget on leading tabs.
pub fn first_sentence(raw: &str) -> Option<String> {
    // Newline splitting happens BEFORE whitespace collapse, because
    // newlines are semantically significant here — they mark the end
    // of a "first line," which is often a markdown-style title that
    // we want to use as the hint. Collapsing first would fuse the
    // title with its following paragraph into one long line and
    // defeat the detection.
    //
    // Find the first non-blank line; that's the candidate. Any
    // content on subsequent lines is discarded (the agent can pull
    // full `instructions` via `server-details` to see it).
    let first_line = raw.lines().find(|l| !l.trim().is_empty())?;

    let collapsed = collapse_whitespace(first_line);
    if collapsed.is_empty() {
        // Should be unreachable given the `find` above, but defend
        // against a future refactor that loosens the predicate.
        return None;
    }

    // Walk once, character-indexed. We need the character position of
    // each candidate terminator because `HINT_CHAR_CAP` counts chars,
    // not bytes.
    let mut chars = collapsed.char_indices().peekable();
    let mut char_count = 0usize;
    let mut end: Option<(usize, TerminatorKind)> = None;

    while let Some((byte_idx, ch)) = chars.next() {
        char_count += 1;

        // Sentence-boundary: `.`, `!`, `?` followed by whitespace or
        // end-of-input. We peek at the next character without
        // consuming it; if we're at the end of input, `peek` is None
        // and that also counts as a boundary.
        if matches!(ch, '.' | '!' | '?') {
            let at_end = chars.peek().is_none();
            let next_is_space = chars.peek().is_some_and(|(_, next)| next.is_whitespace());
            if at_end || next_is_space {
                let after = byte_idx + ch.len_utf8();
                end = Some((after, TerminatorKind::Sentence));
                break;
            }
        }

        // Character cap: once we've consumed HINT_CHAR_CAP chars,
        // cut. Using `>=` rather than `>` so the snapshot is exactly
        // the cap, not cap+1.
        if char_count >= HINT_CHAR_CAP {
            let after = byte_idx + ch.len_utf8();
            end = Some((after, TerminatorKind::Cap));
            break;
        }
    }

    let (cut_at, kind) = match end {
        Some(pair) => pair,
        // Walked the whole (first-line) input without finding a
        // sentence terminator and without hitting the cap. Use the
        // full collapsed line; the end of the line is itself a
        // natural boundary (the agent can pull more via
        // `server-details` if needed).
        None => (collapsed.len(), TerminatorKind::LineEnd),
    };

    let mut body = collapsed[..cut_at].trim_end().to_string();
    if matches!(kind, TerminatorKind::Cap) {
        // Using `…` (U+2026) rather than `...` so the ellipsis is one
        // character. Keeps the "did we truncate?" signal visually
        // distinct from three literal periods that might appear in
        // legitimate content.
        body.push('…');
    }
    Some(body)
}

/// Resolve the hint for an upstream, given the user-authored override
/// (if any) and the upstream's advertised instructions (if any).
///
/// Priority, in order:
///   1. Config override (`UpstreamConfig.hint`): returned verbatim
///      after trimming. User-authored content is trusted and uncapped
///      — if the user set a 500-char hint they meant it.
///   2. First sentence of upstream instructions via `first_sentence`.
///   3. `None` when neither is set.
///
/// An override that is empty or whitespace-only falls through to the
/// upstream instructions — users editing config.json shouldn't accidentally
/// suppress the upstream's own hint with an empty string.
pub fn resolve_hint(
    config_override: Option<&str>,
    upstream_instructions: Option<&str>,
) -> Option<String> {
    if let Some(s) = config_override {
        let trimmed = s.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    upstream_instructions.and_then(first_sentence)
}

enum TerminatorKind {
    /// Hit a `.`/`!`/`?` followed by whitespace or end-of-input.
    Sentence,
    /// Walked off the end of the first non-blank line without hitting
    /// a sentence boundary or the cap. Natural stopping point.
    LineEnd,
    /// Consumed `HINT_CHAR_CAP` characters. The caller adds an
    /// ellipsis marker so the agent knows more content exists.
    Cap,
}

#[cfg(test)]
#[path = "hint_tests.rs"]
mod tests;
