// pattern: Functional Core
//
// Matcher for RFC 6570 URI templates, Level 1 and Level 2 only.
//
// Why a bespoke matcher
// ---------------------
// The `uri-template` crate we already depend on expands templates
// (pour parameters in, get a URI out), but doesn't match (given a
// concrete URI and a template, say whether the URI is an instance
// of the template and bind the captured variables). Matching is
// the direction we need for resource-subscription delivery: when
// an upstream fires `notifications/resources/updated` for the
// concrete URI `incident://42`, we need to answer "does that
// match any template a downstream session subscribed to?".
//
// Level bounds
// ------------
// RFC 6570 has four levels. The matching problem for Levels 3-4
// explodes rapidly:
//
//   - Level 3 adds `{q,r,s}` multi-variable expressions,
//     `{?q}` / `{&x}` / `{;v}` / `{.label}` form-style operators,
//     path-segment `{/p}`. Matching these requires understanding
//     operator-specific separators and their encoding rules.
//   - Level 4 adds `{var*}` (explode composite) and `{var:3}`
//     (prefix truncation), where a single template variable can
//     absorb an unbounded list or an arbitrary-length prefix.
//     Deciding whether a given concrete URI matches `{var*}` is
//     straightforward only if you restrict yourself to one syntax
//     per operator class — and even then, rebinding `var` to the
//     list that produced the expansion requires parser state.
//
// Real-world resource templates we've observed (Rootly, GitLab
// subscribe-capable upstreams) are all Level 1-2: `incident://{id}`,
// `team://{team_id}`, `projects/{project_id}/issues/{issue_iid}`.
// The matcher supports these and rejects Levels 3-4 at compile time
// with a named-construct error the subscribe handler can surface
// cleanly to the client.
//
// Encoding posture
// ----------------
// This module matches on the BARE upstream URI (what the upstream
// emits in `resources/updated`), not the namespaced form. The
// gateway's namespace encoding lives in `resource_uri.rs`; keeping
// the matcher URI-native means it composes with any future
// encoding scheme rather than hard-coding the current one.
//
// Invariant and scope bounds
// --------------------------
// Captures respect complete percent triplets and simple captures decode as
// UTF-8. Repeated names denote one scalar, including across operator kinds.
// Literals remain verbatim; empty captures and adjacent variables remain
// unsupported. Matching is bounded to 2,000,000 suffix cells and 4,000,000
// charged work units; exhaustion returns no match, so completeness is not
// promised outside those limits. See `template_matcher_search` for accounting.

use thiserror::Error;

#[path = "template_matcher_search.rs"]
mod search;

/// A compiled Level 1-2 template, ready for matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledTemplate {
    parts: Vec<Part>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Part {
    /// A literal segment, matched verbatim.
    Literal(String),
    /// A nonempty percent-encoded scalar without raw reserved characters.
    SimpleVar(String),
    /// A nonempty scalar allowing reserved characters and intact triplets.
    ReservedVar(String),
    /// A final nonempty reserved scalar preceded by a literal '#'.
    FragmentVar(String),
}

/// Errors from compiling a template.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CompileError {
    #[error("template is missing the closing '}}' after '{{'")]
    UnterminatedExpression,
    #[error("template has an empty expression '{{}}'")]
    EmptyExpression,
    #[error(
        "template uses unsupported RFC 6570 operator '{op}' (Level 3-4); only Level 1-2 is supported (bare var, '+' reserved, '#' fragment)"
    )]
    UnsupportedOperator { op: char },
    #[error(
        "template uses Level 3 list expression '{expr}' (multiple comma-separated variables); only single-variable expressions are supported"
    )]
    ListExpression { expr: String },
    #[error(
        "template uses Level 4 modifier '{modifier}' in '{var}'; explode (*) and prefix (:N) are not supported"
    )]
    UnsupportedModifier { modifier: String, var: String },
    #[error("template variable '{var}' contains an invalid character '{ch}'")]
    InvalidVarChar { var: String, ch: char },
    #[error("template has more than one fragment expression; RFC 6570 allows at most one")]
    MultipleFragments,
    #[error(
        "template has content after a fragment expression '{{#...}}'; the fragment must be the last part of the template"
    )]
    FragmentNotLast,
}

/// Result of matching a URI against a template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bindings {
    pub values: std::collections::HashMap<String, String>,
}

impl CompiledTemplate {
    /// Compile a template string. Rejects Level 3-4 constructs and
    /// malformed input; accepts any Level 1-2 template.
    pub fn compile(template: &str) -> Result<Self, CompileError> {
        let mut parts = Vec::new();
        let mut remaining = template;
        let mut saw_fragment = false;

        while !remaining.is_empty() {
            match remaining.find('{') {
                None => {
                    parts.push(Part::Literal(remaining.to_string()));
                    break;
                }
                Some(0) => {
                    let end = remaining
                        .find('}')
                        .ok_or(CompileError::UnterminatedExpression)?;
                    let expr = &remaining[1..end];
                    if expr.is_empty() {
                        return Err(CompileError::EmptyExpression);
                    }
                    let part = compile_expression(expr, &mut saw_fragment)?;
                    parts.push(part);
                    remaining = &remaining[end + 1..];
                    // RFC 6570 §3.2.6: fragment expansion always produces
                    // the last component. Nothing may follow {#var}; a
                    // template that has content after the fragment can
                    // never match any URI (FragmentVar sets cursor = ""
                    // and any trailing literal/var fails immediately).
                    // Surface this at compile time rather than silently
                    // producing a permanently-failing matcher.
                    if saw_fragment && !remaining.is_empty() {
                        return Err(CompileError::FragmentNotLast);
                    }
                }
                Some(i) => {
                    parts.push(Part::Literal(remaining[..i].to_string()));
                    remaining = &remaining[i..];
                }
            }
        }

        Ok(CompiledTemplate { parts })
    }

    /// Does `uri` match this template?
    ///
    /// Highly ambiguous repeated-name searches can exhaust the bounded work
    /// allowance and return false; no partial or inconsistent match is returned.
    pub fn matches(&self, uri: &str) -> bool {
        self.try_bind(uri).is_some()
    }

    /// Return nonempty captured bindings, or `None` on mismatch or work exhaustion.
    pub fn bind(&self, uri: &str) -> Option<Bindings> {
        self.try_bind(uri).map(|values| Bindings { values })
    }

    /// Was this template a pure literal (no variables)?
    /// Useful for the registry: a "template" with no variables
    /// reduces to a concrete subscription and can be stored in the
    /// concrete index instead.
    pub fn is_literal(&self) -> bool {
        self.parts.iter().all(|p| matches!(p, Part::Literal(_)))
    }

    /// Rebuild the literal string if `is_literal()` is true.
    pub fn as_literal(&self) -> Option<String> {
        if !self.is_literal() {
            return None;
        }
        Some(
            self.parts
                .iter()
                .filter_map(|p| match p {
                    Part::Literal(s) => Some(s.as_str()),
                    _ => None,
                })
                .collect(),
        )
    }

    fn try_bind(&self, uri: &str) -> Option<std::collections::HashMap<String, String>> {
        search::bind(&self.parts, uri)
    }
}

fn compile_expression(expr: &str, saw_fragment: &mut bool) -> Result<Part, CompileError> {
    // Detect commas — Level 3 multi-variable expressions. Reject
    // outright; the matcher doesn't support them.
    if expr.contains(',') {
        return Err(CompileError::ListExpression {
            expr: expr.to_string(),
        });
    }

    let first = expr.chars().next().expect("caller ensured non-empty");
    let (op, rest) = if is_operator(first) {
        (Some(first), &expr[first.len_utf8()..])
    } else {
        (None, expr)
    };

    // Reject Level 4 modifiers inside the variable name.
    if let Some(star_pos) = rest.find('*') {
        return Err(CompileError::UnsupportedModifier {
            modifier: "*".to_string(),
            var: rest[..star_pos].to_string(),
        });
    }
    if let Some(colon_pos) = rest.find(':') {
        return Err(CompileError::UnsupportedModifier {
            modifier: format!(":{}", &rest[colon_pos + 1..]),
            var: rest[..colon_pos].to_string(),
        });
    }

    // Validate the var name characters. RFC 6570 allows ALPHA /
    // DIGIT / `_` / pct-encoded, plus `.` between varname segments.
    // Enforcing a mildly loose superset here is fine — if the name
    // contains junk, matching will never produce anything useful.
    for ch in rest.chars() {
        if !is_varname_char(ch) {
            return Err(CompileError::InvalidVarChar {
                var: rest.to_string(),
                ch,
            });
        }
    }

    let var = rest.to_string();

    match op {
        None => Ok(Part::SimpleVar(var)),
        Some('+') => Ok(Part::ReservedVar(var)),
        Some('#') => {
            if *saw_fragment {
                return Err(CompileError::MultipleFragments);
            }
            *saw_fragment = true;
            Ok(Part::FragmentVar(var))
        }
        // Level 3-4 operators we actively refuse.
        Some(c @ ('.' | '/' | ';' | '?' | '&')) => Err(CompileError::UnsupportedOperator { op: c }),
        // Any other punctuation at the operator position. RFC 6570
        // reserves these for future use; reject explicitly.
        Some(c) => Err(CompileError::UnsupportedOperator { op: c }),
    }
}

fn is_operator(c: char) -> bool {
    matches!(
        c,
        '+' | '#' | '.' | '/' | ';' | '?' | '&' | '=' | ',' | '!' | '@' | '|'
    )
}

fn is_varname_char(c: char) -> bool {
    // RFC 6570 allows ALPHA / DIGIT / `_` / pct-encoded sequences /
    // `.` between varname segments. We intentionally exclude bare `%`
    // here: accepting `%` as a standalone character (without validating
    // that it is followed by two hex digits) would silently admit
    // malformed names like `%zz` that no sane caller would produce or
    // look up. If a real pct-encoded varname is ever needed, restore
    // `%` with a proper three-character validation pass.
    c.is_ascii_alphanumeric() || c == '_' || c == '.'
}

/// RFC 6570 gen-delims / sub-delims, forbidden raw in simple captures.
fn is_reserved_char(c: char) -> bool {
    matches!(
        c,
        ':' | '/'
            | '?'
            | '#'
            | '['
            | ']'
            | '@'
            | '!'
            | '$'
            | '&'
            | '\''
            | '('
            | ')'
            | '*'
            | '+'
            | ','
            | ';'
            | '='
    )
}

#[cfg(test)]
#[path = "template_matcher_repair_tests.rs"]
mod repair_tests;

#[cfg(test)]
#[path = "template_matcher_tests.rs"]
mod tests;
