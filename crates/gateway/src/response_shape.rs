// pattern: Functional Core
//
// Classifies the shape of upstream `CallToolResult` responses so the
// `tool-details` renderer can tell agents what to expect BEFORE they
// rediscover it the hard way. Without this, agents re-invent
// `.content[1].text` / `json_decode` / `structured_content` drill-down
// per upstream.
//
// Two classification sources:
//
//   - **Schema** (`classify_from_schema`): look at the upstream's declared
//     `outputSchema`. A non-empty output schema is fairly strong evidence
//     that the tool ships `structured_content`; beyond that, schemas in
//     practice don't tell us whether a text-only tool's text is prose or
//     JSON. So the schema path contributes `Structured` only.
//
//   - **Probe** (`classify_from_response`): inspect an actual successful
//     response. This is where we distinguish `TextProse` from `TextJson`
//     (does the first text block parse as JSON?), detect `MultiBlock`,
//     and notice `Mixed` shapes where an upstream returns both `content`
//     and `structured_content`.
//
// Observations merge via a lattice (`merge`). Never downgrade: once we've
// seen JSON in the text, we don't go back to prose on the next bland
// response. `Mixed` sits at the top because it's the most-informative
// answer: "this tool has been observed to return both structured_content
// and content-text, so the agent needs to handle either."
//
// Deliberately does NOT try to classify errors. An `is_error == true`
// response tells us the shape of THIS error, not the shape of the
// tool's success replies. The cache write path filters these out.

use rmcp::model::CallToolResult;

/// The shape of a tool's response, as observed or declared. Drives the
/// `responseShape` and `responseShapeHint` fields in `tool-details`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseShape {
    /// `content[0].text` is present; it does NOT parse as JSON.
    /// Reach for `_gateway.unwrap_content(...)` to drop the envelope.
    TextProse,
    /// `content[0].text` is present AND parses as JSON.
    /// Reach for `_gateway.json_decode(_gateway.unwrap_content(...))`.
    TextJson,
    /// `content` has more than one block. Bounded iteration, per-block
    /// drill-down as needed.
    MultiBlock { len: usize },
    /// `structured_content` is present. Drill into
    /// `result.structured_content`; the `content` mirror is a
    /// human-readable fallback the agent can usually ignore.
    Structured,
    /// Seen `structured_content` AND `content`-bearing text (prose or
    /// JSON) across observations. The tool surfaces both; the agent
    /// should prefer `structured_content` but tolerate either.
    Mixed,
}

impl ResponseShape {
    /// Agent-facing one-line hint pointing at the right `_gateway.*`
    /// builtin(s). Shipped alongside the tag in `tool-details` JSON so
    /// an agent parsing the response gets both the classification and
    /// the next step without a second lookup.
    pub fn hint(self) -> &'static str {
        match self {
            Self::TextProse => {
                "content[0].text is prose; call _gateway.unwrap_content(result) to drop the envelope"
            }
            Self::TextJson => {
                "content[0].text is JSON; call _gateway.json_decode(_gateway.unwrap_content(result))"
            }
            Self::MultiBlock { len: 1 } => {
                "content has one non-text block; inspect result.content[0] (image, resource link, or other structured block)"
            }
            Self::MultiBlock { .. } => {
                "content has multiple blocks; iterate result.content (each block has its own text/data)"
            }
            Self::Structured => {
                "structured_content carries the machine-parseable payload; use result.structured_content"
            }
            Self::Mixed => {
                "tool returns both structured_content AND content-text; prefer result.structured_content, fall back to _gateway.unwrap_content(result)"
            }
        }
    }

    /// Compact machine-readable tag used in the JSON output. Stable
    /// across sessions so agents can pattern-match on the string.
    pub fn tag(self) -> &'static str {
        match self {
            Self::TextProse => "text-prose",
            Self::TextJson => "text-json",
            Self::MultiBlock { .. } => "multi-block",
            Self::Structured => "structured",
            Self::Mixed => "mixed",
        }
    }
}

/// Merge two observations of the same tool's response shape. Monotonic:
/// more-informative classifications win, never downgrade. When the two
/// observations suggest genuinely different shapes (text vs. structured),
/// the result is `Mixed`.
///
/// Lattice (informally):
///
/// ```text
///                Mixed
///                  |
///   +--------------+--------------+
///   |              |              |
/// TextJson    MultiBlock      Structured
///   |
/// TextProse
/// ```
///
/// `MultiBlock` carries a length; when two observations are both
/// `MultiBlock` we take the max length (more-informative bound) so the
/// `tool-details` hint suggests an adequate iteration cap.
pub fn merge(a: ResponseShape, b: ResponseShape) -> ResponseShape {
    use ResponseShape::*;
    // Idempotence: merging with yourself is a no-op (modulo len).
    if a == b {
        return a;
    }
    match (a, b) {
        // `Mixed` absorbs anything.
        (Mixed, _) | (_, Mixed) => Mixed,

        // Text vs. structured → Mixed. The agent needs to handle both.
        (TextProse | TextJson | MultiBlock { .. }, Structured) => Mixed,
        (Structured, TextProse | TextJson | MultiBlock { .. }) => Mixed,

        // Within the text family: JSON beats prose.
        (TextProse, TextJson) | (TextJson, TextProse) => TextJson,

        // Text vs. multi-block → take multi-block (more structural info).
        (TextProse | TextJson, MultiBlock { len }) => MultiBlock { len },
        (MultiBlock { len }, TextProse | TextJson) => MultiBlock { len },

        // Two multi-block observations: keep the max length.
        (MultiBlock { len: a }, MultiBlock { len: b }) => MultiBlock { len: a.max(b) },

        // Structured with itself: already handled by the idempotence shortcut.
        (Structured, Structured) => Structured,
        (TextProse, TextProse) => TextProse,
        (TextJson, TextJson) => TextJson,
    }
}

/// Classify from a successful (non-error) response. Returns `None` when
/// the response carries neither `content` blocks nor `structured_content`
/// — an empty success is uninformative.
///
/// Callers should skip classification on `is_error == Some(true)` — error
/// responses tell us the shape of the error, not of successful replies.
pub fn classify_from_response(response: &CallToolResult) -> Option<ResponseShape> {
    let has_structured = response
        .structured_content
        .as_ref()
        .is_some_and(|v| !v.is_null());
    let content_len = response.content.len();

    if has_structured && content_len == 0 {
        return Some(ResponseShape::Structured);
    }

    if content_len > 1 {
        // Multi-block first: len takes precedence over text-JSON probing
        // because the agent's code path is different (iterate vs. drill).
        let base = ResponseShape::MultiBlock { len: content_len };
        return Some(if has_structured {
            merge(base, ResponseShape::Structured)
        } else {
            base
        });
    }

    if content_len == 1 {
        let text_shape = response.content.first().and_then(|c| c.as_text()).map(|t| {
            if parses_as_json(&t.text) {
                ResponseShape::TextJson
            } else {
                ResponseShape::TextProse
            }
        });

        return match (text_shape, has_structured) {
            (Some(text), true) => Some(merge(text, ResponseShape::Structured)),
            (Some(text), false) => Some(text),
            // Single content block that isn't text (image, resource link,
            // etc.) — treat as structural multi-block-ish but with len=1.
            // Caller can still emit "one block, inspect it" advice.
            (None, true) => Some(ResponseShape::Structured),
            (None, false) => Some(ResponseShape::MultiBlock { len: 1 }),
        };
    }

    // content_len == 0, no structured → nothing to classify.
    None
}

/// Classify from a tool's declared `outputSchema`. Returns `Some(Structured)`
/// when the schema appears to describe a non-trivial structured output;
/// returns `None` otherwise.
///
/// We deliberately do NOT try to infer TextProse / TextJson from schema:
/// schemas in practice describe the `structured_content` shape, not the
/// text mirror, and many tools ship neither an output schema nor
/// `structured_content`. Probe-derived classification covers that axis.
pub fn classify_from_schema(output_schema: Option<&serde_json::Value>) -> Option<ResponseShape> {
    let schema = output_schema?;
    let obj = schema.as_object()?;

    // Empty object schema ({}) or a schema with no `type`/`properties`
    // is uninformative: upstreams that ship this are declaring "there IS
    // a schema" without meaningfully constraining the shape. Don't jump
    // to Structured on that alone.
    let has_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .is_some_and(|s| s == "object" || s == "array");
    let has_properties = obj
        .get("properties")
        .and_then(|v| v.as_object())
        .is_some_and(|p| !p.is_empty());
    let has_items = obj.get("items").is_some_and(|v| !v.is_null());

    if has_type && (has_properties || has_items) {
        Some(ResponseShape::Structured)
    } else {
        None
    }
}

/// Does this string parse as JSON? Used to distinguish `TextProse` from
/// `TextJson`. We require the result to be an object or array — a bare
/// string or number parses as JSON but isn't meaningfully "structured
/// JSON the agent should decode." Drops the false-positive where an
/// upstream returns e.g. `"hello"` as literal prose that happens to be
/// a valid JSON string.
fn parses_as_json(s: &str) -> bool {
    let trimmed = s.trim_start();
    if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
        return false;
    }
    serde_json::from_str::<serde_json::Value>(s)
        .ok()
        .is_some_and(|v| v.is_object() || v.is_array())
}

#[cfg(test)]
#[path = "response_shape_tests.rs"]
mod tests;
