// pattern: Functional Core
//
// Renders an `ExecutionOutcome::Offloaded` into the human-readable
// text block we return to the agent in place of the raw data.
//
// The message has to do three things at once:
//   1. Tell the agent what it's looking at (size, shape summary) so
//      they can decide whether to retrieve it at all.
//   2. Give them the ID to pass to `_gateway.get_result` next call.
//   3. Suggest concrete next-script shapes for the common patterns
//      (array of objects, top-level object, plain string) so the
//      agent doesn't have to guess how to filter the payload.
//
// Pure: no I/O, no clock. Input is a Shape + id + byte count; output
// is a formatted string.

use lua_runtime::{Shape, format_shape};

/// Build the agent-facing text for a single offloaded result.
///
/// Layout (lines, no trailing whitespace):
///
///   Result offloaded: <human-size> (<byte_size> bytes, <top-level-summary>).
///   Execution ID: <uuid>
///
///   Structure:
///   <indented shape tree>
///
///   Retrieve in a follow-up script via:
///     local data = _gateway.get_result({ id = "<uuid>" })
///     <filtering example tailored to the shape>
///
/// The filtering example is shape-sensitive: arrays get a slice, top-
/// level objects get a field-access hint, strings get a substring hint.
/// The Lua on our side doesn't use `:await()`, so the hint text is
/// phrased as a direct expression rather than an awaited call.
pub fn render(execution_id: &str, byte_size: usize, shape: &Shape) -> String {
    let mut out = String::new();
    out.push_str(&headline(byte_size, shape));
    out.push('\n');
    out.push_str(&format!("Execution ID: {execution_id}\n"));

    out.push_str("\nStructure:\n");
    for line in format_shape(shape, 1) {
        out.push_str(&line);
        out.push('\n');
    }

    out.push_str("\nRetrieve in a follow-up script via:\n\n");
    out.push_str(&retrieval_snippet(execution_id, shape));
    out
}

fn headline(byte_size: usize, shape: &Shape) -> String {
    let summary = top_level_summary(shape);
    format!(
        "Result offloaded: {} ({} bytes, {}).",
        human_bytes(byte_size),
        byte_size,
        summary
    )
}

fn top_level_summary(shape: &Shape) -> String {
    match shape {
        Shape::Null => "null".to_string(),
        Shape::Bool => "boolean".to_string(),
        Shape::Number => "number".to_string(),
        Shape::String => "string".to_string(),
        Shape::Array { len, .. } | Shape::ArrayOpaque { len } => {
            format!("array of {len} items")
        }
        Shape::ArrayMixed { len } => format!("array of {len} items (mixed types)"),
        Shape::Object { fields } => format!("object with {} keys", fields.len()),
        Shape::ObjectOpaque { key_count } => format!("object with {key_count} keys"),
    }
}

fn retrieval_snippet(execution_id: &str, shape: &Shape) -> String {
    let fetch_line = format!("  local data = _gateway.get_result({{ id = \"{execution_id}\" }})\n");

    // Envelope detection runs first: when the offloaded payload is a
    // full `CallToolResult` JSON (as stored by the inspect-tool-response
    // offload path), drilling into `data.content` / `data.structured_content`
    // gives the agent a snippet that actually shrinks the response.
    // The generic Object branch below would suggest `result({ content =
    // data.content, ... })` — which, if `content[1].text` is the entire
    // several-megabyte payload, just re-ships the same blob.
    if let Some(snippet) = envelope_snippet(&fetch_line, shape) {
        return snippet;
    }

    match shape {
        Shape::Array { .. } | Shape::ArrayOpaque { .. } => {
            let mut s = fetch_line;
            s.push_str("  -- Example: keep the first 10 items\n");
            s.push_str("  local subset = {}\n");
            s.push_str("  for i = 1, math.min(10, #data) do\n");
            s.push_str("    table.insert(subset, data[i])\n");
            s.push_str("  end\n");
            s.push_str("  result(subset)\n");
            s
        }
        Shape::Object { fields } => {
            let mut s = fetch_line;
            s.push_str("  -- Example: pick a few top-level fields\n");
            // Show up to three top-level fields as example keys. We do
            // NOT show the full set — that would leak shape info into
            // the example that's already in the Structure block above.
            let sample: Vec<&str> = fields.iter().take(3).map(|(k, _)| k.as_str()).collect();
            if sample.is_empty() {
                s.push_str("  result(data)\n");
            } else {
                s.push_str("  result({\n");
                for k in &sample {
                    s.push_str(&format!("    {k} = data.{k},\n"));
                }
                s.push_str("  })\n");
            }
            s
        }
        Shape::String => {
            let mut s = fetch_line;
            s.push_str("  -- Example: first 1000 characters\n");
            s.push_str("  result(string.sub(data, 1, 1000))\n");
            s
        }
        _ => {
            let mut s = fetch_line;
            s.push_str("  result(data)\n");
            s
        }
    }
}

/// Classification of envelope shapes we recognize as `CallToolResult`-like.
///
/// The rmcp `CallToolResult` wire shape is
/// `{ content: [{type, text?, data?, ...}, ...], structured_content?: {...},
/// is_error?: bool, meta?: {...} }`. When an offloaded payload matches one
/// of these shapes, we emit a retrieval snippet that reaches *inside* the
/// envelope rather than duplicating it at the top level.
///
/// We deliberately do NOT depend on the rmcp types here — shape detection
/// uses the already-inferred `Shape` tree, which is what this module
/// receives. Coupling to rmcp's own structs would pull a protocol-layer
/// dependency into a Functional Core module that should stay value-only.
#[derive(Debug, PartialEq, Eq)]
enum Envelope {
    /// `content` is an array whose first item is a text block. The
    /// large payload lives in `data.content[1].text`.
    SingleText,
    /// `content` has multiple items. The agent probably wants to walk
    /// them; we give a bounded-iteration snippet.
    MultiBlock { len: usize },
    /// `structured_content` is present and non-trivial. The payload the
    /// agent likely wants is under `data.structured_content` rather
    /// than the `content` mirror.
    Structured,
}

/// If `shape` looks like a `CallToolResult` JSON envelope, return a
/// targeted retrieval snippet that drills into it. Otherwise return
/// `None` so the caller falls through to the generic shape-based
/// snippet.
fn envelope_snippet(fetch_line: &str, shape: &Shape) -> Option<String> {
    let envelope = classify_envelope(shape)?;
    Some(render_envelope_snippet(fetch_line, envelope))
}

fn classify_envelope(shape: &Shape) -> Option<Envelope> {
    // Envelope detection requires an Object at the top level with a
    // `content` field. That's the invariant for any `CallToolResult`
    // the gateway could have stored — success or error, structured or
    // not, rmcp always emits `content` (possibly empty).
    let Shape::Object { fields } = shape else {
        return None;
    };
    let content_shape = fields
        .iter()
        .find_map(|(k, v)| (k == "content").then_some(v))?;
    let has_structured = fields
        .iter()
        .any(|(k, v)| k == "structured_content" && !matches!(v, Shape::Null));

    // `structured_content`, when present, is the preferred drill target:
    // tools that advertise it ship the "machine-parseable" half of their
    // response there and mirror a human-readable form in `content`.
    if has_structured {
        return Some(Envelope::Structured);
    }

    match content_shape {
        Shape::Array { items, len } if looks_like_text_content(items) => {
            if *len <= 1 {
                Some(Envelope::SingleText)
            } else {
                Some(Envelope::MultiBlock { len: *len })
            }
        }
        // An array with items we can't identify, or an opaque array,
        // still gets the multi-content treatment — bounded iteration
        // is always safer than dumping the whole envelope.
        Shape::Array { len, .. } => {
            if *len <= 1 {
                Some(Envelope::SingleText)
            } else {
                Some(Envelope::MultiBlock { len: *len })
            }
        }
        Shape::ArrayOpaque { len } => {
            if *len <= 1 {
                Some(Envelope::SingleText)
            } else {
                Some(Envelope::MultiBlock { len: *len })
            }
        }
        _ => None,
    }
}

/// A content-item shape looks text-bearing if it's an object carrying a
/// `text` string field. `type` is also part of the spec but we don't
/// hard-require it — the `text` field is the one the snippet drills into.
fn looks_like_text_content(item_shape: &Shape) -> bool {
    let Shape::Object { fields } = item_shape else {
        return false;
    };
    fields
        .iter()
        .any(|(k, v)| k == "text" && matches!(v, Shape::String))
}

fn render_envelope_snippet(fetch_line: &str, envelope: Envelope) -> String {
    let mut s = String::from(fetch_line);
    match envelope {
        Envelope::SingleText => {
            s.push_str("  -- Example: first 1000 characters of the content text\n");
            s.push_str("  result(string.sub(data.content[1].text, 1, 1000))\n");
        }
        Envelope::MultiBlock { len } => {
            let cap = len.min(10);
            s.push_str(&format!(
                "  -- Example: first {cap} content items (of {len})\n"
            ));
            s.push_str("  local subset = {}\n");
            s.push_str(&format!("  for i = 1, math.min({cap}, #data.content) do\n"));
            s.push_str("    table.insert(subset, data.content[i])\n");
            s.push_str("  end\n");
            s.push_str("  result(subset)\n");
        }
        Envelope::Structured => {
            s.push_str("  -- Example: structured_content carries the machine-parseable payload\n");
            s.push_str("  result(data.structured_content)\n");
        }
    }
    s
}

/// Human-readable byte size. Two decimal places at KiB+; whole
/// numbers at B. Keeps the headline glanceable without us having to
/// reach for an extra crate (byte-unit, humansize, etc.) for what's
/// ultimately a short helper function.
fn human_bytes(n: usize) -> String {
    const KIB: usize = 1024;
    const MIB: usize = 1024 * 1024;
    const GIB: usize = 1024 * 1024 * 1024;
    if n >= GIB {
        format!("{:.2} GiB", n as f64 / GIB as f64)
    } else if n >= MIB {
        format!("{:.2} MiB", n as f64 / MIB as f64)
    } else if n >= KIB {
        format!("{:.2} KiB", n as f64 / KIB as f64)
    } else {
        format!("{n} B")
    }
}

#[cfg(test)]
#[path = "offload_message_tests.rs"]
mod tests;
