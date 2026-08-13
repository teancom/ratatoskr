// pattern: Functional Core
//
// Rendering + lookup logic for the `list-server-tools`, `tool-details`,
// and `inspect-tool-response` gateway tools. Pure: input is a `Vec<Tool>`
// plus the agent's query, output is a renderable string or a chosen
// `Tool`. No I/O, no upstream calls — the shell gathers those before
// invoking us.
//
// Lookup matches on the Lua-sanitized form of each tool's name because
// agent scripts address tools by their sanitized identifier
// (`my_server.some_tool`). The mapping is many-to-one in principle
// (`My.Tool` and `My-Tool` both sanitize to `my_tool`); when an upstream
// actually ships a colliding pair, we refuse to expose either, because
// the inspection gate keys off the sanitized name and a silent winner
// would let the agent inspect one tool and dispatch the other.

use std::collections::HashMap;

use lua_runtime::sanitize_lua_identifier;
use rmcp::model::Tool;

use crate::response_shape::ResponseShape;

/// Outcome of looking up a tool by its Lua-sanitized name.
///
/// `Ambiguous` carries the upstream's *original* tool names so callers
/// can produce diagnostics that name the actual offending tools (e.g.
/// `[get-issue, get.issue]` both sanitize to `get_issue`).
#[derive(Debug, PartialEq, Eq)]
pub enum LookupError {
    NotFound,
    Ambiguous { originals: Vec<String> },
}

/// Find the tool whose sanitized name matches `lua_tool_name`.
///
/// Returns `Err(NotFound)` if nothing matches. Returns
/// `Err(Ambiguous { ... })` if two or more upstream tools sanitize to
/// the same identifier — the inspection gate keys off the sanitized
/// name, so picking a winner would let an agent inspect one tool's
/// schema and silently invoke another. Refuse the lookup instead and
/// surface the collision.
pub fn find_by_lua_name<'a>(
    tools: &'a [Tool],
    lua_tool_name: &str,
) -> Result<&'a Tool, LookupError> {
    let mut matches = tools
        .iter()
        .filter(|t| sanitize_lua_identifier(&t.name) == lua_tool_name);
    let Some(first) = matches.next() else {
        return Err(LookupError::NotFound);
    };
    let extras: Vec<&Tool> = matches.collect();
    if extras.is_empty() {
        Ok(first)
    } else {
        let mut originals = vec![first.name.to_string()];
        originals.extend(extras.iter().map(|t| t.name.to_string()));
        Err(LookupError::Ambiguous { originals })
    }
}

/// Partition `tools` into two sets by sanitized name:
///   - `unique`: tools whose sanitized name appears exactly once.
///   - `collisions`: a map from sanitized name to the original names
///     that collided. Used to log warnings and to refuse to expose any
///     of the colliding entries.
///
/// Both `install_upstream_globals` (Lua dispatch) and
/// `render_tool_summary` (agent-facing catalog) use this so the agent's
/// view and the runtime's view stay in lockstep: a colliding name is
/// either visible to both with a clear warning, or hidden from both.
pub fn partition_by_sanitized_name(tools: &[Tool]) -> (Vec<&Tool>, HashMap<String, Vec<String>>) {
    let mut by_sanitized: HashMap<String, Vec<&Tool>> = HashMap::new();
    for tool in tools {
        let sanitized = sanitize_lua_identifier(&tool.name);
        by_sanitized.entry(sanitized).or_default().push(tool);
    }

    let mut unique: Vec<&Tool> = Vec::new();
    let mut collisions: HashMap<String, Vec<String>> = HashMap::new();
    for (sanitized, group) in by_sanitized {
        if group.len() == 1 {
            unique.push(group[0]);
        } else {
            collisions.insert(
                sanitized,
                group.iter().map(|t| t.name.to_string()).collect(),
            );
        }
    }

    (unique, collisions)
}

/// Render the sanitized-name list of *callable* tools — i.e. tools
/// whose sanitized name is unique on this upstream. Used to populate
/// "available tools: a, b, c" diagnostics when a `tool-details` lookup
/// returns `NotFound`.
///
/// Returned in stable, sorted order so diagnostic text is reproducible.
///
/// Colliding tools are deliberately excluded — they are not callable,
/// so listing them as "available" would mislead the agent. They show
/// up in `render_tool_summary`'s `unavailable` section instead.
pub fn sanitized_tool_names(tools: &[Tool]) -> Vec<String> {
    let (unique, _collisions) = partition_by_sanitized_name(tools);
    // Sanitize once per tool, then sort the strings themselves.
    // (`sort_by_key` with an allocating key function re-runs it on
    // every comparison — O(n log n) allocations instead of O(n).)
    let mut names: Vec<String> = unique
        .into_iter()
        .map(|t| sanitize_lua_identifier(&t.name))
        .collect();
    names.sort_unstable();
    names
}

/// Render a tool's list-view entry: one line per tool with the
/// sanitized name and its full description (whitespace-collapsed
/// onto a single line). This is what `list-server-tools` returns as
/// its CallToolResult text content. The consumer is an agent that
/// has already narrowed from "all upstreams" to one, so there is no
/// reason to hide content — the funnel
/// (`list-servers` → `list-server-tools` → `tool-details`) has
/// already paid the specificity cost.
///
/// Tools whose sanitized name collides with a sibling are listed under
/// a separate `unavailable` section: the agent needs to know they
/// exist on the upstream (otherwise upstream-config debugging is
/// impossible) but also needs to know they're not callable, since the
/// runtime refuses to install them.
pub fn render_tool_summary(tools: &[Tool]) -> String {
    if tools.is_empty() {
        return "(this upstream exposes no tools)".to_string();
    }

    let (unique, collisions) = partition_by_sanitized_name(tools);
    // Stable, sanitized-name order so the listing is reproducible.
    // Sanitize once per tool and sort the precomputed pairs, keeping
    // the allocating sanitizer out of the comparison path.
    let mut unique: Vec<(String, &Tool)> = unique
        .into_iter()
        .map(|t| (sanitize_lua_identifier(&t.name), t))
        .collect();
    unique.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let mut out = String::new();
    for (sanitized, tool) in unique {
        let desc = tool
            .description
            .as_deref()
            .map(collapse_whitespace)
            .unwrap_or_default();
        if desc.is_empty() {
            out.push_str(&format!("- {sanitized}\n"));
        } else {
            out.push_str(&format!("- {sanitized}: {desc}\n"));
        }
    }

    if !collisions.is_empty() {
        out.push_str(
            "\nunavailable (sanitized-name collisions; rename one of each pair upstream):\n",
        );
        let mut sorted: Vec<(&String, &Vec<String>)> = collisions.iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(b.0));
        for (sanitized, originals) in sorted {
            out.push_str(&format!("- {sanitized}: {}\n", originals.join(", ")));
        }
    }

    out
}

/// Render the full detail block for a single tool: sanitized name,
/// original name (if different), description, input schema, and an
/// optional `responseShape` tag + hint pointing at the right
/// `_gateway.*` builtin. JSON shape keeps it machine-parseable; agents
/// can pretty-print if they want.
///
/// `response_shape` is threaded through from the handler, which may
/// have observed it on a prior `inspect-tool-response` (most
/// informative) or derived it from the tool's `outputSchema` (weaker
/// signal, but still useful as a first-contact hint). When `None`, the
/// `responseShape` fields are omitted entirely — honest absence is
/// better than confident guessing.
pub fn render_tool_details(tool: &Tool, response_shape: Option<ResponseShape>) -> String {
    let sanitized = sanitize_lua_identifier(&tool.name);
    let original: &str = &tool.name;

    let mut obj = serde_json::Map::new();
    obj.insert(
        "luaToolName".to_string(),
        serde_json::Value::String(sanitized.clone()),
    );
    if original != sanitized {
        obj.insert(
            "originalName".to_string(),
            serde_json::Value::String(original.to_string()),
        );
    }
    if let Some(desc) = &tool.description {
        obj.insert(
            "description".to_string(),
            serde_json::Value::String(desc.to_string()),
        );
    }
    let input_schema_value = serde_json::Value::Object((*tool.input_schema).clone());
    if let Some(params) = summarize_parameters(&input_schema_value) {
        obj.insert("parameters".to_string(), params);
    }
    obj.insert("inputSchema".to_string(), input_schema_value);
    if let Some(out_schema) = &tool.output_schema {
        obj.insert(
            "outputSchema".to_string(),
            serde_json::Value::Object((**out_schema).clone()),
        );
    }
    if let Some(shape) = response_shape {
        obj.insert(
            "responseShape".to_string(),
            serde_json::Value::String(shape.tag().to_string()),
        );
        obj.insert(
            "responseShapeHint".to_string(),
            serde_json::Value::String(shape.hint().to_string()),
        );
        // MultiBlock carries an observed block count accumulated via merge
        // (max across observations). The `tag()` / `hint()` pair can't
        // surface it without breaking the agent-pattern-matchable static
        // tag and hint strings, so we expose it as a separate additive
        // field. Agents that ignore the field are unaffected; agents
        // building a loop can use it as the upper bound without reading
        // the raw content length.
        if let ResponseShape::MultiBlock { len } = shape {
            obj.insert(
                "responseShapeBlockCount".to_string(),
                serde_json::Value::Number(serde_json::Number::from(len)),
            );
        }
    }

    serde_json::to_string_pretty(&serde_json::Value::Object(obj))
        .unwrap_or_else(|e| format!("(failed to render tool details: {e})"))
}

/// Produce a scannable parameter summary from a JSONSchema input
/// object. Returns `None` when the schema has no top-level `properties`
/// map (tools with empty or opaque inputs — `{}` or non-object schemas).
///
/// Agents repeatedly guess wrong param names when the raw `inputSchema` is
/// deeply nested or bulky, because finding the right key means scanning
/// `properties` + cross-referencing `required` + reading descriptions
/// inline. This flat summary pre-joins those three so the agent can
/// pattern-match by name without walking the full schema tree.
///
/// Output shape:
/// ```json
/// [
///   {"name": "query", "required": true, "type": "string", "description": "..."},
///   {"name": "domain_filter", "required": false, "type": "string", "enum": ["app","ops"], "description": "..."}
/// ]
/// ```
///
/// Ordering: required first, then optional. Alphabetical within each
/// group. Description text is preserved verbatim — param descriptions
/// are generally short and disambiguate the name.
///
/// Nested objects / complex types: only top-level keys are summarized.
/// A parameter whose type is `object` or `array` shows up with
/// `type: "object"` / `type: "array"` and the agent reads `inputSchema`
/// for the nested shape. Trying to flatten nested props would risk
/// collision with flat names and bloat the summary.
pub fn summarize_parameters(input_schema: &serde_json::Value) -> Option<serde_json::Value> {
    let obj = input_schema.as_object()?;
    let properties = obj.get("properties")?.as_object()?;
    if properties.is_empty() {
        return None;
    }

    let required: std::collections::HashSet<&str> = obj
        .get("required")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    let mut entries: Vec<serde_json::Value> = properties
        .iter()
        .map(|(name, schema)| {
            build_parameter_entry(name.as_str(), required.contains(name.as_str()), schema)
        })
        .collect();

    // Sort required-first, then alphabetical within each group. Stable
    // order matters for reproducible output and for agents that
    // remember "first entry in parameters" from a prior turn.
    entries.sort_by(|a, b| {
        let a_required = a.get("required").and_then(|v| v.as_bool()).unwrap_or(false);
        let b_required = b.get("required").and_then(|v| v.as_bool()).unwrap_or(false);
        b_required.cmp(&a_required).then_with(|| {
            let a_name = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let b_name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
            a_name.cmp(b_name)
        })
    });

    Some(serde_json::Value::Array(entries))
}

fn build_parameter_entry(
    name: &str,
    required: bool,
    schema: &serde_json::Value,
) -> serde_json::Value {
    let mut entry = serde_json::Map::new();
    entry.insert(
        "name".to_string(),
        serde_json::Value::String(name.to_string()),
    );
    entry.insert("required".to_string(), serde_json::Value::Bool(required));
    entry.insert(
        "type".to_string(),
        serde_json::Value::String(extract_type_label(schema)),
    );
    if let Some(enum_vals) = extract_enum(schema) {
        entry.insert("enum".to_string(), enum_vals);
    }
    if let Some(desc) = schema
        .get("description")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        entry.insert(
            "description".to_string(),
            serde_json::Value::String(desc.to_string()),
        );
    }
    serde_json::Value::Object(entry)
}

/// Render a JSONSchema type constraint as a short human-readable string.
///
/// Handles three common shapes:
///   - `"type": "string"` → `"string"` (most common)
///   - `"type": ["string", "null"]` → `"string|null"` (nullable-union shape)
///   - `anyOf` / `oneOf` / no type at all → `"union"` / `"any"` (fall back;
///     agent reads inputSchema for details)
fn extract_type_label(schema: &serde_json::Value) -> String {
    let Some(obj) = schema.as_object() else {
        return "any".to_string();
    };
    match obj.get("type") {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Array(arr)) => {
            let parts: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            if parts.is_empty() {
                "any".to_string()
            } else {
                parts.join("|")
            }
        }
        _ => {
            if obj.contains_key("anyOf") || obj.contains_key("oneOf") {
                "union".to_string()
            } else {
                "any".to_string()
            }
        }
    }
}

/// If the schema has an `enum` constraint, return it verbatim as a JSON
/// array. Preserves ordering from the source schema.
fn extract_enum(schema: &serde_json::Value) -> Option<serde_json::Value> {
    schema
        .get("enum")
        .and_then(|v| v.as_array())
        .map(|arr| serde_json::Value::Array(arr.clone()))
}

/// Flatten an upstream tool description into a single line.
///
/// Collapse every run of whitespace — spaces, tabs, newlines, paragraph
/// breaks — into a single space, trimming the ends. Shared by any
/// renderer that needs to fit free-form text onto one line: the
/// `list-server-tools` view uses it so one tool stays on one output
/// line (splitting on `\n` reliably partitions the catalog), and
/// `hint::first_sentence` uses it so a single indented line doesn't
/// eat the whole hint budget on leading tabs.
///
/// UTF-8 safe: `split_whitespace` operates on chars, never on bytes,
/// so multi-byte text (non-ASCII, emoji) passes through cleanly.
pub(crate) fn collapse_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
#[path = "tool_details_tests.rs"]
mod tests;
