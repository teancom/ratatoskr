// pattern: Functional Core
//
// Sanitize arbitrary strings (MCP server names, tool names) into valid Lua
// identifiers, plus collision-handling helpers. Pure: no I/O, no time, no
// randomness. Trivially testable.
//
// Rules:
// - replace every non-[A-Za-z0-9_] with '_'
// - prepend '_' if the result starts with a digit
// - prepend '_' if the result collides with a Lua reserved word
//
// Collision surface: because both transformation prefixes use '_', an input
// that already starts with '_' can map to the same output as its shorter
// counterpart. For example "nil" → "_nil" and "_nil" → "_nil"; "1foo" →
// "_1foo" and "_1foo" → "_1foo". partition_unique_by_sanitized handles these
// correctly (both sides are quarantined), but operators should be aware that
// naming tools with a leading '_' narrows the collision-free name space.

use std::collections::HashMap;

const LUA_RESERVED: &[&str] = &[
    "and", "break", "do", "else", "elseif", "end", "false", "for", "function", "goto", "if", "in",
    "local", "nil", "not", "or", "repeat", "return", "then", "true", "until", "while",
];

pub fn sanitize_lua_identifier(name: &str) -> String {
    let mut out: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    if out.is_empty() {
        return "_".to_string();
    }

    if out.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        out.insert(0, '_');
    }

    if LUA_RESERVED.contains(&out.as_str()) {
        out.insert(0, '_');
    }

    out
}

/// Partition a list of original tool names by their sanitized form.
///
/// Returns `(unique, collisions)`:
///   - `unique` is the subset whose sanitized name is unique within the
///     input. These are safe to register as Lua globals.
///   - `collisions` maps each colliding sanitized name to the original
///     names that produced it. Both sides need this: the runtime to
///     refuse installation, and the gateway's catalog to surface the
///     conflict to the agent and the operator.
///
/// Tools sharing a sanitized name are *both* (or all) excluded from
/// `unique` — picking a winner would let an agent inspect one tool's
/// schema and silently invoke another.
pub fn partition_unique_by_sanitized<S: AsRef<str>>(
    names: &[S],
) -> (Vec<String>, HashMap<String, Vec<String>>) {
    let mut by_sanitized: HashMap<String, Vec<String>> = HashMap::new();
    for name in names {
        let sanitized = sanitize_lua_identifier(name.as_ref());
        by_sanitized
            .entry(sanitized)
            .or_default()
            .push(name.as_ref().to_string());
    }

    let mut unique: Vec<String> = Vec::new();
    let mut collisions: HashMap<String, Vec<String>> = HashMap::new();
    for (sanitized, originals) in by_sanitized {
        if originals.len() == 1 {
            unique.push(originals.into_iter().next().expect("len==1"));
        } else {
            collisions.insert(sanitized, originals);
        }
    }

    (unique, collisions)
}

#[cfg(test)]
#[path = "identifier_tests.rs"]
mod tests;
