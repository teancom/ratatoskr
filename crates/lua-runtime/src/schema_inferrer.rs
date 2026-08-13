// pattern: Functional Core
//
// Inferred-schema renderer for large-result offloading.
//
// When a Lua `execute` script returns more than the configured byte
// threshold, the gateway stores the full payload internally and returns
// a summary to the caller that describes the SHAPE of the data without
// shipping the bytes. These two halves are the shape generator and the
// shape renderer:
//
//   - `infer_shape(&Value, max_depth)` walks a `serde_json::Value` and
//     produces a `Shape` enum — a simplified JSON-Schema-ish
//     description with type tags, nested object properties, and array
//     item shapes. Bounded depth: at the limit, objects and arrays
//     become opaque.
//
//   - `format_shape(&Shape, indent)` renders a `Shape` back to indented
//     text lines suitable for inclusion in the offloaded-response
//     message.
//
// Both functions are pure: no I/O, no clock, no randomness. `Shape` is an
// enum-oriented structural description rather than a loose string-keyed
// bag, so callers get exhaustive matches instead of stringly-typed checks.

use serde_json::Value;

/// Default recursion depth for `infer_shape`. Four levels gives enough
/// structure for a typical MCP tool response (outer container, list of
/// items, item fields, nested "metadata" subobjects) without producing
/// unbounded output for pathological inputs like self-referential JSON.
pub const DEFAULT_MAX_DEPTH: usize = 4;

/// Inferred structural description of a JSON value.
///
/// Discriminated enum rather than an open map so downstream renderers
/// get exhaustive-match coverage. The variants mirror the TS
/// `schemaInferrer` output but drop the open-ended `unknown` escape
/// hatch: every `serde_json::Value` has a known type.
#[derive(Debug, Clone, PartialEq)]
pub enum Shape {
    Null,
    Bool,
    Number,
    String,
    /// Array with a known item shape and a recorded length. We carry
    /// the length through so the renderer can say "array of 42 items"
    /// without re-walking the data.
    Array {
        items: Box<Shape>,
        len: usize,
    },
    /// Array whose elements have different types (e.g. `[1, "hello",
    /// null]`). We record the length but cannot assert a single item
    /// type — the rendered output says "mixed types" rather than
    /// asserting a false uniform type.
    ArrayMixed {
        len: usize,
    },
    /// Array that exceeded the depth cap — rendered opaquely as
    /// "array (truncated)" because we didn't walk the children.
    ArrayOpaque {
        len: usize,
    },
    /// Object with a known set of fields. Field order is insertion
    /// order from `serde_json::Map` (BTreeMap under the hood when the
    /// `preserve_order` feature is off — alphabetical).
    Object {
        fields: Vec<(String, Shape)>,
    },
    /// Object that exceeded the depth cap.
    ObjectOpaque {
        key_count: usize,
    },
}

/// Walk `value` and build a `Shape` describing its structure, up to
/// `max_depth` levels deep. Beyond the limit, objects and arrays are
/// rendered as opaque (type only, no children).
///
/// Depth semantics: depth 0 is the root value. An object whose children
/// are primitives fits in depth 1. A list of objects with primitive
/// fields needs depth 2 (list → item → fields).
pub fn infer_shape(value: &Value, max_depth: usize) -> Shape {
    infer_inner(value, 0, max_depth)
}

/// Returns true if two JSON values share the same top-level type tag
/// (null, bool, number, string, array, object). Used to detect
/// heterogeneous arrays without full recursive shape comparison.
fn same_type_tag(a: &Value, b: &Value) -> bool {
    matches!(
        (a, b),
        (Value::Null, Value::Null)
            | (Value::Bool(_), Value::Bool(_))
            | (Value::Number(_), Value::Number(_))
            | (Value::String(_), Value::String(_))
            | (Value::Array(_), Value::Array(_))
            | (Value::Object(_), Value::Object(_))
    )
}

fn infer_inner(value: &Value, depth: usize, max_depth: usize) -> Shape {
    match value {
        Value::Null => Shape::Null,
        Value::Bool(_) => Shape::Bool,
        // Integers and floats both map to Number; the distinction is not
        // preserved. MCP tool schemas typically don't distinguish integer
        // vs. float, and the output shape is stable/familiar to agents —
        // changing it would churn agent-facing text for no semantic gain.
        Value::Number(_) => Shape::Number,
        Value::String(_) => Shape::String,
        Value::Array(arr) => {
            if depth >= max_depth {
                return Shape::ArrayOpaque { len: arr.len() };
            }
            let len = arr.len();
            // Use the first element as the representative shape, then
            // verify all other elements share the same JSON type tag.
            // If any element diverges, emit `ArrayMixed` rather than
            // silently asserting the first element's type for the
            // whole array — a false uniform type is strictly worse
            // than an honest "mixed types" label.
            let first_shape = arr
                .first()
                .map(|v| infer_inner(v, depth + 1, max_depth))
                .unwrap_or(Shape::Null);

            let is_mixed = arr
                .iter()
                .skip(1)
                .any(|v| !same_type_tag(v, arr.first().expect("checked non-empty above")));

            if is_mixed {
                Shape::ArrayMixed { len }
            } else {
                Shape::Array {
                    items: Box::new(first_shape),
                    len,
                }
            }
        }
        Value::Object(map) => {
            if depth >= max_depth {
                return Shape::ObjectOpaque {
                    key_count: map.len(),
                };
            }
            let fields = map
                .iter()
                .map(|(k, v)| (k.clone(), infer_inner(v, depth + 1, max_depth)))
                .collect();
            Shape::Object { fields }
        }
    }
}

/// Format a `Shape` into indented human-readable lines.
///
/// Renders with two-space indentation scaled by `indent`. Top-level
/// callers pass `indent = 0`; nested calls bump by one per level.
/// Output lines never have trailing whitespace.
pub fn format_shape(shape: &Shape, indent: usize) -> Vec<String> {
    let mut lines = Vec::new();
    write_shape(&mut lines, shape, indent);
    lines
}

fn write_shape(out: &mut Vec<String>, shape: &Shape, indent: usize) {
    let pad = "  ".repeat(indent);
    match shape {
        Shape::Null => out.push(format!("{pad}null")),
        Shape::Bool => out.push(format!("{pad}boolean")),
        Shape::Number => out.push(format!("{pad}number")),
        Shape::String => out.push(format!("{pad}string")),
        Shape::Array { items, len } => {
            if *len == 0 {
                // Empty array: there are no items, so there is no item
                // type to assert. Omit the "of:" sub-block entirely —
                // "of: null" would be false (there are no null items;
                // there are no items at all).
                out.push(format!("{pad}array (0 items)"));
            } else {
                out.push(format!("{pad}array ({len} items) of:"));
                write_shape(out, items, indent + 1);
            }
        }
        Shape::ArrayMixed { len } => {
            out.push(format!("{pad}array ({len} items, mixed types)"));
        }
        Shape::ArrayOpaque { len } => {
            out.push(format!("{pad}array ({len} items, truncated)"));
        }
        Shape::Object { fields } => {
            if fields.is_empty() {
                out.push(format!("{pad}object (empty)"));
                return;
            }
            for (name, child) in fields {
                match child {
                    // Compact one-line rendering for primitive fields:
                    //   "  field_name: type"
                    Shape::Null
                    | Shape::Bool
                    | Shape::Number
                    | Shape::String
                    | Shape::ArrayMixed { .. }
                    | Shape::ObjectOpaque { .. }
                    | Shape::ArrayOpaque { .. } => {
                        let type_str = primitive_or_opaque_label(child);
                        out.push(format!("{pad}{name}: {type_str}"));
                    }
                    // Structural children get their own sub-block with
                    // the field name as header.
                    Shape::Array { items, len } => {
                        if *len == 0 {
                            out.push(format!("{pad}{name}: array (0 items)"));
                        } else {
                            out.push(format!("{pad}{name}: array ({len} items) of:"));
                            write_shape(out, items, indent + 1);
                        }
                    }
                    Shape::Object { fields: sub_fields } if sub_fields.is_empty() => {
                        out.push(format!("{pad}{name}: object (empty)"));
                    }
                    Shape::Object { .. } => {
                        out.push(format!("{pad}{name}: object"));
                        write_shape(out, child, indent + 1);
                    }
                }
            }
        }
        Shape::ObjectOpaque { key_count } => {
            out.push(format!("{pad}object ({key_count} keys, truncated)"));
        }
    }
}

/// Returns a compact one-line label for primitive or opaque shapes, for
/// use in the compact object-field rendering path.
///
/// # Panics
///
/// Panics if called with `Shape::Array` or `Shape::Object` — those
/// structural variants require a multi-line sub-block and are handled
/// by separate arms in `write_shape`. Call sites must ensure only
/// primitive/opaque shapes reach this function.
fn primitive_or_opaque_label(shape: &Shape) -> String {
    match shape {
        Shape::Null => "null".to_string(),
        Shape::Bool => "boolean".to_string(),
        Shape::Number => "number".to_string(),
        Shape::String => "string".to_string(),
        Shape::ArrayMixed { len } => format!("array ({len} items, mixed types)"),
        Shape::ArrayOpaque { len } => format!("array ({len} items, truncated)"),
        Shape::ObjectOpaque { key_count } => format!("object ({key_count} keys, truncated)"),
        _ => unreachable!("primitive_or_opaque_label called on a structural shape"),
    }
}

#[cfg(test)]
#[path = "schema_inferrer_tests.rs"]
mod tests;
