/// Describe a JSON value's shape in one word — used by error messages so
/// callers see "got array" or "got number" rather than a raw value dump.
///
/// `Null` maps to `"nil"` because all callers are Lua-facing: a Lua
/// script that passes `nil` explicitly serializes to JSON `null`, and
/// "got nil" is the Lua-idiomatic label the agent wrote. Keeping one
/// canonical definition here prevents the copies in `parallel.rs`,
/// `expand_template.rs`, and `runtime.rs` from drifting independently.
pub(crate) fn describe_json_shape(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "nil",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}
