use super::*;
use serde_json::{Map, Value, json};

fn args_from(v: Value) -> Map<String, Value> {
    v.as_object().expect("test literal is an object").clone()
}

#[test]
fn extracts_non_empty_script() {
    let args = args_from(json!({ "script": "result(1)" }));
    assert_eq!(
        parse_script_argument(Some(&args)).expect("valid"),
        "result(1)"
    );
}

#[test]
fn none_arguments_is_missing() {
    assert_eq!(parse_script_argument(None), Err(ScriptInputError::Missing));
}

#[test]
fn empty_object_is_missing() {
    let args = args_from(json!({}));
    assert_eq!(
        parse_script_argument(Some(&args)),
        Err(ScriptInputError::Missing)
    );
}

#[test]
fn wrong_type_is_reported() {
    let args = args_from(json!({ "script": 42 }));
    assert_eq!(
        parse_script_argument(Some(&args)),
        Err(ScriptInputError::WrongType)
    );
}

#[test]
fn empty_string_is_reported() {
    let args = args_from(json!({ "script": "" }));
    assert_eq!(
        parse_script_argument(Some(&args)),
        Err(ScriptInputError::Empty)
    );
}

#[test]
fn extra_fields_are_ignored() {
    // Defense in depth / forward compat: adding new optional fields to
    // the tool schema shouldn't retroactively break scripts that only
    // populate `script`.
    let args = args_from(json!({ "script": "result(nil)", "extra": true }));
    assert_eq!(
        parse_script_argument(Some(&args)).expect("valid"),
        "result(nil)"
    );
}

#[test]
fn script_at_limit_is_accepted() {
    let script = "x".repeat(MAX_SCRIPT_BYTES);
    let args = args_from(json!({ "script": script }));
    assert!(parse_script_argument(Some(&args)).is_ok());
}

#[test]
fn script_over_limit_is_too_large() {
    let script = "x".repeat(MAX_SCRIPT_BYTES + 1);
    let len = script.len();
    let args = args_from(json!({ "script": script }));
    assert_eq!(
        parse_script_argument(Some(&args)),
        Err(ScriptInputError::TooLarge(len))
    );
}

#[test]
fn null_value_is_wrong_type() {
    let args = args_from(json!({ "script": null }));
    assert_eq!(
        parse_script_argument(Some(&args)),
        Err(ScriptInputError::WrongType)
    );
}

#[test]
fn whitespace_only_is_accepted_as_valid_input() {
    // Whitespace-only scripts are syntactically valid Lua; validation
    // here only checks for empty strings. The runtime will return
    // MissingResult if the script never calls result().
    let args = args_from(json!({ "script": "   " }));
    assert!(parse_script_argument(Some(&args)).is_ok());
}
