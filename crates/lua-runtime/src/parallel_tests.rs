use super::*;
use serde_json::json;

#[test]
fn empty_array_is_valid_and_yields_empty_list() {
    let parsed = parse_task_list(json!([])).expect("empty array is valid");
    assert!(parsed.is_empty());
}

#[test]
fn well_formed_tasks_parse_in_order() {
    let parsed = parse_task_list(json!([
        { "server": "gitlab", "tool": "get_mr", "args": { "id": 1 } },
        { "server": "datadog", "tool": "search_logs", "args": { "q": "*" } },
    ]))
    .expect("well-formed input parses");

    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].server, "gitlab");
    assert_eq!(parsed[0].tool, "get_mr");
    assert_eq!(parsed[0].args.get("id").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(parsed[1].server, "datadog");
}

#[test]
fn args_is_optional_and_defaults_to_empty_object() {
    let parsed = parse_task_list(json!([
        { "server": "gitlab", "tool": "ping" },
    ]))
    .expect("args-less task should parse");
    assert_eq!(parsed.len(), 1);
    assert!(parsed[0].args.is_empty());
}

#[test]
fn args_null_is_treated_as_empty_object() {
    // Lua scripts that write `args = nil` explicitly serialize to
    // JSON null; we want to treat that identically to "absent".
    let parsed = parse_task_list(json!([
        { "server": "gitlab", "tool": "ping", "args": null },
    ]))
    .expect("null args should parse");
    assert!(parsed[0].args.is_empty());
}

#[test]
fn non_array_outer_value_errors() {
    let err = parse_task_list(json!({ "server": "x", "tool": "y" }))
        .expect_err("object at the outer level is an error");
    assert_eq!(
        err,
        ParallelInputError::NotAnArray {
            got: "object".to_string()
        }
    );
}

#[test]
fn outer_null_errors() {
    let err = parse_task_list(json!(null)).expect_err("null at the outer level is an error");
    assert_eq!(
        err,
        ParallelInputError::NotAnArray {
            got: "nil".to_string()
        }
    );
}

#[test]
fn non_table_task_errors_with_1_based_index() {
    let err = parse_task_list(json!([
        { "server": "gitlab", "tool": "a" },
        "not a table",
    ]))
    .expect_err("scalar task is an error");
    assert_eq!(
        err,
        ParallelInputError::TaskNotATable {
            index: 2,
            got: "string".to_string()
        }
    );
}

#[test]
fn missing_server_errors() {
    let err = parse_task_list(json!([
        { "tool": "a" },
    ]))
    .expect_err("no server means error");
    assert_eq!(
        err,
        ParallelInputError::MissingField {
            index: 1,
            field: "server"
        }
    );
}

#[test]
fn missing_tool_errors() {
    let err = parse_task_list(json!([
        { "server": "x" },
    ]))
    .expect_err("no tool means error");
    assert_eq!(
        err,
        ParallelInputError::MissingField {
            index: 1,
            field: "tool"
        }
    );
}

#[test]
fn empty_server_errors() {
    let err = parse_task_list(json!([
        { "server": "", "tool": "t" },
    ]))
    .expect_err("empty server means error");
    assert_eq!(
        err,
        ParallelInputError::EmptyField {
            index: 1,
            field: "server"
        }
    );
}

#[test]
fn empty_tool_errors() {
    let err = parse_task_list(json!([
        { "server": "x", "tool": "" },
    ]))
    .expect_err("empty tool means error");
    assert_eq!(
        err,
        ParallelInputError::EmptyField {
            index: 1,
            field: "tool"
        }
    );
}

#[test]
fn wrong_type_server_errors_with_type_label() {
    let err = parse_task_list(json!([
        { "server": 42, "tool": "t" },
    ]))
    .expect_err("number server means error");
    assert_eq!(
        err,
        ParallelInputError::WrongFieldType {
            index: 1,
            field: "server",
            got: "number".to_string(),
        }
    );
}

#[test]
fn wrong_type_tool_errors_with_type_label() {
    let err = parse_task_list(json!([
        { "server": "x", "tool": true },
    ]))
    .expect_err("boolean tool means error");
    assert_eq!(
        err,
        ParallelInputError::WrongFieldType {
            index: 1,
            field: "tool",
            got: "boolean".to_string(),
        }
    );
}

#[test]
fn non_object_args_errors() {
    let err = parse_task_list(json!([
        { "server": "x", "tool": "t", "args": [1, 2, 3] },
    ]))
    .expect_err("array args means error");
    assert_eq!(
        err,
        ParallelInputError::ArgsNotAnObject {
            index: 1,
            got: "array".to_string()
        }
    );
}

#[test]
fn scalar_args_errors() {
    let err = parse_task_list(json!([
        { "server": "x", "tool": "t", "args": "bogus" },
    ]))
    .expect_err("string args means error");
    assert_eq!(
        err,
        ParallelInputError::ArgsNotAnObject {
            index: 1,
            got: "string".to_string()
        }
    );
}

#[test]
fn second_task_errors_surface_with_correct_index() {
    // Ensures the index in error messages is the OFFENDING task's
    // index, not a prior good task's.
    let err = parse_task_list(json!([
        { "server": "a", "tool": "a" },
        { "server": "b", "tool": "b" },
        { "server": "c" },
    ]))
    .expect_err("third task missing tool");
    assert_eq!(
        err,
        ParallelInputError::MissingField {
            index: 3,
            field: "tool"
        }
    );
}

#[test]
fn describe_json_shape_round_trip() {
    assert_eq!(describe_json_shape(&json!(null)), "nil");
    assert_eq!(describe_json_shape(&json!(true)), "boolean");
    assert_eq!(describe_json_shape(&json!(1)), "number");
    assert_eq!(describe_json_shape(&json!("x")), "string");
    assert_eq!(describe_json_shape(&json!([])), "array");
    assert_eq!(describe_json_shape(&json!({})), "object");
}

#[test]
fn unused_task_keys_are_ignored() {
    // A descriptor with extra fields ("id", "comment") shouldn't
    // error — agents sometimes annotate tasks for their own
    // bookkeeping. We drop unused keys silently so Lua-side
    // comments survive serialization.
    let parsed = parse_task_list(json!([
        { "server": "x", "tool": "t", "comment": "fetch the important one" },
    ]))
    .expect("extra keys should be ignored");
    assert_eq!(parsed.len(), 1);
}
