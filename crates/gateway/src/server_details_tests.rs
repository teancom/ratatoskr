use super::*;

fn base_input<'a>() -> ServerDetailsInput<'a> {
    ServerDetailsInput {
        name: "datadog-us",
        lua_name: "datadog_us",
        kind: "http",
        server_name: None,
        server_version: None,
        hint: None,
        upstream_instructions: None,
        health: None,
    }
}

fn parse(rendered: &str) -> Value {
    serde_json::from_str(rendered).expect("render output must be valid JSON")
}

#[test]
fn minimal_input_renders_only_the_required_fields() {
    // Property: the mandatory three fields (name, luaServerName,
    // transport) always appear. Everything else is opt-in based on
    // whether the upstream or the config provided content.
    let input = base_input();
    let v = parse(&render(&input));
    assert_eq!(v["name"], "datadog-us");
    assert_eq!(v["luaServerName"], "datadog_us");
    assert_eq!(v["transport"], "http");
    // Optional fields must be absent, not null — an agent reader
    // can trust that present keys have content.
    assert!(v.get("serverImplementation").is_none());
    assert!(v.get("hint").is_none());
    assert!(v.get("instructions").is_none());
}

#[test]
fn server_implementation_appears_when_any_part_is_known() {
    let mut input = base_input();
    input.server_name = Some("datadog-mcp");
    input.server_version = Some("1.0.0");
    let v = parse(&render(&input));
    assert_eq!(v["serverImplementation"]["name"], "datadog-mcp");
    assert_eq!(v["serverImplementation"]["version"], "1.0.0");
}

#[test]
fn server_implementation_still_appears_when_only_name_is_known() {
    // Upstreams that forget to set `version` shouldn't cause the
    // whole section to vanish — the `name` alone is useful.
    let mut input = base_input();
    input.server_name = Some("some-server");
    let v = parse(&render(&input));
    assert_eq!(v["serverImplementation"]["name"], "some-server");
    assert!(v["serverImplementation"].get("version").is_none());
}

#[test]
fn server_implementation_is_suppressed_when_only_version_is_known() {
    // Property (mirror of the name-only test above): a version
    // without a name is semantically useless — there's no entity
    // to attach the version to — so the block is suppressed
    // entirely rather than rendered as `{ "version": "..." }`.
    // Pins the "present keys always have meaningful content"
    // invariant stated in the module docstring against future
    // change.
    let mut input = base_input();
    input.server_version = Some("1.0.0");
    let v = parse(&render(&input));
    assert!(
        v.get("serverImplementation").is_none(),
        "serverImplementation must be absent when name is missing, got: {v}"
    );
}

#[test]
fn hint_and_instructions_are_independent_fields() {
    // Property: presence of one does NOT imply the other. An
    // upstream with a hint but no original instructions (e.g.
    // after a config override) gets hint alone; an upstream with
    // raw instructions but no first-sentence extraction would
    // get instructions alone (though today `resolve_hint` will
    // always produce a hint when instructions are present, we
    // don't want the rendering code to depend on that invariant).
    let mut input = base_input();
    input.hint = Some("short hint");
    input.upstream_instructions = Some("the full text goes here");
    let v = parse(&render(&input));
    assert_eq!(v["hint"], "short hint");
    assert_eq!(v["instructions"], "the full text goes here");
}

#[test]
fn long_instructions_survive_verbatim() {
    // Property: the point of `server-details` is to pay the
    // context cost once to get the FULL upstream-advertised
    // text. Truncation here would defeat the tool's purpose.
    // Glean's 2.5KB instructions is the motivating case.
    let long = "word ".repeat(500); // 2500 chars
    let mut input = base_input();
    input.upstream_instructions = Some(&long);
    let v = parse(&render(&input));
    assert_eq!(
        v["instructions"].as_str().expect("string"),
        long,
        "full instructions text must pass through verbatim"
    );
}

#[test]
fn transport_label_is_rendered_literally() {
    // Property: the `transport` value mirrors what `list-servers`
    // shows in its `[stdio]`/`[http]` suffix. Two views of the
    // same fact stay in lockstep.
    let mut input = base_input();
    input.kind = "stdio";
    let v = parse(&render(&input));
    assert_eq!(v["transport"], "stdio");
}

#[test]
fn no_fields_are_rendered_as_null() {
    // Property: if a field is absent, it doesn't appear as a
    // JSON null either. Keeps the reader's mental model simple:
    // "present keys always have content."
    let input = base_input();
    let out = render(&input);
    assert!(
        !out.contains(": null"),
        "render must never emit null values: {out}"
    );
}

#[test]
fn needs_login_health_surfaces_relogin_command() {
    // Property: an OAuth upstream that needs re-auth shows a `health`
    // block with state=NeedsLogin AND the exact remediation command,
    // so an agent reading server-details has the fix inline.
    let mut input = base_input();
    input.name = "Datadog-Acme";
    input.kind = "http-oauth";
    input.health = Some(HealthInfo {
        state: "NeedsLogin",
        relogin_command: Some("rato login Datadog-Acme".to_string()),
    });
    let v = parse(&render(&input));
    assert_eq!(v["health"]["state"], "NeedsLogin");
    assert_eq!(v["health"]["reLoginCommand"], "rato login Datadog-Acme");
}

#[test]
fn transient_health_state_omits_relogin_command() {
    // Property: for a transient state (e.g. Dead), the health block
    // appears but carries no reLoginCommand — there's no single
    // command that fixes a transient outage.
    let mut input = base_input();
    input.health = Some(HealthInfo {
        state: "Dead",
        relogin_command: None,
    });
    let v = parse(&render(&input));
    assert_eq!(v["health"]["state"], "Dead");
    assert!(v["health"].get("reLoginCommand").is_none());
}

#[test]
fn healthy_upstream_omits_health_block_entirely() {
    // The handler passes `health: None` for Healthy upstreams (and
    // when no snapshot exists), mirroring list-servers' "absence is
    // the all-clear" convention.
    let input = base_input();
    let v = parse(&render(&input));
    assert!(v.get("health").is_none());
}

#[test]
fn output_is_deterministic_across_calls() {
    // Same input twice produces the same string — matters for
    // cache-friendly behavior and for tests that compare output.
    // serde_json::Map preserves insertion order, so this holds
    // as long as we never conditionally reorder our inserts.
    let input = base_input();
    assert_eq!(render(&input), render(&input));
}
