use super::*;

/// Test-only convenience: most parser tests only care about the upstream
/// list, not gateway-wide settings. Defined once here so call sites stay
/// terse instead of every test spelling out `parse_full(..).map(..)`.
fn parse(json: &str) -> Result<Vec<UpstreamConfig>, ConfigError> {
    parse_full(json).map(|p| p.upstreams)
}

#[test]
fn shipped_example_config_parses_cleanly() {
    // Self-enforcing docs: the config.example.json we point new users at
    // in the README must parse with the real parser. If a config-schema
    // change lands without updating the example, this fails instead of a
    // new user's first boot. The `//`-prefixed comment keys rely on the
    // parser's tolerate-unknown-fields posture, which this also pins.
    let raw = include_str!("../../../config.example.json");
    let parsed = parse_full(raw).expect("config.example.json must parse");
    assert_eq!(parsed.settings.port, 8910);
    assert_eq!(parsed.settings.result_size_threshold, 50_000);
    assert_eq!(parsed.settings.execute_deadline_ms, 60_000);

    let mut names: Vec<&str> = parsed.upstreams.iter().map(|u| u.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        ["Datadog", "GitLab", "Slack", "filesystem", "rootly"],
        "example upstream roster drifted; update this test AND the README"
    );

    // Every example upstream carries a hint — the README tells users a
    // hint is worth writing for every upstream, so the example must model
    // that advice.
    for u in &parsed.upstreams {
        assert!(
            u.hint.is_some(),
            "example upstream '{}' should model the hint field",
            u.name
        );
    }

    // The per-upstream supervision override on GitLab must survive the
    // merge with the gateway-wide block and validate.
    let gitlab = parsed
        .upstreams
        .iter()
        .find(|u| u.name == "GitLab")
        .expect("GitLab example present");
    let effective = parsed
        .settings
        .effective_supervision_for(gitlab)
        .expect("example supervision blocks must merge and validate");
    assert_eq!(effective.call_timeout, std::time::Duration::from_secs(60));
}

#[test]
fn empty_mcp_clients_produces_empty_vec() {
    // Matches the TS "minimal default config" shape from configuration.md.
    let json = r#"{"mcpClients": {}}"#;
    let configs = parse(json).expect("empty clients should parse");
    assert!(configs.is_empty());
}

#[test]
fn missing_mcp_clients_produces_empty_vec() {
    // Tolerate configs that haven't added `mcpClients` yet — the user
    // shouldn't get a hard error just because the key is absent.
    let json = r#"{"port": 3000}"#;
    let configs = parse(json).expect("missing clients should parse");
    assert!(configs.is_empty());
}

#[test]
fn stdio_client_parses_with_defaults() {
    let json = r#"{
        "mcpClients": {
            "local-server": {
                "type": "stdio",
                "command": "node",
                "args": ["server.js"]
            }
        }
    }"#;
    let configs = parse(json).expect("stdio config should parse");
    assert_eq!(configs.len(), 1);
    let cfg = &configs[0];
    assert_eq!(cfg.name, "local-server");
    // lua_name is always derived, never taken from JSON.
    assert_eq!(cfg.lua_name, "local_server");
    assert!(!cfg.dangerously_enable_sampling);
    match &cfg.kind {
        UpstreamKind::Stdio {
            command, args, env, ..
        } => {
            assert_eq!(command, "node");
            assert_eq!(args, &vec!["server.js".to_string()]);
            assert!(env.is_empty());
        }
        _ => panic!("expected stdio kind, got {:?}", cfg.kind),
    }
}

#[test]
fn http_client_parses_with_headers() {
    let json = r#"{
        "mcpClients": {
            "remote": {
                "type": "http",
                "url": "https://example.com/mcp",
                "headers": {"Authorization": "Bearer t"}
            }
        }
    }"#;
    let configs = parse(json).expect("http config should parse");
    assert_eq!(configs.len(), 1);
    match &configs[0].kind {
        UpstreamKind::Http { url, headers } => {
            assert_eq!(url, "https://example.com/mcp");
            assert_eq!(
                headers.get("Authorization").map(String::as_str),
                Some("Bearer t")
            );
        }
        _ => panic!("expected http kind"),
    }
}

#[test]
fn dangerously_enable_sampling_carries_through() {
    let json = r#"{
        "mcpClients": {
            "trusted": {
                "type": "stdio",
                "command": "node",
                "dangerouslyEnableSampling": true
            }
        }
    }"#;
    let configs = parse(json).expect("sampling flag should parse");
    assert!(configs[0].dangerously_enable_sampling);
}

#[test]
fn sampling_defaults_to_false_when_absent() {
    // Defense-in-depth: the security-critical default must be safe
    // even if the caller forgets the field entirely.
    let json = r#"{"mcpClients": {"x": {"type": "stdio", "command": "true"}}}"#;
    let configs = parse(json).expect("should parse");
    assert!(!configs[0].dangerously_enable_sampling);
}

#[test]
fn lua_name_sanitizes_hyphens_and_dots() {
    let json = r#"{
        "mcpClients": {
            "my.api-v2": {"type": "stdio", "command": "x"}
        }
    }"#;
    let configs = parse(json).expect("should parse");
    assert_eq!(configs[0].lua_name, "my_api_v2");
}

#[test]
fn unknown_top_level_fields_are_ignored() {
    // Leftover TS-era fields (host/transport/dashboard/etc.) that we
    // never wired must not break the parse. (`port` used to be in this
    // set; it's wired now and covered by its own tests below.)
    let json = r#"{
        "host": "localhost",
        "transport": "http",
        "dashboard": {"port": 3100, "host": "localhost"},
        "database": {"retentionDays": 7},
        "resultSizeThreshold": 50000,
        "mcpClients": {
            "x": {"type": "stdio", "command": "true"}
        }
    }"#;
    let configs = parse(json).expect("ignored fields shouldn't break parse");
    assert_eq!(configs.len(), 1);
}

// ── port ──

#[test]
fn port_defaults_when_absent() {
    let parsed = parse_full(r#"{"mcpClients": {}}"#).expect("should parse");
    assert_eq!(parsed.settings.port, DEFAULT_PORT);
}

#[test]
fn port_parses_from_config() {
    let parsed = parse_full(r#"{"port": 3000, "mcpClients": {}}"#).expect("should parse");
    assert_eq!(parsed.settings.port, 3000);
}

#[test]
fn port_rejects_float() {
    // Same discipline as the other numeric knobs: a float is a typo,
    // not a request for truncation.
    let err = parse_full(r#"{"port": 8910.5, "mcpClients": {}}"#).expect_err("float must error");
    assert!(
        matches!(err, ConfigError::NonIntegerPort { .. }),
        "expected NonIntegerPort, got {err:?}"
    );
}

#[test]
fn port_rejects_out_of_range_values() {
    for bad in ["0", "-1", "65536"] {
        let json = format!(r#"{{"port": {bad}, "mcpClients": {{}}}}"#);
        let err = parse_full(&json).expect_err("out-of-range port must error");
        assert!(
            matches!(err, ConfigError::InvalidPort { .. }),
            "port {bad}: expected InvalidPort, got {err:?}"
        );
    }
}

#[test]
fn allowed_tools_is_accepted_but_not_yet_wired() {
    // Accepting the field silently means TS configs parse without
    // edits. When we wire tool filtering, a separate test will cover
    // the behavioral change.
    let json = r#"{
        "mcpClients": {
            "x": {"type": "http", "url": "https://y", "allowedTools": ["a", "b"]}
        }
    }"#;
    parse(json).expect("allowedTools should not be a parse error");
}

#[test]
fn invalid_json_is_reported() {
    let err = parse("not json at all").expect_err("should fail");
    assert!(matches!(err, ConfigError::InvalidJson(_)), "got {err:?}");
}

#[test]
fn missing_transport_type_is_reported() {
    let json = r#"{"mcpClients": {"x": {"command": "node"}}}"#;
    let err = parse(json).expect_err("missing type should fail");
    match err {
        ConfigError::MissingField { name, field } => {
            assert_eq!(name, "x");
            assert_eq!(field, "type");
        }
        other => panic!("expected MissingField type, got {other:?}"),
    }
}

#[test]
fn stdio_missing_command_is_reported() {
    let json = r#"{"mcpClients": {"x": {"type": "stdio"}}}"#;
    let err = parse(json).expect_err("missing command should fail");
    match err {
        ConfigError::MissingField { name, field } => {
            assert_eq!(name, "x");
            assert_eq!(field, "command");
        }
        other => panic!("expected MissingField command, got {other:?}"),
    }
}

#[test]
fn http_missing_url_is_reported() {
    let json = r#"{"mcpClients": {"x": {"type": "http"}}}"#;
    let err = parse(json).expect_err("missing url should fail");
    match err {
        ConfigError::MissingField { name, field } => {
            assert_eq!(name, "x");
            assert_eq!(field, "url");
        }
        other => panic!("expected MissingField url, got {other:?}"),
    }
}

#[test]
fn http_empty_url_is_rejected() {
    // Defense-in-depth: an empty (or whitespace-only) url string passes
    // the "field present" check but produces a confusing reqwest error
    // at first-request time. Reject at parse so the error mentions the
    // config file's upstream name.
    let json = r#"{"mcpClients": {"x": {"type": "http", "url": ""}}}"#;
    let err = parse(json).expect_err("empty url should fail");
    match err {
        ConfigError::InvalidField { name, message } => {
            assert_eq!(name, "x");
            assert!(
                message.contains("url"),
                "expected message to mention url, got: {message}"
            );
        }
        other => panic!("expected InvalidField for empty url, got {other:?}"),
    }

    let whitespace_json = r#"{"mcpClients": {"y": {"type": "http", "url": "   \t\n"}}}"#;
    let err = parse(whitespace_json).expect_err("whitespace url should fail");
    match err {
        ConfigError::InvalidField { name, message } => {
            assert_eq!(name, "y");
            assert!(
                message.contains("url"),
                "expected message to mention url, got: {message}"
            );
        }
        other => panic!("expected InvalidField for whitespace url, got {other:?}"),
    }
}

#[test]
fn unknown_transport_type_is_reported_with_bad_value() {
    let json = r#"{"mcpClients": {"x": {"type": "websocket", "url": "ws://y"}}}"#;
    let err = parse(json).expect_err("unknown transport should fail");
    match err {
        ConfigError::InvalidTransport { name, got } => {
            assert_eq!(name, "x");
            assert_eq!(got, "websocket");
        }
        other => panic!("expected InvalidTransport, got {other:?}"),
    }
}

#[test]
fn wrong_type_for_args_is_reported() {
    // Defense-in-depth: a user mistyping `args` as a string should
    // get a clear error, not a silent drop or a panic. The primary
    // requirement is that the error `message` is non-empty
    // (regression guard against an InvalidField variant that
    // produces the right shape with a blank payload). Asserting
    // the message mentions "args" is a nice-to-have on top;
    // it turns out serde_json's own error text for a
    // typed-field deserialization failure only reports the
    // offending value and the expected shape, not the field name.
    // Enforcing that would require prepending field context to
    // every `e.to_string()` site in `value_into_config`, which is
    // out of scope for a test-improvement finding.
    let json = r#"{
        "mcpClients": {
            "x": {"type": "stdio", "command": "node", "args": "not-an-array"}
        }
    }"#;
    let err = parse(json).expect_err("wrong type should fail");
    match err {
        ConfigError::InvalidField { name, message } => {
            assert_eq!(name, "x");
            assert!(!message.is_empty(), "message should not be empty");
        }
        other => panic!("expected InvalidField, got {other:?}"),
    }
}

#[test]
fn output_is_sorted_by_name_for_determinism() {
    // JSON object iteration order isn't guaranteed. Sorting gives us
    // deterministic logging and test output regardless of insertion
    // order in the source file.
    let json = r#"{
        "mcpClients": {
            "zulu": {"type": "stdio", "command": "z"},
            "alpha": {"type": "stdio", "command": "a"},
            "mike": {"type": "stdio", "command": "m"}
        }
    }"#;
    let configs = parse(json).expect("should parse");
    let names: Vec<_> = configs.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["alpha", "mike", "zulu"]);
}

#[test]
fn multiple_clients_each_get_distinct_lua_names() {
    let json = r#"{
        "mcpClients": {
            "api-one": {"type": "http", "url": "https://1"},
            "api.two":  {"type": "http", "url": "https://2"}
        }
    }"#;
    let configs = parse(json).expect("should parse");
    let lua_names: Vec<_> = configs.iter().map(|c| c.lua_name.as_str()).collect();
    assert!(lua_names.contains(&"api_one"));
    assert!(lua_names.contains(&"api_two"));
}

#[test]
fn colliding_lua_names_are_detected() {
    // Property: two upstream names that sanitize to the same Lua
    // identifier must be rejected at parse time. Without this, the
    // Lua-global injection would silently drop one of the upstreams
    // and the agent would never see a diagnostic.
    let json = r#"{
        "mcpClients": {
            "my-server":  {"type": "stdio", "command": "a"},
            "my.server":  {"type": "stdio", "command": "b"}
        }
    }"#;
    let err = parse(json).expect_err("colliding lua names must fail");
    match err {
        ConfigError::LuaNameCollision { names, lua_name } => {
            assert_eq!(lua_name, "my_server");
            assert_eq!(
                names,
                vec!["my-server".to_string(), "my.server".to_string()]
            );
        }
        other => panic!("expected LuaNameCollision, got {other:?}"),
    }
}

#[test]
fn example_config_from_ts_reference_parses() {
    // Oracle: the TS repo's canonical config.example.json is our
    // contract. If it parses here, a user copying their existing
    // config works on day one.
    let json = r#"{
      "port": 3000,
      "host": "localhost",
      "transport": "http",
      "mcpClients": {
        "mcp-docs": {
          "type": "http",
          "url": "https://modelcontextprotocol.io/mcp"
        },
        "authenticated-api": {
          "type": "http",
          "url": "https://api.example.com/mcp",
          "headers": {"Authorization": "Bearer your-token-here"},
          "allowedTools": ["search", "get-document"]
        },
        "local-server": {
          "type": "stdio",
          "command": "node",
          "args": ["path/to/server.js"],
          "env": {"NODE_ENV": "production"}
        },
        "locked-down-server": {
          "type": "stdio",
          "command": "npx",
          "args": ["-y", "@modelcontextprotocol/server-filesystem"],
          "allowedTools": []
        },
        "trusted-server-with-sampling": {
          "type": "stdio",
          "command": "node",
          "args": ["path/to/trusted-server.js"],
          "dangerouslyEnableSampling": true
        }
      },
      "dashboard": {"port": 3100, "host": "localhost"}
    }"#;
    let configs = parse(json).expect("TS example config must parse");
    assert_eq!(configs.len(), 5);

    let by_name: HashMap<_, _> = configs.iter().map(|c| (c.name.as_str(), c)).collect();
    assert!(by_name["trusted-server-with-sampling"].dangerously_enable_sampling);
    assert!(!by_name["local-server"].dangerously_enable_sampling);
    // Spot-check lua sanitization on a hyphenated name.
    assert_eq!(by_name["mcp-docs"].lua_name, "mcp_docs");
}

// ---- GatewaySettings / resultSizeThreshold ----

#[test]
fn settings_default_when_no_threshold_field() {
    let json = r#"{"mcpClients": {}}"#;
    let parsed = parse_full(json).expect("should parse");
    assert_eq!(
        parsed.settings.result_size_threshold, DEFAULT_RESULT_SIZE_THRESHOLD,
        "missing field must default, not zero"
    );
}

#[test]
fn settings_respect_explicit_threshold() {
    let json = r#"{"mcpClients": {}, "resultSizeThreshold": 100000}"#;
    let parsed = parse_full(json).expect("should parse");
    assert_eq!(parsed.settings.result_size_threshold, 100_000);
}

#[test]
fn settings_zero_is_valid_and_means_disabled() {
    // Property: 0 is a valid explicit value (disables offload). Distinct
    // from "missing", which uses the default. The Lua runtime keys off
    // 0 to skip the offload path entirely.
    let json = r#"{"mcpClients": {}, "resultSizeThreshold": 0}"#;
    let parsed = parse_full(json).expect("zero threshold should parse");
    assert_eq!(parsed.settings.result_size_threshold, 0);
}

#[test]
fn settings_float_threshold_is_an_error() {
    // Property: a JSON float is rejected outright rather than
    // truncated to its integer part. A user who writes `50.0`
    // instead of `50000` wants an error, not a 50-byte threshold.
    let json = r#"{"mcpClients": {}, "resultSizeThreshold": 1.5}"#;
    let err = parse_full(json).expect_err("float threshold must error");
    match err {
        ConfigError::NonIntegerResultSizeThreshold { got } => {
            assert!(
                got.contains("1.5"),
                "expected got to mention 1.5, got: {got}"
            );
        }
        other => panic!("expected NonIntegerResultSizeThreshold, got {other:?}"),
    }
}

#[test]
fn settings_negative_threshold_is_an_error() {
    // Defense-in-depth: i64 at the wire level so we can spot and
    // reject nonsense, rather than silently saturating a usize cast.
    let json = r#"{"mcpClients": {}, "resultSizeThreshold": -1}"#;
    let err = parse_full(json).expect_err("negative threshold must error");
    assert!(
        matches!(err, ConfigError::InvalidResultSizeThreshold { got: -1 }),
        "got: {err:?}"
    );
}

// ---- executeDeadlineMs ----

#[test]
fn settings_default_when_no_execute_deadline_field() {
    // Pins: omitting `executeDeadlineMs` yields the 60 s default
    // rather than silently dropping to zero or some other value.
    // If `DEFAULT_EXECUTE_DEADLINE_MS` changes, this test has to
    // change with it — operator-visible behaviour defaults deserve
    // an explicit pin.
    let json = r#"{"mcpClients": {}}"#;
    let parsed = parse_full(json).expect("should parse");
    assert_eq!(
        parsed.settings.execute_deadline_ms, DEFAULT_EXECUTE_DEADLINE_MS,
        "missing field must default to the 60 s operator-visible contract"
    );
}

#[test]
fn settings_respect_explicit_execute_deadline() {
    let json = r#"{"mcpClients": {}, "executeDeadlineMs": 120000}"#;
    let parsed = parse_full(json).expect("should parse");
    assert_eq!(parsed.settings.execute_deadline_ms, 120_000);
}

#[test]
fn settings_zero_execute_deadline_is_an_error() {
    // Property: 0 is rejected (not silently accepted). A zero
    // deadline means every execute call times out before it can
    // make progress — the user almost certainly meant something
    // else. Distinct from `resultSizeThreshold` where 0 is a
    // valid "feature disabled" value.
    let json = r#"{"mcpClients": {}, "executeDeadlineMs": 0}"#;
    let err = parse_full(json).expect_err("zero deadline must error");
    assert!(
        matches!(err, ConfigError::ZeroExecuteDeadline),
        "got: {err:?}"
    );
}

#[test]
fn settings_negative_execute_deadline_is_an_error() {
    let json = r#"{"mcpClients": {}, "executeDeadlineMs": -1000}"#;
    let err = parse_full(json).expect_err("negative deadline must error");
    assert!(
        matches!(err, ConfigError::InvalidExecuteDeadline { got: -1000 }),
        "got: {err:?}"
    );
}

#[test]
fn settings_float_execute_deadline_is_an_error() {
    // Mirrors the `resultSizeThreshold` float-rejection rule for
    // the same reason: `60000.0` is almost certainly a typo for
    // `60000`; silently truncating would be hostile.
    let json = r#"{"mcpClients": {}, "executeDeadlineMs": 1.5}"#;
    let err = parse_full(json).expect_err("float deadline must error");
    match err {
        ConfigError::NonIntegerExecuteDeadline { got } => {
            assert!(
                got.contains("1.5"),
                "expected got to mention 1.5, got: {got}"
            );
        }
        other => panic!("expected NonIntegerExecuteDeadline, got {other:?}"),
    }
}

#[test]
fn settings_large_execute_deadline_is_accepted() {
    // Property: the upper bound is u64::MAX. A 24-hour deadline
    // is silly but not harmful, and capping it would introduce a
    // policy decision the config layer doesn't need to make.
    let json = r#"{"mcpClients": {}, "executeDeadlineMs": 86400000}"#;
    let parsed = parse_full(json).expect("24h deadline should parse");
    assert_eq!(parsed.settings.execute_deadline_ms, 86_400_000);
}

#[test]
fn settings_execute_deadline_and_threshold_coexist() {
    // Defense-in-depth: the two settings-layer fields don't
    // interfere. One test proving both read correctly from the
    // same document beats two tests that each assume the other.
    let json = r#"{
        "resultSizeThreshold": 123456,
        "executeDeadlineMs": 90000,
        "mcpClients": {}
    }"#;
    let parsed = parse_full(json).expect("should parse");
    assert_eq!(parsed.settings.result_size_threshold, 123_456);
    assert_eq!(parsed.settings.execute_deadline_ms, 90_000);
}

// ---- coexistence ----

#[test]
fn settings_and_upstreams_both_parse_together() {
    // Property: adding the settings path does NOT break the upstream
    // parse. One test proves both halves of the parser coexist.
    let json = r#"{
        "resultSizeThreshold": 200000,
        "mcpClients": {
            "alpha": { "type": "stdio", "command": "cat" }
        }
    }"#;
    let parsed = parse_full(json).expect("combined config parses");
    assert_eq!(parsed.settings.result_size_threshold, 200_000);
    assert_eq!(parsed.upstreams.len(), 1);
    assert_eq!(parsed.upstreams[0].name, "alpha");
}

#[test]
fn hint_field_parses_when_present() {
    let json = r#"{
        "mcpClients": {
            "datadog-us": {
                "type": "http",
                "url": "https://dd",
                "hint": "production logs/metrics/traces, incident investigation"
            }
        }
    }"#;
    let configs = parse(json).expect("should parse");
    assert_eq!(
        configs[0].hint.as_deref(),
        Some("production logs/metrics/traces, incident investigation")
    );
}

#[test]
fn hint_field_is_optional() {
    // Defense-in-depth: every pre-existing config file must keep
    // parsing after we add this field. No `#[serde(default)]`
    // slip-up.
    let json = r#"{
        "mcpClients": {
            "nohint": { "type": "stdio", "command": "true" }
        }
    }"#;
    let configs = parse(json).expect("should parse");
    assert!(configs[0].hint.is_none());
}

#[test]
fn hint_field_is_trimmed_at_parse() {
    // Property: parse-time trim means downstream code sees a
    // consistent value regardless of human-edited whitespace.
    let json = r#"{
        "mcpClients": {
            "x": { "type": "stdio", "command": "y", "hint": "   padded   " }
        }
    }"#;
    let configs = parse(json).expect("should parse");
    assert_eq!(configs[0].hint.as_deref(), Some("padded"));
}

#[test]
fn empty_hint_string_drops_to_none() {
    // Property: a user who scaffolded `hint: ""` but forgot to
    // fill it gets the same behavior as omitting the field. The
    // fallback (upstream-advertised instructions) then kicks in
    // downstream — see `hint::resolve_hint`.
    let json = r#"{
        "mcpClients": {
            "x": { "type": "stdio", "command": "y", "hint": "" }
        }
    }"#;
    let configs = parse(json).expect("should parse");
    assert!(configs[0].hint.is_none());

    let whitespace_json = r#"{
        "mcpClients": {
            "y": { "type": "stdio", "command": "z", "hint": "   \n\t  " }
        }
    }"#;
    let configs = parse(whitespace_json).expect("should parse");
    assert!(configs[0].hint.is_none());
}

#[test]
fn parse_legacy_accessor_drops_settings_cleanly() {
    // Property: the thin back-compat `parse` wrapper hands back
    // just the upstreams, and an error in the settings parse
    // surfaces as-is (not swallowed).
    let json = r#"{"mcpClients": {"x": {"type": "stdio", "command": "y"}}}"#;
    let upstreams = parse(json).expect("parse should work");
    assert_eq!(upstreams.len(), 1);

    let bad_json = r#"{"mcpClients": {}, "resultSizeThreshold": -5}"#;
    let err = parse(bad_json).expect_err("bad settings must still error");
    assert!(matches!(
        err,
        ConfigError::InvalidResultSizeThreshold { .. }
    ));
}

// ── upstreamSupervision (gateway-wide) + per-upstream `supervision` ──

#[test]
fn settings_default_supervision_when_no_field() {
    // Property: omitting `upstreamSupervision` yields the built-in
    // defaults, not an error. This is the "operator wrote nothing,
    // everything works" happy path.
    let json = r#"{"mcpClients": {}}"#;
    let parsed = parse_full(json).expect("should parse");
    assert_eq!(parsed.settings.supervision, SupervisionSettings::default());
}

#[test]
fn settings_accept_full_upstream_supervision_override() {
    // Property: a fully-specified upstreamSupervision block
    // overlays every default field.
    let json = r#"{
        "mcpClients": {},
        "upstreamSupervision": {
            "callTimeoutMs": 30000,
            "unhealthyThreshold": 5,
            "activePingIntervalMs": 10000,
            "activePingTimeoutMs": 2000,
            "respawnMaxAttempts": 10,
            "respawnBackoffBaseMs": 500,
            "respawnBackoffCapMs": 60000,
            "deadRearmIntervalMs": 120000
        }
    }"#;
    let parsed = parse_full(json).expect("should parse");
    let s = parsed.settings.supervision;
    assert_eq!(s.call_timeout, std::time::Duration::from_secs(30));
    assert_eq!(s.unhealthy_threshold, 5);
    assert_eq!(s.active_ping_interval, std::time::Duration::from_secs(10));
    assert_eq!(s.active_ping_timeout, std::time::Duration::from_secs(2));
    assert_eq!(s.respawn_max_attempts, 10);
    assert_eq!(
        s.respawn_backoff_base,
        std::time::Duration::from_millis(500)
    );
    assert_eq!(s.respawn_backoff_cap, std::time::Duration::from_secs(60));
    assert_eq!(s.dead_rearm_interval, std::time::Duration::from_secs(120));
}

#[test]
fn settings_accept_partial_upstream_supervision_override() {
    // Property: partial override overlays only the present fields.
    // The operator's minimum-diff config is the common case.
    let json = r#"{
        "mcpClients": {},
        "upstreamSupervision": {
            "callTimeoutMs": 45000
        }
    }"#;
    let parsed = parse_full(json).expect("should parse");
    let s = parsed.settings.supervision;
    let defaults = SupervisionSettings::default();
    assert_eq!(s.call_timeout, std::time::Duration::from_secs(45));
    // Everything else is still default.
    assert_eq!(s.unhealthy_threshold, defaults.unhealthy_threshold);
    assert_eq!(s.active_ping_interval, defaults.active_ping_interval);
}

#[test]
fn settings_reject_invalid_upstream_supervision_cross_field() {
    // Property: the gateway-wide upstreamSupervision block runs
    // through `SupervisionSettings::validate` after merge. A
    // cross-field violation (e.g. ping timeout >= interval)
    // produces `ConfigError::InvalidSupervision`.
    let json = r#"{
        "mcpClients": {},
        "upstreamSupervision": {
            "activePingIntervalMs": 1000,
            "activePingTimeoutMs": 5000
        }
    }"#;
    let err = parse_full(json).expect_err("cross-field invalid config must fail");
    match err {
        ConfigError::InvalidSupervision(SupervisionError::PingTimeoutExceedsInterval {
            ..
        }) => (),
        other => {
            panic!("expected InvalidSupervision(PingTimeoutExceedsInterval), got {other:?}")
        }
    }
}

#[test]
fn settings_reject_zero_unhealthy_threshold_at_gateway_level() {
    // Property: unhealthy_threshold = 0 is rejected at parse time
    // because it would transition every upstream to Suspect on
    // the first transient hiccup. Classic off-by-one footgun,
    // caught at the config boundary.
    let json = r#"{
        "mcpClients": {},
        "upstreamSupervision": {
            "unhealthyThreshold": 0
        }
    }"#;
    let err = parse_full(json).expect_err("zero threshold must fail");
    assert!(matches!(
        err,
        ConfigError::InvalidSupervision(SupervisionError::ZeroUnhealthyThreshold)
    ));
}

#[test]
fn upstream_supervision_override_is_carried_through() {
    // Property: per-upstream `supervision` on an mcp-client entry
    // is parsed into `UpstreamConfig.supervision` so the merge
    // step at supervisor spawn time can apply it.
    let json = r#"{
        "mcpClients": {
            "slow-datadog": {
                "type": "stdio",
                "command": "datadog-mcp",
                "supervision": {
                    "callTimeoutMs": 120000
                }
            }
        }
    }"#;
    let parsed = parse_full(json).expect("should parse");
    assert_eq!(parsed.upstreams.len(), 1);
    let u = &parsed.upstreams[0];
    assert_eq!(u.name, "slow-datadog");
    let sup = u.supervision.as_ref().expect("supervision must be set");
    assert_eq!(sup.call_timeout_ms, Some(120_000));
    assert!(sup.unhealthy_threshold.is_none());
}

#[test]
fn effective_supervision_merges_gateway_plus_upstream() {
    // Property: `effective_supervision_for` overlays the
    // per-upstream partial atop the gateway-wide baseline.
    //
    // Gateway-wide sets call_timeout=30s and unhealthy_threshold=5.
    // Per-upstream overrides call_timeout=120s but leaves the rest
    // untouched, so the effective config has call_timeout=120s
    // and unhealthy_threshold=5 (inherited).
    let json = r#"{
        "upstreamSupervision": {
            "callTimeoutMs": 30000,
            "unhealthyThreshold": 5
        },
        "mcpClients": {
            "deep-query": {
                "type": "stdio",
                "command": "dq",
                "supervision": { "callTimeoutMs": 120000 }
            }
        }
    }"#;
    let parsed = parse_full(json).expect("should parse");
    let effective = parsed
        .settings
        .effective_supervision_for(&parsed.upstreams[0])
        .expect("effective settings must validate");
    assert_eq!(effective.call_timeout, std::time::Duration::from_secs(120));
    assert_eq!(effective.unhealthy_threshold, 5);
}

#[test]
fn effective_supervision_rejects_invalid_per_upstream_merge() {
    // Property: if the gateway baseline + per-upstream overrides
    // produce a cross-field-invalid result, the error names the
    // offending upstream (not "upstreamSupervision is invalid"
    // — that's the gateway-level error variant).
    let json = r#"{
        "upstreamSupervision": {
            "activePingIntervalMs": 5000
        },
        "mcpClients": {
            "greedy": {
                "type": "stdio",
                "command": "x",
                "supervision": { "activePingTimeoutMs": 10000 }
            }
        }
    }"#;
    let parsed = parse_full(json).expect("parse succeeds — error is at merge time");
    let err = parsed
        .settings
        .effective_supervision_for(&parsed.upstreams[0])
        .expect_err("merged config is cross-field invalid");
    match err {
        ConfigError::InvalidUpstreamSupervision { name, source } => {
            assert_eq!(name, "greedy");
            assert!(matches!(
                source,
                SupervisionError::PingTimeoutExceedsInterval { .. }
            ));
        }
        other => panic!("expected InvalidUpstreamSupervision, got {other:?}"),
    }
}

#[test]
fn effective_supervision_no_per_upstream_overrides_returns_baseline() {
    // Property: an upstream with no `supervision` key inherits
    // exactly the gateway-wide settings. No re-validation work
    // needed (the gateway-wide block was validated at parse time)
    // but calling `effective_supervision_for` still returns Ok
    // because the caller doesn't know whether an override exists.
    let json = r#"{
        "upstreamSupervision": { "callTimeoutMs": 45000 },
        "mcpClients": {
            "vanilla": { "type": "stdio", "command": "x" }
        }
    }"#;
    let parsed = parse_full(json).expect("should parse");
    let effective = parsed
        .settings
        .effective_supervision_for(&parsed.upstreams[0])
        .expect("baseline must validate");
    assert_eq!(effective, parsed.settings.supervision);
}

// ── http-oauth transport variant ──

#[test]
fn http_oauth_parses_with_all_fields() {
    let json = r#"{
        "mcpClients": {
            "Datadog-Acme": {
                "type": "http-oauth",
                "url": "https://mcp.datadoghq.com/api/unstable/mcp-server/mcp",
                "scopes": ["metrics_read", "dashboards_read"],
                "clientName": "ratatoskr",
                "callbackPort": 8765,
                "clientId": "1601185624273.8899143856786"
            }
        }
    }"#;
    let configs = parse(json).expect("should parse");
    match &configs[0].kind {
        UpstreamKind::HttpOAuth {
            url,
            scopes,
            client_name,
            callback_port,
            client_id,
        } => {
            assert_eq!(url, "https://mcp.datadoghq.com/api/unstable/mcp-server/mcp");
            assert_eq!(
                scopes,
                &vec!["metrics_read".to_string(), "dashboards_read".to_string()]
            );
            assert_eq!(client_name.as_deref(), Some("ratatoskr"));
            assert_eq!(*callback_port, 8765);
            assert_eq!(client_id.as_deref(), Some("1601185624273.8899143856786"));
        }
        other => panic!("expected HttpOAuth, got {other:?}"),
    }
}

#[test]
fn http_oauth_defaults_when_optional_fields_omitted() {
    // Property: only `url` is required. scopes → empty, client_name →
    // None, callback_port → 0 (ephemeral). The minimal config is the
    // common case and must parse.
    let json = r#"{
        "mcpClients": {
            "dd-acme": {
                "type": "http-oauth",
                "url": "https://mcp.datadoghq.com/api/unstable/mcp-server/mcp"
            }
        }
    }"#;
    let configs = parse(json).expect("should parse");
    match &configs[0].kind {
        UpstreamKind::HttpOAuth {
            scopes,
            client_name,
            callback_port,
            client_id,
            ..
        } => {
            assert!(scopes.is_empty());
            assert!(client_name.is_none());
            assert_eq!(*callback_port, 0);
            assert!(
                client_id.is_none(),
                "client_id must default to None (DCR flow)"
            );
        }
        other => panic!("expected HttpOAuth, got {other:?}"),
    }
}

#[test]
fn http_oauth_empty_client_id_is_rejected() {
    // An empty clientId would silently select the DCR-less flow with a
    // client id no provider accepts; the parse error is the cheap place
    // to catch the typo.
    let json = r#"{
        "mcpClients": {
            "slack": {
                "type": "http-oauth",
                "url": "https://slack.example/mcp",
                "clientId": "  "
            }
        }
    }"#;
    let err = parse(json).expect_err("empty clientId must error");
    match err {
        ConfigError::InvalidField { name, message } => {
            assert_eq!(name, "slack");
            assert!(
                message.contains("clientId"),
                "message must name the field; got: {message}"
            );
        }
        other => panic!("expected InvalidField, got {other:?}"),
    }
}

#[test]
fn http_oauth_missing_url_is_rejected() {
    let json = r#"{
        "mcpClients": {
            "broken": { "type": "http-oauth" }
        }
    }"#;
    let err = parse(json).expect_err("http-oauth without url must error");
    match err {
        ConfigError::MissingField { name, field } => {
            assert_eq!(name, "broken");
            assert_eq!(field, "url");
        }
        other => panic!("expected MissingField(url), got {other:?}"),
    }
}

#[test]
fn http_oauth_typo_in_type_is_rejected() {
    // A near-miss like "http-oath" must surface as InvalidTransport,
    // not silently parse as something else.
    let json = r#"{
        "mcpClients": {
            "fat-fingered": {
                "type": "http-oath",
                "url": "https://example"
            }
        }
    }"#;
    let err = parse(json).expect_err("typo'd transport must error");
    match err {
        ConfigError::InvalidTransport { name, got } => {
            assert_eq!(name, "fat-fingered");
            assert_eq!(got, "http-oath");
        }
        other => panic!("expected InvalidTransport, got {other:?}"),
    }
}

#[test]
fn existing_api_key_http_upstream_parses_unchanged() {
    // Regression lock: the two pre-existing API-key Datadog upstreams
    // use `"type": "http"` with static headers. Adding the http-oauth
    // variant must not perturb how a plain http client parses — same
    // url, same headers, same UpstreamKind::Http.
    let json = r#"{
        "mcpClients": {
            "datadog-us": {
                "type": "http",
                "url": "https://mcp.datadoghq.com/api/unstable/mcp-server/mcp",
                "headers": {
                    "DD-API-KEY": "key123",
                    "DD-APPLICATION-KEY": "app456"
                }
            }
        }
    }"#;
    let configs = parse(json).expect("should parse");
    match &configs[0].kind {
        UpstreamKind::Http { url, headers } => {
            assert_eq!(url, "https://mcp.datadoghq.com/api/unstable/mcp-server/mcp");
            assert_eq!(
                headers.get("DD-API-KEY").map(String::as_str),
                Some("key123")
            );
            assert_eq!(
                headers.get("DD-APPLICATION-KEY").map(String::as_str),
                Some("app456")
            );
        }
        other => panic!("expected plain Http, got {other:?}"),
    }
}

// `gateway::config::DEFAULT_EXECUTE_DEADLINE_MS` and
// `lua_runtime::DEFAULT_EXECUTE_DEADLINE` document the same default in two
// crates: the gateway config layer needs a plain millisecond integer to
// merge into `GatewaySettings`/serde, while lua-runtime needs a
// `std::time::Duration` to hand to `mlua`'s timeout machinery. Nothing in
// the type system forces these to agree — this test pins the mirror so a
// change to one default doesn't silently desync the other.
#[test]
fn execute_deadline_default_matches_lua_runtime() {
    assert_eq!(
        std::time::Duration::from_millis(DEFAULT_EXECUTE_DEADLINE_MS),
        lua_runtime::DEFAULT_EXECUTE_DEADLINE,
        "gateway's DEFAULT_EXECUTE_DEADLINE_MS must match lua_runtime's DEFAULT_EXECUTE_DEADLINE"
    );
}
