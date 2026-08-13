use super::*;
use mcp_client::FailureKind;
use std::collections::HashMap;

fn stdio(name: &str, lua: &str) -> UpstreamConfig {
    UpstreamConfig::new(
        name,
        lua,
        UpstreamKind::Stdio {
            command: "echo".to_string(),
            args: vec![],
            env: HashMap::new(),
        },
    )
}

fn http(name: &str, lua: &str) -> UpstreamConfig {
    UpstreamConfig::new(
        name,
        lua,
        UpstreamKind::Http {
            url: "http://example.com/mcp".to_string(),
            headers: HashMap::new(),
        },
    )
}

fn http_oauth(name: &str, lua: &str) -> UpstreamConfig {
    UpstreamConfig::new(
        name,
        lua,
        UpstreamKind::HttpOAuth {
            url: "https://mcp.datadoghq.com/api/unstable/mcp-server/mcp".to_string(),
            scopes: vec![],
            client_name: None,
            callback_port: 0,
            client_id: None,
        },
    )
}

fn entry<'a>(cfg: &'a UpstreamConfig) -> CatalogEntry<'a> {
    CatalogEntry::new(cfg, None)
}

fn entry_with_hint<'a>(cfg: &'a UpstreamConfig, hint: &'a str) -> CatalogEntry<'a> {
    CatalogEntry::new(cfg, Some(hint))
}

/// Fixed "now" for deterministic rendering of time-ago strings in
/// tests. Every time-based assertion in this module is expressed
/// as `now - Duration::from_secs(N)` against this anchor.
fn fixed_now() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
}

fn snap(state: HealthState, fails: u32, last_ok_secs_ago: Option<u64>) -> HealthSnapshot {
    HealthSnapshot {
        state,
        consecutive_failures: fails,
        last_failure_kind: FailureKind::None,
        last_success_at: last_ok_secs_ago.map(|s| fixed_now() - Duration::from_secs(s)),
        last_failure_at: None,
        consecutive_self_reported_errors: 0,
    }
}

#[test]
fn empty_list_has_friendly_message() {
    assert_eq!(
        render(&[], fixed_now()),
        "No upstream MCP servers configured."
    );
}

#[test]
fn non_empty_output_starts_with_catalog_header() {
    // Pin the exact header string. It's a stable, renderable
    // contract that agents parsing `list-servers` output rely on;
    // a capitalization or wording change should trip a test.
    let cfg = stdio("example", "example");
    let out = render(&[entry(&cfg)], fixed_now());
    assert!(
        out.starts_with("Available MCP Servers:\n\n"),
        "unexpected header: {out}"
    );
}

#[test]
fn single_stdio_is_listed() {
    let cfg = stdio("calculator", "calculator");
    let out = render(&[entry(&cfg)], fixed_now());
    assert!(out.contains("## calculator (Lua: calculator) [stdio]"));
}

#[test]
fn http_is_tagged_differently_from_stdio() {
    let a = stdio("a", "a");
    let b = http("b", "b");
    let out = render(&[entry(&a), entry(&b)], fixed_now());
    assert!(out.contains("[stdio]"));
    assert!(out.contains("[http]"));
}

#[test]
fn output_is_sorted_by_name() {
    // Property: the rendered output is independent of input order.
    let alpha = stdio("alpha", "alpha");
    let zebra = stdio("zebra", "zebra");

    let a = vec![entry(&zebra), entry(&alpha)];
    let b = vec![entry(&alpha), entry(&zebra)];
    assert_eq!(render(&a, fixed_now()), render(&b, fixed_now()));

    // And alpha comes before zebra in the actual text:
    let out = render(&a, fixed_now());
    let alpha_pos = out.find("alpha").expect("alpha present");
    let zebra_pos = out.find("zebra").expect("zebra present");
    assert!(
        alpha_pos < zebra_pos,
        "alpha should appear before zebra in output"
    );
}

#[test]
fn lua_name_is_rendered_separately_from_display_name() {
    // list-servers is the tool that bridges display name -> lua identifier.
    // Renderers MUST surface both so agents know what to type in scripts.
    let cfg = stdio("data-server", "data_server");
    let out = render(&[entry(&cfg)], fixed_now());
    assert!(out.contains("data-server"));
    assert!(out.contains("data_server"));
}

#[test]
fn hint_appears_under_its_server_when_set() {
    let dd = http("datadog-us", "datadog_us");
    let gh = http("github", "github");
    let out = render(
        &[
            entry_with_hint(&dd, "production logs, metrics, traces"),
            entry(&gh),
        ],
        fixed_now(),
    );

    let dd_header = out.find("## datadog-us").expect("datadog header present");
    let dd_hint = out
        .find("hint: production logs")
        .expect("datadog hint present");
    let gh_header = out.find("## github").expect("github header present");

    assert!(
        dd_header < dd_hint && dd_hint < gh_header,
        "hint must sit between its server's header and the next one:\n{out}"
    );
}

#[test]
fn hint_is_omitted_when_none() {
    let cfg = stdio("quiet", "quiet");
    let out = render(&[entry(&cfg)], fixed_now());
    assert!(
        !out.contains("hint:"),
        "no hint line when none is provided: {out}"
    );
}

#[test]
fn empty_hint_string_is_suppressed() {
    let cfg = stdio("quiet", "quiet");
    let entry = CatalogEntry::new(&cfg, Some(""));
    let out = render(&[entry], fixed_now());
    assert!(!out.contains("hint:"), "blank hint line emitted: {out}");
}

#[test]
fn multiple_hints_each_attach_to_their_own_server() {
    let a = stdio("alpha", "alpha");
    let b = stdio("bravo", "bravo");
    let out = render(
        &[
            entry_with_hint(&a, "alpha hint"),
            entry_with_hint(&b, "bravo hint"),
        ],
        fixed_now(),
    );
    assert!(out.contains("alpha hint"));
    assert!(out.contains("bravo hint"));
    let alpha_hint = out.find("alpha hint").expect("alpha hint");
    let bravo_header = out.find("## bravo").expect("bravo header");
    assert!(alpha_hint < bravo_header, "hint ordering got:\n{out}");
}

// ---- Health annotation tests ----

#[test]
fn healthy_state_renders_no_health_tag() {
    // Property: a Healthy upstream looks exactly like one with no
    // health snapshot at all. Absence-of-tag is the all-clear
    // signal; we don't want `[Healthy]` cluttering the common
    // case.
    let cfg = stdio("fine", "fine");
    let e =
        CatalogEntry::new(&cfg, None).with_health(Some(snap(HealthState::Healthy, 0, Some(30))));
    let out = render(&[e], fixed_now());
    // The header line has `[stdio]` as the kind tag. If a health
    // tag landed we'd see a SECOND bracket group on the same line.
    // Assert the line ends right after the kind tag.
    let header_line = out
        .lines()
        .find(|l| l.starts_with("## fine"))
        .expect("header present");
    assert!(
        header_line.ends_with("[stdio]"),
        "healthy upstream should have no trailing health tag: {header_line}"
    );
}

#[test]
fn missing_health_renders_no_health_tag() {
    // Property: no snapshot → no tag. Mirrors the Healthy case;
    // "we don't know" defaults visually to "probably fine".
    let cfg = stdio("unknown", "unknown");
    let out = render(&[entry(&cfg)], fixed_now());
    let header_line = out
        .lines()
        .find(|l| l.starts_with("## unknown"))
        .expect("header present");
    assert!(header_line.ends_with("[stdio]"));
}

#[test]
fn suspect_state_renders_compact_tag() {
    // Property: non-Healthy state produces a single-bracket
    // annotation after the kind label, with state + fails +
    // last_ok. This is the agent-readable format; changing it
    // breaks downstream reasoning.
    let cfg = stdio("sick", "sick");
    let e =
        CatalogEntry::new(&cfg, None).with_health(Some(snap(HealthState::Suspect, 3, Some(45))));
    let out = render(&[e], fixed_now());
    assert!(
        out.contains("[Suspect fails=3 last_ok=45s]"),
        "expected compact Suspect tag:\n{out}"
    );
}

#[test]
fn dead_state_is_visible() {
    // Load-bearing: Dead upstreams MUST be visible so an agent
    // routing tool calls sees the upstream is down.
    let cfg = stdio("corpse", "corpse");
    let e = CatalogEntry::new(&cfg, None).with_health(Some(snap(HealthState::Dead, 7, Some(3600))));
    let out = render(&[e], fixed_now());
    assert!(
        out.contains("[Dead fails=7 last_ok=1h]"),
        "expected Dead tag with hour-unit last_ok:\n{out}"
    );
}

#[test]
fn needs_login_renders_the_exact_relogin_command() {
    // Load-bearing: the whole point of this state is that the fix is
    // legible inline. The tag MUST contain the literal command an
    // operator/agent runs to re-authenticate, including the upstream's
    // display name — not the fails/last_ok telemetry used for
    // transient states.
    let cfg = http_oauth("Datadog-Acme", "datadog_acme");
    let e = CatalogEntry::new(&cfg, None).with_health(Some(snap(
        HealthState::NeedsLogin,
        0,
        Some(120),
    )));
    let out = render(&[e], fixed_now());
    assert!(
        out.contains("rato login Datadog-Acme"),
        "expected the exact re-login command in the NeedsLogin tag:\n{out}"
    );
    assert!(
        out.contains("NeedsLogin") && out.contains("ask the user"),
        "tag must mark the state and frame it as a human-relay action:\n{out}"
    );
    // It must NOT carry the transient-state telemetry — that framing
    // ("flap vs outage") is wrong for an operator-action state.
    assert!(
        !out.contains("last_ok") && !out.contains("fails="),
        "NeedsLogin tag should not include fails/last_ok telemetry:\n{out}"
    );
}

#[test]
fn never_succeeded_upstream_reports_last_ok_never() {
    let cfg = stdio("brandnew", "brandnew");
    let e = CatalogEntry::new(&cfg, None).with_health(Some(snap(HealthState::Respawning, 2, None)));
    let out = render(&[e], fixed_now());
    assert!(
        out.contains("last_ok=never"),
        "expected last_ok=never when last_success_at is None:\n{out}"
    );
}

#[test]
fn zero_fails_omits_fails_field() {
    // Property: noise suppression. If fails=0 there's no point
    // printing it — the state itself (Suspect / Respawning) is
    // meaningful and the zero is clutter.
    let cfg = stdio("probing", "probing");
    let e = CatalogEntry::new(&cfg, None).with_health(Some(snap(HealthState::Suspect, 0, Some(5))));
    let out = render(&[e], fixed_now());
    assert!(
        out.contains("[Suspect last_ok=5s]") && !out.contains("fails=0"),
        "expected compact Suspect without fails=0:\n{out}"
    );
}

#[test]
fn format_ago_picks_largest_unit() {
    assert_eq!(format_ago(Duration::from_secs(0)), "0s");
    assert_eq!(format_ago(Duration::from_secs(59)), "59s");
    assert_eq!(format_ago(Duration::from_secs(60)), "1m");
    assert_eq!(format_ago(Duration::from_secs(3599)), "59m");
    assert_eq!(format_ago(Duration::from_secs(3600)), "1h");
    assert_eq!(format_ago(Duration::from_secs(86_399)), "23h");
    assert_eq!(format_ago(Duration::from_secs(86_400)), "1d");
}
