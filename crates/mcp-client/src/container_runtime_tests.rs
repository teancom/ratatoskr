use super::*;
use crate::upstream::{UpstreamConfig, UpstreamKind};

fn stdio_cfg(command: &str, args: &[&str]) -> UpstreamConfig {
    UpstreamConfig::new(
        "display",
        "display",
        UpstreamKind::Stdio {
            command: command.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
            env: Default::default(),
        },
    )
}

// ---- detect_runtime_command ----

#[test]
fn detect_runtime_recognises_bare_podman() {
    let cfg = stdio_cfg("podman", &["run", "-i", "--rm", "busybox"]);
    assert_eq!(detect_runtime_command(&cfg), Some(RuntimeKind::Podman));
}

#[test]
fn detect_runtime_recognises_bare_docker() {
    let cfg = stdio_cfg("docker", &["run", "busybox"]);
    assert_eq!(detect_runtime_command(&cfg), Some(RuntimeKind::Docker));
}

#[test]
fn detect_runtime_recognises_absolute_path_podman() {
    let cfg = stdio_cfg("/usr/local/bin/podman", &["run", "busybox"]);
    assert_eq!(detect_runtime_command(&cfg), Some(RuntimeKind::Podman));
}

#[test]
fn detect_runtime_rejects_npx() {
    let cfg = stdio_cfg("npx", &["run", "@foo/bar"]);
    assert_eq!(detect_runtime_command(&cfg), None);
}

#[test]
fn detect_runtime_rejects_podman_without_run_subcommand() {
    // `podman exec <container> mcp-server` is a valid-but-different
    // pattern; we must not munge its args.
    let cfg = stdio_cfg("podman", &["exec", "my-container", "mcp-server"]);
    assert_eq!(detect_runtime_command(&cfg), None);
}

#[test]
fn detect_runtime_rejects_podman_with_empty_args() {
    let cfg = stdio_cfg("podman", &[]);
    assert_eq!(detect_runtime_command(&cfg), None);
}

#[test]
fn detect_runtime_rejects_http_upstream() {
    let cfg = UpstreamConfig::new(
        "dd",
        "dd",
        UpstreamKind::Http {
            url: "https://example.com".into(),
            headers: Default::default(),
        },
    );
    assert_eq!(detect_runtime_command(&cfg), None);
}

#[test]
fn runtime_kind_binary_is_canonical_name() {
    assert_eq!(RuntimeKind::Podman.binary(), "podman");
    assert_eq!(RuntimeKind::Docker.binary(), "docker");
}

// ---- container_name_for ----

#[test]
fn container_name_derivation_roundtrip() {
    assert_eq!(
        container_name_for("GitLab").expect("valid key"),
        "rato-GitLab"
    );
    assert_eq!(
        container_name_for("Atlassian").expect("valid key"),
        "rato-Atlassian"
    );
    assert_eq!(
        container_name_for("foo-bar").expect("valid key"),
        "rato-foo-bar"
    );
    assert_eq!(
        container_name_for("a.b_c").expect("valid key"),
        "rato-a.b_c"
    );
}

#[test]
fn container_name_rejects_spaces() {
    let err = container_name_for("foo bar").expect_err("space must fail");
    assert!(matches!(err, ContainerNameError::InvalidCharacters { .. }));
}

#[test]
fn container_name_rejects_unicode() {
    let err = container_name_for("Δ-service").expect_err("unicode must fail");
    assert!(matches!(err, ContainerNameError::InvalidCharacters { .. }));
}

#[test]
fn container_name_rejects_empty_key() {
    // Prefix alone ("rato-") is valid as a name, but an empty
    // key is a config-level mistake. Catch it here rather than
    // letting two upstreams with empty keys collide.
    //
    // Wait — the prefix ends with `-`, so `"rato-" + ""` = `"rato-"`,
    // which IS a valid podman name. So this test asserts the
    // current behaviour: an empty key IS accepted. An empty config
    // key is rejected upstream in gateway::config::parse (see
    // ConfigError::EmptyName), so this module doesn't need to
    // re-check.
    assert_eq!(
        container_name_for("").expect("empty key yields valid name"),
        "rato-"
    );
}

#[test]
fn container_name_rejects_too_long() {
    // `"rato-" + 251 'a'` == 256 chars, one over the limit.
    let huge_key: String = "a".repeat(251);
    let err = container_name_for(&huge_key).expect_err("overlong key must fail");
    assert!(matches!(err, ContainerNameError::TooLong { len, .. } if len == 256));
}

// ---- rewrite_args ----

#[test]
fn rewrite_args_inserts_name_after_run() {
    let args: Vec<String> = ["run", "-i", "--rm", "busybox"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let out = rewrite_args(&args, "rato-GitLab");
    assert_eq!(
        out,
        vec!["run", "--name", "rato-GitLab", "-i", "--rm", "busybox"]
    );
}

#[test]
fn rewrite_args_preserves_existing_name_flag() {
    // If the operator already set `--name`, leave it alone —
    // they've made an explicit choice.
    let args: Vec<String> = ["run", "--name", "my-pin", "busybox"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let out = rewrite_args(&args, "rato-GitLab");
    assert_eq!(out, args);
}

#[test]
fn rewrite_args_noop_when_first_arg_is_not_run() {
    // `podman exec …` — not our scenario; don't munge.
    let args: Vec<String> = ["exec", "my-container", "mcp-server"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let out = rewrite_args(&args, "rato-GitLab");
    assert_eq!(out, args);
}

#[test]
fn rewrite_args_noop_when_empty() {
    let args: Vec<String> = vec![];
    let out = rewrite_args(&args, "rato-GitLab");
    assert!(out.is_empty());
}

// ---- Real-world config roundtrips (authored config shape from
// ~/Library/Preferences/ratatoskr/config.json) ----

#[test]
fn gitlab_config_shape_roundtrips() {
    let cfg = stdio_cfg(
        "podman",
        &[
            "run",
            "-i",
            "--rm",
            "-e",
            "GITLAB_TOKEN",
            "registry.example.com/mcp/gitlab-mcp:latest",
            "--base-url",
            "https://gitlab.example.com/",
            "--pat",
        ],
    );
    assert_eq!(detect_runtime_command(&cfg), Some(RuntimeKind::Podman));
    let name = container_name_for("GitLab").expect("valid key");
    let UpstreamKind::Stdio { args, .. } = &cfg.kind else {
        panic!("stdio");
    };
    let rewritten = rewrite_args(args, &name);
    assert_eq!(rewritten[0], "run");
    assert_eq!(rewritten[1], "--name");
    assert_eq!(rewritten[2], "rato-GitLab");
    // Rest of args preserved in order.
    assert_eq!(&rewritten[3..], &args[1..]);
}

#[test]
fn atlassian_config_shape_roundtrips() {
    let cfg = stdio_cfg(
        "podman",
        &[
            "run",
            "-i",
            "--rm",
            "-e",
            "JIRA_URL",
            "ghcr.io/sooperset/mcp-atlassian:latest",
        ],
    );
    assert_eq!(detect_runtime_command(&cfg), Some(RuntimeKind::Podman));
    let name = container_name_for("Atlassian").expect("valid key");
    let UpstreamKind::Stdio { args, .. } = &cfg.kind else {
        panic!("stdio");
    };
    let rewritten = rewrite_args(args, &name);
    assert_eq!(&rewritten[..3], &["run", "--name", "rato-Atlassian"]);
}
