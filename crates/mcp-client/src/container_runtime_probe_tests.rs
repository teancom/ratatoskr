//! Shell tests — limited to the *parsing* and *truncation* logic
//! that doesn't require spawning `podman`. End-to-end "reap a real
//! container and verify it's gone" is out of scope here: the
//! author uses a single-user dev setup with real podman, so there's
//! no CI container to run these against, and the FCIS split keeps
//! the interesting decisions in the pure core (`container_runtime.rs`)
//! which is fully covered by unit tests.

use super::*;

// Pins the podman/docker force-remove arg split: podman must pass
// `--time 0` so reaping a still-running container SIGKILLs inside
// PROBE_TIMEOUT instead of waiting out the 10 s stop grace period
// (the regression that made reconnect-upstream fail on healthy
// containerized upstreams); docker `rm -f` kills immediately and
// rejects `--time`, so it must NOT get the flag.
#[test]
fn reap_args_podman_skips_stop_grace_period() {
    assert_eq!(
        reap_args(RuntimeKind::Podman, "rato-Foo"),
        vec!["rm", "-f", "--time", "0", "rato-Foo"]
    );
}

#[test]
fn reap_args_docker_omits_time_flag() {
    assert_eq!(
        reap_args(RuntimeKind::Docker, "rato-Foo"),
        vec!["rm", "-f", "rato-Foo"]
    );
}

#[test]
fn truncate_for_log_passes_short_strings_through() {
    assert_eq!(truncate_for_log("hello"), "hello");
    assert_eq!(truncate_for_log("  trimmed  "), "trimmed");
}

#[test]
fn truncate_for_log_caps_long_output_with_ellipsis_marker() {
    let huge = "a".repeat(1000);
    let out = truncate_for_log(&huge);
    assert!(out.ends_with("…(truncated)"));
    assert!(out.len() < 1000);
}

// PodmanMachineEntry parsing is exercised indirectly via
// probe_podman_machine's integration with serde_json; the parse
// happy path is simple enough that a dedicated unit test would
// only re-test serde itself. The interesting failure modes
// (parse failure, empty array) are covered by the shell
// function's match arms — if those paths matter in production,
// the right test shape is a gateway integration test that
// injects a fake `podman` on `$PATH`, which is future work.
