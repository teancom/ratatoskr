// pattern: Imperative Shell
//
// Shell-side operations for container-runtime integration.
//
// Thin wrappers over `tokio::process::Command` that invoke the
// `podman` / `docker` CLI to:
//
// - Forcibly reap a leaked container by name before spawning its
//   replacement ([`reap_container`]).
// - Check whether `podman machine` is running on macOS so the
//   supervisor can log a specific "start your podman machine" message
//   when respawns are failing for that reason ([`probe_podman_machine`]).
//
// All functions enforce a hard wall-clock timeout — the podman CLI
// itself can wedge when the daemon is sick, and we must never let that
// wedge the supervisor. 3 seconds is generous for a healthy daemon
// and short enough that a wedged daemon doesn't freeze respawn.
//
// These are best-effort, side-effectful operations. Callers treat
// failures as "log and move on" — the next `connect` attempt is the
// authoritative outcome.

use std::time::Duration;

use serde::Deserialize;
use tokio::process::Command;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::container_runtime::RuntimeKind;

/// Hard cap on any single runtime-CLI invocation. `podman ps` /
/// `podman rm` / `podman machine list` all return in well under a
/// second on a healthy machine. Three seconds accommodates a cold VM
/// and a Spinning-Beach-Ball-prone laptop without being generous
/// enough to mask a genuinely wedged daemon.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Outcome of a best-effort [`reap_container`] call. Not an error —
/// the caller doesn't *fail* if we couldn't reap; it just gets a
/// structured report for logging.
#[derive(Debug, PartialEq, Eq)]
pub enum ReapOutcome {
    /// The container was found and forcibly removed.
    Removed,
    /// No container by that name existed. The normal case on a fresh
    /// spawn; not an error.
    NotFound,
    /// The runtime CLI returned a non-zero exit but the container
    /// didn't seem to exist (common: "no such container"). We treat
    /// it as success-ish so the spawn proceeds.
    ///
    /// Carries the CLI's stderr for logging, trimmed to keep log
    /// lines bounded.
    RuntimeError(String),
    /// The runtime CLI didn't return within [`PROBE_TIMEOUT`]. The
    /// daemon is likely wedged; the subsequent `connect` will
    /// probably fail too, and the supervisor will take over from
    /// there.
    TimedOut,
    /// We couldn't even launch the runtime binary (not on `$PATH`,
    /// permission denied, etc). Almost always a configuration issue.
    SpawnFailed(String),
}

/// Force-remove any existing container with the given name, best
/// effort. Runs `<runtime> rm -f <name>` (plus `--time 0` on podman;
/// see [`reap_args`]) with a hard 3 s timeout.
///
/// This is the belt-and-suspenders for [`crate::container_runtime`]'s
/// deterministic naming: if a prior gateway run left a container
/// behind with this name (SIGKILL, crash, whatever), the subsequent
/// `podman run --name rato-Foo …` would fail on a name collision.
/// Reaping first turns that failure into a clean slate.
///
/// Does not fail the caller. Returns a structured outcome so the
/// caller can decide whether to log at debug (NotFound, Removed) or
/// warn (TimedOut, SpawnFailed).
pub async fn reap_container(runtime: RuntimeKind, name: &str) -> ReapOutcome {
    let mut cmd = Command::new(runtime.binary());
    cmd.args(reap_args(runtime, name));
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    match timeout(PROBE_TIMEOUT, cmd.output()).await {
        Err(_elapsed) => {
            warn!(
                runtime = runtime.binary(),
                container = %name,
                timeout_ms = PROBE_TIMEOUT.as_millis() as u64,
                "container reaper timed out; daemon may be wedged"
            );
            ReapOutcome::TimedOut
        }
        Ok(Err(e)) => {
            warn!(
                runtime = runtime.binary(),
                container = %name,
                error = %e,
                "container reaper failed to spawn runtime CLI"
            );
            ReapOutcome::SpawnFailed(e.to_string())
        }
        Ok(Ok(output)) => {
            if output.status.success() {
                debug!(
                    runtime = runtime.binary(),
                    container = %name,
                    "reaped prior container"
                );
                ReapOutcome::Removed
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                // Both podman and docker phrase "not found" slightly
                // differently; we match the common substring
                // "no such container" which they both use.
                if stderr.to_ascii_lowercase().contains("no such container") {
                    debug!(
                        runtime = runtime.binary(),
                        container = %name,
                        "no prior container to reap (clean slate)"
                    );
                    ReapOutcome::NotFound
                } else {
                    let trimmed = truncate_for_log(&stderr);
                    warn!(
                        runtime = runtime.binary(),
                        container = %name,
                        stderr = %trimmed,
                        "container reaper got a non-zero exit from runtime CLI"
                    );
                    ReapOutcome::RuntimeError(trimmed)
                }
            }
        }
    }
}

/// Build the force-remove CLI args for the given runtime.
///
/// Podman's `rm -f` on a *running* container honors the container's
/// stop timeout — a 10 s SIGTERM grace period by default — which is
/// longer than [`PROBE_TIMEOUT`]. Without `--time 0`, reaping a live
/// container always times out here, and the subsequent
/// `run --name <name>` collides with the not-yet-removed container.
/// (This is exactly how explicit `reconnect-upstream` on a healthy
/// containerized upstream used to fail.) `--time 0` skips the grace
/// period and SIGKILLs immediately — appropriate because a reaped
/// container is being replaced, not gracefully retired, and its
/// stdio transport is already severed or about to be.
///
/// Docker's `rm -f` SIGKILLs immediately and has no `--time` flag,
/// so it needs no extra args.
fn reap_args(runtime: RuntimeKind, name: &str) -> Vec<&str> {
    match runtime {
        RuntimeKind::Podman => vec!["rm", "-f", "--time", "0", name],
        RuntimeKind::Docker => vec!["rm", "-f", name],
    }
}

fn truncate_for_log(s: &str) -> String {
    const MAX: usize = 400;
    let trimmed = s.trim();
    if trimmed.len() <= MAX {
        trimmed.to_string()
    } else {
        let cut_at = trimmed
            .char_indices()
            .take_while(|(i, _)| *i < MAX)
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(MAX);
        format!("{}…(truncated)", &trimmed[..cut_at])
    }
}

/// Result of probing `podman machine list`.
#[derive(Debug, PartialEq, Eq)]
pub enum PodmanMachineStatus {
    /// At least one podman machine is running. (We don't distinguish
    /// between multiple — this gateway needs *a* machine, not a
    /// specific one.)
    Running,
    /// `podman machine list` ran but no machine reported `Running:
    /// true`. This is the "start your podman machine" scenario.
    NoneRunning,
    /// No podman machines configured at all. Either a brand-new
    /// install (on macOS, the user needs `podman machine init`) or a
    /// Linux host where `podman machine` is not the relevant probe
    /// (podman runs directly on Linux without a VM). We log at
    /// debug, not warn — on Linux this is the *expected* state.
    NoMachinesConfigured,
    /// We couldn't parse the output. Probably a podman-version skew;
    /// log and move on.
    ParseFailed(String),
    /// `podman machine list` itself failed or timed out.
    ProbeFailed(String),
}

/// Run `podman machine list --format json` and classify the result.
/// Docker has no equivalent subcommand (docker-desktop is managed
/// outside the CLI); this function is podman-specific and should not
/// be invoked when the resolved runtime is Docker.
pub async fn probe_podman_machine() -> PodmanMachineStatus {
    let mut cmd = Command::new(RuntimeKind::Podman.binary());
    cmd.arg("machine").arg("list").arg("--format").arg("json");
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let fut = cmd.output();
    let output = match timeout(PROBE_TIMEOUT, fut).await {
        Err(_) => {
            return PodmanMachineStatus::ProbeFailed("timed out".to_string());
        }
        Ok(Err(e)) => {
            return PodmanMachineStatus::ProbeFailed(e.to_string());
        }
        Ok(Ok(o)) => o,
    };

    if !output.status.success() {
        let stderr = truncate_for_log(&String::from_utf8_lossy(&output.stderr));
        return PodmanMachineStatus::ProbeFailed(stderr);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    // `podman machine list --format json` yields a JSON array of
    // machine objects. Each has a `Running: bool` field. An empty
    // array means no machines configured — distinct from "machines
    // configured but none running," which we must report differently.
    let parsed: Result<Vec<PodmanMachineEntry>, _> = serde_json::from_str(stdout.trim());
    match parsed {
        Ok(entries) if entries.is_empty() => PodmanMachineStatus::NoMachinesConfigured,
        Ok(entries) => {
            if entries.iter().any(|m| m.running) {
                PodmanMachineStatus::Running
            } else {
                PodmanMachineStatus::NoneRunning
            }
        }
        Err(e) => PodmanMachineStatus::ParseFailed(format!(
            "failed to parse `podman machine list --format json`: {e}"
        )),
    }
}

#[derive(Deserialize)]
struct PodmanMachineEntry {
    #[serde(rename = "Running")]
    running: bool,
}

#[cfg(test)]
#[path = "container_runtime_probe_tests.rs"]
mod tests;
