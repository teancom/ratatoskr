// pattern: Functional Core
//
// Container-runtime awareness for stdio upstreams.
//
// Motivation: stdio upstreams configured as `command: "podman"` (or
// `"docker"`) are MCP servers running inside containers. Two failure
// modes recur against containerized upstreams:
//
// 1. **Leaked containers.** Our current configs use `podman run -i --rm
//    <image>`. The `--rm` is a best-effort cleanup that only fires on
//    clean container exit; a wedged or SIGKILL-ed container stays
//    around. Because the configs don't pass `--name`, every respawn
//    creates a new anonymous container and the leaked ones pile up
//    until they eat the machine's RAM.
//
// 2. **`podman machine not running`** on macOS. When the VM is stopped,
//    every `podman run` fails instantly with a "cannot connect to
//    Podman" message. The supervisor burns through `respawn_max_attempts`
//    and goes Dead; the operator has to grep the log for the root cause
//    instead of seeing a clear "start your podman machine" line.
//
// This module contains the *pure* pieces of the fix:
//
// - [`detect_runtime_command`] — classifies an [`UpstreamConfig`]'s
//   `command` as Podman, Docker, or neither. Handles absolute paths
//   (`/usr/local/bin/podman`). The configured command is always the
//   one that gets spawned — this module never rewrites podman to
//   docker or vice versa; it only classifies so naming, reaping, and
//   diagnostics know which runtime CLI they are talking to.
// - [`container_name_for`] — derives the deterministic container name
//   from the config key. `"GitLab"` → `"rato-GitLab"`. Validates the
//   result matches podman's name character class.
// - [`rewrite_args`] — inserts `--name <rato-foo>` into the args
//   right after the `run` subcommand. No-op if the args don't start
//   with `run` (e.g. `podman exec …` — not our scenario, leave alone).
//
// The shell side (spawning `podman rm -f`, reading
// `podman machine list`) lives in `container_runtime_probe.rs`.

use crate::upstream::{UpstreamConfig, UpstreamKind};

/// Which container runtime binary to invoke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeKind {
    Podman,
    Docker,
}

impl RuntimeKind {
    /// The canonical binary name on `$PATH`. Not an absolute path:
    /// callers `Command::new(kind.binary())` and let the OS resolve
    /// via `$PATH`, matching how the config files already invoke the
    /// runtime.
    pub fn binary(self) -> &'static str {
        match self {
            RuntimeKind::Podman => "podman",
            RuntimeKind::Docker => "docker",
        }
    }
}

/// Classify the `command` of an upstream config. Returns `Some(kind)` if
/// this is a containerized stdio upstream whose args we should rewrite;
/// `None` for everything else (HTTP upstreams, `npx`, `uvx`, whatever).
///
/// Uses file-basename match so absolute paths work: `"/usr/local/bin/podman"`
/// → `RuntimeKind::Podman`. Defends against the case where an operator
/// pins the binary path to avoid `$PATH` weirdness.
///
/// Returns `None` when the args don't start with `run`. `podman exec
/// <container> mcp-server` is a legitimate but different pattern — the
/// operator is attaching to an already-running container, not spawning
/// one — and deterministic-naming / pre-respawn-reaping don't apply.
/// Leave that case alone rather than silently mangling the args.
pub fn detect_runtime_command(config: &UpstreamConfig) -> Option<RuntimeKind> {
    let UpstreamKind::Stdio { command, args, .. } = &config.kind else {
        return None;
    };
    let kind = classify_binary(command)?;
    if args.first().map(String::as_str) != Some("run") {
        return None;
    }
    Some(kind)
}

fn classify_binary(command: &str) -> Option<RuntimeKind> {
    let basename = std::path::Path::new(command)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(command);
    match basename {
        "podman" => Some(RuntimeKind::Podman),
        "docker" => Some(RuntimeKind::Docker),
        _ => None,
    }
}

/// Reason a proposed container name is unacceptable. Returned from
/// [`container_name_for`] so the caller can refuse to boot an upstream
/// whose key produces a nonsensical name, rather than silently munging.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ContainerNameError {
    /// The derived name would exceed Podman/Docker's 255-char limit on
    /// names. Realistically this requires a pathological config key;
    /// we catch it for completeness.
    #[error("container name derived from upstream key {key:?} is too long ({len} chars, max 255)")]
    TooLong { key: String, len: usize },

    /// Podman/Docker names must match `[a-zA-Z0-9][a-zA-Z0-9_.-]*`.
    /// An upstream key like `"foo bar"` or `"Δ-service"` produces a
    /// name the runtime will reject at spawn time — and reporting the
    /// real problem up front is kinder than burying it in a `podman
    /// run` stderr.
    #[error(
        "container name derived from upstream key {key:?} contains invalid characters (allowed: [A-Za-z0-9_.-], must start with alphanumeric)"
    )]
    InvalidCharacters { key: String },
}

/// Container-name prefix marking the container as gateway-owned. The
/// `-rs` suffix distinguishes this Rust implementation's containers
/// from any left over from an earlier gateway generation. Future
/// `podman ps` filters can target `^rato-` to list our fleet
/// specifically.
pub const CONTAINER_NAME_PREFIX: &str = "rato-";

/// Derive the deterministic container name for an upstream key.
/// `"GitLab"` → `"rato-GitLab"`, `"Atlassian"` → `"rato-Atlassian"`.
///
/// The key comes from the upstream config's top-level display name
/// (i.e. the key in `mcpClients`). Producing a deterministic name
/// serves two purposes:
/// - A leaked container from a prior gateway run collides with the
///   next spawn attempt, surfacing "container already in use" instead
///   of silently stacking up orphans. The pre-respawn reaper
///   (container_runtime_probe::reap_container) forcibly removes any
///   prior container with this name before attempting spawn, so the
///   collision is a tripwire for the good path, not a blocker.
/// - Operators debugging a wedged upstream can `podman inspect
///   rato-GitLab` without guessing a random container id.
pub fn container_name_for(key: &str) -> Result<String, ContainerNameError> {
    let derived = format!("{CONTAINER_NAME_PREFIX}{key}");
    if derived.len() > 255 {
        return Err(ContainerNameError::TooLong {
            key: key.to_string(),
            len: derived.len(),
        });
    }
    if !is_valid_container_name(&derived) {
        return Err(ContainerNameError::InvalidCharacters {
            key: key.to_string(),
        });
    }
    Ok(derived)
}

/// The regex `[a-zA-Z0-9][a-zA-Z0-9_.-]*` expressed as a pure fn so
/// we don't pull in a regex dep for a one-off check.
fn is_valid_container_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphanumeric() {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
}

/// Rewrite the args of a containerized stdio upstream to pin a
/// deterministic container name via `--name <container_name>`.
///
/// Positioning: immediately after the `run` subcommand. `podman run`
/// accepts `--name` anywhere before the image argument, but putting it
/// first is unambiguous — if a future args list contains another flag
/// that happens to accept `--name` as an argument value (none exists
/// today for `podman run`, but defense in depth), we still win.
///
/// No-op if the args don't start with `run` — returns the original
/// args unchanged. This is the Layer-2 business validation: the
/// detection function already rejects non-`run` args, but we double-
/// check here in case the function is called directly.
///
/// If `--name` is already present in the args (operator manually
/// pinned a name), the original args are returned unchanged. We don't
/// try to override an explicit choice; the operator is responsible for
/// ensuring the name doesn't collide with other gateway-spawned
/// containers.
pub fn rewrite_args(args: &[String], container_name: &str) -> Vec<String> {
    if args.first().map(String::as_str) != Some("run") {
        return args.to_vec();
    }
    if args.iter().any(|a| a == "--name") {
        return args.to_vec();
    }
    let mut out = Vec::with_capacity(args.len() + 2);
    out.push("run".to_string());
    out.push("--name".to_string());
    out.push(container_name.to_string());
    out.extend(args.iter().skip(1).cloned());
    out
}

#[cfg(test)]
#[path = "container_runtime_tests.rs"]
mod tests;
