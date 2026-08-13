// pattern: Imperative Shell
//
// The running gateway's PID file. Written at startup, removed at clean
// shutdown, and read by the `login` subcommand so it can send SIGHUP to
// trigger a config hot-reload after a successful OAuth login.
//
// ## Why a PID file (and not something fancier)
//
// `login` runs as a separate, short-lived process from the gateway. To ask
// the gateway to reload after writing fresh credentials, it needs the
// gateway's PID. A PID file under the shared state directory is the
// simplest mechanism that works without a control socket or a daemon
// registry — the conventional Unix approach.
//
// ## Staleness
//
// A PID file can outlive its process (crash, SIGKILL, reboot). Readers MUST
// treat it as advisory: confirm the process is actually alive before acting
// on it. `login` does this — a stale PID simply means "no running gateway to
// reload," which is a clean degrade (credentials are still saved).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use mcp_client::state_dir;

/// Filename of the gateway PID file within the state directory.
const PID_FILE_NAME: &str = "gateway.pid";

/// Resolve the PID file path under the shared state directory
/// (`$RATO_STATE_DIR` or `~/Library/Application Support/ratatoskr`).
/// Shares the directory with OAuth credentials so login has exactly one
/// state root to reason about. Returns `None` when the state dir can't be
/// resolved (no `$HOME` and no override).
pub fn pid_file_path() -> Option<PathBuf> {
    state_dir().map(|d| d.join(PID_FILE_NAME))
}

/// Write the current process PID to the PID file, creating the state
/// directory if needed. Best-effort by design: a failure to write the PID
/// file must NOT prevent the gateway from starting — it only costs the
/// SIGHUP-on-login convenience, which degrades to the manual reload hint.
/// Logs at warn on failure and returns the path on success so the caller
/// can remove it at shutdown.
pub fn write() -> Option<PathBuf> {
    let path = pid_file_path()?;
    match write_to(&path, std::process::id()) {
        Ok(()) => {
            tracing::debug!(path = %path.display(), pid = std::process::id(), "wrote PID file");
            Some(path)
        }
        Err(e) => {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "could not write PID file; `login` won't be able to auto-reload this gateway \
                 (it will fall back to printing a manual reload hint)"
            );
            None
        }
    }
}

/// Remove the PID file at `path`, ignoring a missing file. Best-effort
/// cleanup at shutdown; a leftover file is handled by readers' liveness
/// checks, so a failure here is logged at debug, not warn.
pub fn remove(path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => tracing::debug!(path = %path.display(), "removed PID file"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::debug!(path = %path.display(), error = %e, "could not remove PID file"),
    }
}

/// Write `pid` to `path`, creating the parent directory if absent. Factored
/// out so tests can target a tempdir without touching the real state dir.
fn write_to(path: &Path, pid: u32) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating state dir {}", parent.display()))?;
    }
    std::fs::write(path, pid.to_string())
        .with_context(|| format!("writing PID file {}", path.display()))?;
    Ok(())
}

/// Read a PID from the file at `path`. Returns `Ok(None)` when the file is
/// absent (no running gateway has registered), `Err` only on a present-but-
/// unreadable/garbage file. The returned PID is NOT liveness-checked here —
/// callers must verify the process is alive before signalling it.
pub fn read_from(path: &Path) -> Result<Option<u32>> {
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let pid = s.trim().parse::<u32>().with_context(|| {
                format!("PID file {} does not contain a valid PID", path.display())
            })?;
            Ok(Some(pid))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => {
            Err(anyhow::Error::new(e).context(format!("reading PID file {}", path.display())))
        }
    }
}

#[cfg(test)]
#[path = "pidfile_tests.rs"]
mod tests;
