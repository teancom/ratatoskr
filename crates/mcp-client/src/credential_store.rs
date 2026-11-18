// pattern: Imperative Shell
//
// On-disk persistence for OAuth 2.0 credentials, implementing rmcp's
// [`CredentialStore`] trait. One file per OAuth upstream, keyed by the
// upstream's display name.
//
// ## Why this exists
//
// rmcp's `AuthorizationManager` owns the OAuth state machine but delegates
// persistence to a `CredentialStore`. The default `InMemoryCredentialStore`
// would force a browser login on every gateway restart — unacceptable for a
// long-lived background proxy. `FileCredentialStore` persists the
// DCR-issued `client_id` plus the access/refresh tokens so that:
//
//   1. The one-time interactive `login` subcommand writes credentials once.
//   2. Every subsequent headless `connect()` loads them and reconnects with
//      no human present.
//   3. rmcp's silent token refresh (which calls `save()` on every rotation)
//      keeps the file current without any gateway-level plumbing.
//
// ## Security posture
//
// The file holds a refresh token — a long-lived bearer secret. We therefore:
//   - create the parent directory `0700` and each file `0600` *at create
//     time* (no create-then-chmod TOCTOU window),
//   - write atomically (temp file in the same dir → fsync → rename) because
//     rmcp's runtime `save()` on a token refresh can race a concurrent
//     `load()` from a reconnect; a half-written file must never be observed,
//   - never log token material (the rmcp `StoredCredentials` Debug already
//     redacts; we only ever log the *path*).

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use rmcp::transport::auth::{AuthError, CredentialStore, StoredCredentials};

/// Environment variable that overrides the credential state directory.
/// Mirrors the `RATO_` prefix convention used by
/// `RATO_CONFIG_PATH` in the gateway's config loader so the two
/// injection points read the same way. Primarily a test seam (point it
/// at a tempdir) but also a legitimate operator override.
pub const STATE_DIR_ENV: &str = "RATO_STATE_DIR";

/// File-backed [`CredentialStore`]. Each instance owns exactly one
/// upstream's credential file; the [`FileCredentialStore::for_upstream`]
/// constructor derives the path from a state directory + upstream name.
#[derive(Debug, Clone)]
pub struct FileCredentialStore {
    path: PathBuf,
    #[cfg(test)]
    publish_barrier: Option<std::sync::Arc<tokio::sync::Barrier>>,
}

impl FileCredentialStore {
    /// Construct a store targeting an explicit file path. Used by tests
    /// and by [`Self::for_upstream`]; production callers normally go
    /// through `for_upstream`.
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            #[cfg(test)]
            publish_barrier: None,
        }
    }

    /// Resolve the credential file for `upstream_name` under `state_dir`,
    /// returning a store bound to `<state_dir>/credentials/<encoded-name>.json`.
    ///
    /// The display name is percent-encoded into a safe, stable, collision-free
    /// filename component — upstream names can legally contain spaces, slashes,
    /// and other characters illegal or ambiguous in a path segment. Encoding
    /// (rather than sanitizing-by-replacement) guarantees two distinct names
    /// never collide on one file.
    ///
    /// This does NOT create the directory or file — that happens lazily on the
    /// first `save()`, so a `load()` against a never-logged-in upstream simply
    /// sees a missing file and returns `Ok(None)` without touching the
    /// filesystem layout.
    pub fn for_upstream(state_dir: &Path, upstream_name: &str) -> Self {
        let encoded = encode_filename(upstream_name);
        let path = state_dir
            .join("credentials")
            .join(format!("{encoded}.json"));
        Self::new(path)
    }

    /// The on-disk path this store reads and writes. Exposed so the
    /// `login` subcommand can tell the operator exactly where credentials
    /// were saved, and so tests can assert file mode/existence.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Resolve the credential state directory from an injected environment.
///
/// Resolution order:
///   1. `$RATO_STATE_DIR` — if set, used verbatim (test seam + operator
///      override).
///   2. macOS default: `$HOME/Library/Application Support/ratatoskr`.
///      Rotating runtime secrets belong in Application Support (app state),
///      deliberately NOT in `Preferences` (declarative user config) where the
///      static-API-key upstreams live.
///
/// Returns `None` only when neither the override nor `$HOME` is set — an
/// environment so degraded that the caller should surface a clear error
/// rather than guess.
pub fn state_dir_with_env(env_fn: impl for<'k> Fn(&'k str) -> Option<OsString>) -> Option<PathBuf> {
    if let Some(override_dir) = env_fn(STATE_DIR_ENV) {
        return Some(PathBuf::from(override_dir));
    }
    let home = env_fn("HOME")?;
    Some(
        PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("ratatoskr"),
    )
}

/// Production wrapper over [`state_dir_with_env`] reading the real process
/// environment.
pub fn state_dir() -> Option<PathBuf> {
    state_dir_with_env(|k| std::env::var_os(k))
}

/// Percent-encode an upstream display name into a single safe filename
/// component. We encode everything outside an unreserved allowlist
/// (`A-Z a-z 0-9 - _ .`), which collapses spaces, slashes, and any other
/// path-significant or shell-significant character into `%XX` escapes.
fn encode_filename(name: &str) -> String {
    use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

    // Start from "encode everything non-alphanumeric", then carve out the
    // three punctuation marks that are safe and readable in a filename so
    // common names like `Datadog-Acme` stay legible on disk.
    const FILENAME_SAFE: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.');

    utf8_percent_encode(name, FILENAME_SAFE).to_string()
}

#[async_trait::async_trait]
impl CredentialStore for FileCredentialStore {
    async fn load(&self) -> Result<Option<StoredCredentials>, AuthError> {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => {
                let creds = serde_json::from_slice::<StoredCredentials>(&bytes).map_err(|e| {
                    AuthError::InternalError(format!(
                        "credential file {} is corrupt or schema-incompatible: {e}; \
                         delete it and re-run `login`",
                        self.path.display()
                    ))
                })?;
                Ok(Some(creds))
            }
            // Missing file is the normal "never logged in" case — not an error.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(AuthError::InternalError(format!(
                "failed to read credential file {}: {e}",
                self.path.display()
            ))),
        }
    }

    async fn save(&self, credentials: StoredCredentials) -> Result<(), AuthError> {
        let bytes = serde_json::to_vec_pretty(&credentials).map_err(|e| {
            AuthError::InternalError(format!("failed to serialize credentials: {e}"))
        })?;
        write_atomic_0600(
            &self.path,
            &bytes,
            #[cfg(test)]
            self.publish_barrier.as_ref(),
        )
        .await
        .map_err(|e| AuthError::InternalError(format!("{e}")))
    }

    async fn clear(&self) -> Result<(), AuthError> {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            // Already gone is success — `clear` is idempotent.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(AuthError::InternalError(format!(
                "failed to clear credential file {}: {e}",
                self.path.display()
            ))),
        }
    }
}

/// Atomically write `bytes` to `path` with `0600` permissions, creating the
/// parent directory `0700` if needed.
///
/// Strategy: reserve `<path>.tmp.<pid>.<sequence>` exclusively in the same directory (same-filesystem
/// guarantee for an atomic rename), fsync the data, then `rename` over the
/// target. A reader either sees the complete old file or the complete new one —
/// never a torn write. The temp file carries the `0600` mode from creation, so
/// the secret is never world-readable even transiently.
///
/// The sequence separates same-process writers; `create_new` protects existing
/// files even after PID reuse. Publishing is last-writer-wins, never a torn file.
async fn write_atomic_0600(
    path: &Path,
    bytes: &[u8],
    #[cfg(test)] publish_barrier: Option<&std::sync::Arc<tokio::sync::Barrier>>,
) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("credential path {} has no parent directory", path.display()),
        )
    })?;

    create_dir_0700(parent).await?;

    let pid = std::process::id();
    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "credentials.json".to_string());
    let parent = parent.to_path_buf();
    let bytes_owned = bytes.to_vec();
    // Exclusive creation reserves a private file even across concurrent saves,
    // PID reuse, stale files, or counter wrap. Never truncate a preexisting path.
    let tmp_path = tokio::task::spawn_blocking(move || -> std::io::Result<PathBuf> {
        use std::io::Write;
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        for _ in 0..1024 {
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let tmp = parent.join(format!("{file_name}.tmp.{pid}.{sequence}"));
            let mut file = match opts.open(&tmp) {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            };
            if let Err(e) = file.write_all(&bytes_owned).and_then(|()| file.sync_all()) {
                drop(file);
                let _ = std::fs::remove_file(&tmp);
                return Err(e);
            }
            return Ok(tmp);
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "unable to reserve a unique credential temporary file",
        ))
    })
    .await
    .map_err(|e| std::io::Error::other(format!("credential write task panicked: {e}")))??;

    // Tests hold both real saves after fsync, making publication races
    // deterministic without changing production scheduling.
    #[cfg(test)]
    if let Some(barrier) = publish_barrier {
        barrier.wait().await;
    }

    // Atomic publish. On failure, clean up the temp file.
    if let Err(e) = tokio::fs::rename(&tmp_path, path).await {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err(e);
    }

    Ok(())
}

/// Create `dir` (and parents) with the leaf directory restricted to `0700`.
/// Idempotent: an existing directory is left as-is (we do not weaken the
/// permissions of a directory the operator may have tightened further).
async fn create_dir_0700(dir: &Path) -> std::io::Result<()> {
    if dir.exists() {
        return Ok(());
    }
    tokio::fs::create_dir_all(dir).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o700);
        tokio::fs::set_permissions(dir, perms).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "credential_store_tests.rs"]
mod tests;
