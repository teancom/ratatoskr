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
    upstream_name: String,
    legacy_path: Option<PathBuf>,
    legacy_cleared: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

const STORAGE_VERSION: u32 = 2;
// 200 hex bytes + .json leaves room below a 255-byte component limit for
// atomic-writer suffixes. Reject rather than truncate distinct identities.
const MAX_NAME_BYTES: usize = 100;

#[derive(serde::Serialize, serde::Deserialize)]
struct CredentialEnvelope {
    version: u32,
    upstream_name: String,
    credentials: StoredCredentials,
}

impl FileCredentialStore {
    /// Construct a store targeting an explicit file path. Used by tests
    /// and by [`Self::for_upstream`]; production callers normally go
    /// through `for_upstream`.
    pub fn new(path: PathBuf, upstream_name: &str) -> Self {
        Self {
            path,
            upstream_name: upstream_name.to_string(),
            legacy_path: None,
            legacy_cleared: Default::default(),
        }
    }

    fn validate_name(&self) -> Result<(), AuthError> {
        if self.upstream_name.len() > MAX_NAME_BYTES {
            return Err(AuthError::InternalError(format!(
                "credential upstream name exceeds {MAX_NAME_BYTES} UTF-8 bytes; choose a shorter upstream name and re-run `login`"
            )));
        }
        Ok(())
    }

    /// Bind an upstream to `credentials-v2/<lowercase-hex-UTF8>.json`.
    /// Names are byte-exact, including case and Unicode normalization. Load and
    /// save reject names longer than 100 UTF-8 bytes to bound filename length.
    ///
    /// Legacy files have no trustworthy identity and are never imported or
    /// read. Their presence causes an explicit relogin error, not fallback.
    /// `clear()` acknowledges relogin for this store and its clones, touching
    /// only the new path; `save()` always writes only the new format.
    pub fn for_upstream(state_dir: &Path, upstream_name: &str) -> Self {
        let encoded = encode_filename(upstream_name);
        let path = state_dir
            .join("credentials-v2")
            .join(format!("{encoded}.json"));
        let mut store = Self::new(path, upstream_name);
        store.legacy_path = Some(
            state_dir
                .join("credentials")
                .join(format!("{}.json", legacy_encode_filename(upstream_name))),
        );
        store
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

/// Lowercase hex preserves byte identity under filesystem casefolding and
/// Unicode normalization. No input characters are emitted literally.
fn encode_filename(name: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(name.len() * 2);
    for byte in name.bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 15)]));
    }
    encoded
}

fn legacy_encode_filename(name: &str) -> String {
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
        self.validate_name()?;
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => {
                let envelope =
                    serde_json::from_slice::<CredentialEnvelope>(&bytes).map_err(|_| {
                        AuthError::InternalError(format!(
                            "credential file {} is corrupt or schema-incompatible; re-run `login`",
                            self.path.display()
                        ))
                    })?;
                if envelope.version != STORAGE_VERSION
                    || envelope.upstream_name != self.upstream_name
                {
                    return Err(AuthError::InternalError(
                        "credential identity or storage version mismatch; refusing credentials; re-run `login`".to_string()
                    ));
                }
                Ok(Some(envelope.credentials))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if !self
                    .legacy_cleared
                    .load(std::sync::atomic::Ordering::Relaxed)
                    && let Some(legacy) = &self.legacy_path
                {
                    match tokio::fs::symlink_metadata(legacy).await {
                        Ok(_) => return Err(AuthError::InternalError(
                            "legacy credentials require explicit migration: identity cannot be verified; re-run `login` for this upstream to save fresh versioned credentials; legacy files are preserved and never imported".to_string()
                        )),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {},
                        Err(_) => return Err(AuthError::InternalError(
                            "unable to inspect legacy credential path; check permissions and re-run `login`".to_string()
                        )),
                    }
                }
                Ok(None)
            }
            Err(e) => Err(AuthError::InternalError(format!(
                "failed to read credential file {}: {e}",
                self.path.display()
            ))),
        }
    }

    async fn save(&self, credentials: StoredCredentials) -> Result<(), AuthError> {
        self.validate_name()?;
        let envelope = CredentialEnvelope {
            version: STORAGE_VERSION,
            upstream_name: self.upstream_name.clone(),
            credentials,
        };
        let bytes = serde_json::to_vec_pretty(&envelope).map_err(|e| {
            AuthError::InternalError(format!("failed to serialize credentials: {e}"))
        })?;
        write_atomic_0600(&self.path, &bytes)
            .await
            .map_err(|e| AuthError::InternalError(format!("{e}")))
    }

    async fn clear(&self) -> Result<(), AuthError> {
        self.validate_name()?;
        self.legacy_cleared
            .store(true, std::sync::atomic::Ordering::Relaxed);
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
/// Strategy: write to `<path>.tmp.<pid>` in the same directory (same-filesystem
/// guarantee for an atomic rename), fsync the data, then `rename` over the
/// target. A reader either sees the complete old file or the complete new one —
/// never a torn write. The temp file carries the `0600` mode from creation, so
/// the secret is never world-readable even transiently.
///
/// The `.tmp.<pid>` suffix keeps two concurrent writers (e.g. a refresh racing
/// a re-login) from clobbering each other's temp file before either renames.
async fn write_atomic_0600(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
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
    let tmp_path = parent.join(format!("{file_name}.tmp.{pid}"));

    // Write + fsync + set mode on the temp file. Done in spawn_blocking
    // because the create-with-mode dance uses std's OpenOptionsExt and a
    // blocking fsync; keeping it off the async reactor avoids stalling it
    // and sidesteps tokio::fs's lack of a create-with-mode primitive.
    let tmp_for_blocking = tmp_path.clone();
    let bytes_owned = bytes.to_vec();
    let write_result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        use std::io::Write;

        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&tmp_for_blocking)?;
        file.write_all(&bytes_owned)?;
        file.sync_all()?;
        Ok(())
    })
    .await
    .map_err(|e| std::io::Error::other(format!("credential write task panicked: {e}")))?;

    if let Err(e) = write_result {
        // Best-effort cleanup of the temp file on write failure so we don't
        // leak `.tmp.<pid>` droppings.
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err(e);
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
