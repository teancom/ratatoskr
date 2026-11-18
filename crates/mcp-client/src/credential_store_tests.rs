use super::*;
use rmcp::transport::auth::StoredCredentials;

fn sample_creds(client_id: &str) -> StoredCredentials {
    // token_response None keeps the fixture independent of oauth2's
    // OAuthTokenResponse internals; the store round-trips the whole
    // struct regardless, and client_id + scopes exercise the
    // serde path that matters for the "did login persist?" check.
    StoredCredentials::new(
        client_id.to_string(),
        None,
        vec!["metrics_read".to_string(), "dashboards_read".to_string()],
        Some(1_700_000_000),
    )
}

#[tokio::test]
async fn case_variant_names_keep_independent_credentials() {
    let dir = tempfile::tempdir().expect("tempdir");
    let upper = FileCredentialStore::for_upstream(dir.path(), "A");
    let lower = FileCredentialStore::for_upstream(dir.path(), "a");
    upper.save(sample_creds("first")).await.expect("save A");
    lower.save(sample_creds("second")).await.expect("save a");
    assert_eq!(
        upper
            .load()
            .await
            .expect("load A")
            .expect("present")
            .client_id,
        "first"
    );
    assert_eq!(
        lower
            .load()
            .await
            .expect("load a")
            .expect("present")
            .client_id,
        "second"
    );
    assert_ne!(
        upper.path().to_string_lossy().to_lowercase(),
        lower.path().to_string_lossy().to_lowercase()
    );
}

#[tokio::test]
async fn save_then_load_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FileCredentialStore::for_upstream(dir.path(), "Datadog-Acme");

    let creds = sample_creds("client-abc");
    store.save(creds.clone()).await.expect("save");

    let loaded = store.load().await.expect("load").expect("present");
    assert_eq!(loaded.client_id, creds.client_id);
    assert_eq!(loaded.granted_scopes, creds.granted_scopes);
    assert_eq!(loaded.token_received_at, creds.token_received_at);
}

#[tokio::test]
async fn load_absent_file_is_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FileCredentialStore::for_upstream(dir.path(), "never-logged-in");
    assert!(store.load().await.expect("load").is_none());
}

#[tokio::test]
async fn clear_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FileCredentialStore::for_upstream(dir.path(), "svc");
    // clear on a missing file is Ok
    store.clear().await.expect("clear-absent");
    store.save(sample_creds("c")).await.expect("save");
    store.clear().await.expect("clear-present");
    assert!(store.load().await.expect("load").is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn saved_file_is_0600_and_dir_is_0700() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let store = FileCredentialStore::for_upstream(dir.path(), "svc");
    store.save(sample_creds("c")).await.expect("save");

    let file_mode = std::fs::metadata(store.path())
        .expect("stat file")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(file_mode, 0o600, "credential file must be owner-only rw");

    let cred_dir = store.path().parent().expect("parent");
    let dir_mode = std::fs::metadata(cred_dir)
        .expect("stat dir")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(dir_mode, 0o700, "credentials dir must be owner-only");
}

#[tokio::test]
async fn no_tmp_file_left_after_save() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FileCredentialStore::for_upstream(dir.path(), "svc");
    store.save(sample_creds("c")).await.expect("save");

    let cred_dir = store.path().parent().expect("parent");
    let mut entries = tokio::fs::read_dir(cred_dir).await.expect("read_dir");
    let mut names = Vec::new();
    while let Some(e) = entries.next_entry().await.expect("entry") {
        names.push(e.file_name().to_string_lossy().into_owned());
    }
    assert!(
        names.iter().all(|n| !n.contains(".tmp.")),
        "atomic write must leave no temp file behind, found: {names:?}"
    );
}

#[tokio::test]
async fn concurrent_saves_never_yield_a_torn_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FileCredentialStore::for_upstream(dir.path(), "svc");
    // Seed so the first reads have something to parse.
    store.save(sample_creds("seed")).await.expect("seed");

    let writer = {
        let store = store.clone();
        tokio::spawn(async move {
            for i in 0..50 {
                store
                    .save(sample_creds(&format!("client-{i}")))
                    .await
                    .expect("save");
            }
        })
    };

    // Interleave reads; every read must parse to a complete record (the
    // atomic rename guarantees no partial file is ever observed).
    for _ in 0..200 {
        if let Some(c) = store.load().await.expect("load") {
            assert!(c.client_id.starts_with("client-") || c.client_id == "seed");
        }
    }
    writer.await.expect("writer joined");
}

#[tokio::test]
async fn identity_and_version_mismatches_fail_closed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let a = FileCredentialStore::for_upstream(dir.path(), "A");
    let b = FileCredentialStore::new(a.path().to_path_buf(), "a");
    a.save(sample_creds("synthetic-sensitive-client"))
        .await
        .expect("save");
    for store in [&b, &a] {
        if store.upstream_name == "A" {
            let mut envelope: CredentialEnvelope =
                serde_json::from_slice(&tokio::fs::read(a.path()).await.expect("read fixture"))
                    .expect("envelope");
            envelope.version = 99;
            tokio::fs::write(
                a.path(),
                serde_json::to_vec(&envelope).expect("serialize fixture"),
            )
            .await
            .expect("tamper version");
        }
        match store.load().await.expect_err("reject mismatched envelope") {
            AuthError::InternalError(message) => assert_eq!(
                message,
                "credential identity or storage version mismatch; refusing credentials; re-run `login`"
            ),
            other => panic!("expected internal error, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn legacy_only_requires_relogin_without_reading_or_deleting_legacy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FileCredentialStore::for_upstream(dir.path(), "A");
    let legacy = store.legacy_path.as_ref().expect("legacy path");
    tokio::fs::create_dir_all(legacy.parent().expect("parent"))
        .await
        .expect("mkdir");
    let original = b"synthetic unparseable legacy fixture";
    tokio::fs::write(legacy, original)
        .await
        .expect("write fixture");
    match store.load().await.expect_err("explicit migration required") {
        AuthError::InternalError(message) => assert_eq!(
            message,
            "legacy credentials require explicit migration: identity cannot be verified; re-run `login` for this upstream to save fresh versioned credentials; legacy files are preserved and never imported"
        ),
        other => panic!("expected internal error, got {other:?}"),
    }
    store.clear().await.expect("clear new path only");
    assert!(
        store
            .load()
            .await
            .expect("acknowledged fresh login")
            .is_none()
    );
    store.save(sample_creds("fresh")).await.expect("fresh save");
    let reopened = FileCredentialStore::for_upstream(dir.path(), "A");
    assert_eq!(
        reopened
            .load()
            .await
            .expect("load new")
            .expect("present")
            .client_id,
        "fresh"
    );
    assert_eq!(
        tokio::fs::read(legacy).await.expect("legacy preserved"),
        original
    );
}

#[tokio::test]
async fn filename_byte_limit_is_explicit_and_accepts_boundary() {
    let dir = tempfile::tempdir().expect("tempdir");
    let valid = FileCredentialStore::for_upstream(dir.path(), &"é".repeat(50));
    valid
        .save(sample_creds("boundary"))
        .await
        .expect("100 UTF-8 bytes supported");
    assert_eq!(valid.path().file_name().expect("filename").len(), 205);
    let invalid = FileCredentialStore::for_upstream(dir.path(), &"a".repeat(101));
    for error in [
        invalid.load().await.expect_err("oversize load"),
        invalid
            .save(sample_creds("x"))
            .await
            .expect_err("oversize save"),
    ] {
        match error {
            AuthError::InternalError(message) => assert_eq!(
                message,
                "credential upstream name exceeds 100 UTF-8 bytes; choose a shorter upstream name and re-run `login`"
            ),
            other => panic!("expected internal error, got {other:?}"),
        }
    }
}

#[test]
fn state_dir_prefers_override() {
    let dir = state_dir_with_env(|k| match k {
        STATE_DIR_ENV => Some(OsString::from("/custom/state")),
        "HOME" => Some(OsString::from("/home/whoever")),
        _ => None,
    })
    .expect("some");
    assert_eq!(dir, PathBuf::from("/custom/state"));
}

#[test]
fn state_dir_falls_back_to_application_support() {
    let dir = state_dir_with_env(|k| match k {
        "HOME" => Some(OsString::from("/home/whoever")),
        _ => None,
    })
    .expect("some");
    assert_eq!(
        dir,
        PathBuf::from("/home/whoever/Library/Application Support/ratatoskr")
    );
}

#[test]
fn state_dir_none_without_home_or_override() {
    assert!(state_dir_with_env(|_| None).is_none());
}

#[test]
fn filenames_are_encoded_and_collision_free() {
    // Names that would collide or break under naive sanitization.
    let a = FileCredentialStore::for_upstream(Path::new("/s"), "Datadog Acme");
    let b = FileCredentialStore::for_upstream(Path::new("/s"), "Datadog/Acme");
    let c = FileCredentialStore::for_upstream(Path::new("/s"), "Datadog-Acme");
    assert_ne!(a.path(), b.path());
    assert_ne!(a.path(), c.path());
    assert_ne!(b.path(), c.path());
    assert_eq!(encode_filename("A"), "41");
    assert_eq!(encode_filename("a"), "61");
    let names = ["A", "a", "%41", "é", "e\u{301}", "/", ".", "", "a/b"];
    for (i, name) in names.iter().enumerate() {
        for other in &names[i + 1..] {
            assert_ne!(
                encode_filename(name).to_lowercase(),
                encode_filename(other).to_lowercase()
            );
        }
    }
    // No path separators leak into the encoded component.
    let a_name = a
        .path()
        .file_name()
        .expect("encoded path has a filename component")
        .to_string_lossy();
    assert!(!a_name.contains(' '));
}
