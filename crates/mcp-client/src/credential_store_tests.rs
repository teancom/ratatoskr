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
async fn simultaneous_saves_have_independent_temporary_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = FileCredentialStore::for_upstream(dir.path(), "svc");
    store.publish_barrier = Some(std::sync::Arc::new(tokio::sync::Barrier::new(2)));
    let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            store.save(sample_creds("first")),
            store.save(sample_creds("second"))
        )
    })
    .await
    .expect("both saves reach publication without sleeps");
    first.expect("first save publishes its own temporary file");
    second.expect("second save publishes its own temporary file");
    let loaded = store.load().await.expect("load").expect("present");
    assert!(["first", "second"].contains(&loaded.client_id.as_str()));
    let mut entries = tokio::fs::read_dir(store.path().parent().expect("parent"))
        .await
        .expect("read directory");
    let mut count = 0;
    while let Some(entry) = entries.next_entry().await.expect("entry") {
        assert!(!entry.file_name().to_string_lossy().contains(".tmp."));
        count += 1;
    }
    assert_eq!(count, 1);
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
    // The legible common case stays readable.
    assert!(c.path().to_string_lossy().ends_with("Datadog-Acme.json"));
    // No path separators leak into the encoded component.
    let a_name = a
        .path()
        .file_name()
        .expect("encoded path has a filename component")
        .to_string_lossy();
    assert!(!a_name.contains(' '));
}
