//! Integration test for the OAuth 2.0 upstream login → headless-connect cycle.
//!
//! Exercises the REAL rmcp `AuthorizationManager`/`AuthorizationSession`
//! machinery and our real [`FileCredentialStore`] against an in-process fake
//! authorization server (axum) — no live Datadog, no browser, no network
//! beyond loopback.
//!
//! What it proves:
//!   1. The full authorization-code exchange (discovery → dynamic client
//!      registration → token exchange) drives our `complete_login` and
//!      **persists credentials to disk** through `FileCredentialStore`.
//!   2. A subsequent headless `AuthorizationManager::initialize_from_store`
//!      against the same store loads those credentials (returns `true`) — i.e.
//!      the gateway's headless connect would get past the "needs login" gate.
//!   3. The persisted credential file holds the DCR-issued client_id and a
//!      refresh token, so silent refresh has what it needs.
//!
//! The fake AS speaks just enough of RFC 8414 / RFC 9728 / RFC 7591 / OAuth
//! 2.1 for rmcp's client to complete the flow.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use mcp_client::FileCredentialStore;
use rmcp::transport::auth::{AuthorizationManager, AuthorizationSession, OAuthClientConfig};
use serde_json::{Value, json};
use tokio::net::TcpListener;

/// Shared base URL of the fake AS, injected into every metadata document so
/// rmcp's discovery resolves every endpoint back to this same server.
#[derive(Clone)]
struct FakeAs {
    base: String,
    /// Counts `/token` POSTs. The first (the authorization-code exchange)
    /// succeeds; when `reject_refresh` is set, every subsequent call (the
    /// refresh grant) is rejected — simulating a dead/revoked refresh token.
    token_calls: Arc<AtomicUsize>,
    /// Counts `/register` POSTs. The static-client-id flow pins this at
    /// zero: a pre-registered client must never attempt DCR.
    register_calls: Arc<AtomicUsize>,
    /// When true, the SECOND+ `/token` call (refresh) returns 401. The
    /// first (initial code exchange) always succeeds so we can persist
    /// credentials, then watch the *refresh* fail like a real expiry.
    reject_refresh: bool,
    /// `expires_in` returned by the initial token exchange. A 0 here makes
    /// rmcp treat the access token as immediately stale, so the very next
    /// `get_access_token` triggers a refresh.
    initial_expires_in: u64,
}

/// Stand up the fake authorization server on an ephemeral loopback port.
/// Returns `(base_url, mcp_endpoint_url, state)` — the state handle exposes
/// the per-endpoint call counters. The server task runs detached for the
/// duration of the test process.
///
/// Happy-path defaults: long-lived access token, refresh always accepted.
async fn start_fake_as() -> (String, String, Arc<FakeAs>) {
    start_fake_as_with(false, 3600).await
}

/// Parameterized variant of [`start_fake_as`]:
///   - `reject_refresh`: the refresh grant (2nd+ `/token` call) returns 401.
///   - `initial_expires_in`: `expires_in` for the initial code exchange.
async fn start_fake_as_with(
    reject_refresh: bool,
    initial_expires_in: u64,
) -> (String, String, Arc<FakeAs>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake AS");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let base = format!("http://{addr}");

    let state = Arc::new(FakeAs {
        base: base.clone(),
        token_calls: Arc::new(AtomicUsize::new(0)),
        register_calls: Arc::new(AtomicUsize::new(0)),
        reject_refresh,
        initial_expires_in,
    });

    // The MCP "resource" lives at /mcp; discovery hangs off that path.
    let app = Router::new()
        // RFC 9728 protected-resource metadata. rmcp probes a few path
        // shapes; serve all of them with one handler.
        .route(
            "/.well-known/oauth-protected-resource/mcp",
            get(protected_resource),
        )
        .route(
            "/.well-known/oauth-protected-resource",
            get(protected_resource),
        )
        .route(
            "/mcp/.well-known/oauth-protected-resource",
            get(protected_resource),
        )
        // RFC 8414 authorization-server metadata (and its path-suffixed form).
        .route(
            "/.well-known/oauth-authorization-server",
            get(authorization_server),
        )
        .route(
            "/.well-known/oauth-authorization-server/mcp",
            get(authorization_server),
        )
        // RFC 7591 dynamic client registration.
        .route("/register", post(register))
        // OAuth 2.1 token endpoint (authorization_code + refresh_token grants).
        .route("/token", post(token))
        .with_state(Arc::clone(&state));

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("fake AS serve");
    });

    let mcp_endpoint = format!("{base}/mcp");
    (base, mcp_endpoint, state)
}

async fn protected_resource(State(s): State<Arc<FakeAs>>) -> Json<Value> {
    Json(json!({
        "resource": format!("{}/mcp", s.base),
        "authorization_servers": [s.base],
        "bearer_methods_supported": ["header"],
    }))
}

async fn authorization_server(State(s): State<Arc<FakeAs>>) -> Json<Value> {
    Json(json!({
        "issuer": s.base,
        "authorization_endpoint": format!("{}/authorize", s.base),
        "token_endpoint": format!("{}/token", s.base),
        "registration_endpoint": format!("{}/register", s.base),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
    }))
}

async fn register(
    State(s): State<Arc<FakeAs>>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    s.register_calls.fetch_add(1, Ordering::SeqCst);
    // Echo back the registered redirect_uris (rmcp's
    // ClientRegistrationResponse requires the field). Public client (no
    // secret), matching Datadog's `none` auth method.
    let redirect_uris = body
        .get("redirect_uris")
        .cloned()
        .unwrap_or_else(|| json!([]));
    (
        StatusCode::CREATED,
        Json(json!({
            "client_id": "fake-client-id-123",
            "redirect_uris": redirect_uris,
            "token_endpoint_auth_method": "none",
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
        })),
    )
}

async fn token(State(s): State<Arc<FakeAs>>) -> (StatusCode, Json<Value>) {
    // Both the initial code exchange and any later refresh land here. The
    // first call is the authorization-code exchange; subsequent calls are
    // refresh grants. When `reject_refresh` is set we 401 the refresh to
    // simulate a dead/revoked refresh token — the exact mid-life expiry
    // case the gateway must surface as NeedsLogin.
    let call = s.token_calls.fetch_add(1, Ordering::SeqCst);
    let is_refresh = call > 0;

    if is_refresh && s.reject_refresh {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "invalid_grant",
                "error_description": "refresh token expired or revoked",
            })),
        );
    }

    (
        StatusCode::OK,
        Json(json!({
            "access_token": "fake-access-token",
            "token_type": "Bearer",
            "expires_in": s.initial_expires_in,
            "refresh_token": "fake-refresh-token",
            "scope": "metrics_read",
        })),
    )
}

/// Build an `AuthorizationSession` exactly as `login::build_session` does, but
/// inline so the test controls the store and can read back the CSRF state. The
/// production path is covered by the binary's CLI wiring; here we drive the
/// rmcp flow + `complete_login` + `FileCredentialStore` together.
async fn build_session(
    mcp_url: &str,
    redirect_uri: &str,
    store: FileCredentialStore,
) -> AuthorizationSession {
    let http = reqwest::Client::builder().build().expect("reqwest client");
    let mut manager = AuthorizationManager::new(mcp_url.to_string())
        .await
        .expect("manager new");
    manager.set_credential_store(store);
    manager.with_client(http).expect("with_client");
    let metadata = manager.discover_metadata().await.expect("discover");
    manager.set_metadata(metadata);
    AuthorizationSession::new(manager, &[], redirect_uri, Some("test-client"), None)
        .await
        .expect("session new")
}

/// Pull the `state` (CSRF token) rmcp embedded in the authorization URL so we
/// can feed it back to `handle_callback`, simulating the provider echoing it
/// on the redirect.
fn csrf_from_auth_url(auth_url: &str) -> String {
    let query = auth_url.split_once('?').expect("auth url has query").1;
    for pair in query.split('&') {
        if let Some(("state", v)) = pair.split_once('=') {
            return percent_encoding::percent_decode_str(v)
                .decode_utf8_lossy()
                .into_owned();
        }
    }
    panic!("no state param in auth url: {auth_url}");
}

#[tokio::test]
async fn login_persists_credentials_and_headless_load_finds_them() {
    let (_base, mcp_url, _fake) = start_fake_as().await;

    let cred_dir = tempfile::tempdir().expect("tempdir");
    let store = FileCredentialStore::for_upstream(cred_dir.path(), "Datadog-Acme");
    let cred_path = store.path().to_path_buf();

    // --- Login (interactive flow, browser-less) ---
    let redirect_uri = "http://127.0.0.1:0/callback";
    let session = build_session(&mcp_url, redirect_uri, store.clone()).await;

    // rmcp generated a CSRF state + stored a PKCE verifier when it built the
    // auth URL. The "provider" would echo that state on the redirect; we read
    // it straight from the URL.
    let state = csrf_from_auth_url(session.get_authorization_url());

    // The authorization "code" is opaque to our fake token endpoint, which
    // accepts anything — so any non-empty value works.
    gateway::login::complete_login(&session, "fake-auth-code", &state)
        .await
        .map_err(|e| e.to_string())
        .expect("complete_login should exchange + persist");

    // --- Assertion 1: credentials landed on disk ---
    assert!(
        cred_path.exists(),
        "login must persist a credential file at {}",
        cred_path.display()
    );
    let raw = std::fs::read_to_string(&cred_path).expect("read cred file");
    assert!(
        raw.contains("fake-client-id-123"),
        "persisted credentials must carry the DCR client_id; got: {raw}"
    );
    assert!(
        raw.contains("fake-refresh-token"),
        "persisted credentials must carry the refresh token for silent renewal"
    );

    // --- Assertion 2: a fresh headless manager loads them past the gate ---
    let http = reqwest::Client::builder().build().expect("reqwest");
    let mut headless = AuthorizationManager::new(mcp_url.clone())
        .await
        .expect("headless manager");
    headless.set_credential_store(store);
    headless.with_client(http).expect("with_client");
    let have = headless
        .initialize_from_store()
        .await
        .expect("initialize_from_store");
    assert!(
        have,
        "headless initialize_from_store must return true after login persisted credentials"
    );
}

#[tokio::test]
async fn static_client_id_login_skips_registration_and_persists_configured_id() {
    // Pins the DCR-less login flow (config carries a pre-registered
    // client_id — e.g. Slack's MCP server, which prohibits RFC 7591
    // registration) against three regressions: the flow silently falling
    // back to DCR, the exchange demanding a client_secret, and the
    // persisted credentials carrying some other client_id (which would
    // break headless refresh).
    let (_base, mcp_url, fake) = start_fake_as().await;

    let cred_dir = tempfile::tempdir().expect("tempdir");
    let store = FileCredentialStore::for_upstream(cred_dir.path(), "Slack");
    let cred_path = store.path().to_path_buf();

    // Mirror `login::build_flow`'s static branch inline (same reason
    // `build_session` above mirrors the DCR branch): configure the
    // pre-registered client, compute the auth URL, no session object.
    let redirect_uri = "http://localhost:3118/callback";
    let http = reqwest::Client::builder().build().expect("reqwest client");
    let mut manager = AuthorizationManager::new(mcp_url.clone())
        .await
        .expect("manager new");
    manager.set_credential_store(store.clone());
    manager.with_client(http).expect("with_client");
    let metadata = manager.discover_metadata().await.expect("discover");
    manager.set_metadata(metadata);
    manager
        .configure_client(OAuthClientConfig::new(
            "preregistered-client-42",
            redirect_uri,
        ))
        .expect("configure_client");
    let auth_url = manager
        .get_authorization_url(&[])
        .await
        .expect("authorization url");
    assert!(
        auth_url.contains("preregistered-client-42"),
        "auth URL must carry the configured client_id; got: {auth_url}"
    );

    let state = csrf_from_auth_url(&auth_url);
    gateway::login::complete_static_login(&manager, "fake-auth-code", &state)
        .await
        .map_err(|e| e.to_string())
        .expect("static exchange should succeed and persist");

    // --- Assertion 1: DCR never happened ---
    assert_eq!(
        fake.register_calls.load(Ordering::SeqCst),
        0,
        "static-client flow must never call the registration endpoint"
    );

    // --- Assertion 2: persisted credentials carry the CONFIGURED id ---
    let raw = std::fs::read_to_string(&cred_path).expect("read cred file");
    assert!(
        raw.contains("preregistered-client-42"),
        "persisted credentials must carry the configured client_id; got: {raw}"
    );
    assert!(
        raw.contains("fake-refresh-token"),
        "persisted credentials must carry the refresh token for silent renewal"
    );

    // --- Assertion 3: a fresh headless manager loads them past the gate ---
    let http = reqwest::Client::builder().build().expect("reqwest");
    let mut headless = AuthorizationManager::new(mcp_url)
        .await
        .expect("headless manager");
    headless.set_credential_store(store);
    headless.with_client(http).expect("with_client");
    let have = headless
        .initialize_from_store()
        .await
        .expect("initialize_from_store");
    assert!(
        have,
        "headless initialize_from_store must return true after a static-client login"
    );
}

#[tokio::test]
async fn dead_refresh_token_surfaces_as_auth_failure_through_real_rmcp() {
    // End-to-end pin of the mid-life / at-restart re-auth detection against
    // the REAL rmcp refresh path. We:
    //   1. Log in normally, but the initial access token has expires_in=0
    //      (immediately stale) so the next use forces a refresh.
    //   2. The fake AS rejects that refresh with 401 invalid_grant —
    //      exactly what a dead/revoked refresh token looks like.
    //   3. Drive rmcp's `AuthClient::get_access_token`, which performs the
    //      refresh, fails, and returns an AuthError.
    //   4. Assert our `is_auth_failure` classifier catches that REAL rmcp
    //      error — proving the wiring in connect_oauth_headless/call_tool
    //      will reclassify it to OAuthReauthRequired rather than a generic
    //      retry-forever failure.
    use rmcp::transport::auth::AuthClient;

    let (_base, mcp_url, _fake) =
        start_fake_as_with(/* reject_refresh */ true, /* expires_in */ 0).await;

    let cred_dir = tempfile::tempdir().expect("tempdir");
    let store = FileCredentialStore::for_upstream(cred_dir.path(), "Datadog-Acme");

    // Login persists a (already-stale) credential set.
    let redirect_uri = "http://127.0.0.1:0/callback";
    let session = build_session(&mcp_url, redirect_uri, store.clone()).await;
    let state = csrf_from_auth_url(session.get_authorization_url());
    gateway::login::complete_login(&session, "fake-auth-code", &state)
        .await
        .map_err(|e| e.to_string())
        .expect("initial login should succeed and persist");

    // Build a headless manager + AuthClient over the same store, exactly as
    // connect_oauth_headless does.
    let http = reqwest::Client::builder().build().expect("reqwest");
    let mut manager = AuthorizationManager::new(mcp_url.clone())
        .await
        .expect("manager");
    manager.set_credential_store(store);
    manager.with_client(http.clone()).expect("with_client");
    assert!(
        manager
            .initialize_from_store()
            .await
            .expect("initialize_from_store"),
        "credentials are present, so the gate passes — the failure must come from the dead refresh"
    );

    let auth_client = AuthClient::new(http, manager);

    // This triggers the refresh (access token is stale) → 401 → AuthError.
    let err = auth_client
        .get_access_token()
        .await
        .expect_err("a dead refresh token must make get_access_token fail");

    assert!(
        mcp_client::is_auth_failure(&err),
        "the REAL rmcp refresh-failure error ({err:?} / {err}) must be classified as an auth \
         failure so the gateway surfaces NeedsLogin instead of retrying forever"
    );
}
