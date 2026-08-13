// pattern: Imperative Shell (with a functional-ish testable core)
//
// One-time interactive OAuth 2.0 login for `http-oauth` upstreams, invoked as
// `rato login <upstream-name>`.
//
// ## Why this is a separate, human-present path
//
// The OAuth authorization-code flow requires a browser: the user authenticates
// with the provider (Datadog), which redirects back to a localhost callback URL
// carrying an authorization `code`. That is fundamentally interactive and can
// only run when a human is at the keyboard. The *running* gateway — boot,
// reload, restart, supervisor respawn — must never trigger it, which is why it
// lives in this subcommand rather than in `connect()`. Once login persists
// credentials to disk (via `FileCredentialStore`), every subsequent connect is
// fully headless and rmcp refreshes the token silently.
//
// ## Flow
//
//   1. Load the gateway config; find the named upstream; assert it's
//      `http-oauth`.
//   2. Bind a localhost TCP listener (ephemeral port unless configured) and
//      derive the redirect URI from the actual bound port.
//   3. Build an `AuthorizationManager` wired to a `FileCredentialStore`,
//      discover the provider's metadata, and open a `LoginFlow`: dynamic
//      client registration (`AuthorizationSession`) by default, or a
//      statically-configured pre-registered client when the config carries
//      a `client_id` (for providers like Slack's MCP server that prohibit
//      DCR).
//   4. Open the auth URL in the user's browser; wait for the provider to
//      redirect back to our callback with `?code=…&state=…`.
//   5. Exchange the code for tokens — rmcp persists the full
//      `StoredCredentials` (client_id + tokens) through the file store,
//      regardless of which variant ran.
//
// Steps 1–4 are the imperative shell (sockets, browser, sleeping). Step 5's
// exchange and the flow-construction in step 3 are the testable core, exposed
// as `complete_login` / `complete_static_login` so an integration test can
// drive them against a fake authorization server without a real browser.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use mcp_client::FileCredentialStore;
use mcp_client::UpstreamKind;
use rmcp::transport::auth::{AuthorizationManager, AuthorizationSession, OAuthClientConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// How long we wait for the user to complete the browser flow before giving up.
/// Generous enough for an SSO detour (MFA, org picker) but bounded so a
/// walked-away terminal doesn't hang forever holding the callback port.
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(300);

/// Default DCR client name when the config doesn't set one. Shows up in the
/// provider's authorized-applications list.
const DEFAULT_CLIENT_NAME: &str = "ratatoskr";

/// Resolved parameters for a single OAuth login, extracted from config.
// Debug is safe here: params carry only config-file values (no tokens,
// no secrets — the static flow is a public client by construction).
#[derive(Debug)]
struct OAuthLoginParams {
    upstream_name: String,
    url: String,
    scopes: Vec<String>,
    client_name: String,
    callback_port: u16,
    /// Pre-registered client_id. `Some` selects the DCR-less flow
    /// ([`LoginFlow::StaticClient`]); `None` selects dynamic registration.
    client_id: Option<String>,
}

/// Entry point for the `login <name>` subcommand. Loads config, validates the
/// named upstream, and runs the interactive flow. Returns `Ok(())` on a
/// successful credential save; any failure is a non-zero-exit error with an
/// operator-actionable message.
pub async fn run(upstream_name: &str) -> Result<()> {
    let params = resolve_params(upstream_name)?;

    let state_dir = mcp_client::state_dir().context(
        "cannot resolve credential state directory (neither $RATO_STATE_DIR nor $HOME is set)",
    )?;
    let store = FileCredentialStore::for_upstream(&state_dir, &params.upstream_name);
    let saved_path = store.path().display().to_string();

    // Host spelling matters: providers validate the redirect against the
    // registered string byte-for-byte. DCR registers whatever we send, so
    // `127.0.0.1` is fine there. Pre-registered clients (RFC 8252 native
    // apps, e.g. the Slack MCP server's blessed apps) conventionally
    // register the `localhost` spelling, so the static flow advertises
    // `localhost` — and BINDS `localhost` too, so the callback lands even
    // on hosts where `localhost` resolves only to `::1` (advertising one
    // address while listening on another would rely on browser loopback
    // fallback behavior we don't control).
    let bind_host = if params.client_id.is_some() {
        "localhost"
    } else {
        "127.0.0.1"
    };

    // Bind the callback listener FIRST so we know the real port before building
    // the redirect URI (which must match what the provider has registered —
    // via DCR for the dynamic flow, or ahead of time for a static client_id).
    let listener = TcpListener::bind((bind_host, params.callback_port))
        .await
        .with_context(|| {
            format!(
                "failed to bind OAuth callback listener on {bind_host}:{}",
                params.callback_port
            )
        })?;
    let bound = listener
        .local_addr()
        .context("reading bound callback port")?;
    let redirect_uri = format!("http://{bind_host}:{}/callback", bound.port());

    // Build the flow: discovery + client setup (dynamic registration or
    // static client_id) + auth-URL computation all happen here.
    let flow = build_flow(&params, &redirect_uri, store).await?;

    let auth_url = flow.authorization_url().to_string();
    println!(
        "Opening your browser to authorize {} …",
        params.upstream_name
    );
    println!("If it doesn't open automatically, visit:\n\n  {auth_url}\n");
    open_in_browser(&auth_url);

    // Wait for the provider's redirect to our callback.
    let CallbackParams { code, state } = wait_for_callback(listener).await?;

    // Exchange + persist. This is the testable core.
    flow.complete(&code, &state).await?;

    println!("\n✓ Login successful. Credentials saved to:\n  {saved_path}");

    // Nudge a running gateway to hot-reload so the freshly-authorized
    // upstream connects right away — no manual reload-config / restart.
    match notify_gateway_reload() {
        ReloadNotify::Signalled(pid) => {
            println!(
                "\nSignalled the running gateway (pid {pid}) to reload — '{}' should connect \
                 within a moment. No restart needed.",
                params.upstream_name
            );
        }
        ReloadNotify::NoGatewayRunning => {
            println!(
                "\nNo running gateway detected. '{}' will connect automatically the next time \
                 the gateway starts (or call the `reload-config` tool / send SIGHUP if it's \
                 running under a client that doesn't write a PID file).",
                params.upstream_name
            );
        }
        ReloadNotify::Failed(reason) => {
            println!(
                "\nCredentials are saved, but I couldn't auto-reload the gateway ({reason}). \
                 Run the `reload-config` MCP tool, send it SIGHUP, or restart it to pick up '{}'.",
                params.upstream_name
            );
        }
    }
    Ok(())
}

/// Outcome of trying to nudge a running gateway to reload after login.
#[derive(Debug)]
enum ReloadNotify {
    /// SIGHUP was delivered to a live gateway process with this PID.
    Signalled(u32),
    /// No PID file, or the PID file pointed at a process that's no longer
    /// alive — there's nothing to signal (a clean, expected case).
    NoGatewayRunning,
    /// A PID file existed and the process was alive, but signalling failed
    /// (permissions, race). Carries a human-readable reason.
    Failed(String),
}

/// Read the gateway PID file, confirm the process is alive, and send it
/// SIGHUP to trigger a config hot-reload. Liveness is checked first (signal
/// 0) so a stale PID file from a crashed/rebooted gateway is treated as
/// "nothing running" rather than accidentally signalling an unrelated
/// process that recycled the PID — the alive check narrows but doesn't fully
/// eliminate that race, which is acceptable for a localhost dev tool.
#[cfg(unix)]
fn notify_gateway_reload() -> ReloadNotify {
    match crate::pidfile::pid_file_path() {
        Some(path) => notify_gateway_reload_at(&path),
        None => ReloadNotify::NoGatewayRunning,
    }
}

#[cfg(not(unix))]
fn notify_gateway_reload() -> ReloadNotify {
    // No SIGHUP off-Unix; the gateway picks up credentials on next start.
    ReloadNotify::NoGatewayRunning
}

/// Testable core of [`notify_gateway_reload`]: read the PID file at `path`,
/// confirm the process is alive, and send it SIGHUP.
///
/// Liveness is checked first (signal 0) so a stale PID file from a
/// crashed/rebooted gateway is treated as "nothing running" rather than
/// signalling an unrelated process that recycled the PID — the alive check
/// narrows but doesn't fully eliminate that race, which is acceptable for a
/// localhost dev tool.
#[cfg(unix)]
fn notify_gateway_reload_at(path: &std::path::Path) -> ReloadNotify {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let pid = match crate::pidfile::read_from(path) {
        Ok(Some(pid)) => pid,
        Ok(None) => return ReloadNotify::NoGatewayRunning,
        Err(e) => return ReloadNotify::Failed(format!("could not read PID file: {e}")),
    };

    let target = Pid::from_raw(pid as i32);

    // Liveness probe: signal 0 checks existence + permission without
    // delivering a signal. Err (ESRCH) → the process is gone.
    if kill(target, None).is_err() {
        return ReloadNotify::NoGatewayRunning;
    }

    match kill(target, Signal::SIGHUP) {
        Ok(()) => ReloadNotify::Signalled(pid),
        Err(e) => ReloadNotify::Failed(format!("sending SIGHUP to pid {pid} failed: {e}")),
    }
}

/// Load config and extract the OAuth params for `upstream_name`, validating
/// that the upstream exists and is `http-oauth`.
fn resolve_params(upstream_name: &str) -> Result<OAuthLoginParams> {
    let (configs, _settings, source) = crate::serve::load_upstream_configs()
        .context("loading gateway config to find the upstream to log in")?;

    let config = configs
        .iter()
        .find(|c| c.name == upstream_name)
        .with_context(|| {
            let names: Vec<&str> = configs.iter().map(|c| c.name.as_str()).collect();
            format!(
                "no upstream named {upstream_name:?} in the config ({}). \
                 Configured upstreams: {names:?}",
                source.as_log_str()
            )
        })?;

    params_from_config(config)
}

/// Pure half of [`resolve_params`]: validate one already-located upstream
/// config and extract its login params. Split out so the kind checks and
/// the clientId/callbackPort coupling rule are unit-testable without a
/// config file on disk.
fn params_from_config(config: &mcp_client::UpstreamConfig) -> Result<OAuthLoginParams> {
    let upstream_name = config.name.as_str();
    match &config.kind {
        UpstreamKind::HttpOAuth {
            url,
            scopes,
            client_name,
            callback_port,
            client_id,
        } => {
            // A pre-registered client's redirect URI must use the exact
            // port registered with the provider — an ephemeral port
            // (callback_port 0) can never match, and the failure would
            // otherwise surface as an opaque redirect_uri-mismatch page
            // from the provider AFTER the browser dance. Fail here, where
            // the fix is a one-line config edit.
            if client_id.is_some() && *callback_port == 0 {
                bail!(
                    "upstream {upstream_name:?} sets `clientId` (pre-registered OAuth \
                     client) but no `callbackPort`. A pre-registered client's redirect \
                     URI must use the port registered with the provider — set \
                     `callbackPort` to that port (the provider expects \
                     http://localhost:<port>/callback among the client's redirect URIs)."
                );
            }
            Ok(OAuthLoginParams {
                upstream_name: config.name.clone(),
                url: url.clone(),
                scopes: scopes.clone(),
                client_name: client_name
                    .clone()
                    .unwrap_or_else(|| DEFAULT_CLIENT_NAME.to_string()),
                callback_port: *callback_port,
                client_id: client_id.clone(),
            })
        }
        UpstreamKind::Http { .. } => bail!(
            "upstream {upstream_name:?} is a plain `http` upstream (static API-key headers), \
             not `http-oauth` — there's nothing to log in to. Did you mean a different upstream?"
        ),
        UpstreamKind::Stdio { .. } => bail!(
            "upstream {upstream_name:?} is a `stdio` upstream, not `http-oauth` — \
             the `login` subcommand only applies to OAuth HTTP upstreams."
        ),
    }
}

/// A ready-to-authorize login flow: the provider's metadata is discovered,
/// the OAuth client is established (dynamically registered or statically
/// configured), and the authorization URL is computed. The two variants
/// differ only in how the client came to exist and which rmcp surface
/// completes the exchange.
enum LoginFlow {
    /// Dynamic client registration (RFC 7591): rmcp's `AuthorizationSession`
    /// registered a fresh client and owns the PKCE/CSRF state.
    Dcr(AuthorizationSession),
    /// Pre-registered client (`client_id` in config): no registration call is
    /// ever made. The manager holds the PKCE/CSRF state in its state store,
    /// keyed by the CSRF token the provider echoes back on the redirect.
    StaticClient {
        manager: AuthorizationManager,
        auth_url: String,
    },
}

impl LoginFlow {
    fn authorization_url(&self) -> &str {
        match self {
            LoginFlow::Dcr(session) => session.get_authorization_url(),
            LoginFlow::StaticClient { auth_url, .. } => auth_url,
        }
    }

    /// Exchange the authorization `code` (+ CSRF `state`) for tokens and
    /// persist them through the flow's credential store.
    async fn complete(&self, code: &str, state: &str) -> Result<()> {
        match self {
            LoginFlow::Dcr(session) => complete_login(session, code, state).await,
            LoginFlow::StaticClient { manager, .. } => {
                complete_static_login(manager, code, state).await
            }
        }
    }
}

/// Construct the `AuthorizationManager` (wired to the credential store and an
/// HTTPS-capable HTTP client), discover the provider's OAuth metadata, and
/// produce a [`LoginFlow`]. With a configured `client_id` the client is set
/// statically and no registration request is made; otherwise rmcp's
/// `AuthorizationSession` performs dynamic client registration.
async fn build_flow(
    params: &OAuthLoginParams,
    redirect_uri: &str,
    store: FileCredentialStore,
) -> Result<LoginFlow> {
    let http = reqwest::Client::builder()
        .build()
        .context("building reqwest client for OAuth login")?;

    let mut manager = AuthorizationManager::new(params.url.clone())
        .await
        .with_context(|| format!("initializing OAuth manager for {}", params.url))?;
    manager.set_credential_store(store);
    manager
        .with_client(http)
        .map_err(|e| anyhow::anyhow!("wiring OAuth HTTP client: {e}"))?;

    let metadata = manager
        .discover_metadata()
        .await
        .map_err(|e| anyhow::anyhow!("OAuth metadata discovery failed: {e}"))?;
    manager.set_metadata(metadata);

    // Both rmcp entry points take `&[&str]`; hold the selected owned
    // strings and pass a borrowed view. An empty config means "discover the
    // provider's requested scopes": rmcp's selector prefers protected-resource
    // metadata (RFC 9728), then authorization-server metadata, and adds
    // `offline_access` when advertised. Passing the empty config through
    // unselected would omit `scope` entirely, which breaks providers such as
    // Fastmail whose authorization UI requires it.
    let selected_scopes = if params.scopes.is_empty() {
        manager.select_scopes(None, &[])
    } else {
        params.scopes.clone()
    };
    let scope_refs: Vec<&str> = selected_scopes.iter().map(String::as_str).collect();

    if let Some(client_id) = &params.client_id {
        // Public client + PKCE: no secret exists, and none is sent at
        // exchange time. Providers that gate on pre-registered apps
        // (e.g. Slack's MCP server) accept exactly this shape.
        //
        // Scopes ride EXCLUSIVELY on the `get_authorization_url` argument
        // below: rmcp's authorization-code path never reads
        // `OAuthClientConfig.scopes` (that field only feeds the
        // client-credentials grant), so setting it here would be inert
        // and misleading.
        let config = OAuthClientConfig::new(client_id.clone(), redirect_uri.to_string());
        manager
            .configure_client(config)
            .map_err(|e| anyhow::anyhow!("configuring pre-registered OAuth client: {e}"))?;
        let auth_url = manager
            .get_authorization_url(&scope_refs)
            .await
            .map_err(|e| anyhow::anyhow!("computing the authorization URL: {e}"))?;
        return Ok(LoginFlow::StaticClient { manager, auth_url });
    }

    AuthorizationSession::new(
        manager,
        &scope_refs,
        redirect_uri,
        Some(params.client_name.as_str()),
        None,
    )
    .await
    .map(LoginFlow::Dcr)
    .map_err(|e| anyhow::anyhow!("starting OAuth authorization (discovery/registration): {e}"))
}

/// The testable core of the DCR flow: exchange the authorization `code`
/// (+ CSRF `state`) for tokens. The session persists the resulting
/// `StoredCredentials` through the `FileCredentialStore` it was built with,
/// so success here means credentials are on disk.
///
/// Exposed (crate-public) so the integration test can drive the exchange
/// against a fake authorization server without a real browser/socket.
pub async fn complete_login(session: &AuthorizationSession, code: &str, state: &str) -> Result<()> {
    session.handle_callback(code, state).await.map_err(|e| {
        anyhow::anyhow!("exchanging the authorization code for a token failed: {e}")
    })?;
    Ok(())
}

/// The testable core of the static-client flow: exchange the authorization
/// `code` (+ CSRF `state`) for tokens on a manager whose client was
/// configured from a pre-registered `client_id`. Persistence semantics match
/// [`complete_login`]: rmcp writes `StoredCredentials` (carrying that
/// client_id, so headless refresh works) through the manager's credential
/// store before returning.
///
/// Exposed (crate-public) for the same integration-test reason as
/// [`complete_login`].
pub async fn complete_static_login(
    manager: &AuthorizationManager,
    code: &str,
    state: &str,
) -> Result<()> {
    manager
        .exchange_code_for_token(code, state)
        .await
        .map_err(|e| {
            anyhow::anyhow!("exchanging the authorization code for a token failed: {e}")
        })?;
    Ok(())
}

/// Authorization-code callback parameters parsed from the redirect request.
#[derive(Debug)]
struct CallbackParams {
    code: String,
    state: String,
}

/// Accept exactly one connection on `listener`, parse the
/// `GET /callback?code=…&state=…` request line, send the browser a small
/// success page, and return the extracted params. Bounded by
/// [`CALLBACK_TIMEOUT`].
async fn wait_for_callback(listener: TcpListener) -> Result<CallbackParams> {
    let accept = async {
        loop {
            let (mut stream, _peer) = listener.accept().await.context("accepting callback")?;

            // Read the request head (request line + headers). The auth code is
            // in the request-target query, so the first line is all we need;
            // we read a bounded buffer to avoid unbounded memory on a hostile
            // client.
            let mut buf = vec![0u8; 8192];
            let n = stream
                .read(&mut buf)
                .await
                .context("reading callback request")?;
            let request = String::from_utf8_lossy(&buf[..n]);

            let Some(target) = request_target(&request) else {
                // Not a parseable GET — respond 400 and keep listening (a
                // browser preflight or favicon probe shouldn't abort login).
                let _ = stream
                    .write_all(http_response(400, "Bad Request").as_bytes())
                    .await;
                continue;
            };

            match parse_callback_query(&target) {
                Ok(params) => {
                    let _ = stream
                        .write_all(
                            http_response(
                                200,
                                "Login complete. You can close this tab and return to the terminal.",
                            )
                            .as_bytes(),
                        )
                        .await;
                    let _ = stream.flush().await;
                    return Ok(params);
                }
                Err(e) => {
                    let _ = stream
                        .write_all(http_response(400, &format!("Login error: {e}")).as_bytes())
                        .await;
                    let _ = stream.flush().await;
                    bail!("OAuth callback did not include a valid code/state: {e}");
                }
            }
        }
    };

    tokio::time::timeout(CALLBACK_TIMEOUT, accept)
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "timed out after {}s waiting for the OAuth callback — \
                 did the browser flow complete?",
                CALLBACK_TIMEOUT.as_secs()
            )
        })?
}

/// Extract the request-target (e.g. `/callback?code=…&state=…`) from an HTTP
/// request head's first line: `GET <target> HTTP/1.1`.
fn request_target(request: &str) -> Option<String> {
    let first_line = request.lines().next()?;
    let mut parts = first_line.split_whitespace();
    let method = parts.next()?;
    if !method.eq_ignore_ascii_case("GET") {
        return None;
    }
    Some(parts.next()?.to_string())
}

/// Parse `code` and `state` out of a `/callback?code=…&state=…` target.
///
/// Pure function — the heart of the callback handling, unit-tested directly.
/// Surfaces a provider-returned `error=` param as an error so a denied consent
/// produces a clear message rather than a confusing "missing code".
fn parse_callback_query(target: &str) -> Result<CallbackParams> {
    let query = target.split_once('?').map(|(_, q)| q).unwrap_or("");

    let mut code: Option<String> = None;
    let mut state: Option<String> = None;
    let mut oauth_error: Option<String> = None;

    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        let decoded = percent_decode(value);
        match key {
            "code" => code = Some(decoded),
            "state" => state = Some(decoded),
            "error" => oauth_error = Some(decoded),
            _ => {}
        }
    }

    if let Some(err) = oauth_error {
        bail!("authorization server returned error: {err}");
    }

    match (code, state) {
        (Some(code), Some(state)) => Ok(CallbackParams { code, state }),
        (None, _) => bail!("missing `code` parameter"),
        (_, None) => bail!("missing `state` parameter"),
    }
}

/// Minimal percent-decoding for query values (handles `%XX` and `+` → space).
/// Sufficient for OAuth `code`/`state` which are URL-safe but may contain
/// percent-encoded `=`/`/` in some providers.
fn percent_decode(s: &str) -> String {
    use percent_encoding::percent_decode_str;
    let plus_to_space = s.replace('+', " ");
    percent_decode_str(&plus_to_space)
        .decode_utf8_lossy()
        .into_owned()
}

/// Build a tiny HTTP/1.1 response with a plain-text body and `Connection:
/// close` so the browser doesn't hold the socket open.
fn http_response(status: u16, body: &str) -> String {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        _ => "OK",
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        len = body.len()
    )
}

/// Best-effort open of `url` in the user's default browser via the platform
/// opener. Failure is non-fatal — we already printed the URL for manual
/// copy-paste.
fn open_in_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let opener = "open";
    #[cfg(all(unix, not(target_os = "macos")))]
    let opener = "xdg-open";

    #[cfg(unix)]
    {
        if let Err(e) = std::process::Command::new(opener).arg(url).spawn() {
            tracing::debug!(error = %e, "could not auto-open browser; user must copy the URL");
        }
    }
    #[cfg(not(unix))]
    {
        let _ = url;
        tracing::debug!("auto-open not supported on this platform; user must copy the URL");
    }
}

#[cfg(test)]
#[path = "login_tests.rs"]
mod tests;
