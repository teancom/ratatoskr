// pattern: Imperative Shell
//
// Establishes upstream MCP connections (stdio child processes, HTTP endpoints)
// and wraps them in a uniform `LiveUpstream` handle.
//
// Critical responsibilities:
// - Spawn child processes in their own process group so SIGTERM to us kills
//   the whole subtree (npx -> node -> real-server chain). Without this, killing
//   npx leaves node and its descendants orphaned — which is the exact class
//   of bug we're rewriting to avoid.
// - Own the rmcp service handle; drop-order guarantees the child is killed
//   when we go away.

use std::collections::HashMap;
use std::time::Duration;

use http::{HeaderName, HeaderValue};
use process_wrap::tokio::{CommandWrap, ProcessGroup};
use rmcp::{
    ServiceExt,
    service::{DynService, RoleClient, RunningService},
    transport::{
        StreamableHttpClientTransport, TokioChildProcess,
        auth::{AuthClient, AuthorizationManager},
        streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use tokio::process::Command;
use tracing::{debug, info};

/// Maximum time allowed for the MCP initialize handshake to complete.
///
/// 30 s is generous enough for `npx -y` cold-starts (which have to download
/// the package on first run) but still provides a bounded failure window if an
/// upstream process spawns but never speaks MCP. Without this, a single hung
/// upstream makes the entire gateway startup hang forever.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

use std::sync::Arc;

use crate::container_runtime::{container_name_for, detect_runtime_command, rewrite_args};
use crate::container_runtime_probe::{ReapOutcome, reap_container};
use crate::credential_store::{FileCredentialStore, state_dir};
use crate::event_bus::UpstreamEventBus;
use crate::health::UpstreamHealth;
use crate::sampling::SharedSamplingBroker;
use crate::upstream::{UpstreamConfig, UpstreamError, UpstreamKind};
use crate::upstream_handler::ProxyUpstreamHandler;

/// One live connection, kind-agnostic at the surface level. Internally keeps
/// two variants because the service type parameter differs between transports.
///
/// Each variant carries the `Arc<UpstreamHealth>` that the attached
/// [`ProxyUpstreamHandler`] holds, so the health record observed by the
/// handler (which sees `notifications/message` and records side-channel
/// signals like `FailureKind::UpstreamSelfReportedError`) is the same
/// record [`UpstreamHandle::new`] adopts. Without that shared identity,
/// an Error log emitted by the upstream would bump a *handler-local*
/// counter that the supervisor never reads. See `upstream_handler.rs`
/// for the demotion path that depends on this.
pub enum LiveUpstream {
    Stdio {
        service: RunningService<RoleClient, Box<dyn DynService<RoleClient>>>,
        pid: Option<u32>,
        health: Arc<UpstreamHealth>,
    },
    Http {
        service: RunningService<RoleClient, Box<dyn DynService<RoleClient>>>,
        health: Arc<UpstreamHealth>,
    },
}

impl LiveUpstream {
    pub fn child_pid(&self) -> Option<u32> {
        match self {
            LiveUpstream::Stdio { pid, .. } => *pid,
            LiveUpstream::Http { .. } => None,
        }
    }

    pub fn peer(&self) -> &rmcp::service::Peer<RoleClient> {
        match self {
            LiveUpstream::Stdio { service, .. } | LiveUpstream::Http { service, .. } => {
                service.peer()
            }
        }
    }

    /// The health record shared with the attached `ProxyUpstreamHandler`.
    /// `UpstreamHandle::new` adopts this so a single record is observed
    /// by everyone — handler writes, handle writes, supervisor reads.
    pub fn health(&self) -> Arc<UpstreamHealth> {
        match self {
            LiveUpstream::Stdio { health, .. } | LiveUpstream::Http { health, .. } => {
                Arc::clone(health)
            }
        }
    }

    pub async fn close(self) -> anyhow::Result<()> {
        match self {
            LiveUpstream::Stdio { service, .. } | LiveUpstream::Http { service, .. } => {
                service
                    .cancel()
                    .await
                    .map_err(|e| anyhow::anyhow!("cancel failed: {e}"))?;
            }
        }
        Ok(())
    }
}

/// Connect an upstream using an already-built transport. Intended for tests
/// that want to stand up an in-memory fake upstream (e.g. via
/// `tokio::io::duplex`). Production code paths use `connect()`.
pub async fn connect_with_transport<R, W>(
    config: &UpstreamConfig,
    bus: &UpstreamEventBus,
    broker: &SharedSamplingBroker,
    read: R,
    write: W,
) -> Result<LiveUpstream, UpstreamError>
where
    R: tokio::io::AsyncRead + Send + Unpin + 'static,
    W: tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let health = Arc::new(UpstreamHealth::new());
    let handler = ProxyUpstreamHandler::new(
        config.name.clone(),
        bus.clone(),
        broker.clone(),
        config.dangerously_enable_sampling,
        Arc::clone(&health),
    );
    let transport =
        rmcp::transport::async_rw::AsyncRwTransport::<RoleClient, _, _>::new_client(read, write);
    let service = tokio::time::timeout(HANDSHAKE_TIMEOUT, handler.into_dyn().serve(transport))
        .await
        .map_err(|_| UpstreamError::Connect {
            name: config.name.clone(),
            source: anyhow::anyhow!(
                "MCP handshake timed out after {}s",
                HANDSHAKE_TIMEOUT.as_secs()
            ),
        })?
        .map_err(|e| UpstreamError::Connect {
            name: config.name.clone(),
            source: anyhow::anyhow!("{e}"),
        })?;
    // Tests don't check PID on this path; model as Http (no PID).
    Ok(LiveUpstream::Http { service, health })
}

/// Convert the domain-level string-pair header map into the http-typed map
/// that rmcp's streamable-http transport wants. Fails fast on invalid
/// header names or values so we surface "bad config" as a clean connection
/// error rather than a mystery ReservedHeaderConflict deep in the worker.
///
/// Note on `Authorization`: users write the full `"Authorization": "Bearer <t>"`
/// header directly in their config. That header is not on rmcp's
/// reserved list, so it flows through here unchanged. We deliberately
/// don't special-case it into the `auth_header` config slot —
/// round-tripping the user's literal JSON is simpler and less
/// surprising.
fn convert_headers(
    raw: &HashMap<String, String>,
) -> Result<HashMap<HeaderName, HeaderValue>, anyhow::Error> {
    let mut out = HashMap::with_capacity(raw.len());
    for (name, value) in raw {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| anyhow::anyhow!("invalid header name {name:?}: {e}"))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|e| anyhow::anyhow!("invalid header value for {name:?}: {e}"))?;
        out.insert(header_name, header_value);
    }
    Ok(out)
}

/// Connect to a single upstream per its configured kind, attaching a
/// `ProxyUpstreamHandler` so list-changed notifications are forwarded onto
/// the shared event bus.
pub async fn connect(
    config: &UpstreamConfig,
    bus: &UpstreamEventBus,
    broker: &SharedSamplingBroker,
) -> Result<LiveUpstream, UpstreamError> {
    let health = Arc::new(UpstreamHealth::new());
    let handler = ProxyUpstreamHandler::new(
        config.name.clone(),
        bus.clone(),
        broker.clone(),
        config.dangerously_enable_sampling,
        Arc::clone(&health),
    );

    match &config.kind {
        UpstreamKind::Stdio { command, args, env } => {
            info!(name = %config.name, command = %command, "spawning stdio upstream");

            // Container-runtime integration: if this config is a
            // `podman run …` or `docker run …` shape, rewrite the
            // invocation so it pins a deterministic name
            // (`rato-<key>`). Reap any leftover container from a
            // prior gateway run before spawning — belt-and-suspenders
            // on top of `--rm`. See
            // `crate::container_runtime` module header for the full
            // motivation.
            let (effective_command, effective_args) =
                prepare_container_invocation(config, command, args).await?;
            let mut cmd = Command::new(&effective_command);
            cmd.args(&effective_args).envs(env);

            let mut wrapped = CommandWrap::from(cmd);
            wrapped.wrap(ProcessGroup::leader());

            let transport =
                TokioChildProcess::new(wrapped).map_err(|source| UpstreamError::StdioSpawn {
                    name: config.name.clone(),
                    source,
                })?;

            let pid = transport.id();

            let service = serve_and_handshake(handler, transport, &config.name).await?;

            info!(
                name = %config.name,
                pid = ?pid,
                server_info = ?service.peer_info().map(|i| i.server_info.name.clone()),
                "stdio upstream connected"
            );

            Ok(LiveUpstream::Stdio {
                service,
                pid,
                health,
            })
        }

        UpstreamKind::Http { url, headers } => {
            info!(
                name = %config.name,
                url = %url,
                header_count = headers.len(),
                "connecting http upstream"
            );

            let custom_headers =
                convert_headers(headers).map_err(|source| UpstreamError::Connect {
                    name: config.name.clone(),
                    source,
                })?;

            // Use the builder chain because StreamableHttpClientTransportConfig
            // is #[non_exhaustive] (a recurring rmcp pitfall; see the
            // non_exhaustive house rule atop gateway's handler/mod.rs).
            let transport_config = StreamableHttpClientTransportConfig::with_uri(url.clone())
                .custom_headers(custom_headers);
            let transport = StreamableHttpClientTransport::from_config(transport_config);

            let service = serve_and_handshake(handler, transport, &config.name).await?;

            info!(name = %config.name, "http upstream connected");

            Ok(LiveUpstream::Http { service, health })
        }

        UpstreamKind::HttpOAuth { url, .. } => {
            // Headless ONLY. The interactive browser flow (and the
            // `scopes`/`client_name`/`callback_port` fields it consumes)
            // belongs to the `login` subcommand; here we exclusively *load*
            // credentials that a prior `login` persisted. This function
            // must never open a browser or bind a callback port — it runs
            // at boot, on reload, and under the supervisor's respawn loop,
            // none of which have a human present.
            info!(name = %config.name, url = %url, "connecting http-oauth upstream (headless)");

            let dir = state_dir().ok_or_else(|| UpstreamError::Connect {
                name: config.name.clone(),
                source: anyhow::anyhow!(
                    "cannot resolve credential state directory \
                     (neither $RATO_STATE_DIR nor $HOME is set)"
                ),
            })?;
            let live = connect_oauth_headless(handler, config, url, &dir, health).await?;
            info!(name = %config.name, "http-oauth upstream connected");
            Ok(live)
        }
    }
}

/// Headless OAuth connect: load persisted credentials, build an
/// auto-refreshing [`AuthClient`] transport, and complete the MCP handshake.
///
/// The credential gate ([`AuthorizationManager::initialize_from_store`])
/// returns `false` when no `login` has ever run for this upstream. In that
/// case we fail **immediately** with [`UpstreamError::OAuthLoginRequired`]
/// — before constructing any transport or touching the network — so:
///   - concurrent boot isn't wedged waiting on a 30 s handshake, and
///   - the supervisor sees an actionable-precondition error it can park in
///     `Dead` rather than spinning fast respawns.
///
/// When credentials *are* present, `AuthClient` injects the bearer on every
/// request and refreshes it silently (re-persisting through the same
/// `FileCredentialStore`). If the refresh token itself is dead, the failure
/// surfaces during the handshake as a `Connect` error and the upstream is
/// demoted normally — re-running `login` is the fix.
async fn connect_oauth_headless(
    handler: ProxyUpstreamHandler,
    config: &UpstreamConfig,
    url: &str,
    state_dir: &std::path::Path,
    health: Arc<UpstreamHealth>,
) -> Result<LiveUpstream, UpstreamError> {
    let connect_err = |source: anyhow::Error| UpstreamError::Connect {
        name: config.name.clone(),
        source,
    };

    let store = FileCredentialStore::for_upstream(state_dir, &config.name);

    // The HTTP client rmcp's auth layer drives. A single client is shared
    // between the AuthorizationManager (token endpoint calls) and the
    // AuthClient transport wrapper (MCP requests) — cheap to clone, and
    // sharing the connection pool is the intended usage.
    let http = reqwest::Client::builder()
        .build()
        .map_err(|e| connect_err(anyhow::anyhow!("failed to build reqwest client: {e}")))?;

    let mut manager = AuthorizationManager::new(url.to_string())
        .await
        .map_err(|e| connect_err(anyhow::anyhow!("OAuth manager init failed: {e}")))?;
    manager.set_credential_store(store);
    manager
        .with_client(http.clone())
        .map_err(|e| connect_err(anyhow::anyhow!("OAuth client wiring failed: {e}")))?;

    // The headless gate: did `login` ever persist credentials for this
    // upstream? `initialize_from_store` short-circuits to `Ok(false)` on a
    // missing credential file *before* any discovery/network call, so this
    // is a fast, offline check.
    let have_credentials = manager
        .initialize_from_store()
        .await
        .map_err(|e| connect_err(anyhow::anyhow!("loading persisted credentials failed: {e}")))?;
    if !have_credentials {
        return Err(UpstreamError::OAuthLoginRequired {
            name: config.name.clone(),
        });
    }

    let auth_client = AuthClient::new(http, manager);
    let transport_config = StreamableHttpClientTransportConfig::with_uri(url.to_string());
    let transport = StreamableHttpClientTransport::with_client(auth_client, transport_config);

    // Credentials exist on disk but may be unusable: the access token is
    // expired AND the refresh token is dead/revoked. `initialize_from_store`
    // can't catch this (it only checks the file is present, not that the
    // tokens still work) — the failure surfaces here during the handshake,
    // when rmcp's AuthClient tries to refresh and the provider rejects it.
    //
    // Reclassify such an auth failure from the generic `Connect` it would
    // otherwise be into the actionable `OAuthReauthRequired`. That makes the
    // bootstrap-retry loop log "needs operator action" instead of the
    // misleading "connect failed; will retry", and gives the supervisor /
    // catalog an actionable-precondition signal. A non-auth handshake
    // failure (network, TLS, upstream down) stays `Connect` and retries
    // normally.
    match serve_and_handshake(handler, transport, &config.name).await {
        Ok(service) => Ok(LiveUpstream::Http { service, health }),
        Err(UpstreamError::Connect { name, source })
            if crate::upstream::is_auth_failure(source.as_ref()) =>
        {
            tracing::warn!(
                name = %name,
                error = %source,
                "http-oauth connect failed authentication (token expired/revoked); \
                 operator must re-run `login`"
            );
            Err(UpstreamError::OAuthReauthRequired { name })
        }
        Err(other) => Err(other),
    }
}

/// Drive the MCP `initialize` handshake for a freshly-built transport,
/// bounded by [`HANDSHAKE_TIMEOUT`], and return the running service.
///
/// Factored out of the per-kind arms because the timeout-wrap +
/// double-`map_err` shape is identical for stdio, plain HTTP, and
/// OAuth-wrapped HTTP. Generic over the transport type: rmcp's
/// `handler.into_dyn().serve(transport)` accepts any `IntoTransport`, and
/// every transport erases to the same `RunningService<RoleClient, Box<dyn
/// DynService<RoleClient>>>` — which is exactly why the OAuth path (whose
/// concrete transport is `StreamableHttpClientTransport<AuthClient<_>>`,
/// distinct from the plain `StreamableHttpClientTransport<_>`) can share
/// the `LiveUpstream::Http` variant.
///
/// `name` is used only for error context. A handshake timeout and a
/// handshake error both surface as [`UpstreamError::Connect`] — the same
/// shape callers already handle for connect failures.
async fn serve_and_handshake<T, E, A>(
    handler: ProxyUpstreamHandler,
    transport: T,
    name: &str,
) -> Result<RunningService<RoleClient, Box<dyn DynService<RoleClient>>>, UpstreamError>
where
    T: rmcp::transport::IntoTransport<RoleClient, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    tokio::time::timeout(HANDSHAKE_TIMEOUT, handler.into_dyn().serve(transport))
        .await
        .map_err(|_| UpstreamError::Connect {
            name: name.to_string(),
            source: anyhow::anyhow!(
                "MCP handshake timed out after {}s",
                HANDSHAKE_TIMEOUT.as_secs()
            ),
        })?
        .map_err(|e| UpstreamError::Connect {
            name: name.to_string(),
            source: anyhow::anyhow!("{e}"),
        })
}

/// Shell glue for container-runtime integration. Given a stdio
/// upstream's configured `command` and `args`, produce the
/// `(command, args)` that actually get spawned.
///
/// If the config isn't a containerized shape (e.g. `command: "npx"`),
/// returns the inputs unchanged — this is the passthrough case.
///
/// For `podman run` / `docker run` configs:
/// 1. Derive the deterministic container name from `config.name` (the
///    top-level config key, e.g. `"GitLab"` → `"rato-GitLab"`).
/// 2. Reap any leftover container with that name from a prior gateway
///    run (best-effort, 3 s timeout).
/// 3. Rewrite the args to include `--name <derived>` right after `run`.
///
/// Errors are surfaced as [`UpstreamError::Connect`] — a bad config
/// key that produces an invalid container name is a config-time
/// problem but can also reach this path if someone bypassed the
/// validation step. See
/// `gateway::serve::validate_container_upstream_keys` for the Layer-1
/// catch that normally fires at boot.
async fn prepare_container_invocation(
    config: &UpstreamConfig,
    command: &str,
    args: &[String],
) -> Result<(String, Vec<String>), UpstreamError> {
    let Some(configured_kind) = detect_runtime_command(config) else {
        return Ok((command.to_string(), args.to_vec()));
    };

    let container_name =
        container_name_for(&config.name).map_err(|source| UpstreamError::Connect {
            name: config.name.clone(),
            source: anyhow::anyhow!(
                "cannot derive container name for upstream {:?}: {source}",
                config.name
            ),
        })?;

    // Best-effort reap. Outcome is logged by `reap_container` itself;
    // we log a summary here so the spawn-time story is visible in
    // tracing output for debugging a wedged container.
    let outcome = reap_container(configured_kind, &container_name).await;
    match &outcome {
        ReapOutcome::Removed => info!(
            name = %config.name,
            container = %container_name,
            "reaped leftover container before respawn"
        ),
        ReapOutcome::NotFound => debug!(
            name = %config.name,
            container = %container_name,
            "no prior container present; clean spawn"
        ),
        ReapOutcome::RuntimeError(stderr) => info!(
            name = %config.name,
            container = %container_name,
            stderr = %stderr,
            "reaper got non-zero exit; proceeding with spawn anyway"
        ),
        ReapOutcome::TimedOut => info!(
            name = %config.name,
            container = %container_name,
            "reaper timed out; proceeding with spawn (will likely fail fast)"
        ),
        ReapOutcome::SpawnFailed(err) => info!(
            name = %config.name,
            container = %container_name,
            error = %err,
            "reaper couldn't launch runtime CLI; proceeding with spawn"
        ),
    }

    let rewritten = rewrite_args(args, &container_name);
    // The configured command is spawned verbatim (an operator who pins
    // an absolute path like /usr/local/bin/podman keeps that path);
    // only the args are rewritten.
    Ok((command.to_string(), rewritten))
}

#[cfg(test)]
#[path = "connect_tests.rs"]
mod tests;
