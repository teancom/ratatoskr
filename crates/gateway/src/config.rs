// pattern: Functional Core
//
// JSON config parser for the gateway. Pure: input is a string, output is a
// `Vec<UpstreamConfig>` or a typed error. File I/O lives in `serve::run`.
//
// The config file is JSON: an `mcpClients` map (object keyed by server
// name, each entry being either HTTP or stdio) plus a handful of
// gateway-wide settings (executeDeadlineMs, resultSizeThreshold, port,
// supervision, ...). We parse only the fields the gateway actually wires
// through today.
//
// Other recognized-but-unused fields (host, transport, dashboard, logging,
// acp, skills, per-client `allowedTools`) are tolerated but ignored —
// unknown fields pass through serde's default. When we wire one of those
// features, we extend the DTO then, not speculatively.
//
// Two-pass parse per client: first into `serde_json::Value` to peel off the
// `type` tag, then into the typed transport shape. That's slightly more
// verbose than a single `#[serde(tag = "type")]` enum, but (a) it lets us
// produce a clean "unknown transport type 'websocket'" error instead of
// serde's generic "unknown variant" message, and (b) `#[serde(other)]` on
// tagged enums requires a unit variant, so we couldn't capture the bad tag
// value in the typed path even if we wanted to.

use std::collections::HashMap;

use lua_runtime::{partition_unique_by_sanitized, sanitize_lua_identifier};
use mcp_client::{
    PartialSupervisionSettings, SupervisionError, SupervisionSettings, UpstreamConfig, UpstreamKind,
};
use serde::Deserialize;
use serde_json::{Number, Value};

/// Errors produced by parsing the config file contents.
///
/// Messages are lowercase sentence fragments so they compose naturally with
/// a "failed to load config: ..." prefix applied at the shell boundary.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid json: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("mcp client {name:?}: missing required field {field:?}")]
    MissingField { name: String, field: &'static str },

    #[error("mcp client {name:?}: invalid transport type {got:?} (expected \"http\" or \"stdio\")")]
    InvalidTransport { name: String, got: String },

    #[error("mcp client {name:?}: {message}")]
    InvalidField { name: String, message: String },

    #[error("mcp client name cannot be empty")]
    EmptyName,

    #[error("resultSizeThreshold must be a non-negative integer, got {got}")]
    InvalidResultSizeThreshold { got: i64 },

    #[error("resultSizeThreshold must be an integer, got {got}")]
    NonIntegerResultSizeThreshold { got: String },

    #[error("executeDeadlineMs must be a positive integer, got {got}")]
    InvalidExecuteDeadline { got: i64 },

    #[error("executeDeadlineMs must be an integer, got {got}")]
    NonIntegerExecuteDeadline { got: String },

    #[error("port must be an integer in 1..=65535, got {got}")]
    InvalidPort { got: i64 },

    #[error("port must be an integer, got {got}")]
    NonIntegerPort { got: String },

    #[error(
        "executeDeadlineMs must be greater than zero; a zero deadline causes \
         every execute call to time out before it can make progress"
    )]
    ZeroExecuteDeadline,

    #[error(
        "mcp client names {names:?} sanitize to the same Lua global {lua_name:?}; \
         rename one of them so each upstream gets a distinct Lua identifier"
    )]
    LuaNameCollision {
        names: Vec<String>,
        lua_name: String,
    },

    /// The top-level `upstreamSupervision` block failed cross-field
    /// validation (per-field validation happens at serde parse time;
    /// this is the struct-level check — e.g. ping timeout >= interval).
    #[error("upstreamSupervision is invalid: {0}")]
    InvalidSupervision(#[source] SupervisionError),

    /// The per-upstream `supervision` block failed cross-field
    /// validation after merging with the gateway-wide defaults.
    /// Named separately so the error message can name the offending
    /// upstream.
    #[error("mcp client {name:?}: supervision overrides invalid: {source}")]
    InvalidUpstreamSupervision {
        name: String,
        #[source]
        source: SupervisionError,
    },
}

/// Default byte threshold for large-result offloading. When a Lua
/// `execute` result's JSON serialization is larger than this, the
/// gateway stores the result and returns a summary + execution ID
/// instead of the full payload. Set to `0` in config to disable
/// the feature.
pub const DEFAULT_RESULT_SIZE_THRESHOLD: usize = 50_000;

/// Default per-execute wall-clock deadline, in milliseconds. Mirrors
/// [`lua_runtime::runtime::DEFAULT_EXECUTE_DEADLINE`] (60 s); kept in
/// ms here so the config-file shape is a plain JSON integer rather
/// than a `Duration`-flavoured string.
///
/// Production code should prefer constructing a [`std::time::Duration`]
/// from [`GatewaySettings::execute_deadline_ms`] at consumption time
/// rather than hard-coding this constant — the whole point of threading
/// the setting through is that operators can override the default.
pub const DEFAULT_EXECUTE_DEADLINE_MS: u64 = 60_000;

/// Default TCP port the gateway listens on when the config file omits
/// `port`.
pub const DEFAULT_PORT: u16 = 8910;

/// Gateway-wide settings read from the config file. Separate from
/// per-upstream config because these tune the gateway's own behavior
/// rather than any particular MCP client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewaySettings {
    /// Byte threshold above which `execute` results are offloaded.
    /// `0` disables the feature; anything above is the ceiling in
    /// bytes of the serialized JSON form.
    pub result_size_threshold: usize,

    /// Wall-clock deadline for a single `execute` call, in
    /// milliseconds. Plumbed into
    /// [`lua_runtime::LuaRuntime::with_execute_deadline`] at boot. The
    /// runtime's default is 60 s, which is generous for every
    /// legitimate upstream call path we've measured; bump this only
    /// when a specific class of upstream (e.g. Datadog deep queries,
    /// Glean cold searches) legitimately needs longer. Must be > 0;
    /// a zero value is rejected at parse time because it would cause
    /// every execute call to time out before it could make progress.
    pub execute_deadline_ms: u64,

    /// Gateway-wide supervisor settings. Each upstream resolves its
    /// effective settings as
    /// `SupervisionSettings::default().merge_partial(gateway-wide-partial).merge_partial(per-upstream-partial)`.
    /// Stored as the fully-resolved form here so consumers (the
    /// supervisor task, `UpstreamHandle::new_with_call_timeout`) don't
    /// have to re-run the merge.
    pub supervision: SupervisionSettings,

    /// TCP port the gateway binds on 127.0.0.1. Like every other
    /// gateway-wide setting this is consumed once at boot
    /// (`serve::run` builds the listener from it), so changing it
    /// requires a `restart-gateway`, not a `reload-config`.
    pub port: u16,
}

impl Default for GatewaySettings {
    fn default() -> Self {
        Self {
            result_size_threshold: DEFAULT_RESULT_SIZE_THRESHOLD,
            execute_deadline_ms: DEFAULT_EXECUTE_DEADLINE_MS,
            supervision: SupervisionSettings::default(),
            port: DEFAULT_PORT,
        }
    }
}

impl GatewaySettings {
    /// Resolve the effective supervision settings for one upstream:
    /// gateway-wide baseline overlaid by per-upstream overrides.
    /// Validates the merged result; any cross-field violation
    /// surfaces with the upstream's name.
    pub fn effective_supervision_for(
        &self,
        upstream: &UpstreamConfig,
    ) -> Result<SupervisionSettings, ConfigError> {
        let merged = if let Some(per) = &upstream.supervision {
            self.supervision.merge_partial(per)
        } else {
            self.supervision
        };
        merged
            .validate()
            .map_err(|source| ConfigError::InvalidUpstreamSupervision {
                name: upstream.name.clone(),
                source,
            })?;
        Ok(merged)
    }
}

/// Fully-parsed config: settings + upstreams. `parse_full` is the
/// canonical entry point; `parse` is a convenience that drops settings
/// for callers that only need the upstream list (today that's just the
/// legacy tests; production uses `parse_full`).
#[derive(Debug, Clone)]
pub struct ParsedConfig {
    pub settings: GatewaySettings,
    pub upstreams: Vec<UpstreamConfig>,
}

/// Parse a JSON config document into settings + upstream list.
///
/// Pure function: no file I/O, no env lookup, no clock. Unknown top-level
/// and per-client fields are tolerated for forward-compat with the TS
/// gateway's superset shape.
pub fn parse_full(json: &str) -> Result<ParsedConfig, ConfigError> {
    let raw: RawConfigFile = serde_json::from_str(json)?;

    let settings = gateway_settings_from_raw(&raw)?;

    let clients = raw.mcp_clients.unwrap_or_default();
    let mut upstreams = Vec::with_capacity(clients.len());

    for (name, value) in clients {
        upstreams.push(value_into_config(name, value)?);
    }

    // JSON object iteration order isn't guaranteed by the spec, and we
    // don't enable serde_json's `preserve_order` feature. Sorting by
    // display name gives deterministic logging and consistent Lua-global
    // injection order regardless of file layout.
    upstreams.sort_by(|a, b| a.name.cmp(&b.name));

    // Two upstream names that sanitize to the same Lua identifier
    // (e.g. "my-server" and "my.server" → "my_server") would otherwise
    // silently overwrite each other at Lua-global injection time,
    // leaving the agent seeing only one upstream with no diagnostic.
    // Catch it here so the operator gets a visible error at startup.
    let display_names: Vec<&str> = upstreams.iter().map(|u| u.name.as_str()).collect();
    let (_unique, collisions) = partition_unique_by_sanitized(&display_names);
    if let Some((lua_name, mut names)) = collisions.into_iter().next() {
        // Deterministic ordering for the error message regardless of
        // the underlying HashMap iteration.
        names.sort();
        return Err(ConfigError::LuaNameCollision { names, lua_name });
    }

    Ok(ParsedConfig {
        settings,
        upstreams,
    })
}

/// Convert the raw file's top-level options into a `GatewaySettings`,
/// applying defaults for anything absent.
fn gateway_settings_from_raw(raw: &RawConfigFile) -> Result<GatewaySettings, ConfigError> {
    let mut settings = GatewaySettings::default();
    if let Some(threshold) = raw.result_size_threshold.as_ref() {
        // Reject JSON floats outright: `1.5` serde-truncates to `1` via
        // `as_i64`, which silently turns a fat-fingered `50.0` (meaning
        // `50000`) into a 50-byte threshold. A user who writes a float
        // in the config wants an error, not a surprise.
        let Some(as_i64) = threshold.as_i64() else {
            return Err(ConfigError::NonIntegerResultSizeThreshold {
                got: threshold.to_string(),
            });
        };
        if as_i64 < 0 {
            return Err(ConfigError::InvalidResultSizeThreshold { got: as_i64 });
        }
        // Clamp the upper side at usize::MAX; on a 64-bit host that's
        // effectively unbounded for any realistic config value.
        settings.result_size_threshold = usize::try_from(as_i64).unwrap_or(usize::MAX);
    }
    if let Some(deadline) = raw.execute_deadline_ms.as_ref() {
        // Same float-rejection discipline as `result_size_threshold`:
        // `60000.0` is almost certainly a fat-fingered `60000` but we
        // won't guess — emit a clear error so the operator fixes the
        // config rather than wondering why their 60-second deadline
        // is firing sooner than expected.
        let Some(as_i64) = deadline.as_i64() else {
            return Err(ConfigError::NonIntegerExecuteDeadline {
                got: deadline.to_string(),
            });
        };
        if as_i64 <= 0 {
            // Split negative vs zero into distinct error variants so
            // the operator gets a message specific to the mistake:
            // negatives are "positive integer expected," zero is "this
            // value makes no sense under the semantics." Both are
            // user-facing at startup.
            if as_i64 == 0 {
                return Err(ConfigError::ZeroExecuteDeadline);
            }
            return Err(ConfigError::InvalidExecuteDeadline { got: as_i64 });
        }
        // Positive i64 always fits in u64. The cast is infallible
        // here; using `unwrap_or(u64::MAX)` would be defensive overkill
        // for a branch that's already bounded above by i64::MAX.
        settings.execute_deadline_ms = as_i64 as u64;
    }
    if let Some(port) = raw.port.as_ref() {
        // Same float-rejection discipline as the other numeric knobs:
        // `8910.0` is almost certainly a fat-fingered `8910`, but a
        // silent truncation would be worse than an error the operator
        // can fix in ten seconds.
        let Some(as_i64) = port.as_i64() else {
            return Err(ConfigError::NonIntegerPort {
                got: port.to_string(),
            });
        };
        // 0 is rejected too: "bind an ephemeral port" is useless for a
        // daemon whose clients are configured with a fixed URL.
        let Ok(as_u16) = u16::try_from(as_i64) else {
            return Err(ConfigError::InvalidPort { got: as_i64 });
        };
        if as_u16 == 0 {
            return Err(ConfigError::InvalidPort { got: as_i64 });
        }
        settings.port = as_u16;
    }
    if let Some(partial) = raw.upstream_supervision.as_ref() {
        // Layer the gateway-wide partial onto the built-in defaults;
        // per-upstream overrides will layer on top of this later when
        // each upstream resolves via `effective_supervision_for`.
        let merged = SupervisionSettings::default().merge_partial(partial);
        merged.validate().map_err(ConfigError::InvalidSupervision)?;
        settings.supervision = merged;
    }
    Ok(settings)
}

/// Convert a single raw-Value client entry to the domain `UpstreamConfig`.
fn value_into_config(name: String, value: Value) -> Result<UpstreamConfig, ConfigError> {
    if name.is_empty() {
        return Err(ConfigError::EmptyName);
    }

    // Peel off the `type` tag first so the typed path for HTTP vs stdio
    // can assume a known transport.
    let transport_type = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or(ConfigError::MissingField {
            name: name.clone(),
            field: "type",
        })?
        .to_string();

    let kind = match transport_type.as_str() {
        "stdio" => {
            let stdio: RawStdioClient =
                serde_json::from_value(value.clone()).map_err(|e| ConfigError::InvalidField {
                    name: name.clone(),
                    message: e.to_string(),
                })?;
            let command = stdio.command.ok_or(ConfigError::MissingField {
                name: name.clone(),
                field: "command",
            })?;
            UpstreamKind::Stdio {
                command,
                args: stdio.args.unwrap_or_default(),
                env: stdio.env.unwrap_or_default(),
            }
        }
        "http" => {
            let http: RawHttpClient =
                serde_json::from_value(value.clone()).map_err(|e| ConfigError::InvalidField {
                    name: name.clone(),
                    message: e.to_string(),
                })?;
            let url = http.url.ok_or(ConfigError::MissingField {
                name: name.clone(),
                field: "url",
            })?;
            if url.trim().is_empty() {
                return Err(ConfigError::InvalidField {
                    name: name.clone(),
                    message: "url must not be empty".to_string(),
                });
            }
            UpstreamKind::Http {
                url,
                headers: http.headers.unwrap_or_default(),
            }
        }
        "http-oauth" => {
            let oauth: RawHttpOAuthClient =
                serde_json::from_value(value.clone()).map_err(|e| ConfigError::InvalidField {
                    name: name.clone(),
                    message: e.to_string(),
                })?;
            let url = oauth.url.ok_or(ConfigError::MissingField {
                name: name.clone(),
                field: "url",
            })?;
            if url.trim().is_empty() {
                return Err(ConfigError::InvalidField {
                    name: name.clone(),
                    message: "url must not be empty".to_string(),
                });
            }
            // An empty clientId would silently select the DCR-less flow
            // with a client id no provider accepts. The author either
            // meant to paste a real id or to omit the field (DCR flow) —
            // both are one-line fixes, so reject rather than guess.
            if let Some(id) = &oauth.client_id
                && id.trim().is_empty()
            {
                return Err(ConfigError::InvalidField {
                    name: name.clone(),
                    message: "clientId must not be empty; omit the field entirely \
                              to use dynamic client registration"
                        .to_string(),
                });
            }
            UpstreamKind::HttpOAuth {
                url,
                scopes: oauth.scopes.unwrap_or_default(),
                client_name: oauth.client_name,
                callback_port: oauth.callback_port.unwrap_or(0),
                client_id: oauth.client_id,
            }
        }
        other => {
            return Err(ConfigError::InvalidTransport {
                name,
                got: other.to_string(),
            });
        }
    };

    // Shared fields across both transports. Extracted via a small DTO so
    // adding another shared-per-client field (e.g. `allowedTools` once
    // we wire it) is a one-line change here.
    let shared: RawSharedClient =
        serde_json::from_value(value).map_err(|e| ConfigError::InvalidField {
            name: name.clone(),
            message: e.to_string(),
        })?;

    let lua_name = sanitize_lua_identifier(&name);
    let mut cfg = UpstreamConfig::new(name, lua_name, kind);
    if shared.dangerously_enable_sampling.unwrap_or(false) {
        cfg = cfg.with_sampling_enabled();
    }
    // Trim the author-authored hint at parse time so both the rendering
    // code AND any diagnostic that echoes `cfg.hint` back see the same
    // post-trim value. Empty-after-trim drops to `None` so the
    // fallback (upstream-advertised instructions) kicks in — matches
    // the "empty override falls through" semantics pinned by
    // `hint::resolve_hint`'s tests.
    if let Some(raw_hint) = shared.hint {
        let trimmed = raw_hint.trim();
        if !trimmed.is_empty() {
            cfg = cfg.with_hint(trimmed.to_string());
        }
    }
    // Carry the per-upstream supervision overrides as-is into the
    // domain config. Cross-field validation happens later at the
    // merge step in `GatewaySettings::effective_supervision_for`, so
    // a per-upstream block that's internally inconsistent produces
    // an error that names the offending upstream (rather than a
    // generic "upstreamSupervision is invalid").
    if let Some(sup) = shared.supervision {
        cfg = cfg.with_supervision(sup);
    }
    Ok(cfg)
}

// -----------------------------------------------------------------------------
// Raw DTOs. Private to the parser; exist only to mirror JSON shape.

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawConfigFile {
    /// `Option` so `{"mcpClients": {}}` and a bare `{}` both produce zero
    /// upstreams rather than a hard error.
    mcp_clients: Option<HashMap<String, Value>>,

    /// Byte threshold for large-result offloading. `None` means "use
    /// the built-in default" (DEFAULT_RESULT_SIZE_THRESHOLD). `Some(0)`
    /// explicitly disables the feature. Deserialized as
    /// `serde_json::Number` rather than `i64` so the validator can
    /// distinguish "not an integer" (e.g. `1.5`) from "negative
    /// integer" (e.g. `-1`). An `i64` field would silently truncate
    /// a float to its integer part at deserialize time, turning
    /// `50.0` into a 50-byte threshold.
    result_size_threshold: Option<Number>,

    /// Per-execute wall-clock deadline, in milliseconds. `None`
    /// means "use the built-in default"
    /// (`DEFAULT_EXECUTE_DEADLINE_MS`, 60 s). Any explicit value
    /// must be > 0; zero is a user error (every call would time
    /// out immediately) and rejected at parse. Same
    /// float-rejection discipline as `result_size_threshold` for
    /// the same reason — a typo shouldn't silently truncate to a
    /// very-short deadline.
    execute_deadline_ms: Option<Number>,

    /// Gateway-wide supervisor overrides. Each present field overlays
    /// the built-in default; absent fields keep the default. See
    /// `mcp_client::supervision` for the full knob surface and
    /// per-field semantics. Per-upstream overrides (`supervision`
    /// on each mcp-client entry) layer on top of this.
    upstream_supervision: Option<PartialSupervisionSettings>,

    /// TCP port to listen on. `None` means [`DEFAULT_PORT`]. Held as
    /// `Number` for the same float-rejection reason as the other
    /// numeric knobs.
    port: Option<Number>,
    // All other top-level fields (host, transport, dashboard,
    // database, logging, skills, acp) are silently ignored by serde's
    // default.
}

#[derive(Debug, Deserialize)]
struct RawStdioClient {
    command: Option<String>,
    args: Option<Vec<String>>,
    env: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct RawHttpClient {
    url: Option<String>,
    headers: Option<HashMap<String, String>>,
}

/// Raw shape of a `"type": "http-oauth"` client entry. Mirrors the JSON
/// surface of [`UpstreamKind::HttpOAuth`]. Deliberately has NO `headers`
/// field: the `Authorization` bearer is owned by the OAuth layer, and an
/// author who puts `headers` on an oauth client has almost certainly
/// confused it with a `"type": "http"` API-key upstream — serde's default
/// ignores the unknown key, but the absence here documents the intent.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawHttpOAuthClient {
    url: Option<String>,
    scopes: Option<Vec<String>>,
    client_name: Option<String>,
    callback_port: Option<u16>,
    /// Pre-registered client_id for providers without dynamic client
    /// registration (see `UpstreamKind::HttpOAuth::client_id`).
    client_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawSharedClient {
    #[serde(rename = "dangerouslyEnableSampling")]
    dangerously_enable_sampling: Option<bool>,
    /// Optional one-line catalog hint. Surfaced to agents in
    /// `list-servers` output so they can see why to pick a given
    /// upstream without drilling into its tool list first. See
    /// `UpstreamConfig::hint` and `gateway::hint::resolve_hint`
    /// for the priority order (override wins over upstream-advertised
    /// `InitializeResult.instructions`).
    hint: Option<String>,
    /// Optional per-upstream supervisor overrides. Layered atop the
    /// gateway-wide `upstreamSupervision` defaults at resolve time
    /// (`GatewaySettings::effective_supervision_for`). Each present
    /// field wins over the gateway-wide value; absent fields fall
    /// through. See `mcp_client::supervision`.
    supervision: Option<PartialSupervisionSettings>,
    // `allowedTools` is accepted silently (serde ignores unknown) so TS
    // configs parse today. When we wire per-upstream tool filtering we'll
    // add a field here and propagate it into `UpstreamConfig`.
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
