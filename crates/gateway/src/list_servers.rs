// pattern: Functional Core
//
// Render the list-servers output. Pure: takes a catalog slice + a
// "now" timestamp, returns a string. No I/O, no randomness, no
// implicit clock. Time is passed in so renderings are deterministic
// and testable.
//
// The catalog entry bundles the upstream config with its resolved hint
// (see `hint::resolve_hint` — config override wins over upstream
// `InitializeResult.instructions`) AND an optional health snapshot. The
// handler builds the catalog by asking each `UpstreamHandle` for its
// live `instructions` string and `health_snapshot()`, then runs the
// resolver; this module stays Functional Core by not knowing where the
// data came from.
//
// Audience note: list-servers output is agent-facing. The health
// annotation is there so the *agent* can reason about which upstreams
// to route around — operators read the gateway's log file for the
// human-side story. Format choice biases to "agent-readable" over
// "pretty printed".

use std::time::{Duration, SystemTime};

use mcp_client::{HealthSnapshot, HealthState, UpstreamConfig, UpstreamKind};

/// One catalog entry for `render`: the upstream's config, hint, and
/// optional live health snapshot. Kept as `&`-references so the
/// renderer doesn't take ownership of anything the handler still needs.
pub struct CatalogEntry<'a> {
    pub config: &'a UpstreamConfig,
    pub hint: Option<&'a str>,
    /// Live health snapshot. `None` means configured but disconnected;
    /// discovery includes entries whose initial connection has failed.
    pub health: Option<HealthSnapshot>,
    pub connection_error: Option<&'a str>,
}

impl<'a> CatalogEntry<'a> {
    pub fn new(config: &'a UpstreamConfig, hint: Option<&'a str>) -> Self {
        Self {
            config,
            hint,
            health: None,
            connection_error: None,
        }
    }

    pub fn with_connection_error(mut self, error: Option<&'a str>) -> Self {
        self.connection_error = error;
        self
    }

    pub fn with_health(mut self, health: Option<HealthSnapshot>) -> Self {
        self.health = health;
        self
    }
}

/// Render the catalog. `now` is the caller-supplied wall-clock used to
/// compute "last_ok Xs ago" stamps; passing it in keeps the function
/// pure and unit-testable.
pub fn render(entries: &[CatalogEntry<'_>], now: SystemTime) -> String {
    if entries.is_empty() {
        return "No upstream MCP servers configured.".to_string();
    }

    let mut out = String::from("Available MCP Servers:\n\n");
    // Stable output: sort by display name so the rendering is
    // deterministic. Helps property tests and makes diffs between
    // gateway versions meaningful.
    let mut sorted: Vec<&CatalogEntry<'_>> = entries.iter().collect();
    sorted.sort_by(|a, b| a.config.name.cmp(&b.config.name));

    for entry in sorted {
        let cfg = entry.config;
        let kind_label = match &cfg.kind {
            UpstreamKind::Stdio { .. } => "stdio",
            UpstreamKind::Http { .. } => "http",
            UpstreamKind::HttpOAuth { .. } => "http-oauth",
        };
        // Compose the optional health tag. Healthy upstreams render
        // nothing — the absence of a tag IS the all-clear signal, and
        // the common case stays visually quiet. Everything else gets a
        // compact `[state …]` annotation so an agent parsing the list
        // can self-route around a sick upstream.
        let health_tag = match entry.health {
            Some(h) if h.state != HealthState::Healthy => {
                format!(" {}", format_health_tag(&h, &cfg.name, now))
            }
            _ => String::new(),
        };
        out.push_str(&format!(
            "## {} (Lua: {}) [{}]{}\n",
            cfg.name, cfg.lua_name, kind_label, health_tag
        ));
        if entry.health.is_none() {
            out.push_str("   status: Disconnected — configured, not callable in execute\n");
        } else if entry.connection_error.is_some() {
            out.push_str("   status: Previous connection retained after failed replacement; live health shown above\n");
        }
        if let Some(error) = entry.connection_error {
            out.push_str(&format!("   latest connection diagnostic: {error}\n"));
        }
        if entry.health.is_none() || entry.connection_error.is_some() {
            let args = serde_json::json!({"luaServerName": cfg.lua_name});
            out.push_str(&format!("   retry: call reconnect-upstream with {args}\n"));
        }
        if let Some(hint) = entry.hint.filter(|h| !h.is_empty()) {
            out.push_str(&format!("   hint: {hint}\n"));
        }
    }
    out
}

/// Format a compact `[state ...]` tag from a health snapshot. Only
/// called for non-Healthy states; Healthy renders nothing at the call
/// site.
///
/// Format choices biased toward agent-readable: short state name,
/// then `fails=N` (the counter the supervisor cares about), then
/// `last_ok=Xago` (so the agent knows whether this is a flap or a
/// sustained outage).
///
/// `name` (the upstream's display name) is used only for the
/// `NeedsLogin` case, which renders the exact `login` command an
/// operator/agent should run — so the fix is legible inline rather than
/// buried in the gateway logs.
fn format_health_tag(h: &HealthSnapshot, name: &str, now: SystemTime) -> String {
    // NeedsLogin is special: it's not a transient outage you wait out,
    // it's a "a human must act" signal. The agent reading this catalog
    // cannot run the command itself (it opens a browser) and the human
    // never sees this output — so the framing tells the agent to RELAY
    // the command to the user, not to attempt it or reason about
    // flap-vs-outage.
    if h.state == HealthState::NeedsLogin {
        return format!(
            "[NeedsLogin — ask the user to run: rato login {name} (interactive; \
             you cannot run it yourself)]"
        );
    }

    let state = match h.state {
        HealthState::Healthy => "Healthy",
        HealthState::Suspect => "Suspect",
        HealthState::Unhealthy => "Unhealthy",
        HealthState::Respawning => "Respawning",
        HealthState::Dead => "Dead",
        // Handled above with a bespoke remediation string.
        HealthState::NeedsLogin => "NeedsLogin",
    };

    let fails_part = if h.consecutive_failures > 0 {
        format!(" fails={}", h.consecutive_failures)
    } else {
        String::new()
    };

    let last_ok_part = match h.last_success_at {
        Some(t) => {
            let ago = now.duration_since(t).unwrap_or(Duration::ZERO);
            format!(" last_ok={}", format_ago(ago))
        }
        None => " last_ok=never".to_string(),
    };

    format!("[{state}{fails_part}{last_ok_part}]")
}

/// Human-compact duration formatter. `57s`, `3m`, `2h`, `1d`. Agents
/// parse these reliably when the unit is appended as a single suffix
/// character (no spaces, no plurals, lowercase).
fn format_ago(d: Duration) -> String {
    let s = d.as_secs();
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m", s / 60)
    } else if s < 86_400 {
        format!("{}h", s / 3600)
    } else {
        format!("{}d", s / 86_400)
    }
}

#[cfg(test)]
#[path = "list_servers_tests.rs"]
mod tests;
