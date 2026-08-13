// pattern: Imperative Shell
//
// Best-effort, human-facing notification that an OAuth upstream needs
// interactive re-authentication.
//
// ## Why this exists (and why it's the ONLY direct line to the human)
//
// The gateway's tool output — `list-servers`, `server-details`, the
// `OAuthReauthRequired` error — is all *agent-facing*: it crosses the wire
// to the LLM driving the proxy, never to the human at the keyboard. But the
// human is the only party who can actually fix a dead OAuth token: `login`
// opens a browser and needs their credentials. The agent can relay the
// command in its chat response, but that depends on the agent's goodwill and
// the human reading that turn.
//
// A native desktop notification is the one channel that reaches the operator
// directly, independent of whether the agent relays anything. We assume the
// gateway runs on the same machine as the user's session (the documented
// single-user deployment), so a local `osascript` alert lands in front of
// them.
//
// ## Anti-spam: this fires on the EDGE, never the level
//
// A token that just died makes EVERY subsequent `call_tool` against that
// upstream fail with an auth error, and a retrying agent can generate those
// in a tight loop. Firing a notification per failure would bury the user in
// identical alerts. The dedup lives at the call site: this is invoked ONLY
// when `UpstreamHealth::enter_needs_login()` reports the Healthy→NeedsLogin
// *edge* (a race-free CAS), so exactly one alert fires per incident. A
// recovery followed by a fresh loss of auth is a new edge → a new alert,
// which is correct (it's a new incident the user must act on).
//
// ## Best-effort
//
// Notification delivery is never load-bearing: failure to spawn `osascript`
// (not on PATH, no GUI session, sandboxed) degrades silently to a debug log.
// The authoritative signals remain the health state and the agent-facing
// error; this is a convenience layer on top.

/// Fire a one-shot desktop notification telling the operator that `name`
/// needs interactive re-authentication. Best-effort and non-blocking: spawns
/// the platform notifier detached and returns immediately, ignoring failures.
///
/// Call this ONLY on the NeedsLogin edge (see module docs) — typically
/// `if health.enter_needs_login() { reauth_notify::notify(name); }`.
pub fn notify(name: &str) {
    notify_impl(name);
}

#[cfg(target_os = "macos")]
fn notify_impl(name: &str) {
    // `display notification` is the dependency-free way to raise a native
    // banner from a CLI process. We pass the upstream name and the exact
    // command as the body so the alert is self-contained — the user can act
    // without hunting for the syntax.
    //
    // The strings are embedded in an AppleScript literal, so we must guard
    // against an upstream name containing a double-quote or backslash
    // breaking out of the literal (a config-controlled value, not hostile,
    // but correctness matters). `sanitize_for_applescript` strips the two
    // metacharacters that matter; upstream names are display strings, so the
    // loss is cosmetic in the pathological case.
    let safe_name = sanitize_for_applescript(name);
    let body = format!("Run in your terminal:  rato login {safe_name}",);
    let title = "ratatoskr: re-authentication needed";
    let script = format!(
        "display notification \"{body}\" with title \"{title}\"",
        body = sanitize_for_applescript(&body),
        title = title,
    );

    match std::process::Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(_child) => {
            // Detached: we deliberately do NOT wait. The child is a
            // fire-and-forget UI poke; reaping it would block, and a
            // lingering zombie for a sub-second process is acceptable for a
            // notification that fires at most once per re-auth incident.
            tracing::debug!(upstream = %name, "fired re-auth desktop notification");
        }
        Err(e) => {
            tracing::debug!(
                upstream = %name,
                error = %e,
                "could not fire re-auth desktop notification (osascript unavailable); \
                 the agent-facing NeedsLogin signal still applies"
            );
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn notify_impl(name: &str) {
    // No native notifier wired for non-macOS targets. The agent-facing
    // signal (NeedsLogin state + OAuthReauthRequired error) is unchanged;
    // this is purely the human-convenience layer.
    tracing::debug!(
        upstream = %name,
        "re-auth needed; desktop notification not supported on this platform"
    );
}

/// Strip the two metacharacters (`"` and `\`) that would let a value break
/// out of an AppleScript string literal. Upstream names are operator-authored
/// display strings, so this is defensive correctness, not untrusted-input
/// hardening; in the common case it changes nothing.
#[cfg(target_os = "macos")]
fn sanitize_for_applescript(s: &str) -> String {
    s.chars().filter(|c| *c != '"' && *c != '\\').collect()
}

#[cfg(test)]
#[path = "reauth_notify_tests.rs"]
mod tests;
