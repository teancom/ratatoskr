// pattern: Functional Core
//
// Upstream event types. Pure data — no I/O, no time, no randomness. The
// broadcast channel that carries these lives in the shell (event_bus.rs).

/// A notification observed from some upstream MCP server that the gateway
/// may want to forward to downstream sessions.
///
/// Carrying the upstream name lets downstream handlers decide whether they
/// care. The list-changed variants fan out to every session (downstream
/// clients see ALL upstream list-changed events rolled up into a single
/// gateway-level list_changed). `ResourceUpdated` is selective: it carries
/// the upstream-native URI so the subscription registry can answer "does
/// any downstream session care about THIS update?" without asking the
/// upstream again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpstreamEvent {
    ToolListChanged {
        upstream: String,
    },
    ResourceListChanged {
        upstream: String,
    },
    /// An upstream that we've subscribed to has published
    /// `notifications/resources/updated` for `uri`. `uri` is the bare
    /// upstream URI (NOT the gateway's namespaced form) — the gateway
    /// translates to `mcp-proxy://<upstream>/<encoded-uri>` at
    /// fan-out time when it knows which subscribers care.
    ResourceUpdated {
        upstream: String,
        uri: String,
    },
    PromptListChanged {
        upstream: String,
    },
}

impl UpstreamEvent {
    pub fn upstream_name(&self) -> &str {
        match self {
            UpstreamEvent::ToolListChanged { upstream }
            | UpstreamEvent::ResourceListChanged { upstream }
            | UpstreamEvent::ResourceUpdated { upstream, .. }
            | UpstreamEvent::PromptListChanged { upstream } => upstream,
        }
    }
}

#[cfg(test)]
#[path = "events_tests.rs"]
mod tests;
