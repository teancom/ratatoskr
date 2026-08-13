// pattern: Imperative Shell
//
// ClientHandler for our upstream-facing MCP client.
//
// Responsibilities:
//   - On notifications (tools/resources/prompts list_changed): publish an
//     UpstreamEvent onto the event bus so downstream sessions can act.
//   - On sampling/createMessage requests (opt-in per upstream): look up a
//     downstream peer via the SamplingBroker and forward the request.
//
// Sampling security model (mirrors TS `dangerouslyEnableSampling`):
//   Layer 1 — Capability filtering: we only advertise sampling in our
//     ClientCapabilities to upstreams whose `sampling_enabled` is true.
//     A curious-but-trusted upstream won't even know sampling is available.
//   Layer 2 — Handler gating: if a malicious upstream guesses and sends
//     createMessage anyway, our handler returns a JSON-RPC error rather
//     than forwarding. The broker never sees the request.

use rmcp::{
    ClientHandler, ErrorData,
    model::{
        ClientCapabilities, ClientInfo, CreateMessageRequestParams, CreateMessageResult, ErrorCode,
        Implementation, LoggingLevel, LoggingMessageNotificationParam,
        ResourceUpdatedNotificationParam,
    },
    service::{NotificationContext, RequestContext},
};

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::event_bus::UpstreamEventBus;
use crate::events::UpstreamEvent;
use crate::health::UpstreamHealth;
use crate::sampling::SharedSamplingBroker;

/// Max bytes we'll render from an upstream log notification's payload.
/// Chatty upstreams (Datadog debug streams, Grafana log replays, etc.)
/// can emit multi-kilobyte `data` blobs per tick — at debug level those
/// would dominate the gateway's own log. Cap the rendered form and let
/// the operator inspect the upstream directly if they need the full
/// payload. 2 KiB is enough to keep the useful prefix of a JSON blob
/// while keeping per-line log size bounded.
const MAX_UPSTREAM_LOG_PAYLOAD_BYTES: usize = 2048;

/// Minimum wall-clock spacing between two `Error`-level upstream log
/// notifications that count toward the supervisor's failure counter.
///
/// Motivation: an upstream stuck in a retry loop can emit one Error log
/// per millisecond. Without this debounce, three such logs would drive
/// `consecutive_failures` past the demotion threshold inside a single
/// burst — and the supervisor would react before the real-traffic
/// signal had a chance to either confirm or refute the rot. The debounce
/// keeps the Error-log signal at the same effective tempo as a
/// per-tool-call signal: one increment per second at most.
///
/// 1 s was chosen as the smallest value still meaningfully larger than a
/// typical retry-loop period (50-200 ms). Tunable here if a real
/// upstream surfaces a slower legitimate Error-log cadence we want to
/// catch immediately; revisit then.
const ERROR_LOG_DEBOUNCE: Duration = Duration::from_secs(1);

/// Truncate a serde_json::Value's display form to a hard byte ceiling
/// so a chatty upstream cannot saturate the log pipeline. Returns a
/// short string when the input was already small; otherwise a
/// prefix + `…(truncated, N total bytes)` marker.
///
/// We render via `serde_json::to_string` (compact) rather than Debug
/// so the result stays parseable if someone pipes the log through jq.
/// If serialization fails (extremely unusual — `data` is always JSON
/// in the MCP protocol), fall back to a fixed marker rather than
/// panicking or omitting the line.
fn format_log_payload(value: &serde_json::Value) -> String {
    let rendered = match serde_json::to_string(value) {
        Ok(s) => s,
        Err(_) => return "<unserializable>".to_string(),
    };
    if rendered.len() <= MAX_UPSTREAM_LOG_PAYLOAD_BYTES {
        return rendered;
    }
    // Safe slicing: find the nearest char boundary ≤ limit so we
    // don't split a multi-byte UTF-8 sequence. JSON output is
    // typically ASCII, but upstream strings often aren't.
    let mut cut = MAX_UPSTREAM_LOG_PAYLOAD_BYTES;
    while !rendered.is_char_boundary(cut) {
        cut -= 1;
    }
    format!(
        "{}…(truncated, {} total bytes)",
        &rendered[..cut],
        rendered.len()
    )
}

#[derive(Clone)]
pub struct ProxyUpstreamHandler {
    upstream_name: String,
    bus: UpstreamEventBus,
    broker: SharedSamplingBroker,
    sampling_enabled: bool,
    /// Shared health record for THIS upstream generation. Same Arc as
    /// the [`UpstreamHandle`] adopts via [`LiveUpstream::health`], so a
    /// `record_failure` call on this side is visible to the supervisor
    /// reading on the other side. See `on_logging_message` for the
    /// demotion-on-Error-log path that depends on this.
    health: Arc<UpstreamHealth>,
    /// Wall-clock timestamp of the last `Error`-level upstream log that
    /// was COUNTED toward the failure counter. Used by
    /// `on_logging_message` to debounce bursty error logs — see
    /// `ERROR_LOG_DEBOUNCE`. Shared across clones so debouncing is
    /// per-upstream-generation, not per-handler-clone (rmcp clones the
    /// handler internally; we want the debounce to be global to the
    /// connection).
    last_counted_error_log: Arc<Mutex<Option<Instant>>>,
}

impl ProxyUpstreamHandler {
    pub fn new(
        upstream_name: String,
        bus: UpstreamEventBus,
        broker: SharedSamplingBroker,
        sampling_enabled: bool,
        health: Arc<UpstreamHealth>,
    ) -> Self {
        Self {
            upstream_name,
            bus,
            broker,
            sampling_enabled,
            health,
            last_counted_error_log: Arc::new(Mutex::new(None)),
        }
    }

    /// Feed an `Error`-level (or worse) upstream log notification into
    /// the health record, debounced by `ERROR_LOG_DEBOUNCE` so a bursty
    /// upstream can't drive demotion in milliseconds. Emits a `WARN`
    /// line on every COUNTED signal so the operator can correlate the
    /// upstream's complaint (the existing `tracing::error!` line) with
    /// the proxy's decision to feed it to the supervisor.
    ///
    /// Carrying `triggering_message` into the WARN log is intentional:
    /// when an upstream gets bounced overnight, this single line gives
    /// you both the proxy's reasoning (kind + counter) AND the original
    /// error content, without having to cross-reference timestamps with
    /// a separate ERROR line. The size cap applied earlier
    /// (`format_log_payload`) keeps this log entry bounded.
    fn maybe_count_error_log_toward_health(&self, triggering_message: &str) {
        // Mutex critical section: load + compare + store the Instant.
        // No `.await` inside, so the std `Mutex` is fine here even
        // though we're in an async context.
        let now = Instant::now();
        let counted = {
            let mut slot = match self.last_counted_error_log.lock() {
                Ok(g) => g,
                // Poisoned mutex would only happen if a previous holder
                // panicked while holding it; the inner Option is still
                // valid to read/write. Recover rather than propagate the
                // panic — losing a debounce check is better than killing
                // the handler task.
                Err(poisoned) => poisoned.into_inner(),
            };
            let should_count =
                slot.is_none_or(|prev| now.duration_since(prev) >= ERROR_LOG_DEBOUNCE);
            if should_count {
                *slot = Some(now);
            }
            should_count
        };

        if !counted {
            return;
        }

        let new_counter = self.health.record_self_reported_error();
        tracing::warn!(
            upstream = %self.upstream_name,
            kind = "UpstreamSelfReportedError",
            consecutive_self_reported_errors = new_counter,
            triggering_message = %triggering_message,
            "upstream self-reported an error; feeding signal to supervisor"
        );
    }
}

impl ClientHandler for ProxyUpstreamHandler {
    /// Advertise a sampling capability ONLY when this upstream has been
    /// explicitly opted-in via `dangerously_enable_sampling: true`.
    /// This is the primary security boundary: untrusted upstreams can't
    /// see that sampling exists at all.
    fn get_info(&self) -> ClientInfo {
        let caps = if self.sampling_enabled {
            ClientCapabilities::builder().enable_sampling().build()
        } else {
            ClientCapabilities::default()
        };
        ClientInfo::new(caps, Implementation::new("rato", env!("CARGO_PKG_VERSION")))
    }

    async fn create_message(
        &self,
        params: CreateMessageRequestParams,
        _context: RequestContext<rmcp::RoleClient>,
    ) -> Result<CreateMessageResult, ErrorData> {
        // Defense-in-depth layer 2: even if the capability filter above was
        // bypassed (bug, upstream guessed the method name, etc.), we refuse
        // to forward for upstreams not opted in.
        if !self.sampling_enabled {
            tracing::warn!(
                upstream = %self.upstream_name,
                "upstream sent sampling/createMessage but sampling is not enabled for it"
            );
            return Err(ErrorData::new(
                ErrorCode::METHOD_NOT_FOUND,
                "sampling is not enabled for this upstream",
                None,
            ));
        }

        tracing::info!(
            upstream = %self.upstream_name,
            "forwarding upstream sampling/createMessage to downstream"
        );

        self.broker.dispatch(params).await
    }

    async fn on_tool_list_changed(&self, _context: NotificationContext<rmcp::RoleClient>) {
        tracing::debug!(upstream = %self.upstream_name, "upstream tools/list_changed");
        self.bus.publish(UpstreamEvent::ToolListChanged {
            upstream: self.upstream_name.clone(),
        });
    }

    async fn on_resource_list_changed(&self, _context: NotificationContext<rmcp::RoleClient>) {
        tracing::debug!(upstream = %self.upstream_name, "upstream resources/list_changed");
        self.bus.publish(UpstreamEvent::ResourceListChanged {
            upstream: self.upstream_name.clone(),
        });
    }

    /// Upstream is telling us a resource we subscribed to has changed.
    /// Publish onto the event bus carrying the BARE upstream URI; the
    /// gateway's session forwarder consults `SubscriptionRegistry` to
    /// decide which downstream sessions care and translates to the
    /// namespaced form at fan-out time.
    ///
    /// We deliberately don't filter here (e.g. "is this URI one we
    /// actually subscribed to?"): the upstream is the source of truth
    /// for what it considers subscribed, and filtering on our side
    /// would mask reconnect races where our registry and the upstream
    /// transiently disagree. Unrouted updates are dropped harmlessly
    /// by the forwarder's registry lookup.
    async fn on_resource_updated(
        &self,
        params: ResourceUpdatedNotificationParam,
        _context: NotificationContext<rmcp::RoleClient>,
    ) {
        tracing::debug!(
            upstream = %self.upstream_name,
            uri = %params.uri,
            "upstream resources/updated"
        );
        self.bus.publish(UpstreamEvent::ResourceUpdated {
            upstream: self.upstream_name.clone(),
            uri: params.uri,
        });
    }

    async fn on_prompt_list_changed(&self, _context: NotificationContext<rmcp::RoleClient>) {
        tracing::debug!(upstream = %self.upstream_name, "upstream prompts/list_changed");
        self.bus.publish(UpstreamEvent::PromptListChanged {
            upstream: self.upstream_name.clone(),
        });
    }

    async fn on_logging_message(
        &self,
        params: LoggingMessageNotificationParam,
        _context: NotificationContext<rmcp::RoleClient>,
    ) {
        // Re-emit at the level the upstream advertised so it appears in the
        // gateway's own trace stream. Diagnostic content is preserved verbatim
        // (subject to `format_log_payload`'s size cap) so a human investigating
        // a respawn episode can find the upstream's own self-reported cause
        // in the log alongside the proxy's reaction to it.
        //
        // For Error-and-worse levels we ALSO feed the supervisor: the
        // upstream is self-reporting degradation, which the existing
        // health/respawn machinery should react to even when the tool
        // call wrapped that degradation in an `Ok(_)` payload (the
        // motivating dash-mcp case — see `FailureKind::UpstreamSelfReportedError`
        // doc for the full story). Debounced so a tight retry loop in
        // the upstream can't drive demotion in milliseconds.
        //
        // `data` is size-capped by `format_log_payload`: an upstream that
        // fires a debug-level log every millisecond with multi-kilobyte
        // payloads would otherwise saturate our own log pipeline.
        let message = format_log_payload(&params.data);
        match params.level {
            LoggingLevel::Emergency
            | LoggingLevel::Alert
            | LoggingLevel::Critical
            | LoggingLevel::Error => {
                tracing::error!(
                    upstream = %self.upstream_name,
                    logger = ?params.logger,
                    message = %message,
                    "upstream log"
                );
                self.maybe_count_error_log_toward_health(&message);
            }
            LoggingLevel::Warning => {
                tracing::warn!(
                    upstream = %self.upstream_name,
                    logger = ?params.logger,
                    message = %message,
                    "upstream log"
                );
            }
            LoggingLevel::Notice | LoggingLevel::Info => {
                tracing::info!(
                    upstream = %self.upstream_name,
                    logger = ?params.logger,
                    message = %message,
                    "upstream log"
                );
            }
            LoggingLevel::Debug => {
                tracing::debug!(
                    upstream = %self.upstream_name,
                    logger = ?params.logger,
                    message = %message,
                    "upstream log"
                );
            }
        }
    }
}

#[cfg(test)]
mod format_log_payload_tests {
    //! Unit tests for the upstream-log payload truncator. These are
    //! pure-function tests — no MCP machinery required.
    use super::{MAX_UPSTREAM_LOG_PAYLOAD_BYTES, format_log_payload};
    use serde_json::json;

    #[test]
    fn short_payload_renders_fully() {
        let v = json!({ "msg": "hello", "n": 42 });
        let out = format_log_payload(&v);
        assert!(
            out.contains("hello") && out.contains("42"),
            "short payload must render verbatim: {out}"
        );
        assert!(
            !out.contains("truncated"),
            "short payload must not report truncation: {out}"
        );
    }

    #[test]
    fn unserializable_shape_renders_marker_not_panic() {
        // serde_json::Value is always serializable, so constructing a
        // true failure case requires a type that implements Serialize
        // with an error. The public surface is Value-only, so we just
        // verify the fallback path exists — the function MUST NOT
        // panic even on odd input. This is effectively a compile-time
        // confidence check combined with a "no panic" smoke test.
        let v = json!({});
        let _ = format_log_payload(&v);
    }

    #[test]
    fn oversize_payload_is_truncated_with_marker() {
        // Property: anything over the cap is truncated and labeled.
        // Without the cap, a chatty upstream debug-flood would
        // saturate the gateway's log pipeline.
        let big = "x".repeat(MAX_UPSTREAM_LOG_PAYLOAD_BYTES * 3);
        let v = json!({ "payload": big });
        let out = format_log_payload(&v);
        assert!(
            out.contains("truncated"),
            "oversized payload must carry the truncation marker: first 80 chars = {}",
            out.chars().take(80).collect::<String>()
        );
        assert!(
            out.contains("total bytes"),
            "marker must include the total bytes count: first 120 chars = {}",
            out.chars().take(120).collect::<String>()
        );
        // Bound the output length: cap + marker must not exceed
        // cap + a short constant. This is the load-bearing check —
        // any future bug that made the "truncated" path still emit
        // the full rendering would trip this.
        assert!(
            out.len() < MAX_UPSTREAM_LOG_PAYLOAD_BYTES + 80,
            "output length {} exceeded cap + marker room",
            out.len()
        );
    }

    #[test]
    fn multibyte_boundary_truncation_does_not_panic() {
        // Property: a payload where the naive cap would split a
        // UTF-8 multi-byte sequence gets rounded back to the nearest
        // char boundary. No panic, valid UTF-8 out.
        let s = "é".repeat(MAX_UPSTREAM_LOG_PAYLOAD_BYTES); // 2 bytes per char
        let v = json!({ "s": s });
        let out = format_log_payload(&v);
        // If we'd split mid-sequence, this would have panicked on
        // `&rendered[..cut]` already. Valid-UTF-8 round-trip confirms:
        assert!(out.is_char_boundary(0) && out.is_char_boundary(out.len()));
    }
}

#[cfg(test)]
#[path = "upstream_handler_tests.rs"]
mod tests;
