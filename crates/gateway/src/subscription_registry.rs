// pattern: Imperative Shell
//
// SubscriptionRegistry: the routing table for `resources/subscribe`.
//
// Shape of the problem
// --------------------
// Downstream sessions subscribe to `mcp-proxy://<upstream>/<encoded-uri>`.
// We decode that into `(upstream, bare_uri)` at the handler edge and hand
// it to the registry, keyed by the MCP session ID.
//
// Ref-counting exists because many downstream sessions can end up caring
// about the same `(upstream, bare_uri)` pair, and each upstream pair
// should correspond to exactly one live `resources/subscribe` upstream
// call — five sessions watching Rootly's `incident://42` should produce
// one upstream subscribe, not five. Mirror on unsubscribe: only the
// LAST session's removal tears down the upstream subscription.
//
// Why `HashSet<SessionId>` and not `usize` counter
// ------------------------------------------------
// A session that registers twice for the same URI should be idempotent
// (second call is a no-op, not a ref bump). A raw counter can't
// represent that — set semantics can. It also means session-drop
// cleanup is "walk the set, remove entries with this ID, emit the keys
// that emptied", which is the right shape for the teardown path.
//
// Two indices live side-by-side: concrete subscriptions (bare URI →
// session set) and template subscriptions (compiled matcher + template
// string → session set). They're kept decoupled because the upstream
// RPCs are shape-specific (a concrete unsubscribe carries the bare URI;
// a template unsubscribe carries the original template string), and a
// merged enum key would push the dispatch cost to every caller.
//
// The template matcher itself lives in `mcp-client::template_matcher`
// — it's RFC 6570 machinery with no gateway-specific state, so the
// dependency direction is gateway → mcp-client just like the codec.
//
// Template-fan-out performance ceiling
// ------------------------------------
// `template_sessions_for` linear-scans the `templates: Vec` on every
// incoming `ResourceUpdated`. Measured under release on Apple Silicon
// (rustc 1.95.0) with 100 entries (10 upstreams × 10 distinct templates)
// and a worst-case event stream that matches one template per event:
// p50 ≈ 1.0 µs, p99 ≈ 1.25 µs per call, mean ≈ 0.95 µs. That's ~800× below
// the 1 ms/event bar considered "worth sharding." The measured
// ceiling is documented here so the next reader doesn't re-run the
// decision; if real deployments ever push template counts into the
// thousands, or if p99 ever climbs past ~100 µs in a repeat measurement,
// re-run `cargo run --release --example bench_template_fanout` and
// consider sharding the Vec by upstream (`HashMap<upstream, Vec<...>>`).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use mcp_client::template_matcher::CompiledTemplate;

/// Key for the concrete-subscription index: (upstream name, bare URI).
///
/// "Bare" means the upstream-native URI (e.g. `incident://42`), not the
/// gateway's `mcp-proxy://` namespaced form. The handler decodes before
/// calling the registry.
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct UpstreamSubKey {
    pub upstream: String,
    pub uri: String,
}

impl UpstreamSubKey {
    pub fn new(upstream: impl Into<String>, uri: impl Into<String>) -> Self {
        Self {
            upstream: upstream.into(),
            uri: uri.into(),
        }
    }
}

/// Key for the template-subscription index: (upstream name, template
/// string). The template string is kept verbatim because upstream
/// `resources/subscribe` is issued against it unchanged — a Rootly
/// sub on `incident://{id}` tells Rootly "give me every incident
/// update", and rewriting the template on the wire would change
/// that contract.
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct UpstreamTemplateKey {
    pub upstream: String,
    pub template: String,
}

impl UpstreamTemplateKey {
    pub fn new(upstream: impl Into<String>, template: impl Into<String>) -> Self {
        Self {
            upstream: upstream.into(),
            template: template.into(),
        }
    }
}

/// What the handler should do upstream AFTER a `subscribe` call returns.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SubscribeDelta {
    /// The registry had no prior subscribers for this key; the handler
    /// MUST call `upstream.subscribe(...)` now. Returning this also
    /// means the session-URI pair was freshly added.
    FirstSubscriber,
    /// Other sessions are already subscribed to this key upstream, so
    /// no upstream call is needed. The session is now tracked.
    AdditionalSubscriber,
    /// This session was already subscribed to this key. Idempotent
    /// no-op; no upstream call, no state change.
    AlreadySubscribed,
}

/// What the handler should do upstream AFTER an `unsubscribe` call.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum UnsubscribeDelta {
    /// The session was the last subscriber; the handler MUST call
    /// `upstream.unsubscribe(...)` now.
    LastSubscriberGone,
    /// Other sessions are still subscribed; no upstream call needed.
    OtherSubscribersRemain,
    /// The session wasn't subscribed to this key. Idempotent no-op.
    NotSubscribed,
}

/// Registry state, behind a single mutex.
///
/// A Mutex is the right primitive here: the operations are short
/// (hash lookup, small set mutation, sometimes a Vec append), there's
/// no read-heavy workload to favor an RwLock, and holding the lock
/// across a tracing log line is fine. If this ever becomes a hot path
/// we can split by upstream or move to a sharded map, but the baseline
/// is "one mutex for the whole table."
#[derive(Default)]
pub struct SubscriptionRegistry {
    inner: Mutex<RegistryState>,
}

#[derive(Default)]
struct RegistryState {
    /// Concrete subscriptions keyed by (upstream, bare uri).
    /// The set stores session IDs. Invariant: no empty sets — remove
    /// the whole entry when the set would empty.
    concrete: HashMap<UpstreamSubKey, HashSet<String>>,
    /// Template subscriptions. We keep a `Vec` rather than a `HashMap`
    /// because matching at notification time is linear-scan anyway
    /// (we have to consult every template per upstream), and `Vec`
    /// avoids the duplicate-compile problem where two sessions
    /// subscribe to the same template and we'd need to dedupe the
    /// `CompiledTemplate` across map keys. Each entry stores the
    /// compiled matcher plus the session set.
    ///
    /// Invariant: at most one entry per `(upstream, template_str)`.
    /// Lookup is linear but the vec is always small (per-upstream
    /// templates are rare in real deployments — Rootly ships 2).
    templates: Vec<TemplateEntry>,
}

struct TemplateEntry {
    key: UpstreamTemplateKey,
    compiled: CompiledTemplate,
    sessions: HashSet<String>,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `session_id` has subscribed to `key`. Returns a
    /// `SubscribeDelta` telling the caller whether an upstream call is
    /// required, and whether this was idempotent.
    ///
    /// The caller is responsible for issuing the upstream
    /// `subscribe` call when `FirstSubscriber` comes back — the registry
    /// deliberately doesn't own the upstream handle. Keeping it pure
    /// state means test doubles and unit tests stay trivial.
    pub fn add_concrete(&self, session_id: &str, key: UpstreamSubKey) -> SubscribeDelta {
        let mut state = self.lock();
        let entry = state.concrete.entry(key).or_default();
        let was_empty = entry.is_empty();
        let inserted = entry.insert(session_id.to_string());
        match (was_empty, inserted) {
            (true, true) => SubscribeDelta::FirstSubscriber,
            (false, true) => SubscribeDelta::AdditionalSubscriber,
            (_, false) => SubscribeDelta::AlreadySubscribed,
        }
    }

    /// Record that `session_id` has unsubscribed from `key`. Returns an
    /// `UnsubscribeDelta` telling the caller whether the upstream
    /// subscription should be torn down.
    pub fn remove_concrete(&self, session_id: &str, key: &UpstreamSubKey) -> UnsubscribeDelta {
        let mut state = self.lock();
        let Some(entry) = state.concrete.get_mut(key) else {
            return UnsubscribeDelta::NotSubscribed;
        };
        if !entry.remove(session_id) {
            // Set existed but didn't contain this session — idempotent
            // no-op. Don't touch the set; other sessions still care.
            return UnsubscribeDelta::NotSubscribed;
        }
        if entry.is_empty() {
            state.concrete.remove(key);
            UnsubscribeDelta::LastSubscriberGone
        } else {
            UnsubscribeDelta::OtherSubscribersRemain
        }
    }

    /// Record a template subscription. The caller compiles the template
    /// up front (so compile errors surface as `invalid_params` before
    /// we reach the registry) and passes the compiled form along with
    /// the raw string for upstream propagation.
    ///
    /// Delta semantics mirror `add_concrete`: `FirstSubscriber` means
    /// the caller must issue `upstream.subscribe` against the template
    /// string; `AdditionalSubscriber` means piggyback on the existing
    /// upstream sub; `AlreadySubscribed` is idempotent.
    pub fn add_template(
        &self,
        session_id: &str,
        key: UpstreamTemplateKey,
        compiled: CompiledTemplate,
    ) -> SubscribeDelta {
        let mut state = self.lock();
        if let Some(entry) = state.templates.iter_mut().find(|e| e.key == key) {
            // Existing `TemplateEntry`s always have a non-empty session set
            // (the invariant at line 126 is mirrored here: we remove the
            // entry from the Vec the moment its sessions empty). So the
            // only positive outcomes are `AdditionalSubscriber` or
            // `AlreadySubscribed` — no representable path to
            // `FirstSubscriber` from this branch.
            return if entry.sessions.insert(session_id.to_string()) {
                SubscribeDelta::AdditionalSubscriber
            } else {
                SubscribeDelta::AlreadySubscribed
            };
        }
        let mut sessions = HashSet::new();
        sessions.insert(session_id.to_string());
        state.templates.push(TemplateEntry {
            key,
            compiled,
            sessions,
        });
        SubscribeDelta::FirstSubscriber
    }

    /// Remove a session's template subscription.
    pub fn remove_template(&self, session_id: &str, key: &UpstreamTemplateKey) -> UnsubscribeDelta {
        let mut state = self.lock();
        let Some(idx) = state.templates.iter().position(|e| &e.key == key) else {
            return UnsubscribeDelta::NotSubscribed;
        };
        let entry = &mut state.templates[idx];
        if !entry.sessions.remove(session_id) {
            return UnsubscribeDelta::NotSubscribed;
        }
        if entry.sessions.is_empty() {
            state.templates.remove(idx);
            UnsubscribeDelta::LastSubscriberGone
        } else {
            UnsubscribeDelta::OtherSubscribersRemain
        }
    }

    /// Find every session that has a TEMPLATE subscription on `upstream`
    /// whose matcher accepts `uri`.
    ///
    /// Called by the session forwarder at update-fan-out time, alongside
    /// the concrete-index lookup. A session can appear in both results
    /// (concrete sub on `incident://42` AND template sub on
    /// `incident://{id}`): deduplication at fan-out time keeps the
    /// "one update per session" property.
    pub fn template_sessions_for(&self, upstream: &str, uri: &str) -> Vec<String> {
        let state = self.lock();
        let mut out = Vec::new();
        for entry in &state.templates {
            if entry.key.upstream != upstream {
                continue;
            }
            if entry.compiled.matches(uri) {
                for sid in &entry.sessions {
                    out.push(sid.clone());
                }
            }
        }
        out
    }

    /// Remove every subscription owned by `session_id`. Returns the list
    /// of concrete keys whose last subscriber just left — the caller
    /// must issue an upstream `unsubscribe` for each one.
    ///
    /// Template-side orphans are returned separately via
    /// `drop_session_templates` because upstream unsubscribe issues a
    /// different RPC shape (the template string, not a concrete URI).
    /// Splitting the return values keeps the two teardown paths type-
    /// safe — a caller can't accidentally pass a template string where
    /// a bare URI is expected.
    ///
    /// Called from the session-close path (via `InspectingSessionManager`)
    /// so a downstream session that terminates without an explicit
    /// unsubscribe doesn't leave a zombie upstream subscription live.
    pub fn drop_session(&self, session_id: &str) -> Vec<UpstreamSubKey> {
        let mut state = self.lock();
        let mut orphaned = Vec::new();
        state.concrete.retain(|key, sessions| {
            if sessions.remove(session_id) && sessions.is_empty() {
                orphaned.push(key.clone());
                false
            } else {
                true
            }
        });
        orphaned
    }

    /// Template-side counterpart to [`drop_session`]. Returns the
    /// template keys whose last session just left; caller issues
    /// `upstream.unsubscribe(template_string)` for each.
    pub fn drop_session_templates(&self, session_id: &str) -> Vec<UpstreamTemplateKey> {
        let mut state = self.lock();
        let mut orphaned = Vec::new();
        state.templates.retain_mut(|entry| {
            if entry.sessions.remove(session_id) && entry.sessions.is_empty() {
                orphaned.push(entry.key.clone());
                false
            } else {
                true
            }
        });
        orphaned
    }

    /// Is `session_id` subscribed to `key`? Used by the session forwarder
    /// at fan-out time to decide whether to deliver an update to a given
    /// downstream session.
    pub fn session_is_subscribed(&self, session_id: &str, key: &UpstreamSubKey) -> bool {
        let state = self.lock();
        state
            .concrete
            .get(key)
            .map(|sessions| sessions.contains(session_id))
            .unwrap_or(false)
    }

    /// Snapshot of every session that cares about `key`. Used by the
    /// forwarder when a single `ResourceUpdated` event needs to reach
    /// multiple downstream peers. Cloning the session IDs out avoids
    /// holding the registry mutex across the async notification path.
    pub fn sessions_for(&self, key: &UpstreamSubKey) -> Vec<String> {
        let state = self.lock();
        state
            .concrete
            .get(key)
            .map(|sessions| sessions.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        // Poisoned mutexes mean a panic happened while holding the lock
        // in another thread. For this registry that would indicate a
        // bug (we never panic under the lock), so unwrapping after
        // poison is the right behavior: the process is already in an
        // indeterminate state and we want the next caller to see that.
        self.inner
            .lock()
            .expect("subscription registry mutex poisoned (panic under lock)")
    }
}

#[cfg(test)]
#[path = "subscription_registry_tests.rs"]
mod tests;
