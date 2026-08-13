// pattern: Imperative Shell
//
// Session-scoped in-memory store for offloaded `execute` results.
//
// When a Lua script's final result exceeds the configured byte
// threshold (default 50 KB, tuned via `resultSizeThreshold` in the
// config file), the gateway stores the serialized JSON here under a
// fresh execution ID and returns a summary to the agent. The agent's
// next `execute` call can retrieve the stored value via
// `_gateway.get_result({ id = "..." })` inside its Lua script — so the
// full payload never enters the MCP response, never enters Claude's
// context, but stays addressable for filtering and reshaping.
//
// Storage shape
// -------------
// We store the **serialized JSON string**, not the `serde_json::Value`
// tree. Two reasons:
//
//  1. Predictable memory accounting. A 50 KB JSON text parses into a
//     ~3-10x larger `Value` tree (enum discriminants, String headers,
//     per-node heap allocations, HashMap bucket overhead). Keeping the
//     serialized form means the byte cap we advertise is the byte cost
//     we actually pay.
//
//  2. Simpler eviction. Byte-accounting an unparsed String is trivial
//     (`s.len()`); accounting a `Value` tree would require a walker.
//
// The cost: one parse on retrieval via `get`. That parse is cheap
// relative to the network round trip that originally produced the
// payload, so it doesn't move the wall-clock needle.
//
// Bounded-memory discipline
// -------------------------
//  - Per-entry ceiling: `max_result_bytes` (default 256 MiB). A single
//    payload above this is rejected at store time; the agent receives
//    an error suggesting they filter/paginate at source rather than
//    ship the full blob through the gateway.
//  - Per-session caps: `max_entries_per_session` (default 128) and
//    `max_bytes_per_session` (default 1 GiB). When either cap is
//    exceeded we evict in LRU order (oldest-used first) until we fit.
//    LRU is access-based: `get` and `save` both refresh the entry's
//    freshness.
//  - Session-level isolation: every entry is keyed by (session_id, id),
//    so session B cannot read session A's results, period. This
//    mirrors the tool inspection store's isolation boundary.
//  - Session teardown: `forget_session` wipes every entry for that
//    session. Called from the inspecting session manager's
//    `close_session` path the same way we already handle tool
//    inspections.
//
// Why in-memory, not SQLite
// -------------------------
// Same rationale as `ToolInspectionStore`: this gateway deliberately
// keeps no persistent session state. Offloaded results live with the
// session that produced them; when the session ends (or the gateway
// restarts), they go with it. The agent that runs a large query and
// then immediately retrieves part of it gets everything it needs; the
// agent that runs a large query and walks away for a week gets nothing
// — which is fine, because it can re-run the query.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use uuid::Uuid;

/// Built-in ceilings applied when the caller doesn't override them.
/// Matched to a single-dev-laptop deployment with 48 GiB of RAM:
/// spending up to ~1 GiB on pending offloaded results is generous for
/// the usual workload (Datadog log queries, Glean searches) and leaves
/// plenty of headroom for everything else.
pub const DEFAULT_MAX_ENTRIES_PER_SESSION: usize = 128;
pub const DEFAULT_MAX_BYTES_PER_SESSION: usize = 1024 * 1024 * 1024; // 1 GiB
pub const DEFAULT_MAX_RESULT_BYTES: usize = 256 * 1024 * 1024; // 256 MiB

/// Tunable limits. Use `..Default::default()` in struct-literal syntax
/// to fill unpinned fields with the built-in defaults so tests that
/// only care about one knob don't have to spell out the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreLimits {
    pub max_entries_per_session: usize,
    pub max_bytes_per_session: usize,
    pub max_result_bytes: usize,
}

impl Default for StoreLimits {
    fn default() -> Self {
        Self {
            max_entries_per_session: DEFAULT_MAX_ENTRIES_PER_SESSION,
            max_bytes_per_session: DEFAULT_MAX_BYTES_PER_SESSION,
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
        }
    }
}

/// Why a `save` rejected a payload. Exported so the offload-response
/// builder can produce a helpful message for the agent rather than a
/// generic "internal error."
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SaveError {
    #[error(
        "offloaded result is {got} bytes, which exceeds the per-result limit of {limit} bytes; \
         narrow the query at the source (e.g. a smaller time range or page size) or filter in Lua \
         before returning"
    )]
    ResultTooLarge { got: usize, limit: usize },
}

/// A single stored offloaded result.
///
/// `last_touched` drives LRU eviction: both `save` (at insertion) and
/// `get` (at retrieval) refresh it, so entries the agent is actively
/// working with stay warm while abandoned ones age out first.
///
/// The freshness value is a strictly-increasing counter drawn from the
/// store's `tick` atomic rather than a wall-clock `Instant`. This keeps
/// ordering deterministic even on platforms with coarse timer
/// resolution (where two operations within the same tick could read
/// equal `Instant`s and leave LRU selection to HashMap iteration
/// order).
#[derive(Debug, Clone)]
struct StoredEntry {
    payload: String,
    byte_len: usize,
    last_touched: u64,
}

/// Shared handle. Cheap to clone (wraps an `Arc`). Gateway hands one
/// into each `GatewayHandler` on construction; the handler passes a
/// clone into each `LuaRuntime::execute` call so the runtime can
/// save (on offload) and read (via `_gateway.get_result`).
#[derive(Debug, Clone)]
pub struct ExecutionResultStore {
    inner: Arc<RwLock<Inner>>,
}

#[derive(Debug)]
struct Inner {
    sessions: HashMap<String, HashMap<String, StoredEntry>>,
    limits: StoreLimits,
    /// Monotonic counter that drives LRU freshness. Each `save` and
    /// `get` claims `fetch_add(1)` and stamps the entry with the
    /// returned value. Strict monotonicity guarantees every operation
    /// is strictly newer than every prior one, so eviction never has
    /// to fall back to HashMap iteration order to break a tie.
    tick: AtomicU64,
}

impl Inner {
    fn next_tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for ExecutionResultStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionResultStore {
    pub fn new() -> Self {
        Self::with_limits(StoreLimits::default())
    }

    /// Produce a `lua_runtime::ExecutionResultBackend` bound to a
    /// specific session. The Lua runtime sees a single `save/get`
    /// interface; this adapter threads the session key through so
    /// one store can serve every session without the runtime needing
    /// to know about session identity.
    pub fn backend_for_session(
        &self,
        session_id: impl Into<String>,
    ) -> std::sync::Arc<dyn lua_runtime::ExecutionResultBackend> {
        std::sync::Arc::new(SessionScopedBackend {
            store: self.clone(),
            session_id: session_id.into(),
        })
    }

    pub fn with_limits(limits: StoreLimits) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                sessions: HashMap::new(),
                limits,
                tick: AtomicU64::new(0),
            })),
        }
    }

    /// Store `payload` for `session_id` and return the assigned
    /// execution ID. LRU-evicts older entries from this session as
    /// needed to stay under the configured caps.
    ///
    /// Errors with [`SaveError::ResultTooLarge`] if a single payload
    /// is larger than `max_result_bytes`. That's a hard ceiling even
    /// an empty session can't bypass, because storing one 10 GiB blob
    /// and evicting everything else isn't usually what the caller
    /// actually wanted.
    pub fn save(&self, session_id: &str, payload: String) -> Result<String, SaveError> {
        let byte_len = payload.len();
        let mut guard = self.inner.write().expect("result store lock poisoned");

        if byte_len > guard.limits.max_result_bytes {
            return Err(SaveError::ResultTooLarge {
                got: byte_len,
                limit: guard.limits.max_result_bytes,
            });
        }

        let id = Uuid::new_v4().to_string();
        let tick = guard.next_tick();
        let entry = StoredEntry {
            payload,
            byte_len,
            last_touched: tick,
        };

        let limits = guard.limits;
        let session = guard.sessions.entry(session_id.to_string()).or_default();
        session.insert(id.clone(), entry);

        // Post-insert eviction: trim the session until both caps hold.
        evict_until_under_caps(session, &limits);

        tracing::debug!(
            session_id,
            execution_id = %id,
            bytes = byte_len,
            remaining_entries = session.len(),
            "execution result stored"
        );

        Ok(id)
    }

    /// Fetch a previously-stored result. Returns `None` if the session
    /// doesn't own an entry under that ID — including the case where
    /// the agent asks session A to read session B's data (sessions
    /// are strictly isolated by key).
    ///
    /// Side-effect: refreshes the entry's LRU freshness. An agent
    /// repeatedly revisiting the same execution ID keeps it alive
    /// indefinitely (bounded by the total-bytes cap, which does
    /// still evict cold neighbors first).
    pub fn get(&self, session_id: &str, execution_id: &str) -> Option<String> {
        let mut guard = self.inner.write().expect("result store lock poisoned");
        let tick = guard.next_tick();
        let session = guard.sessions.get_mut(session_id)?;
        let entry = session.get_mut(execution_id)?;
        entry.last_touched = tick;
        Some(entry.payload.clone())
    }

    /// Drop every stored result for `session_id`. Called from the
    /// inspecting session manager's `close_session` path so an MCP
    /// session's offloaded results go away when the session does.
    pub fn forget_session(&self, session_id: &str) {
        let mut guard = self.inner.write().expect("result store lock poisoned");
        if let Some(session) = guard.sessions.remove(session_id) {
            tracing::debug!(
                session_id,
                entries = session.len(),
                "execution results cleared for session"
            );
        }
    }

    /// Test/diagnostic accessor: number of entries for a session.
    #[cfg(test)]
    pub fn entry_count(&self, session_id: &str) -> usize {
        let guard = self.inner.read().expect("result store lock poisoned");
        guard.sessions.get(session_id).map(|s| s.len()).unwrap_or(0)
    }

    /// Test/diagnostic accessor: total byte usage for a session.
    #[cfg(test)]
    pub fn byte_count(&self, session_id: &str) -> usize {
        let guard = self.inner.read().expect("result store lock poisoned");
        guard
            .sessions
            .get(session_id)
            .map(|s| s.values().map(|e| e.byte_len).sum())
            .unwrap_or(0)
    }
}

/// Evict entries from `session` (LRU-first) until both the entry count
/// and byte total are under their configured caps. Pure in the sense
/// that it doesn't touch any other session's data; mutates `session`
/// in place.
///
/// We compute an ordered list of (id, instant) pairs once per eviction
/// pass rather than maintaining a parallel LRU structure. For the
/// expected entry counts (tens to low hundreds) this is fast and keeps
/// the data structures honest.
fn evict_until_under_caps(session: &mut HashMap<String, StoredEntry>, limits: &StoreLimits) {
    let over_count = |s: &HashMap<String, StoredEntry>| s.len() > limits.max_entries_per_session;
    let over_bytes = |s: &HashMap<String, StoredEntry>| {
        s.values().map(|e| e.byte_len).sum::<usize>() > limits.max_bytes_per_session
    };

    while over_count(session) || over_bytes(session) {
        // Find the single oldest entry. For N <= ~128 this is O(N)
        // per call and O(N^2) per insertion burst — which is fine at
        // these sizes. A heap or linked list would pay for itself
        // only if N got into the thousands, and we have per-entry
        // caps that prevent that.
        let oldest = session
            .iter()
            .min_by_key(|(_, e)| e.last_touched)
            .map(|(k, _)| k.clone());
        match oldest {
            Some(key) => {
                let evicted = session.remove(&key);
                if let Some(e) = evicted {
                    tracing::debug!(
                        execution_id = %key,
                        bytes = e.byte_len,
                        "LRU-evicted execution result"
                    );
                }
            }
            None => break, // empty; shouldn't reach here given the while predicates
        }
    }
}

/// Adapter that binds the store to a single session and implements
/// the runtime's `ExecutionResultBackend` trait. Constructed via
/// [`ExecutionResultStore::backend_for_session`].
struct SessionScopedBackend {
    store: ExecutionResultStore,
    session_id: String,
}

impl lua_runtime::ExecutionResultBackend for SessionScopedBackend {
    fn save(&self, payload: String) -> Result<String, String> {
        self.store
            .save(&self.session_id, payload)
            .map_err(|e| e.to_string())
    }

    fn get(&self, id: &str) -> Option<String> {
        self.store.get(&self.session_id, id)
    }
}

#[cfg(test)]
#[path = "execution_result_store_tests.rs"]
mod tests;
