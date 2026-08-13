// pattern: Imperative Shell
//
// Process-wide cache of observed/declared `ResponseShape` values, keyed
// by `(lua_server, lua_tool)`. Read at `tool-details` render time, written
// by the `inspect-tool-response` success path (and on first successful
// `execute`-time tool call, once that wiring lands).
//
// The cache is NOT session-scoped. Rationale: the shape of an upstream
// tool's response is a property of the upstream, not of the downstream
// caller — two sessions inspecting the same tool should see the same
// hint, and the first observation informs the next agent's first call.
//
// Staleness budget: cached classifications live for the gateway's
// process lifetime. Upstreams that change their response shape
// mid-uptime (rare: versions are pinned to a process lifetime, and
// shape-changing updates usually coincide with an upstream restart
// visible to us as a bootstrap cycle) would mislead the agent until
// restart. In practice upstream restarts mostly coincide with ours,
// and for the one case they don't the cost is low: the agent's
// builtin wrappers (`unwrap_content`, `json_decode`) degrade
// gracefully, and the stale hint just isn't useful rather than being
// actively wrong.
//
// Writes are monotonic via `response_shape::merge` — classifications
// only refine, never regress. That means a tool whose first probe saw
// JSON-in-text stays `TextJson` even if a later prose-only error reply
// would otherwise classify it as `TextProse`.
//
// The lattice that `merge` defines (idempotent, commutative, associative,
// with `Mixed` at the top) is documented on `response_shape::merge`. The
// cache adds nothing on top of that: for any observation sequence
// `s1, s2, …, sn`, the stored value is equal to
// `merge(merge(…merge(s1, s2)…), sn)`. The
// `observe_is_faithful_merge_over_all_pairs` and
// `observe_chains_merge_over_three_observations` tests pin that claim
// exhaustively over the variant set.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::response_shape::{self, ResponseShape};

/// Identifier pair for a tool, in the Lua-sanitized form used by the
/// inspection gate and by agents in `tool-details` / script arguments.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ShapeKey {
    pub lua_server: String,
    pub lua_tool: String,
}

impl ShapeKey {
    pub fn new(lua_server: impl Into<String>, lua_tool: impl Into<String>) -> Self {
        Self {
            lua_server: lua_server.into(),
            lua_tool: lua_tool.into(),
        }
    }
}

/// Shared handle. Cheap to clone (wraps an `Arc`). One is handed to
/// `GatewayHandler` via its constructor.
#[derive(Debug, Clone, Default)]
pub struct ResponseShapeCache {
    inner: Arc<RwLock<HashMap<ShapeKey, ResponseShape>>>,
}

impl ResponseShapeCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an observation. Monotonic: if a classification already
    /// exists for this tool, the stored value is `merge(old, new)`,
    /// which either refines or upgrades to `Mixed`. Never downgrades.
    ///
    /// Sync API on purpose: the inspect-tool-response handler already
    /// runs on the async side, but the critical section is a hash
    /// lookup + a merge — cheap enough to hold a `std::sync::RwLock`
    /// without contention noticeably affecting anything.
    pub fn observe(&self, key: ShapeKey, shape: ResponseShape) {
        let mut guard = self
            .inner
            .write()
            .expect("response shape cache lock poisoned");
        guard
            .entry(key)
            .and_modify(|existing| *existing = response_shape::merge(*existing, shape))
            .or_insert(shape);
    }

    /// Look up the best-known classification for this tool. Returns
    /// `None` when nothing's been observed — callers should then try
    /// `classify_from_schema` as a fallback.
    pub fn lookup(&self, key: &ShapeKey) -> Option<ResponseShape> {
        let guard = self
            .inner
            .read()
            .expect("response shape cache lock poisoned");
        guard.get(key).copied()
    }

    /// Test/diagnostic helper.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner
            .read()
            .expect("response shape cache lock poisoned")
            .len()
    }

    /// Test/diagnostic helper. Present alongside `len` so clippy's
    /// `len_without_is_empty` lint is satisfied.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
#[path = "response_shape_cache_tests.rs"]
mod tests;
