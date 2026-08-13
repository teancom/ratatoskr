// pattern: Imperative Shell
//
// Per-upstream Tool-list cache with event-bus-driven invalidation.
//
// Structural twin: `resource_list_cache.rs`. Both are per-upstream
// `Mutex<Option<Arc<Vec<_>>>>` slot caches with single-flight on miss
// and event-bus-driven invalidation via a dedicated spawn_* function.
// They are intentionally NOT generic — `ResourceListCache` bundles
// resources + templates as one invalidation unit, handles pagination,
// and fires two RPCs per miss; `ToolListCache` fires one. The twin was
// considered for generalization and rejected on the grounds that the
// invariants diverge enough to make a shared `UpstreamListCache<T>`
// leakier than the duplication it would replace. If a third cache of
// this shape appears, revisit. If you change single-flighting, lag
// handling, or invalidation semantics HERE, check whether the same
// change belongs in `resource_list_cache.rs`.
//
// Why this exists
// ---------------
// Before this cache, every path that needed an upstream's tool list fired a
// fresh MCP `tools/list` request:
//   - `list-server-tools`  -> one round trip
//   - `tool-details`       -> one round trip
//   - `inspect-tool-response` -> one round trip
//   - `execute` (Lua)      -> one round trip PER registered upstream, every
//                             time a script runs, just to decide what Lua
//                             globals to inject
//
// On the user's real 10-upstream config that's 10 serial round trips before
// any Lua code executes. Fine for ad-hoc use, unacceptable in a hot loop,
// and the round-trip cost is entirely wasted after the first call because
// upstreams rarely change their tool list.
//
// Shape
// -----
// - Keyed by upstream display name (the same key `UpstreamManager` uses).
// - Values are `Arc<Vec<Tool>>` so callers get a cheap clone without
//   holding the cache lock while they iterate or format.
// - Each slot carries its own async mutex for **single-flighting**: if
//   N callers miss the same upstream simultaneously, exactly one fires
//   a `list_tools` RPC and the rest wait on the mutex and observe the
//   populated slot on wake. This matters because `install_upstream_globals`
//   in the Lua runtime calls `get` for every upstream back-to-back, and
//   an early cold `execute` would otherwise race with itself if the agent
//   ran two scripts in parallel.
// - Invalidation is event-bus-driven: `spawn_invalidator` subscribes to
//   `UpstreamEventBus` and wipes the entry for any upstream that
//   publishes `ToolListChanged`. The freshly-invalidated next caller
//   refills the cache.
//
// What this is NOT
// ----------------
// - Not persistent. No SQLite, no disk; the cache lives with the process.
//   The gateway deliberately keeps no persistent session state, and the
//   same rationale applies here: a cold process has no interesting state
//   to resume, and persistence complicates invalidation-on-restart.
// - Not TTL'd. We rely on `tools/list_changed` notifications for
//   correctness. Upstreams that change their tool list without emitting
//   the notification will serve stale entries until the gateway restarts;
//   that's an upstream bug worth reporting, not a reason to sprinkle
//   defensive TTLs into every cache path.
// - Not responsible for pagination. `UpstreamHandle::list_tools` delegates
//   to rmcp's `list_all_tools` and returns the complete set; pagination is
//   transparent to this cache.

use std::collections::HashMap;
use std::sync::Arc;

use rmcp::ServiceError;
use rmcp::model::Tool;
use tokio::sync::{Mutex, RwLock, broadcast::error::RecvError};
use tokio_util::sync::CancellationToken;

use crate::event_bus::UpstreamEventBus;
use crate::events::UpstreamEvent;
use crate::upstream::UpstreamHandle;

/// One cache slot. The outer `Mutex` serializes concurrent misses for a
/// single upstream (single-flight); the inner `Option` holds the fetched
/// tools.
///
/// We keep the Mutex alive across invalidations by resetting the `Option`
/// to `None` rather than removing the slot from the outer map. That way a
/// concurrent reader that's already grabbed the slot's `Arc<Mutex<...>>`
/// will see the empty inner state and re-fetch, instead of racing against
/// a slot-insertion storm.
type SlotState = Mutex<Option<Arc<Vec<Tool>>>>;
type Slot = Arc<SlotState>;

/// Per-upstream cache of tool lists.
///
/// Cheap to clone: the inner map lives behind an `Arc<RwLock<...>>`.
///
/// ## Timeout & wedge resilience: owned by the handle, not the cache
///
/// Earlier revisions of this cache bounded `handle.list_tools()` with
/// its own `fetch_timeout` so a wedged upstream wouldn't keep the slot
/// mutex locked forever. That fix worked but lived at the wrong layer:
/// when the cache's outer timeout fired, it dropped the in-flight
/// `list_tools` future without giving the handle's health record a
/// chance to record the failure, so the supervisor never noticed and
/// the upstream stayed "Healthy" indefinitely.
///
/// The bound now lives on [`UpstreamHandle::list_tools`] itself —
/// wrapped in `tokio::time::timeout(call_timeout, ...)` with a
/// `record_failure(FailureKind::CallTimeout)` on elapse, mirroring
/// `call_tool`. The slot mutex is released because the handle's future
/// resolves with `Err(ServiceError::Timeout)` instead of pending
/// forever; the supervisor sees the same `consecutive_failures` bump
/// it would on a `call_tool` wedge and can flip Suspect → respawn.
///
/// One timeout, one canonical observation point, one health-record
/// update. The cache is back to being just a memoize-with-single-flight.
#[derive(Clone, Default)]
pub struct ToolListCache {
    slots: Arc<RwLock<HashMap<String, Slot>>>,
}

impl ToolListCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the tool list for `handle`, fetching if the cache is empty.
    ///
    /// Concurrent callers for the same upstream single-flight: one does
    /// the actual `list_tools` RPC, the rest wait on the slot's mutex
    /// and observe the populated result.
    ///
    /// Slot release on a wedged upstream is the handle's responsibility:
    /// `UpstreamHandle::list_tools` is itself bounded by
    /// `UpstreamHandle::call_timeout` and resolves with
    /// `ServiceError::Timeout` rather than hanging forever, which lets
    /// `slot.lock()` drop and the next caller try again. See the type-
    /// level docs for the historical rationale.
    ///
    /// Returns `Arc<Vec<Tool>>` so callers can hold the result without
    /// keeping any of the cache's locks. Cloning the Arc is O(1).
    pub async fn get(&self, handle: &UpstreamHandle) -> Result<Arc<Vec<Tool>>, ServiceError> {
        let name = handle.config.name.as_str();
        let slot = self.slot_for(name).await;

        // Hold the slot mutex across the fetch: this is the single-flight
        // point. A second caller racing us will block here and find the
        // inner Option populated on wake. The handle's own timeout
        // bounds how long this guard can be held on a wedge.
        let mut guard = slot.lock().await;
        if let Some(cached) = guard.as_ref() {
            tracing::trace!(upstream = %name, "tool list cache hit");
            return Ok(Arc::clone(cached));
        }

        tracing::debug!(upstream = %name, "tool list cache miss; fetching");
        let tools = handle.list_tools().await?;
        let tools = Arc::new(tools);
        *guard = Some(Arc::clone(&tools));
        Ok(tools)
    }

    /// Drop the cached entry for `name`, if any. Subsequent `get` calls
    /// for this upstream will re-fetch.
    ///
    /// We reset the inner `Option` rather than removing the outer map
    /// entry so concurrent readers holding the slot mutex see a clean
    /// empty state rather than racing us for slot creation.
    pub async fn invalidate(&self, name: &str) {
        let slots = self.slots.read().await;
        if let Some(slot) = slots.get(name) {
            let mut inner = slot.lock().await;
            if inner.take().is_some() {
                tracing::debug!(upstream = %name, "tool list cache invalidated");
            }
        }
    }

    /// Drop every cached entry. Useful during administrative operations
    /// (config reload, explicit refresh tool) — not on the hot path today.
    pub async fn invalidate_all(&self) {
        let slots = self.slots.read().await;
        for (name, slot) in slots.iter() {
            let mut inner = slot.lock().await;
            if inner.take().is_some() {
                tracing::debug!(upstream = %name, "tool list cache invalidated (bulk)");
            }
        }
    }

    /// Internal helper: return the `Slot` for this upstream, creating it
    /// under a write lock if it doesn't exist yet.
    ///
    /// Separated from `get` so the read-lock fast path for "slot already
    /// exists" (the common case after warmup) doesn't have to upgrade to
    /// a write lock. Double-checked under the write lock to avoid
    /// overwriting a slot a racing caller just created.
    async fn slot_for(&self, name: &str) -> Slot {
        {
            let slots = self.slots.read().await;
            if let Some(slot) = slots.get(name) {
                return Arc::clone(slot);
            }
        }
        let mut slots = self.slots.write().await;
        Arc::clone(
            slots
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(None))),
        )
    }
}

/// Spawn a background task that subscribes to `bus` and wipes the cache
/// entry for any upstream that publishes `UpstreamEvent::ToolListChanged`.
///
/// Returns a `JoinHandle<()>` so the caller can await clean shutdown or
/// simply drop it (the task exits naturally when the bus closes or the
/// cancel token fires).
///
/// `cancel` is honored so callers that want deterministic shutdown order
/// (drop the invalidator before closing upstreams, etc.) can reach for it.
/// The task also exits if the broadcast channel itself closes — i.e. the
/// last `UpstreamEventBus` clone is dropped — so in practice a caller
/// that doesn't care about ordering can ignore `cancel` entirely.
pub fn spawn_invalidator(
    cache: ToolListCache,
    bus: Arc<UpstreamEventBus>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut rx = bus.subscribe();
        // Drop the bus Arc immediately after subscribing. If we held on to
        // it, the task's own clone would keep the underlying broadcast
        // Sender alive forever and the channel would never close even
        // after every other holder dropped — turning the "bus dropped"
        // shutdown signal into a deadlock.
        drop(bus);
        tracing::debug!("tool list cache invalidator started");
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!("tool list cache invalidator cancelled");
                    return;
                }
                recv = rx.recv() => {
                    match recv {
                        Ok(UpstreamEvent::ToolListChanged { upstream }) => {
                            cache.invalidate(&upstream).await;
                        }
                        // Resource/prompt list changes and per-resource
                        // updates don't affect the tool cache. No-op,
                        // don't touch the cache.
                        Ok(UpstreamEvent::ResourceListChanged { .. })
                        | Ok(UpstreamEvent::ResourceUpdated { .. })
                        | Ok(UpstreamEvent::PromptListChanged { .. }) => {}
                        Err(RecvError::Lagged(skipped)) => {
                            // A slow invalidator is a correctness hazard:
                            // dropped events mean stale cache. Wipe
                            // everything rather than risk serving stale
                            // entries for the dropped upstreams we can't
                            // identify. Conservative, cheap, and rare.
                            tracing::warn!(
                                skipped,
                                "tool list cache invalidator lagged; \
                                 dropping all cached entries as a precaution"
                            );
                            cache.invalidate_all().await;
                        }
                        Err(RecvError::Closed) => {
                            tracing::debug!(
                                "upstream event bus closed; invalidator exiting"
                            );
                            return;
                        }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
#[path = "tool_list_cache_tests.rs"]
mod tests;
