// pattern: Imperative Shell
//
// Per-upstream cache for `resources/list` + `resources/templates/list`
// with event-bus-driven invalidation. Sibling of `tool_list_cache.rs`;
// see that file for the original rationale. This doc focuses on what's
// specifically different for resources.
//
// Generalization posture: the two caches share a structural shape but
// not their invariants — see the header comment in `tool_list_cache.rs`
// for the pinned decision against `UpstreamListCache<T>`. If you change
// single-flighting, lag handling, or invalidation semantics HERE, check
// whether the same change belongs in `tool_list_cache.rs`.
//
// Why this exists
// ---------------
// Before this cache, every downstream `resources/list` or
// `resources/read` that came through the gateway would have to fan
// out to every upstream and assemble a catalog. The
// user's real config has 10 upstreams and only 3 ship resources; the
// other 7 would each pay a network round trip per downstream call, for
// no content. Unacceptable once the gateway is on any kind of hot path.
//
// Shape
// -----
// - Keyed by upstream display name (the same key `UpstreamManager` uses,
//   same as `ToolListCache`).
// - One slot per upstream carries BOTH resources and templates as a
//   single `ResourceListing` bundle. MCP ships them as two endpoints
//   but a `resources/list_changed` notification conventionally
//   invalidates both — a pinned design decision, since the spec's single
//   notification for "the list has changed" would be ambiguous to
//   forward otherwise. Bundling them keeps invalidation atomic.
// - Each slot carries its own async mutex for single-flighting:
//   concurrent misses on the same upstream elect one caller to fire
//   the two RPCs (`list_all_resources` + `list_all_resource_templates`)
//   while the rest wait. Same pattern as the tool cache.
// - Invalidation is event-bus-driven: `spawn_resource_invalidator`
//   subscribes to `UpstreamEventBus` and wipes the slot for any
//   upstream that publishes `ResourceListChanged`.
//
// Differences from `ToolListCache`
// --------------------------------
// 1. Bundled listing. The slot holds `ResourceListing { resources,
//    templates }` rather than `Vec<Tool>`. A `resources/list_changed`
//    event clears both halves.
// 2. Pagination is already handled. rmcp's `list_all_resources` and
//    `list_all_resource_templates` paginate until `next_cursor` is
//    None. The tool cache punts on this ("single-page for now"); we
//    don't have to.
// 3. Two RPCs per cache miss. They run sequentially because they
//    share the single upstream connection and we don't want to
//    double the peak concurrency for what are typically sub-second
//    responses. If an upstream turns out to be slow enough on BOTH
//    endpoints to matter, parallelizing via `tokio::try_join!` is a
//    one-line change.
//
// What this is NOT
// ----------------
// - Not persistent. Same rationale as `ToolListCache`.
// - Not TTL'd. We rely on `resources/list_changed` notifications for
//   correctness. Upstreams that mutate their resource list without
//   emitting the notification will serve stale entries until the
//   gateway restarts — an upstream bug, not a reason to add defensive
//   TTLs.
// - Not a read-through for `resources/read`. Reads are always live;
//   resource *content* is not cached. Caching content would need
//   invalidation we don't have (`resources/updated` per URI), plus
//   unbounded memory without an eviction policy.

use std::collections::HashMap;
use std::sync::Arc;

use rmcp::ServiceError;
use rmcp::model::{Resource, ResourceTemplate};
use tokio::sync::{Mutex, RwLock, broadcast::error::RecvError};
use tokio_util::sync::CancellationToken;

use crate::event_bus::UpstreamEventBus;
use crate::events::UpstreamEvent;
use crate::upstream::UpstreamHandle;

/// Bundled result of a resource-list fetch: concrete resources and
/// URI templates, owned together so an invalidation drops both halves
/// atomically.
///
/// Cheap to clone (the inner fields are in `Arc`), so callers hold the
/// listing without keeping any cache locks.
#[derive(Debug, Clone)]
pub struct ResourceListing {
    pub resources: Arc<Vec<Resource>>,
    pub templates: Arc<Vec<ResourceTemplate>>,
}

impl ResourceListing {
    pub fn new(resources: Vec<Resource>, templates: Vec<ResourceTemplate>) -> Self {
        Self {
            resources: Arc::new(resources),
            templates: Arc::new(templates),
        }
    }
}

/// One cache slot. Same shape as `ToolListCache::SlotState`:
///
/// - Outer `Mutex` serializes misses for a single upstream (single-flight).
/// - Inner `Option` holds the fetched listing.
///
/// We keep the Mutex alive across invalidations by resetting the
/// `Option` to `None` rather than removing the slot from the outer
/// map. Concurrent readers that already grabbed the slot's Arc see an
/// empty inner and re-fetch, instead of racing against slot-insertion.
type SlotState = Mutex<Option<ResourceListing>>;
type Slot = Arc<SlotState>;

/// Per-upstream cache of `(resources, templates)` listings.
///
/// Cheap to clone: the inner map lives behind an `Arc<RwLock<...>>`.
#[derive(Clone, Default)]
pub struct ResourceListCache {
    slots: Arc<RwLock<HashMap<String, Slot>>>,
}

impl ResourceListCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the resource listing for `handle`, fetching if the cache is
    /// empty.
    ///
    /// Concurrent callers for the same upstream single-flight: one
    /// does the two RPCs (`list_resources` + `list_resource_templates`),
    /// the rest wait on the slot's mutex and observe the populated
    /// result on wake.
    ///
    /// Returns `ResourceListing` (cheap to clone) so callers can hold
    /// the result without keeping any of the cache's locks.
    ///
    /// Errors: returns the first `ServiceError` from either RPC. A
    /// failure does NOT populate the slot — the next caller tries
    /// again. This keeps transient upstream errors from poisoning the
    /// cache.
    pub async fn get(&self, handle: &UpstreamHandle) -> Result<ResourceListing, ServiceError> {
        let name = handle.config.name.as_str();
        let slot = self.slot_for(name).await;

        let mut guard = slot.lock().await;
        if let Some(cached) = guard.as_ref() {
            tracing::trace!(upstream = %name, "resource list cache hit");
            return Ok(cached.clone());
        }

        tracing::debug!(upstream = %name, "resource list cache miss; fetching");
        // Sequential on purpose: both RPCs share the same upstream
        // connection, and most upstreams return these in milliseconds.
        // Parallelizing via `tokio::try_join!` is a one-line change if
        // a slow upstream later makes the serial latency matter.
        let resources = handle.list_resources().await?;
        let templates = handle.list_resource_templates().await?;
        let listing = ResourceListing::new(resources, templates);
        *guard = Some(listing.clone());
        Ok(listing)
    }

    /// Drop the cached entry for `name`, if any. Subsequent `get` calls
    /// for this upstream will re-fetch.
    ///
    /// Resets the inner `Option` rather than removing the outer map
    /// entry so concurrent readers holding the slot mutex see a clean
    /// empty state rather than racing for slot creation.
    pub async fn invalidate(&self, name: &str) {
        let slots = self.slots.read().await;
        if let Some(slot) = slots.get(name) {
            let mut inner = slot.lock().await;
            if inner.take().is_some() {
                tracing::debug!(upstream = %name, "resource list cache invalidated");
            }
        }
    }

    /// Drop every cached entry.
    pub async fn invalidate_all(&self) {
        let slots = self.slots.read().await;
        for (name, slot) in slots.iter() {
            let mut inner = slot.lock().await;
            if inner.take().is_some() {
                tracing::debug!(upstream = %name, "resource list cache invalidated (bulk)");
            }
        }
    }

    /// Internal helper: return the `Slot` for this upstream, creating
    /// it under a write lock if it doesn't exist yet.
    ///
    /// Separated from `get` so the read-lock fast path for "slot
    /// already exists" (the common case after warmup) doesn't have to
    /// upgrade to a write lock. Double-checked under the write lock to
    /// avoid overwriting a slot a racing caller just created.
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

/// Spawn a background task that subscribes to `bus` and wipes the
/// cache entry for any upstream that publishes
/// `UpstreamEvent::ResourceListChanged`.
///
/// Returns a `JoinHandle<()>` so the caller can await clean shutdown
/// or simply drop it (the task exits naturally when the bus closes or
/// the cancel token fires).
///
/// Name parallels `tool_list_cache::spawn_invalidator`. We don't share
/// the name because both are re-exported at the crate root and would
/// collide; each cache has its own event variant anyway.
pub fn spawn_resource_invalidator(
    cache: ResourceListCache,
    bus: Arc<UpstreamEventBus>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut rx = bus.subscribe();
        // Drop the bus Arc immediately after subscribing. Holding it
        // would keep the underlying broadcast Sender alive forever and
        // turn the "bus dropped" shutdown signal into a deadlock. This
        // exact mistake bit the tool-list cache once.
        drop(bus);
        tracing::debug!("resource list cache invalidator started");
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!("resource list cache invalidator cancelled");
                    return;
                }
                recv = rx.recv() => {
                    match recv {
                        Ok(UpstreamEvent::ResourceListChanged { upstream }) => {
                            cache.invalidate(&upstream).await;
                        }
                        // Tool / prompt list changes and per-resource
                        // updates don't affect the resource-LIST cache:
                        // a `resources/updated` means the content of a
                        // specific resource changed, not the catalog.
                        // No-op.
                        Ok(UpstreamEvent::ToolListChanged { .. })
                        | Ok(UpstreamEvent::ResourceUpdated { .. })
                        | Ok(UpstreamEvent::PromptListChanged { .. }) => {}
                        Err(RecvError::Lagged(skipped)) => {
                            // Conservative: a missed event could mean a
                            // stale entry. Wipe everything rather than
                            // risk serving stale listings for the
                            // upstreams whose events we dropped.
                            tracing::warn!(
                                skipped,
                                "resource list cache invalidator lagged; \
                                 dropping all cached entries as a precaution"
                            );
                            cache.invalidate_all().await;
                        }
                        Err(RecvError::Closed) => {
                            tracing::debug!(
                                "upstream event bus closed; resource invalidator exiting"
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
#[path = "resource_list_cache_tests.rs"]
mod tests;
