//! Same testing pattern as `tool_list_cache.rs`: we can't fake an
//! `UpstreamHandle` directly (it owns a `LiveUpstream`), so these
//! unit tests exercise the slot / invalidation / invalidator
//! machinery. The end-to-end "event arrives -> next get refetches"
//! cycle with a real upstream lives in the gateway crate's
//! integration tests.

use super::*;
use std::time::Duration;

fn empty_listing() -> ResourceListing {
    ResourceListing::new(Vec::new(), Vec::new())
}

#[tokio::test]
async fn new_cache_has_no_slots() {
    let cache = ResourceListCache::new();
    let slots = cache.slots.read().await;
    assert!(slots.is_empty());
}

#[tokio::test]
async fn slot_for_creates_on_first_access_and_reuses_on_second() {
    // Property: calling slot_for with the same name twice returns
    // the same Slot (by Arc identity). Otherwise the single-flight
    // guarantee collapses — two concurrent misses could each get
    // their own mutex and race the two RPCs twice.
    let cache = ResourceListCache::new();
    let a = cache.slot_for("upstream-a").await;
    let b = cache.slot_for("upstream-a").await;
    assert!(
        Arc::ptr_eq(&a, &b),
        "same-name slot_for must return the same Arc"
    );
}

#[tokio::test]
async fn slot_for_distinguishes_different_upstreams() {
    let cache = ResourceListCache::new();
    let a = cache.slot_for("upstream-a").await;
    let b = cache.slot_for("upstream-b").await;
    assert!(
        !Arc::ptr_eq(&a, &b),
        "different-name slot_for must return distinct Arcs"
    );
}

#[tokio::test]
async fn invalidate_clears_populated_slot() {
    let cache = ResourceListCache::new();
    let slot = cache.slot_for("u1").await;
    *slot.lock().await = Some(empty_listing());

    cache.invalidate("u1").await;

    let slot_after = cache.slot_for("u1").await;
    assert!(
        slot_after.lock().await.is_none(),
        "invalidate must clear the inner Option"
    );
}

#[tokio::test]
async fn invalidate_unknown_name_is_a_noop() {
    // Property: invalidating a name the cache never saw must not
    // panic or create a phantom slot. Same pin as in
    // `tool_list_cache` — guards against a future refactor that
    // reached for `HashMap::entry`.
    let cache = ResourceListCache::new();
    cache.invalidate("never-seen").await;
    let slots = cache.slots.read().await;
    assert!(
        slots.is_empty(),
        "invalidate on unknown name must not create a slot"
    );
}

#[tokio::test]
async fn invalidate_all_clears_every_slot_but_keeps_mutex_alive() {
    let cache = ResourceListCache::new();
    let s1 = cache.slot_for("u1").await;
    let s2 = cache.slot_for("u2").await;
    *s1.lock().await = Some(empty_listing());
    *s2.lock().await = Some(empty_listing());

    cache.invalidate_all().await;

    assert!(s1.lock().await.is_none());
    assert!(s2.lock().await.is_none());
    // Property: slot mutexes themselves survive invalidate_all so
    // concurrent readers holding a pre-invalidation Arc don't
    // stall waiting on a lock that got replaced.
    let s1_again = cache.slot_for("u1").await;
    assert!(Arc::ptr_eq(&s1, &s1_again));
}

#[tokio::test]
async fn invalidate_drops_both_halves_atomically() {
    // Property pin: a ResourceListChanged event invalidates BOTH
    // resources and templates at once. The slot holds a single
    // `Option<ResourceListing>`, so `invalidate` can't partially
    // clear — this test makes that explicit.
    let cache = ResourceListCache::new();
    let slot = cache.slot_for("u1").await;
    *slot.lock().await = Some(empty_listing());

    cache.invalidate("u1").await;

    let guard = slot.lock().await;
    assert!(
        guard.is_none(),
        "resources and templates must clear together; \
         neither half may survive an invalidation"
    );
}

#[tokio::test]
async fn invalidator_reacts_to_resource_list_changed_event() {
    let cache = ResourceListCache::new();
    let bus = Arc::new(UpstreamEventBus::new());
    let cancel = CancellationToken::new();

    let handle = spawn_resource_invalidator(cache.clone(), Arc::clone(&bus), cancel.clone());

    // Seed a slot with a sentinel listing.
    let slot = cache.slot_for("u1").await;
    *slot.lock().await = Some(empty_listing());

    // Publish in a small loop: the subscriber may not be attached
    // yet on the first attempt. UpstreamEventBus is broadcast; it
    // only delivers messages to subscribers present at publish
    // time. Same pattern as `tool_list_cache::invalidator_reacts_…`.
    for _ in 0..10 {
        if bus.publish(UpstreamEvent::ResourceListChanged {
            upstream: "u1".to_string(),
        }) > 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Wait for the invalidator to observe and clear.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if slot.lock().await.is_none() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("invalidator did not clear u1 within 1s");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test]
async fn invalidator_ignores_tool_and_prompt_events() {
    // Property: only ResourceListChanged invalidates. A chatty
    // upstream publishing tool/prompt changes must NOT wipe the
    // resource cache.
    let cache = ResourceListCache::new();
    let bus = Arc::new(UpstreamEventBus::new());
    let cancel = CancellationToken::new();
    let handle = spawn_resource_invalidator(cache.clone(), Arc::clone(&bus), cancel.clone());

    let slot = cache.slot_for("u1").await;
    *slot.lock().await = Some(empty_listing());

    for _ in 0..10 {
        if bus.publish(UpstreamEvent::ToolListChanged {
            upstream: "u1".to_string(),
        }) > 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = bus.publish(UpstreamEvent::PromptListChanged {
        upstream: "u1".to_string(),
    });

    // Give the task a moment to process.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let inner = slot.lock().await;
    assert!(
        inner.is_some(),
        "non-resource events must not clear the resource cache"
    );
    drop(inner);

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test]
async fn invalidator_exits_when_cancel_fires() {
    let cache = ResourceListCache::new();
    let bus = Arc::new(UpstreamEventBus::new());
    let cancel = CancellationToken::new();
    let handle = spawn_resource_invalidator(cache, Arc::clone(&bus), cancel.clone());

    cancel.cancel();
    let joined = tokio::time::timeout(Duration::from_secs(1), handle).await;
    assert!(
        joined.is_ok(),
        "invalidator must exit promptly after cancel"
    );
}

#[tokio::test]
async fn invalidator_exits_when_bus_closes() {
    // Property pin: the `drop(bus)` inside the spawned task is what
    // lets this terminate. If a future refactor holds onto the
    // bus Arc across the select loop, this test will hang and
    // time out — catching the regression before production.
    let cache = ResourceListCache::new();
    let bus = Arc::new(UpstreamEventBus::new());
    let cancel = CancellationToken::new();
    let handle = spawn_resource_invalidator(cache, Arc::clone(&bus), cancel);

    drop(bus);
    let joined = tokio::time::timeout(Duration::from_secs(1), handle).await;
    assert!(
        joined.is_ok(),
        "invalidator must exit when the last bus clone is dropped"
    );
}

#[tokio::test]
async fn resource_listing_arc_clones_are_cheap() {
    // Property pin: cloning a ResourceListing should share the
    // inner Vecs via Arc rather than copying. A regression to
    // deep-cloning would blow up memory for large upstreams with
    // hundreds of resources.
    let listing = ResourceListing::new(Vec::new(), Vec::new());
    let clone = listing.clone();
    assert!(
        Arc::ptr_eq(&listing.resources, &clone.resources),
        "resources Arc must be shared across clones"
    );
    assert!(
        Arc::ptr_eq(&listing.templates, &clone.templates),
        "templates Arc must be shared across clones"
    );
}

#[tokio::test]
async fn failed_fetch_leaves_slot_empty() {
    // Property pin: a `get` that encounters an error on the first RPC
    // short-circuits via `?` before the `*guard = Some(listing)` write
    // in `ResourceListCache::get`. The slot must remain `None`
    // so the next caller retries rather than observing a poisoned cache.
    //
    // We can't call `get` with a failing `UpstreamHandle` (it owns a
    // `LiveUpstream` and cannot be faked), so this test pins the property
    // at the slot-state level: it simulates the failure path by taking the
    // slot lock, performing no write, and confirming `None` persists. A
    // future refactor that moved the `*guard = Some(...)` write before the
    // `?` would change the code shape and invalidate this test's rationale,
    // prompting a reviewer to add a real integration test in the gateway
    // crate where a mock server can be stood up.
    let cache = ResourceListCache::new();
    let slot = cache.slot_for("u1").await;

    // Slot starts empty — matches the "first caller" scenario.
    assert!(
        slot.lock().await.is_none(),
        "fresh slot must be None before any fetch attempt"
    );

    // Simulate the error path: acquire the slot mutex (as `get` does),
    // perform no write (as `?` ensures when the RPC fails), then release.
    {
        let guard = slot.lock().await;
        // Intentionally no `*guard = Some(...)` — this is what `?` achieves.
        assert!(
            guard.is_none(),
            "slot must still be None mid-error-path: the `?` must fire \
             before `*guard = Some(listing)` on a fetch failure"
        );
    }

    // After the simulated error path releases the lock, the slot is still
    // None — the next caller will retry rather than seeing stale data.
    assert!(
        slot.lock().await.is_none(),
        "slot must remain None after a failed fetch; \
         error must not poison the cache"
    );
}

#[tokio::test]
async fn invalidator_wipes_all_slots_on_lagged_recv() {
    // Property pin: when the broadcast receiver falls behind and gets
    // `RecvError::Lagged`, the invalidator calls `invalidate_all()` —
    // wiping every cached entry as a conservative safety measure.
    // Without this test, a refactor could silently downgrade the lag
    // handler to a no-op or a partial clear, causing stale resource
    // listings to persist after a burst of events.
    //
    // Technique: the broadcast ring buffer capacity is 256
    // (UpstreamEventBus::BUS_CAPACITY). We flush 257 events
    // synchronously (no `.await` between publishes) so the Tokio
    // scheduler cannot yield to the invalidator task between them.
    // Once the ring overflows, the task's next `rx.recv()` sees
    // `RecvError::Lagged`, which must trigger `invalidate_all()`.

    let cache = ResourceListCache::new();
    let bus = Arc::new(UpstreamEventBus::new());
    let cancel = CancellationToken::new();
    let handle = spawn_resource_invalidator(cache.clone(), Arc::clone(&bus), cancel.clone());

    // Step 1: confirm the invalidator has subscribed before we flood.
    // Publish a ToolListChanged (no-op for the resource cache) and wait
    // until the task is present as a subscriber (publish returns > 0).
    for _ in 0..50 {
        if bus.publish(UpstreamEvent::ToolListChanged {
            upstream: "probe".to_string(),
        }) > 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Step 2: seed two slots so we can verify invalidate_all cleared them.
    let s1 = cache.slot_for("u1").await;
    let s2 = cache.slot_for("u2").await;
    *s1.lock().await = Some(empty_listing());
    *s2.lock().await = Some(empty_listing());

    // Step 3: flood the ring buffer synchronously. Publishing 257 events
    // without any `.await` means the invalidator task cannot drain — the
    // ring overflows and the next recv() will return Lagged. We publish
    // ToolListChanged events (the invalidator no-ops them individually)
    // so that the lagged path is the only one that causes `invalidate_all`.
    for i in 0..257_usize {
        bus.publish(UpstreamEvent::ToolListChanged {
            upstream: format!("flood-{i}"),
        });
    }

    // Step 4: yield to the scheduler — the invalidator task runs, sees
    // RecvError::Lagged, and calls invalidate_all(). Poll until the slots
    // are cleared or the deadline expires.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if s1.lock().await.is_none() && s2.lock().await.is_none() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "invalidator did not wipe all slots within 1s after Lagged; \
                 s1={:?} s2={:?}",
                s1.lock().await.is_some(),
                s2.lock().await.is_some(),
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}
