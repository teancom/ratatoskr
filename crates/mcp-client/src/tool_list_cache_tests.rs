//! Unit tests here cover the cache's pure-ish behavior against a
//! minimal fake `UpstreamHandle` path. The integration test at
//! `crates/gateway/tests/tool_list_cache.rs` proves the full
//! event-bus -> invalidator -> next-get-refetches cycle with a
//! real upstream; that's where the load-bearing property lives.
//!
//! We can't fake an `UpstreamHandle` directly (it owns a
//! `LiveUpstream`, which requires a real rmcp client). So the
//! unit tests exercise the bits that don't need a handle: slot
//! creation, invalidation semantics, and the invalidator task's
//! response to events.

use super::*;
use std::time::Duration;

#[tokio::test]
async fn new_cache_has_no_slots() {
    let cache = ToolListCache::new();
    let slots = cache.slots.read().await;
    assert!(slots.is_empty());
}

#[tokio::test]
async fn slot_for_creates_on_first_access_and_reuses_on_second() {
    // Property: calling slot_for with the same name twice returns
    // the same Slot (by Arc identity). Otherwise the single-flight
    // guarantee collapses — two concurrent misses could each get
    // their own mutex and race.
    let cache = ToolListCache::new();
    let a = cache.slot_for("upstream-a").await;
    let b = cache.slot_for("upstream-a").await;
    assert!(
        Arc::ptr_eq(&a, &b),
        "same-name slot_for must return the same Arc"
    );
}

#[tokio::test]
async fn slot_for_distinguishes_different_upstreams() {
    let cache = ToolListCache::new();
    let a = cache.slot_for("upstream-a").await;
    let b = cache.slot_for("upstream-b").await;
    assert!(
        !Arc::ptr_eq(&a, &b),
        "different-name slot_for must return distinct Arcs"
    );
}

#[tokio::test]
async fn invalidate_clears_populated_slot() {
    // Populate a slot manually (bypassing `get`, since we have no
    // real UpstreamHandle to fetch from), then invalidate it.
    let cache = ToolListCache::new();
    let slot = cache.slot_for("u1").await;
    *slot.lock().await = Some(Arc::new(vec![]));

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
    // panic or create a phantom slot. (It currently does the
    // no-cost-no-op thing, but a future refactor that reached for
    // `HashMap::entry` would invisibly start inserting empty
    // slots, so pin the property.)
    let cache = ToolListCache::new();
    cache.invalidate("never-seen").await;
    let slots = cache.slots.read().await;
    assert!(
        slots.is_empty(),
        "invalidate on unknown name must not create a slot"
    );
}

#[tokio::test]
async fn invalidate_all_clears_every_slot_but_keeps_mutex_alive() {
    let cache = ToolListCache::new();
    let s1 = cache.slot_for("u1").await;
    let s2 = cache.slot_for("u2").await;
    *s1.lock().await = Some(Arc::new(vec![]));
    *s2.lock().await = Some(Arc::new(vec![]));

    cache.invalidate_all().await;

    assert!(s1.lock().await.is_none());
    assert!(s2.lock().await.is_none());
    // Property: the slot mutexes themselves survive invalidate_all
    // so concurrent readers holding a pre-invalidation Arc don't
    // stall waiting on a lock that got replaced.
    let s1_again = cache.slot_for("u1").await;
    assert!(Arc::ptr_eq(&s1, &s1_again));
}

#[tokio::test]
async fn invalidator_reacts_to_tool_list_changed_event() {
    // End-to-end unit test for the invalidator task: publish a
    // ToolListChanged for "u1", observe that u1's cache entry
    // gets cleared. Proves the bus -> invalidate wiring without
    // needing a full gateway/upstream harness.
    let cache = ToolListCache::new();
    let bus = Arc::new(UpstreamEventBus::new());
    let cancel = CancellationToken::new();

    let handle = spawn_invalidator(cache.clone(), Arc::clone(&bus), cancel.clone());

    // Seed the slot with fake data.
    let slot = cache.slot_for("u1").await;
    *slot.lock().await = Some(Arc::new(vec![]));

    // Give the invalidator a moment to subscribe before we publish.
    // Without this the send() may land before the subscriber exists,
    // leaving the receiver with nothing queued. UpstreamEventBus is
    // broadcast; it only delivers messages to subscribers present at
    // publish time.
    for _ in 0..10 {
        if bus.publish(UpstreamEvent::ToolListChanged {
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
async fn invalidator_ignores_resource_and_prompt_events() {
    // Property: only ToolListChanged invalidates. A chatty upstream
    // publishing resource/prompt changes must NOT wipe the tool
    // cache (nothing about the tool list has changed).
    let cache = ToolListCache::new();
    let bus = Arc::new(UpstreamEventBus::new());
    let cancel = CancellationToken::new();
    let handle = spawn_invalidator(cache.clone(), Arc::clone(&bus), cancel.clone());

    let slot = cache.slot_for("u1").await;
    let sentinel = Arc::new(vec![]);
    *slot.lock().await = Some(Arc::clone(&sentinel));

    // Publish the two non-tool events. Loop because the subscriber
    // might not be attached on the first try.
    for _ in 0..10 {
        if bus.publish(UpstreamEvent::ResourceListChanged {
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

    // Negative-assertion sleep: there is no signal to wait for — we want to
    // prove the cache entry was NOT cleared. Give the invalidator task enough
    // time to process the published events; if the entry is still populated
    // after this window, the events were correctly ignored.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Slot must still be populated.
    let inner = slot.lock().await;
    assert!(
        inner.as_ref().is_some_and(|v| Arc::ptr_eq(v, &sentinel)),
        "non-tool events must not clear the tool cache"
    );
    drop(inner);

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test]
async fn invalidator_exits_when_cancel_fires() {
    let cache = ToolListCache::new();
    let bus = Arc::new(UpstreamEventBus::new());
    let cancel = CancellationToken::new();
    let handle = spawn_invalidator(cache, Arc::clone(&bus), cancel.clone());

    cancel.cancel();
    let joined = tokio::time::timeout(Duration::from_secs(1), handle).await;
    assert!(
        joined.is_ok(),
        "invalidator must exit promptly after cancel"
    );
}

#[tokio::test]
async fn invalidator_exits_when_bus_closes() {
    let cache = ToolListCache::new();
    let bus = Arc::new(UpstreamEventBus::new());
    let cancel = CancellationToken::new();
    let handle = spawn_invalidator(cache, Arc::clone(&bus), cancel);

    drop(bus);
    let joined = tokio::time::timeout(Duration::from_secs(1), handle).await;
    assert!(
        joined.is_ok(),
        "invalidator must exit when the last bus clone is dropped"
    );
}
