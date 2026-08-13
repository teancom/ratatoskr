use super::*;

fn big_payload(n: usize) -> String {
    "x".repeat(n)
}

#[test]
fn save_roundtrips_a_small_payload() {
    let store = ExecutionResultStore::new();
    let id = store
        .save("sess-a", "hello".to_string())
        .expect("small save");
    assert_eq!(
        store.get("sess-a", &id).as_deref(),
        Some("hello"),
        "get must return exactly what save stored"
    );
}

#[test]
fn get_with_wrong_session_returns_none() {
    // Load-bearing isolation property: the inspection store keys on
    // session and we want the same boundary here. A misbehaving or
    // compromised session must not read another session's blobs.
    let store = ExecutionResultStore::new();
    let id = store
        .save("sess-a", "secret".to_string())
        .expect("test setup save should succeed");
    assert_eq!(
        store.get("sess-b", &id),
        None,
        "session B must not read session A's entry"
    );
}

#[test]
fn get_with_unknown_id_returns_none() {
    let store = ExecutionResultStore::new();
    assert_eq!(store.get("sess-a", "nonexistent-id"), None);
}

#[test]
fn save_rejects_payload_over_max_result_bytes() {
    // Property: a single blob larger than the per-entry cap is
    // refused, not stored-and-evicts-everything-else. Otherwise
    // one runaway query would wipe the session's other offloads.
    let store = ExecutionResultStore::with_limits(StoreLimits {
        max_result_bytes: 100,
        ..Default::default()
    });
    let err = store
        .save("sess", big_payload(101))
        .expect_err("over-cap save must error");
    match err {
        SaveError::ResultTooLarge { got, limit } => {
            assert_eq!(got, 101);
            assert_eq!(limit, 100);
        }
    }
}

#[test]
fn save_at_exact_max_result_bytes_is_accepted() {
    // Boundary test: the comparison is strictly-greater, so exact
    // ceiling matches fit.
    let store = ExecutionResultStore::with_limits(StoreLimits {
        max_result_bytes: 100,
        ..Default::default()
    });
    let _ = store
        .save("sess", big_payload(100))
        .expect("exact-ceiling save must succeed");
}

#[test]
fn entry_count_cap_triggers_lru_eviction() {
    // Property: once the entry count would exceed the cap,
    // the oldest-touched entry evicts first. Freshness is the
    // store's internal monotonic counter, so saves are strictly
    // ordered by construction — no wall-clock sleeps needed.
    let store = ExecutionResultStore::with_limits(StoreLimits {
        max_entries_per_session: 3,
        ..Default::default()
    });
    let id1 = store
        .save("s", "one".to_string())
        .expect("test setup save should succeed");
    let id2 = store
        .save("s", "two".to_string())
        .expect("test setup save should succeed");
    let id3 = store
        .save("s", "three".to_string())
        .expect("test setup save should succeed");
    let id4 = store
        .save("s", "four".to_string())
        .expect("test setup save should succeed");

    assert_eq!(
        store.entry_count("s"),
        3,
        "entry count must be exactly at the cap after overflow"
    );
    assert!(
        store.get("s", &id1).is_none(),
        "oldest (id1) must be evicted"
    );
    assert!(store.get("s", &id2).is_some(), "id2 must remain");
    assert!(store.get("s", &id3).is_some(), "id3 must remain");
    assert!(store.get("s", &id4).is_some(), "id4 must remain");
}

#[test]
fn byte_count_cap_triggers_lru_eviction() {
    // Property: even with room in the entry-count budget, a
    // payload that pushes total bytes over the byte cap evicts
    // the oldest until it fits. Ordering relies on the store's
    // monotonic tick — not wall-clock sleeps.
    let store = ExecutionResultStore::with_limits(StoreLimits {
        max_entries_per_session: 100, // plenty of entry headroom
        max_bytes_per_session: 1000,
        ..Default::default()
    });
    let id1 = store
        .save("s", big_payload(400))
        .expect("test setup save should succeed");
    let id2 = store
        .save("s", big_payload(400))
        .expect("test setup save should succeed");
    assert_eq!(store.byte_count("s"), 800);
    // Third 400-byte entry would push us to 1200; LRU should
    // evict id1 to bring us back down to 800.
    let id3 = store
        .save("s", big_payload(400))
        .expect("test setup save should succeed");
    assert_eq!(store.byte_count("s"), 800);
    assert!(store.get("s", &id1).is_none(), "id1 should be evicted");
    assert!(store.get("s", &id2).is_some());
    assert!(store.get("s", &id3).is_some());
}

#[test]
fn get_refreshes_lru_freshness() {
    // Property: an agent that keeps revisiting id1 while running
    // other scripts should NOT have id1 evicted first. `get`
    // touches the entry so its freshness rises above untouched
    // neighbors. Each save/get claims a fresh monotonic tick, so
    // ordering is deterministic without wall-clock sleeps.
    let store = ExecutionResultStore::with_limits(StoreLimits {
        max_entries_per_session: 2,
        ..Default::default()
    });
    let id1 = store
        .save("s", "one".to_string())
        .expect("test setup save should succeed");
    let id2 = store
        .save("s", "two".to_string())
        .expect("test setup save should succeed");
    // Touch id1 so it's now fresher than id2.
    let _ = store.get("s", &id1);
    let id3 = store
        .save("s", "three".to_string())
        .expect("test setup save should succeed");
    assert!(
        store.get("s", &id1).is_some(),
        "id1 was touched; must survive eviction"
    );
    assert!(
        store.get("s", &id2).is_none(),
        "id2 was NOT touched; must be evicted"
    );
    assert!(store.get("s", &id3).is_some());
}

#[test]
fn forget_session_drops_all_entries() {
    let store = ExecutionResultStore::new();
    let _ = store
        .save("s", "a".repeat(100))
        .expect("test setup save should succeed");
    let _ = store
        .save("s", "b".repeat(200))
        .expect("test setup save should succeed");
    assert_eq!(store.entry_count("s"), 2);
    assert_eq!(store.byte_count("s"), 300);
    store.forget_session("s");
    assert_eq!(store.entry_count("s"), 0);
    assert_eq!(
        store.byte_count("s"),
        0,
        "byte total must reset on session teardown"
    );
}

#[test]
fn forget_session_is_a_noop_for_unknown_session() {
    let store = ExecutionResultStore::new();
    store.forget_session("never-existed"); // must not panic
    assert_eq!(store.entry_count("never-existed"), 0);
}

#[test]
fn sessions_do_not_share_eviction_budgets() {
    // Property: session A filling up its cap must not evict
    // session B's entries. Each session has its own accounting.
    let store = ExecutionResultStore::with_limits(StoreLimits {
        max_entries_per_session: 1,
        ..Default::default()
    });
    let _a1 = store
        .save("a", "a1".to_string())
        .expect("test setup save should succeed");
    let b1 = store
        .save("b", "b1".to_string())
        .expect("test setup save should succeed");
    // Filling session A beyond its cap must not touch B.
    let _a2 = store
        .save("a", "a2".to_string())
        .expect("test setup save should succeed");
    assert_eq!(store.entry_count("a"), 1, "A's cap is still 1");
    assert_eq!(store.entry_count("b"), 1, "B is untouched");
    assert!(
        store.get("b", &b1).is_some(),
        "session B's entry must still be retrievable"
    );
}

#[test]
fn ids_are_unique_across_saves() {
    // Property: we lean on UUID v4 for ID generation. Asserting
    // they differ across saves is cheap insurance against a
    // future refactor that accidentally used a deterministic
    // source.
    let store = ExecutionResultStore::new();
    let mut ids = Vec::new();
    for _ in 0..16 {
        ids.push(
            store
                .save("s", "x".to_string())
                .expect("test setup save should succeed"),
        );
    }
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "IDs must be unique across saves");
}
