use super::*;

fn key(server: &str, tool: &str) -> ShapeKey {
    ShapeKey::new(server, tool)
}

#[test]
fn empty_cache_returns_none() {
    let cache = ResponseShapeCache::new();
    assert_eq!(cache.lookup(&key("datadog", "search")), None);
}

#[test]
fn observe_then_lookup_returns_value() {
    let cache = ResponseShapeCache::new();
    cache.observe(key("gitlab", "download_job_log"), ResponseShape::TextProse);
    assert_eq!(
        cache.lookup(&key("gitlab", "download_job_log")),
        Some(ResponseShape::TextProse)
    );
}

#[test]
fn observe_promotes_text_prose_to_text_json() {
    // Property: once JSON has been observed in text, a later prose
    // observation (e.g. an error reply from the same tool) must NOT
    // overwrite the more-informative classification. Pinned because
    // a naive `insert` would silently regress the hint.
    let cache = ResponseShapeCache::new();
    cache.observe(key("rootly", "get_incident"), ResponseShape::TextProse);
    cache.observe(key("rootly", "get_incident"), ResponseShape::TextJson);
    assert_eq!(
        cache.lookup(&key("rootly", "get_incident")),
        Some(ResponseShape::TextJson)
    );

    // Reverse order produces the same stored value.
    let cache2 = ResponseShapeCache::new();
    cache2.observe(key("rootly", "get_incident"), ResponseShape::TextJson);
    cache2.observe(key("rootly", "get_incident"), ResponseShape::TextProse);
    assert_eq!(
        cache2.lookup(&key("rootly", "get_incident")),
        Some(ResponseShape::TextJson)
    );
}

#[test]
fn observe_promotes_text_plus_structured_to_mixed() {
    let cache = ResponseShapeCache::new();
    cache.observe(key("tool", "t"), ResponseShape::TextProse);
    cache.observe(key("tool", "t"), ResponseShape::Structured);
    assert_eq!(cache.lookup(&key("tool", "t")), Some(ResponseShape::Mixed));
}

#[test]
fn observations_are_keyed_per_tool() {
    // Critical: inspecting one tool must NOT populate another's
    // shape. Otherwise the hint on tool B would be wrong whenever
    // tool A was inspected first.
    let cache = ResponseShapeCache::new();
    cache.observe(key("s", "tool_a"), ResponseShape::TextJson);
    assert_eq!(
        cache.lookup(&key("s", "tool_a")),
        Some(ResponseShape::TextJson)
    );
    assert_eq!(cache.lookup(&key("s", "tool_b")), None);
}

#[test]
fn observations_are_keyed_per_server() {
    // Two upstreams might both expose a `search` tool; inspecting
    // `datadog.search` must not populate `glean.search`'s shape.
    let cache = ResponseShapeCache::new();
    cache.observe(key("datadog", "search"), ResponseShape::Structured);
    assert_eq!(cache.lookup(&key("glean", "search")), None);
}

#[test]
fn multi_block_observations_keep_max_length() {
    // The stored hint drives the "iterate bounded by N" suggestion;
    // a smaller N is less useful than a larger one we've actually
    // seen.
    let cache = ResponseShapeCache::new();
    cache.observe(key("s", "t"), ResponseShape::MultiBlock { len: 3 });
    cache.observe(key("s", "t"), ResponseShape::MultiBlock { len: 10 });
    assert_eq!(
        cache.lookup(&key("s", "t")),
        Some(ResponseShape::MultiBlock { len: 10 })
    );
}

#[test]
fn concurrent_clones_share_storage() {
    // Clones are Arc handles; writes through one are visible to the
    // others. This is the invariant that lets the handler factory
    // hand every session a fresh clone.
    let cache = ResponseShapeCache::new();
    let clone = cache.clone();
    clone.observe(key("s", "t"), ResponseShape::TextJson);
    assert_eq!(
        cache.lookup(&key("s", "t")),
        Some(ResponseShape::TextJson),
        "writes through a clone must be visible on the original"
    );
}

#[test]
fn concurrent_writes_on_same_key_merge_not_clobber() {
    // Property: two clones calling observe(same_key, different_shape)
    // concurrently must produce the merge of both shapes, not just
    // whichever thread won the write-lock race. A naive `insert`
    // instead of `entry(...).and_modify(...)` would silently drop one
    // of the two observations. TextProse + TextJson → TextJson under
    // the merge lattice regardless of ordering, making this a stable
    // assertion across scheduling orders.
    use std::sync::Arc as StdArc;
    use std::sync::Barrier;

    let cache = ResponseShapeCache::new();
    // Seed the key so both threads will hit the `and_modify` path.
    cache.observe(key("s", "t"), ResponseShape::TextProse);

    let barrier = StdArc::new(Barrier::new(2));

    let cache_a = cache.clone();
    let barrier_a = StdArc::clone(&barrier);
    let handle_a = std::thread::spawn(move || {
        barrier_a.wait(); // synchronise start to maximise overlap
        cache_a.observe(key("s", "t"), ResponseShape::TextProse);
    });

    let cache_b = cache.clone();
    let barrier_b = StdArc::clone(&barrier);
    let handle_b = std::thread::spawn(move || {
        barrier_b.wait();
        cache_b.observe(key("s", "t"), ResponseShape::TextJson);
    });

    handle_a.join().expect("thread a panicked");
    handle_b.join().expect("thread b panicked");

    // Regardless of which thread won the lock, TextProse merged with
    // TextJson is always TextJson — so this assertion is deterministic.
    assert_eq!(
        cache.lookup(&key("s", "t")),
        Some(ResponseShape::TextJson),
        "concurrent observe calls must merge, not clobber"
    );
}

#[test]
fn len_reflects_inserted_keys() {
    let cache = ResponseShapeCache::new();
    assert_eq!(cache.len(), 0);
    cache.observe(key("a", "x"), ResponseShape::TextProse);
    cache.observe(key("a", "y"), ResponseShape::TextProse);
    cache.observe(key("b", "x"), ResponseShape::TextProse);
    assert_eq!(cache.len(), 3);
    // Same key observed again doesn't grow the map.
    cache.observe(key("a", "x"), ResponseShape::TextJson);
    assert_eq!(cache.len(), 3);
}

/// The case set used by the lattice property tests. Matches the set
/// used in `response_shape::tests::merge_is_commutative` /
/// `merge_is_associative` so the cache-level and function-level
/// claims land on the same ground truth. Two `MultiBlock` lengths
/// are included because the len-max behaviour lives inside `merge`
/// and needs coverage here too.
const LATTICE_CASES: &[ResponseShape] = &[
    ResponseShape::TextProse,
    ResponseShape::TextJson,
    ResponseShape::MultiBlock { len: 5 },
    ResponseShape::MultiBlock { len: 3 },
    ResponseShape::Structured,
    ResponseShape::Mixed,
];

#[test]
fn observe_is_faithful_merge_over_all_pairs() {
    // Property: for every pair `(a, b)` in the lattice,
    // `observe(a); observe(b)` stores `merge(a, b)`. Pinned
    // exhaustively because the cache's value proposition —
    // monotonic, never-downgrading classification — rests on this
    // one equivalence. A naive `insert` in place of
    // `entry(...).and_modify(merge)` would silently break it for
    // the subset of pairs where `b` downgrades `a` (e.g. TextJson
    // then TextProse).
    for &a in LATTICE_CASES {
        for &b in LATTICE_CASES {
            let cache = ResponseShapeCache::new();
            cache.observe(key("s", "t"), a);
            cache.observe(key("s", "t"), b);
            let expected = response_shape::merge(a, b);
            assert_eq!(
                cache.lookup(&key("s", "t")),
                Some(expected),
                "observe({a:?}) then observe({b:?}) must store merge(a, b) = {expected:?}"
            );
        }
    }
}

#[test]
fn observe_chains_merge_over_three_observations() {
    // Property: for every triple `(a, b, c)`, observing them in
    // order stores `merge(merge(a, b), c)`. This is the real claim
    // in the module docstring — the cache faithfully applies the
    // lattice across arbitrary-length observation sequences, not
    // just pairs. Associativity is proved on `merge` itself in
    // `response_shape::tests::merge_is_associative`, so either
    // grouping is a valid expected value; we pick the left-fold
    // because that mirrors how the cache actually folds (each
    // `observe` calls `merge(existing, new)`).
    for &a in LATTICE_CASES {
        for &b in LATTICE_CASES {
            for &c in LATTICE_CASES {
                let cache = ResponseShapeCache::new();
                cache.observe(key("s", "t"), a);
                cache.observe(key("s", "t"), b);
                cache.observe(key("s", "t"), c);
                let expected = response_shape::merge(response_shape::merge(a, b), c);
                assert_eq!(
                    cache.lookup(&key("s", "t")),
                    Some(expected),
                    "observe sequence [{a:?}, {b:?}, {c:?}] must store merge-fold = {expected:?}"
                );
            }
        }
    }
}

#[test]
fn observe_is_monotonic_under_the_lattice() {
    // Property: every observation step produces a stored value that
    // is greater-than-or-equal-to the previous stored value under
    // the lattice. "Greater" here is defined operationally as
    // `merge(prev, stored) == stored` — i.e. adding `prev` back in
    // is a no-op. This is the durable phrasing of "never regresses"
    // and complements the pair/triple tests by asserting the
    // invariant holds at every intermediate step of a longer
    // sequence, not just at the end.
    use ResponseShape::*;
    let sequence = [
        TextProse,
        TextJson,
        MultiBlock { len: 3 },
        Structured,
        MultiBlock { len: 7 },
        TextProse,
        Mixed,
        TextProse,
    ];
    let cache = ResponseShapeCache::new();
    let mut prev: Option<ResponseShape> = None;
    for (i, &obs) in sequence.iter().enumerate() {
        cache.observe(key("s", "t"), obs);
        let now = cache
            .lookup(&key("s", "t"))
            .expect("value present after observe");
        if let Some(p) = prev {
            assert_eq!(
                response_shape::merge(p, now),
                now,
                "step {i}: stored {now:?} regressed from previous {p:?} after observing {obs:?}"
            );
        }
        prev = Some(now);
    }
}
