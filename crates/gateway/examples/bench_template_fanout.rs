// pattern: Imperative Shell
//
// Diagnostic benchmark: measure `SubscriptionRegistry::template_sessions_for`
// throughput at larger-than-real-world template counts.
//
// The registry's template index is the `templates: Vec<TemplateEntry>`
// field in crates/gateway/src/subscription_registry.rs, and `template_sessions_for`
// linear-scans it on every incoming `ResourceUpdated`. At "2 templates per
// upstream" the scan is obviously fine; the open question is whether a 10x-50x
// growth in template count pushes the per-event cost past the "negligible"
// bar. Target: <1ms per event at 100 templates × 10 upstreams × 50 events;
// meeting that means document the measured ceiling and move on, missing it
// means shard by upstream first.
//
// Re-run with:
//
//     cargo run --release --example bench_template_fanout
//
// `--release` matters: debug-build measurements for a hash-lookup-plus-regex
// hot path are not representative of production. The binary refuses to
// produce a report when built without optimizations (see the
// `debug_assertions` guard) so a debug-profile accidental run can't seed
// the module docstring with misleading numbers.
//
// Design of the workload:
// - 10 upstreams (matching the real deployment's rough ceiling).
// - Each upstream carries 10 distinct templates → 100 `TemplateEntry`s total.
//   The template shapes cycle through a handful of realistic patterns
//   (`incident://{id}`, `projects/{p}/issues/{i}`, …) so the RFC 6570 matcher
//   exercises prefix, infix, and multi-variable paths.
// - 50 incoming `ResourceUpdated` events, each carrying an upstream + concrete
//   URI that matches exactly one template (forces the scan to evaluate every
//   same-upstream `matches()` before finding its hit — the worst-case shape
//   for the linear-scan design).
// - Warm-up pass before timing so the OS/allocator/CPU have reached steady
//   state.
// - The timed pass iterates `EVENTS_PER_PASS` events and collects per-event
//   durations. We then report min/p50/p99/max and the mean, plus the whole-
//   workload wall time. Simple `std::time::Instant` — no criterion.

use std::time::Instant;

use gateway::subscription_registry::{SubscriptionRegistry, UpstreamTemplateKey};
use mcp_client::template_matcher::CompiledTemplate;

const UPSTREAMS: usize = 10;
const TEMPLATES_PER_UPSTREAM: usize = 10;
const EVENTS_PER_PASS: usize = 50;
const WARMUP_PASSES: usize = 3;
const TIMED_PASSES: usize = 20;

/// A small library of template shapes exercised by the bench. The bench loops
/// these so each upstream ends up with 10 structurally different templates
/// rather than 10 near-copies. Level-1 and level-2 shapes only — the
/// CompiledTemplate compiler rejects level-3+ (see tests in template_matcher).
const TEMPLATE_SHAPES: &[&str] = &[
    "incident://{id}",
    "team://{team_id}",
    "projects/{project_id}/issues/{issue_iid}",
    "alert/{severity}/{id}",
    "service/{name}/metrics/{metric}",
    "user/{user_id}",
    "run/{run_id}/logs",
    "deploy/{env}/{sha}",
    "query/{q}",
    "dashboard/{id}/widget/{widget_id}",
];

/// Concrete URIs that match each shape above, in the same index order. Keeps
/// the bench honest: a concrete event's URI does match some template in the
/// registry, so the scan can't short-circuit on all-misses and look
/// artificially fast.
const CONCRETE_URIS: &[&str] = &[
    "incident://42",
    "team://eng-platform",
    "projects/my-proj/issues/1337",
    "alert/critical/9001",
    "service/auth/metrics/p99_latency",
    "user/u-12345",
    "run/run-abc/logs",
    "deploy/prod/deadbeef",
    "query/rate%28errors%29",
    "dashboard/d-1/widget/w-2",
];

fn main() {
    // Debug builds produce numbers that don't reflect production. Refuse to
    // report so a stale command history doesn't silently poison the
    // documented ceiling. The `return` keeps the rest of `main` statically
    // reachable in release builds (where the guard body is elided), so the
    // release compile doesn't flag unreachable code.
    if cfg!(debug_assertions) {
        eprintln!(
            "bench_template_fanout: debug build detected — rerun with \
             `cargo run --release --example bench_template_fanout` for \
             meaningful numbers. Refusing to report."
        );
        std::process::exit(2);
    }

    assert_eq!(
        TEMPLATE_SHAPES.len(),
        TEMPLATES_PER_UPSTREAM,
        "template shape library size must match TEMPLATES_PER_UPSTREAM"
    );
    assert_eq!(
        CONCRETE_URIS.len(),
        TEMPLATES_PER_UPSTREAM,
        "concrete URI library size must match the template library"
    );

    let reg = build_populated_registry();

    // The event stream we'll replay on every pass: 50 events round-robining
    // through upstreams and the matching concrete URI for each template shape
    // the upstream carries. This is the worst-case shape for a linear scan:
    // every event ends up touching every same-upstream entry before finding
    // the one that matches.
    let events: Vec<(String, String)> = (0..EVENTS_PER_PASS)
        .map(|i| {
            let upstream = format!("upstream-{}", i % UPSTREAMS);
            let uri = CONCRETE_URIS[i % TEMPLATES_PER_UPSTREAM].to_string();
            (upstream, uri)
        })
        .collect();

    // Warm-up: run the full timed workload a few times with the results
    // discarded. Lets the allocator, TLB, branch predictor settle.
    for _ in 0..WARMUP_PASSES {
        let _ = run_pass(&reg, &events);
    }

    // Timed: collect per-event durations across every pass. One pass = 50
    // events → TIMED_PASSES * EVENTS_PER_PASS samples total.
    let mut samples: Vec<u128> = Vec::with_capacity(TIMED_PASSES * EVENTS_PER_PASS);
    let wall_start = Instant::now();
    for _ in 0..TIMED_PASSES {
        let pass_samples = run_pass(&reg, &events);
        samples.extend(pass_samples);
    }
    let wall = wall_start.elapsed();

    report(&samples, wall);
}

/// Build a registry populated with UPSTREAMS × TEMPLATES_PER_UPSTREAM entries.
/// One session ID per entry to keep the session-set path non-trivial (empty
/// session sets are an impossible state per the module invariant, but
/// 1-session sets are the minimum real case).
fn build_populated_registry() -> SubscriptionRegistry {
    let reg = SubscriptionRegistry::new();
    for u in 0..UPSTREAMS {
        let upstream = format!("upstream-{u}");
        for (t, shape) in TEMPLATE_SHAPES.iter().enumerate() {
            let session = format!("sess-{u}-{t}");
            let compiled = CompiledTemplate::compile(shape)
                .unwrap_or_else(|e| panic!("bench template '{shape}' failed to compile: {e}"));
            let key = UpstreamTemplateKey::new(&upstream, *shape);
            reg.add_template(&session, key, compiled);
        }
    }
    reg
}

/// Run one pass of 50 events, returning the per-event wall-clock nanosecond
/// counts. Black-box prevents the optimizer from hoisting the work out when
/// the return value is unused in the warm-up phase (Instant::elapsed + Vec
/// extend keeps it honest for the timed phase).
fn run_pass(reg: &SubscriptionRegistry, events: &[(String, String)]) -> Vec<u128> {
    let mut out = Vec::with_capacity(events.len());
    for (upstream, uri) in events {
        let t0 = Instant::now();
        let sessions = reg.template_sessions_for(upstream, uri);
        let dt = t0.elapsed().as_nanos();
        std::hint::black_box(sessions);
        out.push(dt);
    }
    out
}

fn report(samples: &[u128], wall: std::time::Duration) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    let min = sorted[0];
    let max = sorted[n - 1];
    let p50 = sorted[n / 2];
    let p99 = sorted[(n * 99) / 100];
    let mean = sorted.iter().sum::<u128>() / (n as u128);

    let ns_per_us: f64 = 1_000.0;

    println!(
        "bench_template_fanout: {UPSTREAMS} upstreams × {TEMPLATES_PER_UPSTREAM} templates = {} entries",
        UPSTREAMS * TEMPLATES_PER_UPSTREAM
    );
    println!(
        "  {} events/pass × {} timed passes = {} samples",
        EVENTS_PER_PASS, TIMED_PASSES, n
    );
    println!("  wall time: {:.3} ms", wall.as_secs_f64() * 1_000.0);
    println!();
    println!("per-event duration (template_sessions_for, single call):");
    println!("  min:  {:8} ns  ({:.3} µs)", min, min as f64 / ns_per_us);
    println!("  p50:  {:8} ns  ({:.3} µs)", p50, p50 as f64 / ns_per_us);
    println!("  mean: {:8} ns  ({:.3} µs)", mean, mean as f64 / ns_per_us);
    println!("  p99:  {:8} ns  ({:.3} µs)", p99, p99 as f64 / ns_per_us);
    println!("  max:  {:8} ns  ({:.3} µs)", max, max as f64 / ns_per_us);
    println!();

    let one_ms_ns = 1_000_000_u128;
    if p99 < one_ms_ns {
        let ratio = (one_ms_ns as f64) / (p99.max(1) as f64);
        println!(
            "VERDICT: p99 ({:.3} µs) is {:.0}× below the 1 ms ceiling. \
             Linear scan at this scale is comfortably fine; the recommendation \
             is to document the measured ceiling and move on.",
            p99 as f64 / ns_per_us,
            ratio
        );
    } else {
        println!(
            "VERDICT: p99 ({:.3} µs) meets or exceeds the 1 ms ceiling. \
             the recommendation is to shard `templates` by upstream.",
            p99 as f64 / ns_per_us
        );
    }
}
