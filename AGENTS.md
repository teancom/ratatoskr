# AGENTS.md

Rust workspace: an MCP gateway/proxy. Crates: `gateway` (server, tools),
`mcp-client` (upstream connections, supervision), `lua-runtime`
(Luau `execute` sandbox).

## Commands

- Build/lint: `cargo clippy --workspace --all-targets` (warnings are not
  tolerated; workspace lints include `unwrap_used = "warn"`)
- Test: `cargo test --workspace`
- Format: `cargo fmt --all` (required before commit)
- Deploy: `scripts/deploy.sh` — validates, builds release, installs to
  `~/.local/bin/`, and restarts the launchd-managed gateway. This is the
  ONLY way a code change reaches the running daemon; `cargo build` alone
  never touches the live binary.

## Conventions

- Unit tests are NOT inline. Each module's tests live in a sibling file
  (`foo.rs` → `foo_tests.rs`; `mod.rs` → `tests.rs`), declared at the
  bottom of the module as:
  `#[cfg(test)] #[path = "foo_tests.rs"] mod tests;`
- Comments explain present-tense invariants and tradeoffs, not history
  ("used to be X") — changelogs belong in commit messages. Exception:
  warnings against a tempting-but-wrong approach.
- Never cite line numbers in comments — they rot silently as files
  grow. Reference the symbol, function, or section heading instead
  ("the `?` short-circuit in `ResourceListCache::get`", not
  "resource_list_cache.rs lines 152–155"). File paths are fine;
  file+line is not.
- Prefer `.expect("why")` over `.unwrap()`, even in tests.
- Perf decisions that reject an "obvious" optimization are documented
  where they live, with measurements (see subscription_registry.rs).

## Patterns worth spreading

Verified-good patterns to reuse when working near a candidate site.
Each names an exemplar by path + symbol (never line numbers).

Testing:

- **Error-variant tests check payloads, not just variants.** Use
  `match err { Variant { field } => assert_eq!(field, …), other =>
  panic!("expected Variant, got {other:?}") }` — not `.is_err()` or a
  bare `matches!`. Exemplar: `gateway/src/config_tests.rs`.
- **Oracle roundtrips for inverse operations.** Run a value through
  the forward op (the oracle), then the inverse, assert preservation.
  Exemplar: `mcp-client/src/template_matcher_tests.rs`
  (`roundtrip_*`). Apply to any encoder/decoder pair.
- **Sleeps only for negative assertions.** A `sleep` is legitimate
  only when proving "X did not happen" within a bounded window, and
  must carry a comment saying so; deterministic events get an
  `eventually()`-style condition wait. Exemplar:
  `gateway/tests/subscriptions.rs`.
- **Property-comment prefix on every integration test**: a short
  "this pins behavior X against regression Y" comment before setup.
- **Self-enforcing cross-reference tests** when two lists must agree
  across crates or languages (prose ↔ code, Lua VM state ↔ Rust
  constants): enumerate the canonical side, assert each entry appears
  on the drift-prone side. Prefer outright deduplication (share one
  constant) when both sides are Rust in the same crate. Match the
  specific form (backtick-wrapped needle), not a naked substring.
  Exemplars: `gateway/src/handler/execute_tests.rs` (builtin roster,
  both directions), `gateway/src/tools_schema_tests.rs`
  (`*_constructor_names_match_*`).
- **Negative data-leak assertions on summary/offload output**: feed
  realistic sensitive-looking values in, assert the literals are
  absent from the rendered output. Apply to anything whose contract
  is "shape/metadata only". Exemplar:
  `lua-runtime/src/schema_inferrer_tests.rs`
  (`format_never_includes_raw_data_only_types_and_counts`).

Source:

- **Structured module-level rationale comment** for modules with
  non-obvious design choices: one block, named sub-sections ("Why a
  bespoke X", "Scope bounds", "Invariant"), 3–8 lines each.
  Exemplar: `mcp-client/src/template_matcher.rs` header.
- **Functional Core / Imperative Shell**: pure validation/transform
  logic in its own module, testable without a VM or runtime; the
  shell handles I/O and conversions. Exemplar:
  `lua-runtime/src/parallel.rs` (core) + `run_parallel` in
  `runtime.rs` (shell).
- **`field: &'static str` in validation helpers** so error messages
  embed the field name with zero allocation and call sites must pass
  a literal. Exemplar: `take_non_empty_string` in
  `lua-runtime/src/parallel.rs`.
- **Drop the channel `Arc` immediately after `subscribe()`** in
  spawned tasks whose shutdown relies on channel close — a retained
  clone keeps the Sender alive forever. The comment on the `drop` is
  load-bearing. Exemplar: `spawn_invalidator` in
  `mcp-client/src/tool_list_cache.rs`.
- **Per-signal fallback in multi-signal handlers**: install SIGTERM
  and SIGINT independently; if either registration fails, warn and
  fall back to the other so shutdown never panics or hangs.
  Exemplar: `wait_for_shutdown_signal` in `gateway/src/serve.rs`.
- **Aggressive workspace lints** (`unsafe_code = "forbid"`,
  `unwrap_used`/`todo`/`dbg_macro = "warn"`): new crates inherit the
  workspace lints explicitly.
