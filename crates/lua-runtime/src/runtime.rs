// pattern: Imperative Shell
//
// Owns the mlua Lua VM lifecycle for a single `execute` call.
//
// Dependency direction: this crate takes `Arc<UpstreamManager>` and
// `Arc<UpstreamHandle>` from `mcp-client` as concrete types, rather
// than consuming traits. This was a deliberate call, not an oversight:
//
//   1. There is one and only one implementation of an upstream
//      registry in the system. A trait with one implementer is
//      overhead with no payoff.
//   2. `UpstreamConfig` / `UpstreamHandle` are the *domain model* —
//      they describe what an "upstream MCP server" is — not a
//      vendor-specific shape to abstract over.
//   3. Testability doesn't need inversion: the unit tests in this
//      crate stand up real `UpstreamHandle`s over `tokio::io::duplex`
//      via `connect_with_transport`, giving us genuine handles at
//      near-zero cost. See the `empty_upstreams()` helper in the
//      tests module below.
//
// Inversion IS applied where the dependency direction would otherwise
// be reversed — see [`ExecutionResultBackend`] below, which exists
// because `gateway -> lua-runtime` is the allowed direction and
// storing results lives in the gateway crate.
//
// Pinned design decisions:
//
// - **Fresh VM per call.** `execute()` creates a new `mlua::Lua`, runs the
//   script, returns the result, drops the VM. No state bleeds between calls.
//
// - **Luau sandbox.** `lua.sandbox(true)` after we've injected the upstream
//   globals. Freezes globals into readonly tables — a script cannot
//   reassign `fake_upstream` to something else, cannot reach `os.execute`,
//   etc. Stronger than a blocklist.
//
// - **No mlua async.** We deliberately don't enable the `async` mlua feature
//   (known-deadlock history documented in the mluau fork's README). Host
//   functions look synchronous to Lua; internally they block on a tokio
//   handle. Since the whole VM runs inside `spawn_blocking`, blocking here
//   is safe — we don't stall any async worker thread.
//
// - **`result(value)`** is how scripts yield their output. The host function
//   writes into an `Arc<Mutex<Option<Value>>>`. If the script doesn't call
//   `result`, we return `MissingResult` rather than silently claiming
//   success on a script that forgot to do its job.
//
// - **Instruction budget.** Luau's `set_interrupt` fires periodically during
//   execution; we increment a counter and error out past budget. Default
//   budget is generous (1_000_000 instructions, which is plenty for a
//   legitimate script that awaits an upstream tool call and formats text).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use mcp_client::{ToolListCache, UpstreamHandle, UpstreamManager};
use mlua::{Error as LuaError, Lua, Table, Value, VmState};
use rmcp::model::{CallToolRequestParams, CallToolResult};
use tokio::runtime::Handle;

use crate::error::LuaExecutionError;
use crate::identifier::{partition_unique_by_sanitized, sanitize_lua_identifier};
use crate::json_shape::describe_json_shape;

/// Predicate the runtime calls before dispatching each upstream tool
/// call. Returns `true` if the call is allowed, `false` if it should
/// be rejected with `LuaExecutionError::NotInspected`.
///
/// Parameters are the Lua-sanitized server and tool names (the pair
/// the Lua script actually writes). Leaving this as a plain closure
/// rather than a trait keeps the runtime unaware of inspection
/// semantics — the gateway decides what "allowed" means.
///
/// A `None` guard means "allow everything" — the gateway passes `None`
/// for contexts that predate the inspection feature (e.g. unit tests
/// that exercise the Lua bridge directly).
pub type ToolCallGuard = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

/// Backend for large-result offloading. The runtime doesn't know
/// anything about how results are stored or how execution IDs are
/// generated; the gateway implements this trait over its
/// `ExecutionResultStore`. Keeps the crate dependency direction
/// clean: gateway -> runtime, not the other way around.
pub trait ExecutionResultBackend: Send + Sync {
    /// Persist `payload` (serialized JSON) and return a retrieval ID.
    /// Errors bubble back as `LuaExecutionError::OffloadFailed` so
    /// the caller sees a clear diagnostic ("result was too large
    /// even for offload").
    fn save(&self, payload: String) -> Result<String, String>;

    /// Fetch a previously-stored payload by ID. Returns `None` for
    /// unknown IDs or cross-session lookups; the runtime surfaces
    /// this to Lua as a plain `nil` return so scripts can branch
    /// on availability rather than catching an exception.
    fn get(&self, id: &str) -> Option<String>;
}

/// Configuration for the large-result offload path, passed to
/// [`LuaRuntime::execute`]. When `None` (or when `threshold == 0`),
/// offload is disabled and the full result is returned as usual.
///
/// `backend` is shared with the caller so follow-up `execute` calls
/// can retrieve via `_gateway.get_result(id)` — the store persists
/// across Lua VMs for the same session.
#[derive(Clone)]
pub struct OffloadConfig {
    pub threshold_bytes: usize,
    pub backend: Arc<dyn ExecutionResultBackend>,
}

/// Outcome of `LuaRuntime::execute`. Either the full JSON value
/// (small results, or offload disabled), or a summary that points at
/// the stored full payload (large results).
///
/// The gateway translates `Offloaded` into a human-readable
/// `CallToolResult` whose text describes the data's shape and tells
/// the agent how to retrieve it via `_gateway.get_result(id)` in a
/// follow-up script. The full `bytes`/`shape` payload never leaves
/// the gateway as part of the MCP response.
#[derive(Debug, Clone)]
pub enum ExecutionOutcome {
    Direct(serde_json::Value),
    Offloaded {
        execution_id: String,
        byte_size: usize,
        shape: crate::schema_inferrer::Shape,
    },
}

/// Default per-execution instruction budget. Generous enough for legitimate
/// scripts (tool call + small formatting), tight enough to cut off a
/// `while true do end` within milliseconds.
pub const DEFAULT_INSTRUCTION_BUDGET: u64 = 1_000_000;

/// Default per-execute wall-clock deadline.
///
/// Bounds the worst case when an upstream tool call stalls: without this,
/// a `handle.block_on(future_that_never_completes)` pins a blocking
/// worker thread forever, and a hung upstream call must never be allowed
/// to leave a zombie thread pinned indefinitely. 60 s is far longer than
/// any legitimate upstream call path we've measured, so a real timeout
/// here points at a misbehaving upstream rather than a user scripting
/// pattern worth accommodating.
pub const DEFAULT_EXECUTE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

/// Runtime façade. Today it's zero-state (all state lives per-execute).
/// Kept as a type so future config (budget, memory limit, logging hooks)
/// has a natural home without rewriting call sites.
pub struct LuaRuntime {
    instruction_budget: u64,
    execute_deadline: std::time::Duration,
}

impl LuaRuntime {
    pub fn new() -> Self {
        Self {
            instruction_budget: DEFAULT_INSTRUCTION_BUDGET,
            execute_deadline: DEFAULT_EXECUTE_DEADLINE,
        }
    }

    /// Override the per-execution instruction budget. Useful for tests that
    /// want to prove the budget actually fires.
    pub fn with_instruction_budget(mut self, budget: u64) -> Self {
        self.instruction_budget = budget;
        self
    }

    /// Override the per-execute wall-clock deadline. Tests use a short
    /// value to prove the timeout actually fires without slowing the
    /// suite; production uses [`DEFAULT_EXECUTE_DEADLINE`].
    pub fn with_execute_deadline(mut self, deadline: std::time::Duration) -> Self {
        self.execute_deadline = deadline;
        self
    }

    /// Execute a Lua script. Returns the value the script passed to
    /// `result(...)`, serialized as JSON.
    ///
    /// `upstreams` may be empty; each registered upstream becomes a Lua
    /// global keyed by its `lua_name`, with one function per upstream tool
    /// exposed as `upstream_global.lua_tool_name(args)` (the tool name is
    /// sanitized so it's a valid Lua identifier).
    ///
    /// `guard` is optional. When `Some`, the runtime calls it before each
    /// upstream tool dispatch; rejection produces
    /// `LuaExecutionError::NotInspected`. When `None`, every call is
    /// allowed.
    ///
    /// `tool_cache` is optional: when `Some`, tool-list fetches during
    /// global injection go through the cache (single-flight + bus-driven
    /// invalidation); when `None`, the runtime falls back to a fresh
    /// `list_tools` RPC on every call. Production always passes `Some`;
    /// unit tests that bypass the gateway pass `None`.
    ///
    /// `offload` is optional: when `Some` with a non-zero threshold,
    /// large results are persisted via the backend and the return shape
    /// is [`ExecutionOutcome::Offloaded`] with shape metadata. When
    /// `None` (or threshold == 0), the full value is always returned.
    /// The same `backend` is used to install the `_gateway.get_result`
    /// Lua builtin, so follow-up scripts in the same session can
    /// retrieve prior results.
    ///
    /// `cancel` is a cooperative cancellation signal. When it fires —
    /// because the downstream MCP client cancelled the request, or the
    /// gateway is shutting down — the runtime aborts promptly with
    /// [`LuaExecutionError::Cancelled`] rather than pinning a blocking
    /// worker thread on a future that will never complete. Every
    /// `handle.block_on(...)` inside the blocking worker races against
    /// this token, so in-flight upstream calls unwind instead of
    /// leaking. The worker thread also respects the runtime's
    /// [`Self::with_execute_deadline`] hard deadline; whichever fires
    /// first wins.
    pub async fn execute(
        &self,
        script: String,
        upstreams: Arc<UpstreamManager>,
        guard: Option<ToolCallGuard>,
        tool_cache: Option<Arc<ToolListCache>>,
        offload: Option<OffloadConfig>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<ExecutionOutcome, LuaExecutionError> {
        let budget = self.instruction_budget;
        let deadline = self.execute_deadline;

        // Snapshot the current tokio handle. spawn_blocking runs on a blocking
        // thread that still has access to this handle via Handle::current() —
        // but we capture it explicitly to make the bridge code's dependency
        // visible rather than relying on thread-local state.
        let handle = Handle::current();

        let script_for_task = script;
        let cancel_for_task = cancel.clone();

        // The blocking worker owns its own clone of the cancel token so
        // each `handle.block_on` inside can race the token against the
        // upstream future. Meanwhile the outer select! here guarantees
        // the caller sees a prompt `Err(Cancelled)` / `Err(Timeout)`
        // even if, for some reason, the worker is still winding down.
        let join = tokio::task::spawn_blocking(move || {
            run_script_blocking(
                script_for_task,
                upstreams,
                guard,
                tool_cache,
                offload,
                handle,
                budget,
                cancel_for_task,
            )
        });

        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(LuaExecutionError::Cancelled),
            _ = tokio::time::sleep(deadline) => Err(LuaExecutionError::Timeout {
                deadline_ms: deadline.as_millis() as u64,
            }),
            joined = join => joined
                .map_err(|e| LuaExecutionError::Internal(format!("spawn_blocking join failed: {e}")))?,
        }
    }
}

impl Default for LuaRuntime {
    fn default() -> Self {
        Self::new()
    }
}

// ---------- cancel-bailout macros ----------
//
// `macro_rules!` is lexical-scope, so these live above every caller.
// They name one of the three layers of the cancellation defense (outer
// `select!` in `execute`, `block_on_cancellable` at every `block_on`
// site, entry fast-path at each Lua host-function and at the top of
// `run_script_blocking`). The fast-path layer appears three times with
// two different return shapes (domain error vs. mlua-external-wrapped);
// each shape gets one macro so call sites stay one line without hiding
// the return statement.

/// Fast-path cancel check used at the entry of `run_script_blocking`.
///
/// Why a macro rather than a function: we want a bare `return` from the
/// caller's frame, not a value a caller must `?`-propagate. A function
/// returning `Result<(), LuaExecutionError>` at the call site would read
/// as `check_cancelled(&cancel)?;`, which is fine — but the whole point
/// of this fast-path is that it's a `cancel.is_cancelled()` check in the
/// hot path, and three sites across this file all share the same two-line
/// shape. Naming the shape makes the cancellation posture (a three-layer
/// defense — outer `select!`, `block_on_cancellable`, entry fast-path)
/// visible and greppable.
///
/// This macro is the `run_script_blocking`-flavoured variant that returns
/// the domain error directly (no `LuaError::external` wrapping). For the
/// mlua host-function variant that must wrap, see [`bail_on_cancel_lua`].
macro_rules! bail_on_cancel {
    ($cancel:expr) => {
        if $cancel.is_cancelled() {
            return Err($crate::error::LuaExecutionError::Cancelled);
        }
    };
}

/// Fast-path cancel check used at the entry of mlua host-function
/// bridges (`call_upstream_tool_sync`, `run_parallel`). Wraps the domain
/// error in `LuaError::external` so it flows through mlua's FFI boundary
/// and surfaces at the script site as a catchable error object.
///
/// See [`bail_on_cancel`] for the run-time-internal variant and the
/// overall rationale for having a named shape.
macro_rules! bail_on_cancel_lua {
    ($cancel:expr) => {
        if $cancel.is_cancelled() {
            return Err(::mlua::Error::external(
                $crate::error::LuaExecutionError::Cancelled,
            ));
        }
    };
}

// ---------- blocking body ----------

// Installation order contract for `run_script_blocking`:
//
// 1. **`install_result_capture`** — sets up the `result()` global and its
//    backing slot before anything else runs.
//
// 2. **`install_interrupt`** — sets up the instruction-budget hook.
//    Must run before `lua.load(...).exec()`, but order relative to
//    the other installs doesn't matter.
//
// 3. **`install_gateway_builtins`** — populates the `_gateway` table.
//    The `parallel` builtin captures `Arc<UpstreamManager>` but resolves
//    upstreams by name at call time, so it's safe to install before the
//    upstream globals are ready.
//
// 4. **`install_upstream_globals`** — installs one Lua global per
//    upstream (e.g. `gitlab`, `rootly`). These land on `lua.globals()`
//    directly, not inside `_gateway`, so their relative order to step 3
//    is flexible. The `block_on(upstreams.list())` fetch must happen on
//    the spawn_blocking thread; it's safe to block here because we're
//    already off the async executor.
//
// 5. **`lua.sandbox(true)`** — freezes the parent globals table into a
//    readonly environment. This MUST run last: any global set after
//    sandbox() lands in the mutable sandbox-thread env and scripts could
//    rebind it (e.g. `gitlab = nil`).

// Internal entry point. Every argument here is context already held by
// `LuaRuntime::execute` — collapsing into a struct would just add a
// rename layer, so we accept the `too_many_arguments` lint for this one
// crate-private function.
#[allow(clippy::too_many_arguments)]
fn run_script_blocking(
    script: String,
    upstreams: Arc<UpstreamManager>,
    guard: Option<ToolCallGuard>,
    tool_cache: Option<Arc<ToolListCache>>,
    offload: Option<OffloadConfig>,
    handle: Handle,
    budget: u64,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<ExecutionOutcome, LuaExecutionError> {
    // Fast-path: if the caller has already cancelled before we even
    // started, don't bother spinning up a VM.
    bail_on_cancel!(cancel);

    let lua = Lua::new();

    // Installation order matters: inject everything first, then sandbox
    // freezes the parent globals into readonly tables. If we sandboxed
    // first, our globals would land in the mutable sandbox-thread env
    // and scripts could rebind them (e.g. `fake_upstream = nil`).
    let result_slot = install_result_capture(&lua)?;
    install_interrupt(&lua, budget)?;
    // `_gateway` always gets installed — `expand_template` is available
    // in every script regardless of offload configuration. `get_result`
    // is only installed when a backend is present (it needs one to
    // retrieve).
    install_gateway_builtins(
        &lua,
        offload.as_ref().map(|o| Arc::clone(&o.backend)),
        Arc::clone(&upstreams),
        tool_cache.clone(),
        guard.clone(),
        &handle,
        cancel.clone(),
    )?;

    // Snapshot the set of upstream configs once. We block on this because
    // we're already inside spawn_blocking; doing it up front keeps the
    // global-injection loop synchronous and self-contained.
    //
    // The fetch races the cancel token so a cancelled request doesn't
    // pin the blocking worker on an upstream-manager lock.
    let upstream_snapshot = match block_on_cancellable(&handle, &cancel, upstreams.list()) {
        Ok(list) => list,
        Err(BlockOnCancelled) => return Err(LuaExecutionError::Cancelled),
    };

    install_upstream_globals(
        &lua,
        &upstream_snapshot,
        &upstreams,
        guard.as_ref(),
        tool_cache.as_ref(),
        &handle,
        &cancel,
    )?;

    // Sandbox AFTER injection so our globals are part of the frozen parent
    // environment (scripts cannot rebind them).
    lua.sandbox(true)
        .map_err(|e| LuaExecutionError::Internal(format!("failed to enable luau sandbox: {e}")))?;

    // Execute the user script.
    let exec_result = lua.load(script.as_str()).set_name("user_script").exec();

    if let Err(lua_err) = exec_result {
        return Err(map_lua_error(lua_err));
    }

    let captured = result_slot
        .lock()
        .map_err(|_| LuaExecutionError::Internal("result slot mutex poisoned".to_string()))?
        .take();

    let value = captured.ok_or(LuaExecutionError::MissingResult)?;

    // Offload decision. Measure the serialized byte length once; if it
    // crosses the threshold (and offload is configured at all), persist
    // the payload and return a shape summary. Otherwise hand back the
    // raw value unchanged.
    if let Some(off) = offload.as_ref()
        && off.threshold_bytes > 0
    {
        let payload = serde_json::to_string(&value).map_err(|e| {
            LuaExecutionError::Internal(format!(
                "failed to serialize result for offload check: {e}"
            ))
        })?;
        if payload.len() > off.threshold_bytes {
            let byte_size = payload.len();
            let shape = crate::schema_inferrer::infer_shape(
                &value,
                crate::schema_inferrer::DEFAULT_MAX_DEPTH,
            );
            let execution_id = off.backend.save(payload).map_err(|e| {
                // Offload save failing means either the store is
                // poisoned (mutex) or disk/memory resource pressure.
                // Either way, the agent is about to see an error that
                // doesn't explain itself — operator needs the real
                // reason in the log.
                tracing::error!(
                    bytes = byte_size,
                    error = %e,
                    "execute result offload failed; agent will receive OffloadFailed"
                );
                LuaExecutionError::OffloadFailed(e)
            })?;
            tracing::info!(
                execution_id = %execution_id,
                bytes = byte_size,
                "execute result offloaded"
            );
            return Ok(ExecutionOutcome::Offloaded {
                execution_id,
                byte_size,
                shape,
            });
        }
    }

    Ok(ExecutionOutcome::Direct(value))
}

/// Sentinel returned by [`block_on_cancellable`] when the cancel token
/// fired before the future resolved. Callers translate this into the
/// appropriate domain error (either [`LuaExecutionError::Cancelled`] at
/// the top level or `LuaError::external(...)` wrapped form inside a
/// host function bridge).
///
/// Kept as a zero-size marker type so `Result<T, BlockOnCancelled>` is
/// exactly as cheap as a `Result<T, ()>` while reading more
/// self-documentingly at call sites.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockOnCancelled;

/// Run `fut` to completion on `handle`, returning early if `cancel`
/// fires. This is the load-bearing cancellation primitive for the
/// blocking worker: every `handle.block_on` goes through here so a
/// cancelled downstream request doesn't pin a blocking thread on an
/// upstream future that will never resolve.
///
/// Returns `Ok(T)` on normal completion, `Err(BlockOnCancelled)` when
/// the token fires first. The cancel arm is `biased` so cancellation
/// wins over a future that happens to have produced a value at the
/// same instant — prompt unwind over eventual-consistency.
pub(crate) fn block_on_cancellable<F, T>(
    handle: &Handle,
    cancel: &tokio_util::sync::CancellationToken,
    fut: F,
) -> Result<T, BlockOnCancelled>
where
    F: std::future::Future<Output = T>,
{
    handle.block_on(async move {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(BlockOnCancelled),
            v = fut => Ok(v),
        }
    })
}

/// Create a Lua function from `f` and register it as `name` on `table`.
///
/// Centralises the boilerplate that every `_gateway.*` builtin registration
/// shares: `lua.create_function` + error-wrap + `table.set` + error-wrap.
/// Using a named helper means both error messages reference `name` from a
/// single source rather than duplicating the format string seven times —
/// a typo or renaming is caught once.
///
/// Builtins that capture extra state (`parallel`, `get_result`) keep their
/// inline form because they need a custom closure that can't be expressed
/// as a plain `Fn(&Lua, Value) -> mlua::Result<Value>`.
fn install_builtin<F>(
    lua: &Lua,
    table: &Table,
    name: &'static str,
    f: F,
) -> Result<(), LuaExecutionError>
where
    F: Fn(&Lua, Value) -> mlua::Result<Value> + 'static + Send,
{
    let func = lua
        .create_function(move |lua_ref, args: Value| f(lua_ref, args))
        .map_err(|e| LuaExecutionError::Internal(format!("create {name} fn: {e}")))?;
    table
        .set(name, func)
        .map_err(|e| LuaExecutionError::Internal(format!("set {name}: {e}")))
}

/// Install the `_gateway` global with builtins available to every
/// `execute` script.
///
/// Always-on builtins:
///
/// - **`_gateway.expand_template({server, template, params})`**
///   Delegates to the Functional Core in
///   [`crate::expand_template::expand_and_namespace`]. Agents call it
///   to instantiate upstream resource templates (Level 1-4 RFC 6570)
///   and get back a `mcp-proxy://…` URI ready for `resources/read`.
///
/// - **`_gateway.json_decode(text)`**
///   Parses a JSON string into a Lua value (table / number / string /
///   boolean / nil). Standard Lua has no JSON support in the Luau
///   sandbox; without this builtin, agents parsing a JSON response
///   have to fall back to regex-over-string scaffolding. This
///   removes that entire class of scaffolding.
///
/// - **`_gateway.json_encode(value)`**
///   Serializes any `result(...)`-compatible Lua value to a JSON
///   string. Uses the same dense-integer-keys-are-arrays heuristic as
///   the result capture path so output matches what agents see from
///   upstream tool calls.
///
/// - **`_gateway.unwrap_content(value)`**
///   MCP tool-call responses ship inside `{content:[{type:"text",
///   text:"..."}], ...}`. Upstreams that passthrough JSON inside
///   `text` force agents to write `x.content[1].text` every time —
///   this is common across unrelated upstreams (GitLab
///   `download_job_log`, Unifi `unifi_execute`, …), so this builtin
///   removes that boilerplate. It returns the first text block
///   when the input matches that shape, and returns the input
///   unchanged otherwise. Callers can always wrap upstream results
///   in `unwrap_content(...)` without type-checking first.
///
/// - **`_gateway.rfc3339(unix_seconds)`** and
///   **`_gateway.now_minus(seconds)`**
///   RFC3339-UTC string formatting helpers. Agents querying
///   time-windowed upstreams (Loki, Datadog, Grafana) reinvented
///   `os.date("!%Y-%m-%dT%H:%M:%SZ", ...)` and
///   `os.date("!%Y-%m-%dT%H:%M:%SZ", os.time() - 3600)` across
///   sessions. Shipping them as builtins kills that scaffolding.
///   Output is always the canonical `YYYY-MM-DDTHH:MM:SSZ` form.
///
/// - **`_gateway.parallel(tasks)`**
///   Dispatch a batch of upstream tool calls concurrently. Each task is
///   a `{ server, tool, args }` table; the return is an array of
///   `{ ok = true, value = ... } | { ok = false, error = "..." }`
///   entries in input order. Inspection enforcement runs up-front — any
///   un-inspected `(server, tool)` pair aborts the whole call. Per-task
///   upstream failures become `{ ok = false, error }` envelopes so a
///   single bad call doesn't kill the batch. Used by scripts that need
///   to fan out (bulk-fetch N MRs, correlate incident signals across
///   Rootly + GitLab + Datadog, etc.).
///
/// Conditional builtin:
///
/// - **`_gateway.get_result({id})`** is installed only when `backend`
///   is `Some`. It pulls a previously-offloaded execution result back
///   into the script. Without a backend there's nothing to retrieve,
///   and an always-nil version would be strictly worse than absence
///   (an agent would write retrieval code that silently no-ops).
///
/// Argument shape conventions:
///
/// - `expand_template` / `get_result` take named-key tables
///   (`{server=..., template=...}` / `{id=...}`) because they have
///   multiple inputs or benefit from future extensibility.
/// - `json_decode` / `json_encode` take the value directly (a string
///   to decode, any value to encode) because they're single-arg
///   operations; wrapping in a table would be noise.
///
/// Missing/wrong-shape args produce a clear error — each builtin
/// validates its own input and embeds the builtin name in the
/// message so `pcall(...)` callers see which call failed.
///
/// Installation order is load-bearing: all builtins must go in
/// BEFORE `lua.sandbox(true)` so they land in the frozen parent
/// globals and scripts can't rebind them.
/// The roster of every `_gateway.*` builtin [`install_gateway_builtins`]
/// can install, including the conditional `get_result`.
///
/// This is the shared contract between the runtime and the gateway's
/// agent-facing `execute` tool description: the description enumerates
/// exactly this roster (in its own deliberately different, agent-tuned
/// prose), and tests on both sides pin roster agreement in both
/// directions. A test in this crate pins that this list matches what
/// `install_gateway_builtins` actually installs, so adding or removing
/// a builtin without updating the roster (and, transitively, the
/// description) fails loudly.
pub const GATEWAY_BUILTIN_NAMES: &[&str] = &[
    "expand_template",
    "json_decode",
    "json_encode",
    "unwrap_content",
    "rfc3339",
    "now_minus",
    "parallel",
    "get_result",
];

#[allow(clippy::too_many_arguments)] // Bundles every piece of context the
// parallel builtin needs (upstreams, tool cache, guard, tokio handle).
// Collapsing them into a struct is noise for a function that's called once.
fn install_gateway_builtins(
    lua: &Lua,
    backend: Option<Arc<dyn ExecutionResultBackend>>,
    upstreams: Arc<UpstreamManager>,
    tool_cache: Option<Arc<ToolListCache>>,
    guard: Option<ToolCallGuard>,
    handle: &Handle,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), LuaExecutionError> {
    let table = lua
        .create_table()
        .map_err(|e| LuaExecutionError::Internal(format!("create _gateway table: {e}")))?;

    // Six stateless builtins share the same 4-line scaffold
    // (create_function → map_err → table.set → map_err). The private helper
    // `install_builtin` collapses that boilerplate to one line per builtin so
    // a typo in either error message is caught in one place, not seven.
    //
    // `parallel` and `get_result` keep their inline form because they capture
    // extra state (upstreams, tool cache, guard, handle, backend).
    install_builtin(lua, &table, "expand_template", move |lua_ref, args| {
        run_expand_template(lua_ref, args)
    })?;
    install_builtin(lua, &table, "json_decode", |lua_ref, args| {
        run_json_decode(lua_ref, args)
    })?;
    install_builtin(lua, &table, "json_encode", |lua_ref, args| {
        run_json_encode(lua_ref, args)
    })?;
    install_builtin(lua, &table, "unwrap_content", |_lua_ref, args| {
        Ok(run_unwrap_content(args))
    })?;
    install_builtin(lua, &table, "rfc3339", |lua_ref, args| {
        run_rfc3339(lua_ref, args)
    })?;
    install_builtin(lua, &table, "now_minus", |lua_ref, args| {
        run_now_minus(lua_ref, args)
    })?;

    // `parallel` — always installed. Captures the same upstream-
    // dispatch context as `install_upstream_globals`, so a script can
    // fire N upstream calls concurrently rather than one at a time.
    // Input validation lives in `parallel.rs`; the closure body does
    // only what requires Lua or I/O — table-to-JSON conversion,
    // resolution, dispatch, result conversion.
    let upstreams_for_parallel = Arc::clone(&upstreams);
    let tool_cache_for_parallel = tool_cache.clone();
    let guard_for_parallel = guard.clone();
    // Clone the handle here, at the capture site, where the reason is visible:
    // the parallel closure must own a Handle to call block_on inside Lua's FFI.
    let handle_for_parallel = handle.clone();
    let cancel_for_parallel = cancel.clone();
    let parallel_fn = lua
        .create_function(move |lua_ref, args: Value| -> mlua::Result<Value> {
            run_parallel(
                lua_ref,
                args,
                &upstreams_for_parallel,
                tool_cache_for_parallel.as_ref(),
                guard_for_parallel.as_ref(),
                &handle_for_parallel,
                &cancel_for_parallel,
            )
        })
        .map_err(|e| LuaExecutionError::Internal(format!("create parallel fn: {e}")))?;
    table
        .set("parallel", parallel_fn)
        .map_err(|e| LuaExecutionError::Internal(format!("set parallel: {e}")))?;

    if let Some(backend) = backend {
        let backend_for_fn = Arc::clone(&backend);
        let get_result_fn = lua
            .create_function(move |lua_ref, args: Value| -> mlua::Result<Value> {
                // Accept either {id="..."} or bare "..." for one-off scripts
                // that just paste the ID without wrapping it. The table form
                // is the documented path; the string fallback is an
                // ergonomic convenience.
                let id = match args {
                    Value::Table(t) => t
                        .get::<Option<String>>("id")
                        .map_err(|e| LuaError::external(LuaExecutionError::Runtime(
                            format!("_gateway.get_result: failed to read 'id' field: {e}"),
                        )))?
                        .ok_or_else(|| {
                            LuaError::external(LuaExecutionError::Runtime(
                                "_gateway.get_result: argument table must have an 'id' field".to_string(),
                            ))
                        })?,
                    Value::String(s) => s
                        .to_str()
                        .map_err(|e| LuaError::external(LuaExecutionError::Runtime(
                            format!("_gateway.get_result: non-utf8 id: {e}"),
                        )))?
                        .to_string(),
                    other => {
                        return Err(LuaError::external(LuaExecutionError::Runtime(format!(
                            "_gateway.get_result: expected a table {{ id = \"...\" }} or a string id, got {other:?}"
                        ))));
                    }
                };

                match backend_for_fn.get(&id) {
                    Some(serialized) => {
                        let decoded: serde_json::Value = serde_json::from_str(&serialized)
                            .map_err(|e| {
                                LuaError::external(LuaExecutionError::Internal(format!(
                                    "stored offloaded result failed to parse as JSON: {e}"
                                )))
                            })?;
                        json_to_lua_value(lua_ref, &decoded)
                    }
                    None => Ok(Value::Nil),
                }
            })
            .map_err(|e| LuaExecutionError::Internal(format!("create get_result fn: {e}")))?;

        table
            .set("get_result", get_result_fn)
            .map_err(|e| LuaExecutionError::Internal(format!("set get_result: {e}")))?;
    }

    lua.globals()
        .set("_gateway", table)
        .map_err(|e| LuaExecutionError::Internal(format!("set _gateway global: {e}")))?;

    Ok(())
}

/// Host-side body of `_gateway.expand_template`. Extracts the
/// `{server, template, params}` table from Lua, converts params to
/// JSON via the existing `lua_value_to_json` path, calls the
/// Functional Core (`expand_template::expand_and_namespace`), and
/// returns the resulting namespaced URI string as a Lua value.
///
/// Any Rust-side error becomes a `LuaExecutionError::Runtime` wrapping
/// the structured error message, so scripts that want to catch the
/// failure via `pcall` get a readable `err.message`.
fn run_expand_template(lua: &Lua, args: Value) -> mlua::Result<Value> {
    let table = match args {
        Value::Table(t) => t,
        other => {
            return Err(LuaError::external(LuaExecutionError::Runtime(format!(
                "_gateway.expand_template: expected a table \
                 {{ server=\"...\", template=\"...\", params={{...}} }}, got {other:?}"
            ))));
        }
    };

    let server = table
        .get::<Option<String>>("server")
        .map_err(|e| {
            LuaError::external(LuaExecutionError::Runtime(format!(
                "_gateway.expand_template: failed to read 'server' field: {e}"
            )))
        })?
        .ok_or_else(|| {
            LuaError::external(LuaExecutionError::Runtime(
                "_gateway.expand_template: missing required 'server' field".to_string(),
            ))
        })?;

    let template = table
        .get::<Option<String>>("template")
        .map_err(|e| {
            LuaError::external(LuaExecutionError::Runtime(format!(
                "_gateway.expand_template: failed to read 'template' field: {e}"
            )))
        })?
        .ok_or_else(|| {
            LuaError::external(LuaExecutionError::Runtime(
                "_gateway.expand_template: missing required 'template' field".to_string(),
            ))
        })?;

    // `params` is optional — a template with no placeholders doesn't
    // need any. Default to an empty object when absent so the FC gets
    // a consistent shape. When the caller passes `params = nil`
    // explicitly, we see `Value::Nil` here and also take the default.
    let params_value: Value = table.get("params").map_err(|e| {
        LuaError::external(LuaExecutionError::Runtime(format!(
            "_gateway.expand_template: failed to read 'params' field: {e}"
        )))
    })?;
    let params_json = match params_value {
        Value::Nil => serde_json::Value::Object(serde_json::Map::new()),
        other => lua_value_to_json(lua, other).map_err(|err| {
            LuaError::external(LuaExecutionError::Runtime(format!(
                "_gateway.expand_template: failed to convert 'params' to JSON: {err}"
            )))
        })?,
    };

    let namespaced = crate::expand_template::expand_and_namespace(&server, &template, &params_json)
        .map_err(|e| {
            LuaError::external(LuaExecutionError::Runtime(format!(
                "_gateway.expand_template: {e}"
            )))
        })?;

    Ok(Value::String(lua.create_string(namespaced.as_str())?))
}

/// Host-side body of `_gateway.json_decode(text)`.
///
/// Takes a single Lua string, parses it as JSON, and converts the
/// resulting `serde_json::Value` into a Lua value via the existing
/// `json_to_lua_value` path. Empty strings, malformed JSON, and
/// non-string args all produce a `Runtime` error naming the builtin
/// — callers can `pcall` and read `.message` to branch.
///
/// JSON `null` becomes Lua `nil`, which means a top-level `null`
/// payload is indistinguishable from "decode returned no value." We
/// accept that ambiguity: it's the same ambiguity Lua itself has,
/// and real-world JSON payloads almost never ship a bare `null` as
/// the top-level document.
fn run_json_decode(lua: &Lua, args: Value) -> mlua::Result<Value> {
    let text = match args {
        Value::String(s) => s
            .to_str()
            .map_err(|e| {
                LuaError::external(LuaExecutionError::Runtime(format!(
                    "_gateway.json_decode: non-utf8 input: {e}"
                )))
            })?
            .to_string(),
        other => {
            return Err(LuaError::external(LuaExecutionError::Runtime(format!(
                "_gateway.json_decode: expected a string, got {other:?}"
            ))));
        }
    };

    let parsed: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
        LuaError::external(LuaExecutionError::Runtime(format!(
            "_gateway.json_decode: failed to parse JSON: {e}"
        )))
    })?;

    json_to_lua_value(lua, &parsed)
}

/// Host-side body of `_gateway.json_encode(value)`.
///
/// Takes any Lua value that's shaped like a `result(...)` payload —
/// tables, numbers, strings, booleans, nil — and returns its JSON
/// serialization as a Lua string. Reuses `lua_value_to_json`, which
/// means the same dense-integer-keys-starting-at-1 → array heuristic
/// applies. Non-serializable values (functions, threads, userdata)
/// surface as a `Runtime` error identifying the offending type.
///
/// **Nil encodes to the literal JSON string `"null"`.** This is the
/// inverse of `json_decode('null')`, which produces nil — the pair
/// roundtrips. `json_decode(json_encode(nil)) == nil`. If you want
/// to reject nil callers, check before calling `json_encode`.
///
/// Symmetric with `json_decode`: `json_decode(json_encode(x))` returns
/// a deep-equal copy of `x` for any round-trippable input.
fn run_json_encode(lua: &Lua, args: Value) -> mlua::Result<Value> {
    let as_json = lua_value_to_json(lua, args).map_err(|err| {
        LuaError::external(LuaExecutionError::Runtime(format!(
            "_gateway.json_encode: {err}"
        )))
    })?;

    let serialized = serde_json::to_string(&as_json).map_err(|e| {
        LuaError::external(LuaExecutionError::Internal(format!(
            "_gateway.json_encode: serialization failed: {e}"
        )))
    })?;

    Ok(Value::String(lua.create_string(serialized.as_str())?))
}

/// Pure helper: unix-epoch seconds → `YYYY-MM-DDTHH:MM:SSZ` string.
///
/// The canonical RFC3339-UTC form for time-windowed upstream queries
/// (Loki, Datadog, Grafana). Sub-second precision is deliberately
/// dropped — time-windowed upstream queries don't need it, and the
/// narrower output means fewer "did you mean `T12:34:56Z` or
/// `T12:34:56.789Z`?" moments at query assembly time.
///
/// Returns `None` for timestamps outside chrono's representable
/// range (roughly year ±262,000). Callers turn that into a
/// `Runtime` error.
fn rfc3339_from_epoch_seconds(secs: i64) -> Option<String> {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)?;
    // `%Y-%m-%dT%H:%M:%SZ` → no sub-seconds, always Z. Matches the
    // exact shape agents write by hand with `os.date("!%Y-%m-%dT%H:%M:%SZ", t)`.
    Some(dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

/// Coerce a Lua numeric argument to i64 seconds, accepting integer or
/// float. Floats are truncated toward zero. Non-numeric inputs produce
/// a `Runtime` error naming the builtin.
fn lua_arg_to_seconds(args: &Value, builtin_name: &str) -> Result<i64, LuaError> {
    match args {
        Value::Integer(i) => Ok(*i),
        Value::Number(n) => {
            if !n.is_finite() {
                return Err(LuaError::external(LuaExecutionError::Runtime(format!(
                    "{builtin_name}: seconds must be finite, got {n}"
                ))));
            }
            // Guard against saturating cast: values beyond i64's representable
            // range would silently wrap to i64::MAX / i64::MIN (since Rust 1.45
            // saturates rather than UB). The comparison is slightly conservative
            // for large f64 values (>2^53 lose precision), but erring toward
            // rejection is safe here.
            if n.abs() >= i64::MAX as f64 {
                return Err(LuaError::external(LuaExecutionError::Runtime(format!(
                    "{builtin_name}: seconds magnitude {n} exceeds i64 range"
                ))));
            }
            Ok(*n as i64)
        }
        other => Err(LuaError::external(LuaExecutionError::Runtime(format!(
            "{builtin_name}: expected a number (seconds), got {other:?}"
        )))),
    }
}

/// Host-side body of `_gateway.rfc3339(unix_seconds)`.
///
/// Takes a unix epoch seconds value (integer or float; float is
/// truncated toward zero) and returns the `YYYY-MM-DDTHH:MM:SSZ`
/// RFC3339-UTC string.
fn run_rfc3339(lua: &Lua, args: Value) -> mlua::Result<Value> {
    let secs = lua_arg_to_seconds(&args, "_gateway.rfc3339")?;
    let formatted = rfc3339_from_epoch_seconds(secs).ok_or_else(|| {
        LuaError::external(LuaExecutionError::Runtime(format!(
            "_gateway.rfc3339: timestamp {secs} is outside representable range"
        )))
    })?;
    Ok(Value::String(lua.create_string(formatted.as_str())?))
}

/// Host-side body of `_gateway.now_minus(seconds)`.
///
/// Convenience wrapper: returns the RFC3339-UTC string for
/// `Utc::now() - seconds`. Agents querying "last N minutes" don't have
/// to combine `os.time()` + `rfc3339` themselves.
///
/// Accepts integer or float seconds; negative values (to time-travel
/// into the future) are permitted — the return is just a formatted
/// string and callers can freely use the helper for both backward and
/// forward time windows.
fn run_now_minus(lua: &Lua, args: Value) -> mlua::Result<Value> {
    let secs = lua_arg_to_seconds(&args, "_gateway.now_minus")?;
    let now = chrono::Utc::now().timestamp();
    // Saturating subtraction avoids a theoretical overflow when someone
    // passes i64::MIN or similar. Agents won't hit this in practice,
    // but "returns the representable-range floor" beats "panics."
    let target = now.saturating_sub(secs);
    let formatted = rfc3339_from_epoch_seconds(target).ok_or_else(|| {
        LuaError::external(LuaExecutionError::Runtime(format!(
            "_gateway.now_minus: computed timestamp {target} is outside representable range"
        )))
    })?;
    Ok(Value::String(lua.create_string(formatted.as_str())?))
}

/// Host-side body of `_gateway.unwrap_content(value)`.
///
/// The MCP tool-call response shape is:
/// ```json
/// { "content": [{ "type": "text", "text": "…" }, …], "isError": false }
/// ```
/// Agents unwrapping that shape always reach for `x.content[1].text`.
/// This helper does the check-and-reach in a single call and returns
/// the input unchanged when it doesn't match — so callers can wrap
/// upstream results unconditionally.
///
/// Match rules (all must hold for the "unwrap" path):
///
/// - input is a table
/// - input has a `content` field that is itself a table
/// - `content[1]` is a table with a `text` field that is a string
///
/// The `type` field is deliberately NOT checked — in practice, all
/// MCP content blocks in an envelope have matching type, and some
/// upstreams omit it entirely while still packing the text in the
/// `text` field. Trusting `content[1].text` when it's a string is the
/// lowest-friction rule that still doesn't false-positive on
/// structured returns that happen to have a `content` field.
///
/// Never returns an error — mismatched shapes return the input as-is.
/// This is what lets callers write `unwrap_content(result)` without
/// first branching on the shape.
fn run_unwrap_content(args: Value) -> Value {
    let Value::Table(table) = &args else {
        return args;
    };

    let Ok(content) = table.get::<Value>("content") else {
        return args;
    };
    let Value::Table(content_table) = content else {
        return args;
    };

    let Ok(first) = content_table.raw_get::<Value>(1) else {
        return args;
    };
    let Value::Table(first_table) = first else {
        return args;
    };

    let Ok(text) = first_table.get::<Value>("text") else {
        return args;
    };
    match text {
        Value::String(_) => text,
        _ => args,
    }
}

// ---------- parallel dispatch ----------

/// Host-side body of `_gateway.parallel`. Receives a Lua value the user
/// claims is an array of `{ server, tool, args }` tables and returns a
/// Lua array of `{ ok, value | error }` envelopes.
///
/// The flow, in order:
///
/// 1. Convert Lua input → JSON via `lua_value_to_json`. This is the
///    only step that needs `&Lua` — everything after is plain Rust.
/// 2. Run the validator (`parallel::parse_task_list`). Pure; errors
///    include the offending task index.
/// 3. Resolve each task's `(lua_server, lua_tool)` to a dispatch
///    plan: the upstream handle + the original (unsanitized) tool
///    name. Resolution failures become per-task `{ ok = false }`
///    envelopes, not whole-call errors — this matches the "a single
///    bad task doesn't kill the batch" semantics. The inspection
///    guard is checked here too; **un-inspected tasks abort the
///    whole call** per the design decision.
/// 4. Under one `handle.block_on`, spawn N futures (`uh.call_tool`),
///    await via `join_all`. This is where the actual parallelism
///    lives.
/// 5. Convert every per-task `CallToolResult` / error into a Lua
///    table via `render_call_tool_result` and build the result array.
fn run_parallel(
    lua: &Lua,
    args: Value,
    upstreams: &Arc<UpstreamManager>,
    tool_cache: Option<&Arc<ToolListCache>>,
    guard: Option<&ToolCallGuard>,
    handle: &Handle,
    cancel: &tokio_util::sync::CancellationToken,
) -> mlua::Result<Value> {
    // Fast-path: if the request was already cancelled, don't dispatch.
    bail_on_cancel_lua!(cancel);
    // ---------- (1) Lua → JSON ----------
    //
    // Lua conflates empty arrays and empty objects (`{}` is both). The
    // table-to-JSON heuristic in this crate resolves that to an empty
    // object, which would then fail validation. Re-interpret an empty
    // object at this specific entry point as an empty array — an agent
    // writing `_gateway.parallel({})` obviously means "no tasks", not
    // "an object without an 'args' field".
    let args_json = match args {
        Value::Nil => serde_json::Value::Array(Vec::new()),
        other => {
            let raw = lua_value_to_json(lua, other).map_err(|err| {
                LuaError::external(LuaExecutionError::Runtime(format!(
                    "_gateway.parallel: failed to convert task list to JSON: {err}"
                )))
            })?;
            match raw {
                serde_json::Value::Object(ref m) if m.is_empty() => {
                    serde_json::Value::Array(Vec::new())
                }
                other => other,
            }
        }
    };

    // ---------- (2) validate ----------
    let descriptors = crate::parallel::parse_task_list(args_json)
        .map_err(|e| LuaError::external(LuaExecutionError::Runtime(e.to_string())))?;

    // Short-circuit on empty input — skip the block_on entirely.
    if descriptors.is_empty() {
        let empty = lua.create_table().map_err(|e| {
            LuaError::external(LuaExecutionError::Internal(format!(
                "_gateway.parallel: create empty result table failed: {e}"
            )))
        })?;
        return Ok(Value::Table(empty));
    }

    // ---------- (3) resolve + inspection gate (serial, cheap) ----------
    //
    // Each task either resolves to `Ok(Plan)` — dispatchable — or to
    // `Err(envelope)` — a pre-formed `{ok=false}` entry we'll place
    // directly in the output slot. We do NOT dispatch those; only
    // `Ok` entries go into `join_all`.
    //
    // The inspection guard is special: a single `NotInspected` aborts
    // the whole call (returns Err from the host function), matching
    // the design decision that inspection enforcement is not
    // per-task-bypassable. Every other resolution failure is per-task.

    // Record (index, plan) pairs for dispatch, plus index→envelope
    // for tasks that resolved to errors up front.
    struct DispatchPlan {
        upstream_handle: Arc<UpstreamHandle>,
        lua_server: String,
        original_tool: String,
        lua_tool: String,
        args: serde_json::Map<String, serde_json::Value>,
    }

    let mut plans: Vec<(usize, DispatchPlan)> = Vec::new();
    let mut pre_errors: Vec<(usize, String)> = Vec::new();

    // Resolution needs upstream look-ups; wrap the whole thing in one
    // block_on (cheap — all in-memory cache reads for the common case).
    // The resolution future races the cancel token: if the caller
    // cancels between input-validation and dispatch, we don't want to
    // push through only to discover cancellation on the first
    // call_tool.
    let resolution: Result<(), LuaError> = match block_on_cancellable(handle, cancel, async {
        for (i, desc) in descriptors.iter().enumerate() {
            // Inspection gate runs on the requested names directly.
            // We don't need a valid upstream/tool pair yet — the guard
            // already returns `false` for unknown pairs in production,
            // which this treats the same as any other un-inspected
            // tool. That uniformity is the point.
            if let Some(g) = guard
                && !g(&desc.server, &desc.tool)
            {
                return Err(LuaError::external(LuaExecutionError::NotInspected {
                    lua_server: desc.server.clone(),
                    lua_tool: desc.tool.clone(),
                }));
            }

            // Resolve lua_server -> UpstreamHandle.
            let Some(uh) = upstreams.handle_by_lua_name(&desc.server).await else {
                pre_errors.push((
                    i,
                    format!(
                        "upstream '{}' not registered (check list-servers)",
                        desc.server
                    ),
                ));
                continue;
            };

            // Resolve sanitized lua_tool -> original tool name.
            // `fetch_upstream_tools` uses the display name (cfg.name)
            // as its cache key, so we go through that path.
            let original = match resolve_original_tool_name(
                upstreams,
                tool_cache,
                &uh.config.name,
                &desc.tool,
            )
            .await
            {
                Ok(Some(name)) => name,
                Ok(None) => {
                    pre_errors.push((
                        i,
                        format!(
                            "tool '{}.{}' not found on upstream (check list-server-tools)",
                            desc.server, desc.tool
                        ),
                    ));
                    continue;
                }
                Err(e) => {
                    pre_errors.push((
                        i,
                        format!("failed to resolve '{}.{}': {e}", desc.server, desc.tool),
                    ));
                    continue;
                }
            };

            plans.push((
                i,
                DispatchPlan {
                    upstream_handle: uh,
                    lua_server: desc.server.clone(),
                    original_tool: original,
                    lua_tool: desc.tool.clone(),
                    args: desc.args.clone(),
                },
            ));
        }
        Ok(())
    }) {
        Ok(inner) => inner,
        Err(BlockOnCancelled) => Err(LuaError::external(LuaExecutionError::Cancelled)),
    };

    resolution?;

    // ---------- (4) dispatch all plans concurrently ----------
    //
    // Each future returns its original task index so we can slot the
    // result back into the output array at the right position. We
    // also pre-build the CallToolRequestParams per plan; that work
    // is not async and costs only a clone.
    let mut per_task: Vec<Option<Result<serde_json::Value, String>>> =
        (0..descriptors.len()).map(|_| None).collect();

    // Populate pre-resolved errors.
    for (i, err) in pre_errors {
        per_task[i] = Some(Err(err));
    }

    if !plans.is_empty() {
        // Build the futures. Each one owns a clone of the upstream
        // handle (cheap Arc clone) and its pre-rendered request.
        //
        // The whole join_all is wrapped in a cancel-aware block_on so
        // if the caller cancels mid-flight (or a deadline fires at the
        // outer `execute` layer), every in-flight upstream future is
        // dropped instead of pinning the blocking worker on a oneshot
        // that will never fire.
        let dispatch_results: Vec<(usize, Result<CallToolResult, anyhow::Error>)> =
            match block_on_cancellable(handle, cancel, async move {
                let futures_iter = plans.into_iter().map(|(i, plan)| {
                    let request = CallToolRequestParams::new(plan.original_tool.clone())
                        .with_arguments(plan.args);
                    async move {
                        let result = plan.upstream_handle.call_tool(request).await.map_err(|e| {
                            anyhow::anyhow!(
                                "upstream tool call failed on {}.{}: {e}",
                                plan.lua_server,
                                plan.lua_tool
                            )
                        });
                        (i, result)
                    }
                });
                futures::future::join_all(futures_iter).await
            }) {
                Ok(results) => results,
                Err(BlockOnCancelled) => {
                    return Err(LuaError::external(LuaExecutionError::Cancelled));
                }
            };

        for (i, result) in dispatch_results {
            per_task[i] = Some(match result {
                Ok(call_result) => Ok(render_call_tool_result(&call_result)),
                Err(e) => Err(format!("{e}")),
            });
        }
    }

    // ---------- (5) build Lua result array ----------
    let out_table = lua.create_table().map_err(|e| {
        LuaError::external(LuaExecutionError::Internal(format!(
            "_gateway.parallel: create result table failed: {e}"
        )))
    })?;

    for (i, slot) in per_task.into_iter().enumerate() {
        let entry_table = lua.create_table().map_err(|e| {
            LuaError::external(LuaExecutionError::Internal(format!(
                "_gateway.parallel: create entry table failed: {e}"
            )))
        })?;

        #[allow(clippy::expect_used)]
        let entry = slot.expect(
            "every slot filled by construction: pre_errors and dispatch_results partition 0..len",
        );

        match entry {
            Ok(value_json) => {
                let value_lua = json_to_lua_value(lua, &value_json).map_err(|e| {
                    LuaError::external(LuaExecutionError::Internal(format!(
                        "_gateway.parallel: convert result #{} to lua: {e}",
                        i + 1
                    )))
                })?;
                entry_table.set("ok", true).map_err(|e| {
                    LuaError::external(LuaExecutionError::Internal(format!(
                        "_gateway.parallel: set ok: {e}"
                    )))
                })?;
                entry_table.set("value", value_lua).map_err(|e| {
                    LuaError::external(LuaExecutionError::Internal(format!(
                        "_gateway.parallel: set value: {e}"
                    )))
                })?;
            }
            Err(msg) => {
                entry_table.set("ok", false).map_err(|e| {
                    LuaError::external(LuaExecutionError::Internal(format!(
                        "_gateway.parallel: set ok=false: {e}"
                    )))
                })?;
                entry_table.set("error", msg).map_err(|e| {
                    LuaError::external(LuaExecutionError::Internal(format!(
                        "_gateway.parallel: set error: {e}"
                    )))
                })?;
            }
        }

        // 1-based indexing — Lua convention.
        out_table.set(i + 1, entry_table).map_err(|e| {
            LuaError::external(LuaExecutionError::Internal(format!(
                "_gateway.parallel: set result[{}]: {e}",
                i + 1
            )))
        })?;
    }

    Ok(Value::Table(out_table))
}

/// Look up the original (unsanitized) tool name for a requested
/// sanitized `lua_tool` on the upstream identified by its display
/// name. Returns `Ok(Some)` if found, `Ok(None)` if the upstream
/// doesn't publish a tool that sanitizes to the requested name,
/// `Err(...)` if the tool-list fetch itself failed.
///
/// Shares the `fetch_upstream_tools` cached-then-fallback path that
/// `install_upstream_globals` uses so production calls hit the cache
/// and unit tests without a cache still work via direct `list_tools`.
async fn resolve_original_tool_name(
    upstreams: &Arc<UpstreamManager>,
    tool_cache: Option<&Arc<ToolListCache>>,
    upstream_display_name: &str,
    requested_lua_tool: &str,
) -> Result<Option<String>, anyhow::Error> {
    let tools = fetch_upstream_tools(upstreams, tool_cache, upstream_display_name).await?;
    Ok(resolve_sanitized_among(&tools, requested_lua_tool))
}

/// Pure core of [`resolve_original_tool_name`]: find the tool whose
/// sanitized name matches the request. Exactly one match → callable;
/// more than one → sanitized-name collision, which (matching
/// `partition_unique_by_sanitized` semantics) makes NONE of them
/// callable, so we report `None`. A "first match wins" answer here
/// would be a capability bypass: the agent could inspect one colliding
/// tool and silently dispatch the other.
///
/// This runs once per task in the `_gateway.parallel` resolution loop,
/// so keep it one pass with no intermediate collections — building the
/// full partition here would cost O(tools) heap churn per task.
fn resolve_sanitized_among(tools: &[String], requested_lua_tool: &str) -> Option<String> {
    let mut found: Option<&str> = None;
    for original in tools {
        if sanitize_lua_identifier(original) == requested_lua_tool {
            if found.is_some() {
                return None;
            }
            found = Some(original.as_str());
        }
    }
    found.map(str::to_string)
}

// ---------- result() capture ----------

type ResultSlot = Arc<Mutex<Option<serde_json::Value>>>;

fn install_result_capture(lua: &Lua) -> Result<ResultSlot, LuaExecutionError> {
    let slot: ResultSlot = Arc::new(Mutex::new(None));
    let slot_for_fn = Arc::clone(&slot);

    let result_fn = lua
        .create_function(move |lua_ref, value: Value| {
            let json = lua_value_to_json(lua_ref, value)
                .map_err(|err| LuaError::external(LuaExecutionError::NonSerializableResult(err)))?;
            let mut guard = slot_for_fn.lock().map_err(|_| {
                LuaError::external(LuaExecutionError::Internal(
                    "result slot mutex poisoned".to_string(),
                ))
            })?;
            *guard = Some(json);
            Ok(())
        })
        .map_err(|e| LuaExecutionError::Internal(format!("failed to create result fn: {e}")))?;

    lua.globals()
        .set("result", result_fn)
        .map_err(|e| LuaExecutionError::Internal(format!("failed to set result global: {e}")))?;

    Ok(slot)
}

// ---------- interrupt / instruction budget ----------

fn install_interrupt(lua: &Lua, budget: u64) -> Result<(), LuaExecutionError> {
    // Interrupt fires periodically during execution (Luau decides the cadence;
    // roughly every few thousand instructions). We count invocations and
    // terminate the VM via an error past the budget.
    let counter = Arc::new(AtomicU64::new(0));
    let counter_for_fn = Arc::clone(&counter);

    lua.set_interrupt(move |_lua_ref| {
        // AcqRel rather than Relaxed: the interrupt callback fires on the
        // VM thread, so Relaxed is technically safe for today's single-VM-
        // per-execute model. AcqRel costs nothing at interrupt cadence (every
        // few thousand bytecode ops) and remains correct if a future refactor
        // migrates the VM between threads or shares a counter across VMs.
        let n = counter_for_fn.fetch_add(1, Ordering::AcqRel);
        if n >= budget {
            // Returning an error here terminates the VM; the error surfaces
            // as LuaError::CallbackError at the exec() call site.
            Err(LuaError::external(
                LuaExecutionError::InstructionBudgetExceeded { budget },
            ))
        } else {
            Ok(VmState::Continue)
        }
    });

    Ok(())
}

/// Convert an mlua::Error into our structured LuaExecutionError.
///
/// Strategy: walk the error chain looking for an external cause of type
/// `LuaExecutionError` — that's the case where one of our host functions
/// (result, a tool bridge, the interrupt hook) returned a structured error
/// via `LuaError::external`. If we find one, return it directly. Otherwise
/// classify by the surface variant.
fn map_lua_error(err: LuaError) -> LuaExecutionError {
    // Walk the CallbackError / WithContext chain first. `parent()` unwraps
    // CallbackError-style nesting; `downcast_ref` on any link returns our
    // type if present.
    let mut cursor: &LuaError = &err;
    loop {
        if let Some(recovered) = cursor.downcast_ref::<LuaExecutionError>() {
            return clone_lua_execution_error(recovered);
        }
        match cursor.parent() {
            Some(inner) => cursor = inner,
            None => break,
        }
    }

    // No structured cause recoverable — fall back to mlua's surface variant.
    match &err {
        LuaError::SyntaxError { message, .. } => LuaExecutionError::Compile(message.clone()),
        _ => LuaExecutionError::Runtime(err.to_string()),
    }
}

/// LuaExecutionError isn't Clone (anyhow::Error isn't Clone), so we hand-roll
/// a clone for the downcast case. `UpstreamCall` loses its rich `source` chain
/// and becomes a string — but the message is preserved and the variant tag
/// is intact, which is what tests and callers actually match on.
///
/// **What is lost:** any `.context()` frames added below the top-level anyhow
/// message. The two current `UpstreamCall` construction sites (single-upstream
/// tool call and `parallel` dispatch) both embed the upstream name and tool
/// name in the top-level message, so the most actionable information survives.
/// If a future site adds a multi-frame context chain and that chain is
/// important, consider either making the chain part of the top-level message
/// or switching `source` to `Box<dyn StdError + Send + Sync>` with a
/// custom Clone impl.
fn clone_lua_execution_error(e: &LuaExecutionError) -> LuaExecutionError {
    match e {
        LuaExecutionError::Compile(m) => LuaExecutionError::Compile(m.clone()),
        LuaExecutionError::Runtime(m) => LuaExecutionError::Runtime(m.clone()),
        LuaExecutionError::MissingResult => LuaExecutionError::MissingResult,
        LuaExecutionError::InstructionBudgetExceeded { budget } => {
            LuaExecutionError::InstructionBudgetExceeded { budget: *budget }
        }
        LuaExecutionError::UpstreamCall {
            upstream,
            tool,
            source,
        } => LuaExecutionError::UpstreamCall {
            upstream: upstream.clone(),
            tool: tool.clone(),
            source: anyhow::anyhow!("{source}"),
        },
        LuaExecutionError::NonSerializableResult(m) => {
            LuaExecutionError::NonSerializableResult(m.clone())
        }
        LuaExecutionError::Internal(m) => LuaExecutionError::Internal(m.clone()),
        LuaExecutionError::NotInspected {
            lua_server,
            lua_tool,
        } => LuaExecutionError::NotInspected {
            lua_server: lua_server.clone(),
            lua_tool: lua_tool.clone(),
        },
        LuaExecutionError::OffloadFailed(m) => LuaExecutionError::OffloadFailed(m.clone()),
        LuaExecutionError::Timeout { deadline_ms } => LuaExecutionError::Timeout {
            deadline_ms: *deadline_ms,
        },
        LuaExecutionError::Cancelled => LuaExecutionError::Cancelled,
    }
}

// ---------- upstream global injection ----------

fn install_upstream_globals(
    lua: &Lua,
    configs: &[mcp_client::UpstreamConfig],
    upstreams: &Arc<UpstreamManager>,
    guard: Option<&ToolCallGuard>,
    tool_cache: Option<&Arc<ToolListCache>>,
    handle: &Handle,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<(), LuaExecutionError> {
    for cfg in configs {
        let table = lua
            .create_table()
            .map_err(|e| LuaExecutionError::Internal(format!("create_table failed: {e}")))?;

        // Discover the upstream's tools once at injection time. This is a
        // blocking fetch; it runs on our spawn_blocking thread, so it's fine.
        // With a cache wired in, this usually hits an Arc clone instead
        // of a real RPC.
        //
        // Races the cancel token: an upstream whose list_tools hangs
        // during session init must not pin the blocking worker.
        //
        // Per-upstream failures (timeouts, rmcp service errors, the
        // upstream momentarily unregistered) are SKIP-WITH-WARNING:
        // we don't install a Lua global for that upstream, but the
        // script continues. The agent's script will get a Lua-level
        // "attempt to index nil" if it tries to use the missing
        // upstream — a clear, narrowly-scoped failure mode rather
        // than the whole session collapsing because one bridge is
        // sick. Cancellation is still a hard fail (the request was
        // explicitly cancelled by the client).
        let upstream_name = cfg.name.clone();
        let tools = match block_on_cancellable(handle, cancel, async {
            fetch_upstream_tools(upstreams, tool_cache, &upstream_name).await
        }) {
            Ok(Ok(tools)) => tools,
            Ok(Err(e)) => {
                tracing::warn!(
                    upstream = %upstream_name,
                    error = %e,
                    "skipping upstream global: list_tools failed at session init; \
                     scripts that reference this upstream will hit 'attempt to index nil'"
                );
                continue;
            }
            Err(BlockOnCancelled) => return Err(LuaExecutionError::Cancelled),
        };

        // Partition by sanitized name. Colliding tools (e.g. `get-issue`
        // and `get.issue` both sanitizing to `get_issue`) are NOT
        // installed: the inspection gate keys off the sanitized name, so
        // letting the agent inspect one and dispatch the other would be
        // a silent capability bypass. We log a warning per collision so
        // operators can rename one of the offending tools upstream.
        let (callable, collisions) = partition_unique_by_sanitized(&tools);
        for (sanitized, originals) in &collisions {
            tracing::warn!(
                upstream = %cfg.name,
                lua_name = %sanitized,
                originals = ?originals,
                "skipping colliding tools — neither will be callable from Lua. \
                 rename one of them upstream to break the collision."
            );
        }

        for original_tool_name in callable {
            // The Lua global table is keyed by the SANITIZED tool name so
            // scripts can write `gitlab.download_job_log(...)` regardless of
            // what the upstream calls it (`download-job-log`,
            // `downloadJobLog`, etc.). The original name is preserved for
            // the actual rmcp call since the upstream doesn't know about
            // the sanitized form.
            let lua_tool_name = sanitize_lua_identifier(&original_tool_name);
            let upstreams_for_fn = Arc::clone(upstreams);
            let handle_for_fn = handle.clone();
            let upstream_name_for_fn = cfg.name.clone();
            let lua_server_name_for_fn = cfg.lua_name.clone();
            let original_tool_name_for_fn = original_tool_name.clone();
            let lua_tool_name_for_fn = lua_tool_name.clone();
            let guard_for_fn: Option<ToolCallGuard> = guard.cloned();
            let cancel_for_fn = cancel.clone();

            let tool_fn = lua
                .create_function(move |lua_ref, args: Value| {
                    call_upstream_tool_sync(
                        lua_ref,
                        &upstreams_for_fn,
                        &handle_for_fn,
                        &upstream_name_for_fn,
                        &lua_server_name_for_fn,
                        &original_tool_name_for_fn,
                        &lua_tool_name_for_fn,
                        guard_for_fn.as_ref(),
                        &cancel_for_fn,
                        args,
                    )
                })
                .map_err(|e| LuaExecutionError::Internal(format!("create_function failed: {e}")))?;

            table
                .set(lua_tool_name.as_str(), tool_fn)
                .map_err(|e| LuaExecutionError::Internal(format!("table.set failed: {e}")))?;
        }

        lua.globals()
            .set(cfg.lua_name.as_str(), table)
            .map_err(|e| {
                LuaExecutionError::Internal(format!(
                    "failed to set upstream global {}: {e}",
                    cfg.lua_name
                ))
            })?;
    }
    Ok(())
}

async fn fetch_upstream_tools(
    upstreams: &Arc<UpstreamManager>,
    tool_cache: Option<&Arc<ToolListCache>>,
    name: &str,
) -> anyhow::Result<Vec<String>> {
    let handle: Arc<UpstreamHandle> = upstreams
        .handle_by_name(name)
        .await
        .ok_or_else(|| anyhow::anyhow!("upstream not registered: {name}"))?;

    // Cache path: hand back Arc-shared tool names without a round trip
    // on the common case where the cache is already populated. Fallback
    // path (no cache): direct RPC, used by unit tests that drive the
    // Lua bridge without a full gateway.
    let tools = match tool_cache {
        Some(cache) => cache
            .get(&handle)
            .await
            .map_err(|e| anyhow::anyhow!("upstream list_tools (cached) failed: {e}"))?
            .iter()
            .map(|t| t.name.to_string())
            .collect(),
        None => handle
            .list_tools()
            .await
            .map_err(|e| anyhow::anyhow!("upstream list_tools failed: {e}"))?
            .into_iter()
            .map(|t| t.name.to_string())
            .collect(),
    };
    // The Lua injection path only needs names. The richer Tool metadata
    // is consumed by the gateway's `tool-details` tool elsewhere.
    Ok(tools)
}

#[allow(clippy::too_many_arguments)] // bridge fn; shape is dictated by what mlua's closure captures need.
fn call_upstream_tool_sync(
    lua: &Lua,
    upstreams: &Arc<UpstreamManager>,
    handle: &Handle,
    upstream_name: &str,
    lua_server_name: &str,
    original_tool_name: &str,
    lua_tool_name: &str,
    guard: Option<&ToolCallGuard>,
    cancel: &tokio_util::sync::CancellationToken,
    args: Value,
) -> mlua::Result<Value> {
    // Fast-path: bail before any Lua→JSON conversion work if the
    // request is already cancelled. Matches the early-return cheapness
    // of the inspection-guard check below.
    bail_on_cancel_lua!(cancel);

    // Inspection gate: if a guard is configured, reject uninspected
    // tools before doing any work. This is the first thing we do so
    // that `tool arguments must be an object` and similar diagnostic
    // noise never masks a more fundamental "you weren't supposed to
    // call this" error.
    if let Some(g) = guard
        && !g(lua_server_name, lua_tool_name)
    {
        return Err(LuaError::external(LuaExecutionError::NotInspected {
            lua_server: lua_server_name.to_string(),
            lua_tool: lua_tool_name.to_string(),
        }));
    }

    // Convert Lua args (typically a table) into JSON. If the script passed
    // nothing or passed nil, treat it as an empty object — most MCP tools
    // accept a bare `{}` call, and requiring scripts to write `{}`
    // explicitly for no-arg calls would be needless ceremony.
    let args_json = match args {
        Value::Nil => serde_json::Value::Object(serde_json::Map::new()),
        other => lua_value_to_json(lua, other)
            .map_err(|err| LuaError::external(LuaExecutionError::NonSerializableResult(err)))?,
    };

    let args_map = match args_json {
        serde_json::Value::Object(m) => m,
        serde_json::Value::Null => serde_json::Map::new(),
        // A non-object top-level arg (array, string, number) is a legit
        // protocol error — MCP tool arguments are a JSON object.
        other => {
            return Err(LuaError::external(LuaExecutionError::UpstreamCall {
                upstream: upstream_name.to_string(),
                tool: original_tool_name.to_string(),
                source: anyhow::anyhow!(
                    "tool arguments must be a table/object, got {}",
                    describe_json_shape(&other)
                ),
            }));
        }
    };

    let upstreams_for_call = Arc::clone(upstreams);
    let upstream_name_owned = upstream_name.to_string();
    let original_tool_name_owned = original_tool_name.to_string();

    // Use the ORIGINAL (unsanitized) tool name for the rmcp call — the
    // upstream doesn't know about our Lua sanitization.
    let request =
        CallToolRequestParams::new(original_tool_name_owned.clone()).with_arguments(args_map);

    // A bare `handle.block_on(upstream_future)` pins a blocking worker
    // thread for as long as the upstream takes, including forever if it
    // stalls. Racing against the cancel token here means a cancelled
    // downstream request (or deadline fired at the outer `execute`
    // layer) unwinds the upstream future promptly instead of leaking
    // a zombie worker.
    //
    // Script-internal timing: the enclosing call_tool span carries the
    // downstream tool name ("execute"), not which upstream tool the
    // script invoked. This debug line fills that gap — one line per
    // upstream call with `upstream + tool + duration_ms` so the log
    // shows exactly which call inside an execute got slow. Note that
    // `UpstreamHandle::call_tool` ALSO logs a debug line; the two
    // together differ in perspective (this one includes the sync
    // bridge overhead, the handler one is dispatch-only) and you
    // usually want both when debugging.
    let inner_start = std::time::Instant::now();
    let call_result: CallToolResult = match block_on_cancellable(handle, cancel, async move {
        let uh: Arc<UpstreamHandle> = upstreams_for_call
            .handle_by_name(&upstream_name_owned)
            .await
            .ok_or_else(|| anyhow::anyhow!("upstream not registered: {upstream_name_owned}"))?;
        // `UpstreamError::Display` already names the upstream and the
        // failure shape (CallTimeout / ServiceCall(TransportClosed) /
        // peer McpError), so we forward it verbatim — the
        // `LuaExecutionError::UpstreamCall` wrapper below adds the tool
        // name alongside for the downstream agent-facing message.
        let r: CallToolResult = uh
            .call_tool(request)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok::<CallToolResult, anyhow::Error>(r)
    }) {
        Ok(inner) => inner,
        Err(BlockOnCancelled) => {
            tracing::debug!(
                upstream = %upstream_name,
                tool = %original_tool_name,
                duration_ms = inner_start.elapsed().as_millis() as u64,
                outcome = "cancelled",
                "call_upstream_tool_sync cancelled"
            );
            return Err(LuaError::external(LuaExecutionError::Cancelled));
        }
    }
    .map_err(|e| {
        tracing::debug!(
            upstream = %upstream_name,
            tool = %original_tool_name,
            duration_ms = inner_start.elapsed().as_millis() as u64,
            outcome = "error",
            error = %e,
            "call_upstream_tool_sync failed"
        );
        LuaError::external(LuaExecutionError::UpstreamCall {
            upstream: upstream_name.to_string(),
            tool: original_tool_name.to_string(),
            source: e,
        })
    })?;
    tracing::debug!(
        upstream = %upstream_name,
        tool = %original_tool_name,
        duration_ms = inner_start.elapsed().as_millis() as u64,
        outcome = "ok",
        "call_upstream_tool_sync complete"
    );

    let rendered = render_call_tool_result(&call_result);
    json_to_lua_value(lua, &rendered).map_err(|err| {
        LuaError::external(LuaExecutionError::Internal(format!(
            "failed to convert upstream result to lua: {err}"
        )))
    })
}

// ---------- conversion helpers ----------

/// Render an rmcp CallToolResult as a JSON value suitable for passing back
/// into a Lua script. Collapse a single text content to a plain string —
/// the common case, and the one scripts want to index directly — and
/// otherwise return the structured shape `{ content = [...], isError = bool }`
/// so scripts can still see multi-block or error responses in full.
fn render_call_tool_result(result: &CallToolResult) -> serde_json::Value {
    let is_error = result.is_error.unwrap_or(false);
    if !is_error
        && result.content.len() == 1
        && let Some(text) = result.content[0].as_text()
    {
        return serde_json::Value::String(text.text.clone());
    }
    // Generic fallback: serde-serialize the whole result. CallToolResult is
    // derive-Serialize over its own types, so this is infallible in practice.
    // If it ever fails we want a panic rather than a fake error envelope that
    // a Lua script can't distinguish from a real upstream {"error":"..."}.
    #[allow(clippy::expect_used)]
    serde_json::to_value(result).expect("CallToolResult is serialize-safe by construction")
}

fn lua_value_to_json(lua: &Lua, v: Value) -> Result<serde_json::Value, String> {
    // mlua's serialize feature gives us `lua.from_value` / `lua.to_value`,
    // but we want tight control over the table-detection heuristic (dense
    // integer keys starting at 1 → array; otherwise object). Walk it.
    match v {
        Value::Nil => Ok(serde_json::Value::Null),
        Value::Boolean(b) => Ok(serde_json::Value::Bool(b)),
        Value::Integer(i) => Ok(serde_json::Value::Number(i.into())),
        Value::Number(n) => serde_json::Number::from_f64(n)
            .map(serde_json::Value::Number)
            .ok_or_else(|| {
                format!("non-finite numbers (NaN, ±Inf) are not JSON-serializable, got {n}")
            }),
        Value::String(s) => Ok(serde_json::Value::String(
            s.to_str()
                .map_err(|e| format!("non-utf8 string: {e}"))?
                .to_string(),
        )),
        Value::Table(t) => lua_table_to_json(lua, t),
        Value::Function(_) => Err("functions are not JSON-serializable".to_string()),
        Value::Thread(_) => Err("threads are not JSON-serializable".to_string()),
        Value::UserData(_) => Err("userdata is not JSON-serializable".to_string()),
        Value::LightUserData(_) => Err("lightuserdata is not JSON-serializable".to_string()),
        Value::Error(e) => Err(format!("lua error value: {e}")),
        Value::Buffer(_) => Err("buffers are not JSON-serializable".to_string()),
        Value::Vector(_) => Err("vectors are not JSON-serializable".to_string()),
        Value::Other(_) => Err("unsupported lua value".to_string()),
    }
}

fn lua_table_to_json(lua: &Lua, t: Table) -> Result<serde_json::Value, String> {
    // Single-pass heuristic: iterate all pairs once, collecting key/value
    // pairs while tracking whether the keys form a dense positive-integer
    // sequence starting at 1. A table is an array iff:
    //   - every key is a positive integer (no string keys, no 0, no negatives,
    //     no floats), AND
    //   - the keys are exactly 1..=N with no holes (max_key == total count).
    //
    // This replaces the old three-pass approach (raw_len → hole check →
    // pairs scan), which could silently drop keys outside the raw_len border
    // when the table had "holes" (e.g. {[1]=a, [2]=b, [5]=c} with raw_len
    // returning 2: key [5] was never emitted). Any mlua error propagates
    // rather than silently flipping to object mode.
    //
    // Empty table {} becomes an empty JSON object for consistency — a Lua
    // script has no reliable way to say "this is an empty array," and the
    // historical behaviour is preserved.
    let mut pairs_buf: Vec<(Value, Value)> = Vec::new();
    for pair in t.pairs::<Value, Value>() {
        let (k, v) = pair.map_err(|e| format!("bad table pair: {e}"))?;
        pairs_buf.push((k, v));
    }

    let total = pairs_buf.len();
    let mut max_positive_int: i64 = 0;
    let mut non_integer_key_seen = false;

    for (k, _) in &pairs_buf {
        match k {
            Value::Integer(i) if *i >= 1 => {
                if *i > max_positive_int {
                    max_positive_int = *i;
                }
            }
            _ => {
                non_integer_key_seen = true;
                break;
            }
        }
    }

    // Array iff: no non-integer keys, at least one entry, and keys are
    // exactly 1..=N (max == total, so no holes and no skipped indices).
    let is_array = !non_integer_key_seen && total > 0 && max_positive_int == total as i64;

    if is_array {
        // We know the keys are 1..=N; sort by key so they come out in order.
        // pairs() does not guarantee ordering for integer-keyed tables.
        let mut indexed: Vec<(i64, Value)> = pairs_buf
            .into_iter()
            .map(|(k, v)| {
                let i = match k {
                    Value::Integer(i) => i,
                    _ => unreachable!("is_array guarantees integer keys"),
                };
                (i, v)
            })
            .collect();
        indexed.sort_unstable_by_key(|(i, _)| *i);
        let arr: Result<Vec<serde_json::Value>, String> = indexed
            .into_iter()
            .map(|(_, v)| lua_value_to_json(lua, v))
            .collect();
        Ok(serde_json::Value::Array(arr?))
    } else {
        let mut map = serde_json::Map::new();
        for (k, v) in pairs_buf {
            let key = match k {
                Value::String(s) => s
                    .to_str()
                    .map_err(|e| format!("non-utf8 key: {e}"))?
                    .to_string(),
                Value::Integer(i) => i.to_string(),
                Value::Number(n) => n.to_string(),
                other => return Err(format!("unsupported table key type: {other:?}")),
            };
            map.insert(key, lua_value_to_json(lua, v)?);
        }
        Ok(serde_json::Value::Object(map))
    }
}

fn json_to_lua_value(lua: &Lua, v: &serde_json::Value) -> mlua::Result<Value> {
    match v {
        serde_json::Value::Null => Ok(Value::Nil),
        serde_json::Value::Bool(b) => Ok(Value::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Number(f))
            } else {
                // This branch is unreachable with default serde_json (every
                // Number is either i64 or f64). If the `arbitrary_precision`
                // Cargo feature is ever enabled, serde_json stores numbers as
                // strings internally, causing both as_i64() and as_f64() to
                // return None for values that don't round-trip — triggering
                // this panic as a clear signal rather than silently producing
                // nil. See: https://docs.rs/serde_json/latest/serde_json/struct.Number.html
                panic!(
                    "serde_json::Number is always i64 or f64 without \
                     the `arbitrary_precision` feature; got: {n:?}"
                )
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(lua.create_string(s.as_str())?)),
        serde_json::Value::Array(items) => {
            let t = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                t.raw_set((i + 1) as i64, json_to_lua_value(lua, item)?)?;
            }
            Ok(Value::Table(t))
        }
        serde_json::Value::Object(obj) => {
            let t = lua.create_table()?;
            for (k, v) in obj {
                t.raw_set(k.as_str(), json_to_lua_value(lua, v)?)?;
            }
            Ok(Value::Table(t))
        }
    }
}

// ============================================================================
// Tests
// ============================================================================
//
// Unit tests live in `runtime_tests.rs` (via `#[path]`) rather than
// inline: the test module is as large as the implementation itself
// (~2,200 lines), and keeping it out of this file means readers and
// tooling can load the implementation without paying for the tests.
// The `#[path]` child-module form (not a sibling `mod`) is what
// preserves the tests' access to this module's private items.

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
