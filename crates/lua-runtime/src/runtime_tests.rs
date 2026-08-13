use super::*;

fn empty_upstreams() -> Arc<UpstreamManager> {
    Arc::new(UpstreamManager::new())
}

/// Tiny helper for tests that care about the happy-path value and
/// don't exercise the offload variant. Panics with a legible
/// message if the outcome is Offloaded.
fn expect_direct(outcome: ExecutionOutcome) -> serde_json::Value {
    match outcome {
        ExecutionOutcome::Direct(v) => v,
        ExecutionOutcome::Offloaded { execution_id, .. } => {
            panic!("expected Direct outcome; got Offloaded(execution_id={execution_id})")
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pure_lua_result_arithmetic() {
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            "result(1 + 1)".to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("script should succeed");
    assert_eq!(expect_direct(outcome), serde_json::json!(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_accepts_strings() {
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            "result('hello')".to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("script should succeed");
    assert_eq!(expect_direct(outcome), serde_json::json!("hello"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_accepts_tables_as_objects() {
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            "result({ a = 1, b = 'two' })".to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("script should succeed");
    assert_eq!(
        expect_direct(outcome),
        serde_json::json!({"a": 1, "b": "two"})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_accepts_sequences_as_arrays() {
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            "result({10, 20, 30})".to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("script should succeed");
    assert_eq!(expect_direct(outcome), serde_json::json!([10, 20, 30]));
}

// ----- lua_table_to_json edge-case regression suite -----
//
// These tests pin the single-pass heuristic against inputs that the old
// raw_len-based approach would either silently mangle or classify wrongly.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_with_mixed_keys_becomes_object_preserving_every_key() {
    // {[1]=a, [2]=b, name='bob'} — mixed integer + string keys.
    // Must become an object with three keys, no key dropped.
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            r#"result({[1]="a", [2]="b", name="bob"})"#.to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("mixed-key table must succeed");
    let got = expect_direct(outcome);
    assert!(got.is_object(), "expected object; got: {got}");
    assert_eq!(
        got.as_object()
            .expect("is_object check above guarantees Some")
            .len(),
        3,
        "must have 3 keys"
    );
    assert_eq!(got["1"], serde_json::json!("a"));
    assert_eq!(got["2"], serde_json::json!("b"));
    assert_eq!(got["name"], serde_json::json!("bob"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_integer_only_table_becomes_object_with_stringified_keys() {
    // {[1]='a', [5]='c'} — integer keys but not 1..=N (hole at 2..4).
    // Must become object {"1":"a","5":"c"}, not a 5-element array with
    // nils in the middle (which would be data loss in the old path).
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            r#"result({[1]="a", [5]="c"})"#.to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("sparse table must succeed");
    let got = expect_direct(outcome);
    assert!(got.is_object(), "sparse table must be object; got: {got}");
    assert_eq!(got["1"], serde_json::json!("a"));
    assert_eq!(got["5"], serde_json::json!("c"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_indexed_table_becomes_object_not_array() {
    // {[0]='x', [1]='y'} — 0 is not a positive integer so must be object.
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            r#"result({[0]="x", [1]="y"})"#.to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("zero-indexed table must succeed");
    let got = expect_direct(outcome);
    assert!(
        got.is_object(),
        "0-indexed table must be object; got: {got}"
    );
    assert_eq!(got["0"], serde_json::json!("x"));
    assert_eq!(got["1"], serde_json::json!("y"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negative_integer_keys_become_string_keys_in_object() {
    // {[-1]='x', [1]='y'} — negative key is not a positive integer.
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            r#"result({[-1]="x", [1]="y"})"#.to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("negative-key table must succeed");
    let got = expect_direct(outcome);
    assert!(
        got.is_object(),
        "negative-key table must be object; got: {got}"
    );
    assert_eq!(got["-1"], serde_json::json!("x"));
    assert_eq!(got["1"], serde_json::json!("y"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn float_keys_become_stringified_object_keys() {
    // {[1.5]='x'} — float key is not a positive integer.
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            r#"result({[1.5]="x"})"#.to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("float-key table must succeed");
    let got = expect_direct(outcome);
    assert!(
        got.is_object(),
        "float-key table must be object; got: {got}"
    );
    // The key becomes the float's Display form.
    let has_key = got
        .as_object()
        .expect("is_object check above guarantees Some")
        .contains_key("1.5");
    assert!(has_key, "expected key '1.5'; got: {got}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_table_encodes_as_empty_object_for_consistency() {
    // {} — both empty array and empty object in Lua; we always produce
    // an empty JSON object to match historical behaviour.
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            "result({})".to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("empty table must succeed");
    let got = expect_direct(outcome);
    assert_eq!(
        got,
        serde_json::json!({}),
        "empty table must be empty object"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_dense_array_round_trips_correctly() {
    // 1000-element dense array: smoke-test that the single-pass rewrite
    // handles scale and preserves order.
    let rt = LuaRuntime::new();
    let script = r#"
        local t = {}
        for i = 1, 1000 do t[i] = i * 2 end
        result(t)
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("large array must succeed");
    let got = expect_direct(outcome);
    let arr = got.as_array().expect("must be array");
    assert_eq!(arr.len(), 1000);
    for (i, v) in arr.iter().enumerate() {
        assert_eq!(*v, serde_json::json!((i + 1) as i64 * 2));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_key_in_otherwise_integer_sequence_becomes_object() {
    // {[1]='a', [2]='b', [4]='d'} — hole at 3. Must be object, not a
    // 4-element array with a nil in the middle (which would be data loss
    // in the old raw_len approach when raw_len happened to return 2).
    let rt = LuaRuntime::new();
    let outcome = rt
        .execute(
            r#"result({[1]="a", [2]="b", [4]="d"})"#.to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("holey table must succeed");
    let got = expect_direct(outcome);
    assert!(got.is_object(), "holey table must be object; got: {got}");
    assert_eq!(got["1"], serde_json::json!("a"));
    assert_eq!(got["2"], serde_json::json!("b"));
    assert_eq!(got["4"], serde_json::json!("d"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_result_is_an_error() {
    let rt = LuaRuntime::new();
    let err = rt
        .execute(
            "local x = 1 + 1".to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("script without result() must error");
    assert!(
        matches!(err, LuaExecutionError::MissingResult),
        "got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn syntax_error_is_compile_error() {
    let rt = LuaRuntime::new();
    let err = rt
        .execute(
            "result(".to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("unfinished script must error");
    assert!(matches!(err, LuaExecutionError::Compile(_)), "got: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_error_is_runtime_error() {
    let rt = LuaRuntime::new();
    let err = rt
        .execute(
            "error('nope')".to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("error() call must error");
    assert!(matches!(err, LuaExecutionError::Runtime(_)), "got: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sandbox_blocks_os_execute() {
    // Property: after sandbox(true), os.execute is unreachable (either
    // os itself is gone/frozen, or os.execute is absent). Either way,
    // a script that tries to call it errors.
    let rt = LuaRuntime::new();
    let err = rt
        .execute(
            "result(os.execute('echo pwned'))".to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("os.execute must not work in sandbox");
    assert!(
        matches!(err, LuaExecutionError::Runtime(_)),
        "got non-runtime error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn instruction_budget_terminates_runaway_loop() {
    let rt = LuaRuntime::new().with_instruction_budget(1_000);
    let err = rt
        .execute(
            "while true do end".to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("infinite loop must be terminated");
    assert!(
        matches!(err, LuaExecutionError::InstructionBudgetExceeded { .. }),
        "got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_serializable_result_is_reported() {
    let rt = LuaRuntime::new();
    let err = rt
        .execute(
            "result(function() end)".to_string(),
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("functions can't be serialized");
    assert!(
        matches!(err, LuaExecutionError::NonSerializableResult(_)),
        "got: {err}"
    );
}

// ----- Offload-path unit tests -----

/// Trivial in-memory backend for runtime tests that don't want to
/// depend on the gateway's ExecutionResultStore. Thread-safe
/// because the runtime's spawn_blocking can grab the backend from
/// a different thread than the one that constructed it.
struct FakeBackend {
    inner: std::sync::Mutex<std::collections::HashMap<String, String>>,
}

impl FakeBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }
}

impl ExecutionResultBackend for FakeBackend {
    fn save(&self, payload: String) -> Result<String, String> {
        let id = format!(
            "fake-{}",
            self.inner
                .lock()
                .expect("fake backend mutex poisoned")
                .len()
        );
        self.inner
            .lock()
            .expect("fake backend mutex poisoned")
            .insert(id.clone(), payload);
        Ok(id)
    }

    fn get(&self, id: &str) -> Option<String> {
        self.inner
            .lock()
            .expect("fake backend mutex poisoned")
            .get(id)
            .cloned()
    }
}

fn offload_config_with(
    backend: Arc<dyn ExecutionResultBackend>,
    threshold: usize,
) -> OffloadConfig {
    OffloadConfig {
        threshold_bytes: threshold,
        backend,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_result_below_threshold_is_returned_directly() {
    // Property: a 5-byte result with a 1000-byte threshold stays
    // direct, even with offload wired. Only oversized payloads
    // take the offload path.
    let rt = LuaRuntime::new();
    let backend: Arc<dyn ExecutionResultBackend> = FakeBackend::new();
    let cfg = offload_config_with(Arc::clone(&backend), 1000);
    let outcome = rt
        .execute(
            "result('hi')".to_string(),
            empty_upstreams(),
            None,
            None,
            Some(cfg),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("small result must succeed");
    assert!(
        matches!(outcome, ExecutionOutcome::Direct(_)),
        "below-threshold result must stay Direct"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_result_above_threshold_is_offloaded() {
    // Property: the payload is persisted, the outcome names the
    // execution ID, and the byte_size reported matches what was
    // actually stored.
    let rt = LuaRuntime::new();
    let backend = FakeBackend::new();
    let backend_trait: Arc<dyn ExecutionResultBackend> = Arc::clone(&backend) as _;
    let cfg = offload_config_with(backend_trait, 50);
    // 200 copies of "x" = 202 bytes once serialized with quotes.
    let script = r#"
        local big = string.rep("x", 200)
        result(big)
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            Some(cfg),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("large result must succeed the offload path");

    let ExecutionOutcome::Offloaded {
        execution_id,
        byte_size,
        shape,
    } = outcome
    else {
        panic!("expected Offloaded outcome; got Direct");
    };
    assert!(byte_size > 50, "reported size must exceed threshold");
    assert_eq!(shape, crate::schema_inferrer::Shape::String);
    // Stored payload is what we'd decode on retrieval.
    let stored = backend
        .get(&execution_id)
        .expect("backend must hold the payload under the reported id");
    let decoded: serde_json::Value =
        serde_json::from_str(&stored).expect("stored payload round-trips as JSON");
    assert!(decoded.is_string());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn threshold_zero_disables_offload_entirely() {
    // Property: threshold == 0 means "offload is off". Even a
    // 1 MB payload stays Direct.
    let rt = LuaRuntime::new();
    let backend: Arc<dyn ExecutionResultBackend> = FakeBackend::new();
    let cfg = offload_config_with(Arc::clone(&backend), 0);
    let script = r#"
        local big = string.rep("x", 1024 * 1024)
        result(big)
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            Some(cfg),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("script with threshold=0 succeeds");
    assert!(
        matches!(outcome, ExecutionOutcome::Direct(_)),
        "threshold=0 must mean offload is disabled"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_builtin_names_roster_matches_installed_builtins() {
    // GATEWAY_BUILTIN_NAMES is the shared contract with the gateway's
    // execute-tool description (which enumerates the roster in its own
    // agent-tuned prose). This pins the roster to what
    // install_gateway_builtins actually installs: a builtin added or
    // removed without updating the roster fails here, and the gateway's
    // description tests fail transitively via the same constant.
    //
    // A backend is wired so the conditional get_result installs too —
    // the roster deliberately lists the FULL installable surface.
    let rt = LuaRuntime::new();
    let backend: Arc<dyn ExecutionResultBackend> = FakeBackend::new();
    let cfg = offload_config_with(backend, 1_000_000);
    let script = r#"
        local keys = {}
        for k in pairs(_gateway) do
            table.insert(keys, k)
        end
        table.sort(keys)
        result(keys)
    "#;
    let outcome = rt
        .execute(
            script.to_string(),
            empty_upstreams(),
            None,
            None,
            Some(cfg),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("key enumeration must succeed");
    let ExecutionOutcome::Direct(value) = outcome else {
        panic!("tiny key list must not offload");
    };
    let installed: Vec<String> = serde_json::from_value(value.clone())
        .expect("script returns a JSON array of builtin names");
    let mut roster: Vec<String> = GATEWAY_BUILTIN_NAMES
        .iter()
        .map(|s| s.to_string())
        .collect();
    roster.sort();
    assert_eq!(
        installed, roster,
        "GATEWAY_BUILTIN_NAMES must exactly match the installed _gateway.* surface"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_result_builtin_returns_stored_value() {
    // Property: a script can retrieve a previously-stored result
    // via _gateway.get_result({ id = ... }). Unknown IDs return nil.
    let rt = LuaRuntime::new();
    let backend = FakeBackend::new();
    // Pre-populate the backend so the test doesn't need two
    // executes.
    backend
        .inner
        .lock()
        .expect("fake backend mutex poisoned")
        .insert(
            "prestored-id".to_string(),
            r#"{"hello":"world"}"#.to_string(),
        );
    let backend_trait: Arc<dyn ExecutionResultBackend> = Arc::clone(&backend) as _;
    let cfg = offload_config_with(backend_trait, 10_000); // offload disabled by being high

    let script = r#"
        local data = _gateway.get_result({ id = "prestored-id" })
        result(data.hello)
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            Some(cfg),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("get_result script must succeed");
    assert_eq!(expect_direct(outcome), serde_json::json!("world"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_result_on_unknown_id_returns_nil() {
    let rt = LuaRuntime::new();
    let backend: Arc<dyn ExecutionResultBackend> = FakeBackend::new();
    let cfg = offload_config_with(Arc::clone(&backend), 10_000);
    let script = r#"
        local data = _gateway.get_result({ id = "no-such-id" })
        if data == nil then
            result("missing")
        else
            result("found")
        end
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            Some(cfg),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("script must succeed");
    assert_eq!(expect_direct(outcome), serde_json::json!("missing"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offload_backend_error_is_surfaced_as_offload_failed() {
    // Property: a backend that refuses to save (typically because
    // the single payload exceeds the per-entry ceiling) must
    // produce LuaExecutionError::OffloadFailed, not a generic
    // internal error.
    struct RefusingBackend;
    impl ExecutionResultBackend for RefusingBackend {
        fn save(&self, _payload: String) -> Result<String, String> {
            Err("no way, too big".to_string())
        }
        fn get(&self, _id: &str) -> Option<String> {
            None
        }
    }
    let rt = LuaRuntime::new();
    let backend: Arc<dyn ExecutionResultBackend> = Arc::new(RefusingBackend);
    let cfg = offload_config_with(backend, 10);
    let script = r#"
        result(string.rep("x", 100))
    "#
    .to_string();
    let err = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            Some(cfg),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("refusing backend must error");
    assert!(
        matches!(err, LuaExecutionError::OffloadFailed(_)),
        "got: {err}"
    );
}

// ----- `_gateway.expand_template` Lua-level smoke tests -----
//
// These cover the host-closure wiring only. Expansion correctness
// across RFC 6570 levels lives in `expand_template::tests` (FC),
// which exercises the pure Rust path without Lua overhead.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expand_template_available_without_offload_config() {
    // Property pin: `_gateway.expand_template` is installed for
    // every `execute` call, regardless of offload. If a future
    // refactor gates `_gateway` installation on offload again,
    // this test would fail with "attempt to index nil" at the
    // `_gateway` access.
    let rt = LuaRuntime::new();
    let script = r#"
        local uri = _gateway.expand_template({
            server = "rootly",
            template = "incident://{incident_id}",
            params = { incident_id = "42" },
        })
        result(uri)
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("expand_template without offload must succeed");
    assert_eq!(
        expect_direct(outcome),
        serde_json::json!("mcp-proxy://rootly/incident%3A%2F%2F42")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expand_template_coexists_with_get_result() {
    // Property pin: both builtins live on the same `_gateway`
    // table. A regression that overwrote one with the other's
    // installation (e.g. two `create_table` calls where only the
    // second wins) would fail this test.
    let rt = LuaRuntime::new();
    let backend = FakeBackend::new();
    backend
        .inner
        .lock()
        .expect("fake backend mutex poisoned")
        .insert("id-0".to_string(), r#""stored-value""#.to_string());
    let cfg = offload_config_with(
        Arc::clone(&backend) as Arc<dyn ExecutionResultBackend>,
        10_000,
    );
    let script = r#"
        local stored = _gateway.get_result({ id = "id-0" })
        local uri = _gateway.expand_template({
            server = "u",
            template = "x://{v}",
            params = { v = stored },
        })
        result(uri)
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            Some(cfg),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("both builtins must work in the same VM");
    // "stored-value" expanded and then re-encoded.
    let got = expect_direct(outcome);
    let as_str = got.as_str().expect("string outcome");
    assert!(as_str.starts_with("mcp-proxy://u/"), "got: {as_str}");
    assert!(
        as_str.contains("stored-value") || as_str.contains("stored%2Dvalue"),
        "expected expanded value in the URI, got: {as_str}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expand_template_handles_empty_params_table() {
    // Template with no placeholders doesn't need params. Agents
    // shouldn't be required to pass an empty table; `params = nil`
    // or absent `params` should both work.
    let rt = LuaRuntime::new();
    let script = r#"
        local a = _gateway.expand_template({
            server = "u",
            template = "static://resource",
        })
        local b = _gateway.expand_template({
            server = "u",
            template = "static://resource",
            params = nil,
        })
        local c = _gateway.expand_template({
            server = "u",
            template = "static://resource",
            params = {},
        })
        result({ a = a, b = b, c = c })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("no-params expand must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["a"], got["b"], "absent params must equal nil params");
    assert_eq!(got["a"], got["c"], "absent params must equal empty params");
    assert_eq!(
        got["a"],
        serde_json::json!("mcp-proxy://u/static%3A%2F%2Fresource")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expand_template_missing_server_errors_clearly() {
    // Property: the error message should point at the missing
    // field by name. An agent debugging a broken script wants to
    // know exactly which field they forgot.
    let rt = LuaRuntime::new();
    let script = r#"
        result(_gateway.expand_template({
            template = "x://{a}",
            params = { a = "b" },
        }))
    "#
    .to_string();
    let err = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("missing server must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("server"),
        "error must name the missing field; got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expand_template_non_table_arg_errors() {
    // Passing a string instead of a table should be caught with a
    // clear message rather than a cryptic deserialize failure.
    let rt = LuaRuntime::new();
    let script = r#"
        result(_gateway.expand_template("not-a-table"))
    "#
    .to_string();
    let err = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("string arg must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("expand_template") && msg.contains("table"),
        "error must mention expand_template and table shape; got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expand_template_level_4_list_round_trips_through_lua() {
    // Smoke test for the serde-deserialization path handling a
    // Lua list of strings correctly. `lua_value_to_json`
    // converts the table to a JSON array; the FC then delegates
    // to `uri_template`'s serde impl for `Value::List`. If any
    // step in that chain broke, we'd see a parse error here.
    let rt = LuaRuntime::new();
    let script = r#"
        local uri = _gateway.expand_template({
            server = "api",
            template = "https://example.com/search{?tag*}",
            params = { tag = {"rust", "mcp"} },
        })
        result(uri)
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("level 4 list must roundtrip");
    let got = expect_direct(outcome);
    // Decode the namespaced URI to verify both expansion and
    // namespacing.
    let s = got.as_str().expect("string outcome");
    let (name, uri) = mcp_client::resource_uri::decode(s).expect("namespaced URI must decode");
    assert_eq!(name, "api");
    assert_eq!(uri, "https://example.com/search?tag=rust&tag=mcp");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expand_template_invalid_template_errors_with_template_text() {
    // Property: the error message should include the offending
    // template so the agent can pinpoint what they sent wrong.
    // This is the FC's ParseFailed variant being surfaced
    // through the Lua layer.
    let rt = LuaRuntime::new();
    let script = r#"
        result(_gateway.expand_template({
            server = "u",
            template = "broken://{unclosed",
            params = { unclosed = "v" },
        }))
    "#
    .to_string();
    let err = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("unclosed template must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("broken"),
        "error must include the malformed template; got: {msg}"
    );
}

// ----- `_gateway.json_decode` / `_gateway.json_encode` smoke tests -----
//
// These pin the observable Lua-level behavior of the two builtins.
// Conversion semantics (array-vs-object heuristic, number
// precision, utf-8 handling) are covered by `lua_value_to_json` /
// `json_to_lua_value` tests; these scripts focus on:
//
// - the builtins are reachable from inside `execute`
// - they work without any offload backend
// - round-trips preserve shape for the value types agents use
// - error messages name the builtin for `pcall` callers

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_decode_parses_object_into_table() {
    let rt = LuaRuntime::new();
    let script = r#"
        local t = _gateway.json_decode('{"a": 1, "b": "two", "c": true, "d": null}')
        result({ a = t.a, b = t.b, c = t.c, d_absent = (t.d == nil) })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("json_decode must succeed on valid JSON");
    let got = expect_direct(outcome);
    assert_eq!(got["a"], serde_json::json!(1));
    assert_eq!(got["b"], serde_json::json!("two"));
    assert_eq!(got["c"], serde_json::json!(true));
    assert_eq!(got["d_absent"], serde_json::json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_decode_parses_array_into_sequence() {
    // Arrays must come back as dense 1-indexed tables so they
    // round-trip correctly through `result(...)`.
    let rt = LuaRuntime::new();
    let script = r#"
        local arr = _gateway.json_decode('[10, 20, 30]')
        result(arr)
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("json_decode must succeed on array");
    assert_eq!(expect_direct(outcome), serde_json::json!([10, 20, 30]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_decode_handles_nested_shapes() {
    // The shape agents most often face: a CallToolResult-style
    // envelope with nested arrays and objects. Without json_decode,
    // agents reinvent regex extractors to pick this apart.
    let rt = LuaRuntime::new();
    let script = r#"
        local t = _gateway.json_decode([[
            {
                "content": [
                    { "type": "text", "text": "hello" },
                    { "type": "text", "text": "world" }
                ],
                "isError": false
            }
        ]])
        result({
            first = t.content[1].text,
            second = t.content[2].text,
            err = t.isError,
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("json_decode must succeed on nested shape");
    let got = expect_direct(outcome);
    assert_eq!(got["first"], serde_json::json!("hello"));
    assert_eq!(got["second"], serde_json::json!("world"));
    assert_eq!(got["err"], serde_json::json!(false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_decode_preserves_string_escapes_and_unicode() {
    // Hand-rolled regex JSON extractors famously break on JSON
    // escapes and non-ASCII. Pin that json_decode handles both.
    //
    // The Lua source below uses a long-string literal (`[[...]]`)
    // so we can write JSON `\n` / `\"` escapes without getting
    // eaten by Lua's own short-string escape processing. The JSON
    // parser then sees the literal backslash-n / backslash-quote
    // sequences and decodes them correctly.
    let rt = LuaRuntime::new();
    let script = r#"
        local t = _gateway.json_decode([[{"msg": "line1\nline2 \"quoted\" — ✨"}]])
        result(t.msg)
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("json_decode must handle escapes + unicode");
    assert_eq!(
        expect_direct(outcome),
        serde_json::json!("line1\nline2 \"quoted\" — ✨")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_decode_malformed_input_errors_clearly() {
    // Property: the error must name the builtin so `pcall(...)`
    // callers inspecting the message know which call failed.
    let rt = LuaRuntime::new();
    let script = r#"
        result(_gateway.json_decode('{"unterminated'))
    "#
    .to_string();
    let err = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("malformed JSON must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("json_decode"),
        "error must name the builtin; got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_decode_non_string_arg_errors() {
    // Passing a table or number should produce a clean type
    // error rather than a serde parse failure on Lua's `tostring`
    // coercion. Agents passing the wrong shape should see
    // "expected a string" and know to wrap with tostring.
    let rt = LuaRuntime::new();
    let script = r#"
        result(_gateway.json_decode({ a = 1 }))
    "#
    .to_string();
    let err = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("table arg must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("json_decode") && msg.contains("string"),
        "error must name builtin and expected type; got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_decode_available_without_offload_backend() {
    // Property pin: `json_decode` is installed unconditionally,
    // not gated on an offload backend. A regression that hid
    // the builtin behind `if let Some(backend)` would fail here.
    let rt = LuaRuntime::new();
    let script = r#"
        local t = _gateway.json_decode('{"x": 42}')
        result(t.x)
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("json_decode must work without offload config");
    assert_eq!(expect_direct(outcome), serde_json::json!(42));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_encode_round_trips_tables() {
    // Encode then decode back and assert structural equality.
    // Stringly comparing output to a literal would over-specify
    // (JSON object key order is not stable).
    let rt = LuaRuntime::new();
    let script = r#"
        local original = { name = "alpha", count = 3, tags = {"a", "b"} }
        local text = _gateway.json_encode(original)
        local decoded = _gateway.json_decode(text)
        result({
            name_ok = (decoded.name == original.name),
            count_ok = (decoded.count == original.count),
            tag1_ok = (decoded.tags[1] == original.tags[1]),
            tag2_ok = (decoded.tags[2] == original.tags[2]),
            text_is_string = (type(text) == "string"),
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("encode + decode round-trip must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["name_ok"], serde_json::json!(true));
    assert_eq!(got["count_ok"], serde_json::json!(true));
    assert_eq!(got["tag1_ok"], serde_json::json!(true));
    assert_eq!(got["tag2_ok"], serde_json::json!(true));
    assert_eq!(got["text_is_string"], serde_json::json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_encode_handles_scalars() {
    // Numbers, strings, booleans, nil should all encode.
    let rt = LuaRuntime::new();
    let script = r#"
        result({
            num = _gateway.json_encode(42),
            str = _gateway.json_encode("hi"),
            tru = _gateway.json_encode(true),
            fal = _gateway.json_encode(false),
            nul = _gateway.json_encode(nil),
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("scalar encode must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["num"], serde_json::json!("42"));
    assert_eq!(got["str"], serde_json::json!("\"hi\""));
    assert_eq!(got["tru"], serde_json::json!("true"));
    assert_eq!(got["fal"], serde_json::json!("false"));
    assert_eq!(got["nul"], serde_json::json!("null"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_encode_rejects_functions() {
    // Functions/threads/userdata aren't JSON-serializable;
    // agents who accidentally try to encode a function should
    // see a clear error rather than silent null-replacement.
    let rt = LuaRuntime::new();
    let script = r#"
        local f = function() return 1 end
        result(_gateway.json_encode({ thing = f }))
    "#
    .to_string();
    let err = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("function encode must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("json_encode") && msg.contains("function"),
        "error must name builtin and offending type; got: {msg}"
    );
}

// ----- `_gateway.unwrap_content` Lua-level smoke tests -----
//
// The helper has two output paths: "envelope matched, return the
// text" and "anything else, return input unchanged." Tests pin
// both directions across the shapes agents actually encounter.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwrap_content_returns_text_for_single_block_envelope() {
    // The textbook MCP tool-call response shape. Must unwrap.
    let rt = LuaRuntime::new();
    let script = r#"
        local envelope = {
            content = { { type = "text", text = "hello world" } },
            isError = false,
        }
        result(_gateway.unwrap_content(envelope))
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("envelope unwrap must succeed");
    assert_eq!(expect_direct(outcome), serde_json::json!("hello world"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwrap_content_returns_first_block_for_multi_block_envelope() {
    // Multi-block is less common but valid. Matches what an
    // agent writing `x.content[1].text` by hand would get.
    let rt = LuaRuntime::new();
    let script = r#"
        local envelope = {
            content = {
                { type = "text", text = "first" },
                { type = "text", text = "second" },
            },
        }
        result(_gateway.unwrap_content(envelope))
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("multi-block envelope unwrap must succeed");
    assert_eq!(expect_direct(outcome), serde_json::json!("first"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwrap_content_passes_structured_return_through_unchanged() {
    // Structured returns (no `content` field) are the OTHER half
    // of the real-world shape-space. Must pass through unchanged
    // so callers can `unwrap_content(result)` unconditionally.
    let rt = LuaRuntime::new();
    let script = r#"
        local structured = { status = "ok", items = { "a", "b" } }
        local out = _gateway.unwrap_content(structured)
        result({
            status = out.status,
            first = out.items[1],
            second = out.items[2],
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("structured unwrap must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["status"], serde_json::json!("ok"));
    assert_eq!(got["first"], serde_json::json!("a"));
    assert_eq!(got["second"], serde_json::json!("b"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwrap_content_passes_scalars_through_unchanged() {
    // Strings, numbers, booleans, nil — none of them match the
    // envelope shape. They come out exactly as they went in.
    let rt = LuaRuntime::new();
    let script = r#"
        result({
            str = _gateway.unwrap_content("already a string"),
            num = _gateway.unwrap_content(42),
            tru = _gateway.unwrap_content(true),
            nul_is_nil = (_gateway.unwrap_content(nil) == nil),
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("scalar passthrough must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["str"], serde_json::json!("already a string"));
    assert_eq!(got["num"], serde_json::json!(42));
    assert_eq!(got["tru"], serde_json::json!(true));
    assert_eq!(got["nul_is_nil"], serde_json::json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwrap_content_passes_through_empty_content_array() {
    // `content = {}` → no block to unwrap → return input. The
    // caller can then test `#out.content == 0` themselves.
    let rt = LuaRuntime::new();
    let script = r#"
        local envelope = { content = {}, isError = false }
        local out = _gateway.unwrap_content(envelope)
        result({
            is_table = (type(out) == "table"),
            empty_content = (#out.content == 0),
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("empty content passthrough must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["is_table"], serde_json::json!(true));
    assert_eq!(got["empty_content"], serde_json::json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwrap_content_passes_through_when_first_block_has_no_text() {
    // An image block, or any shape where `content[1].text` is
    // absent. Must NOT substitute nil / error — return input so
    // the caller can inspect the block and do something sensible.
    let rt = LuaRuntime::new();
    let script = r#"
        local envelope = {
            content = { { type = "image", data = "base64..." } },
        }
        local out = _gateway.unwrap_content(envelope)
        result({
            preserved_type = out.content[1].type,
            preserved_data = out.content[1].data,
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("no-text-field passthrough must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["preserved_type"], serde_json::json!("image"));
    assert_eq!(got["preserved_data"], serde_json::json!("base64..."));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwrap_content_passes_through_when_text_is_not_a_string() {
    // Defensive: if some upstream ships `text = 42` (not spec
    // but observable in the wild), don't silently coerce. Leave
    // the caller to inspect.
    let rt = LuaRuntime::new();
    let script = r#"
        local envelope = { content = { { type = "text", text = 42 } } }
        local out = _gateway.unwrap_content(envelope)
        result({
            is_table = (type(out) == "table"),
            raw_text = out.content[1].text,
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("non-string text passthrough must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["is_table"], serde_json::json!(true));
    assert_eq!(got["raw_text"], serde_json::json!(42));
}

// ----- `rfc3339_from_epoch_seconds` pure-fn unit tests -----
//
// The public builtins layer on top of this. These tests pin the
// format shape and edge cases without any Lua-VM overhead.

#[test]
fn rfc3339_epoch_zero_is_unix_epoch_start() {
    assert_eq!(
        rfc3339_from_epoch_seconds(0),
        Some("1970-01-01T00:00:00Z".to_string())
    );
}

#[test]
fn rfc3339_known_recent_timestamp_matches_expected() {
    // 2023-01-01T00:00:00Z = 1672531200 (Datadog-friendly round date).
    assert_eq!(
        rfc3339_from_epoch_seconds(1_672_531_200),
        Some("2023-01-01T00:00:00Z".to_string())
    );
}

#[test]
fn rfc3339_pre_epoch_negative_timestamps_are_valid() {
    // -1 → one second before epoch. RFC3339 has no opinion about
    // BCE, chrono handles AD 1..=9999 fine, and we don't restrict.
    assert_eq!(
        rfc3339_from_epoch_seconds(-1),
        Some("1969-12-31T23:59:59Z".to_string())
    );
}

#[test]
fn rfc3339_leap_day_formats_correctly() {
    // 2024-02-29T12:00:00Z = 1709208000. Leap-year math via
    // chrono — pin that we get the actual leap day rather than
    // wrapping to March.
    assert_eq!(
        rfc3339_from_epoch_seconds(1_709_208_000),
        Some("2024-02-29T12:00:00Z".to_string())
    );
}

#[test]
fn rfc3339_extreme_timestamp_returns_none() {
    // Far outside chrono's representable range. We choose to
    // surface this as an error rather than panicking or wrapping.
    assert_eq!(rfc3339_from_epoch_seconds(i64::MAX), None);
    assert_eq!(rfc3339_from_epoch_seconds(i64::MIN), None);
}

#[test]
fn rfc3339_format_always_ends_with_z() {
    // Property: every returned string has a trailing `Z`. Agents
    // copy-paste output into upstream query args that demand the
    // `Z` suffix; any variant (offset, no-suffix) would break
    // them silently.
    for secs in [0, 1, -1, 86_400, 1_000_000_000, 1_700_000_000, -100_000_000] {
        let out = rfc3339_from_epoch_seconds(secs).expect("finite timestamp must format");
        assert!(
            out.ends_with('Z'),
            "expected trailing Z for {secs}; got: {out}"
        );
        assert!(
            !out.contains('.'),
            "expected no sub-seconds for {secs}; got: {out}"
        );
    }
}

// ----- `_gateway.rfc3339` / `_gateway.now_minus` Lua smoke tests -----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rfc3339_builtin_formats_epoch_seconds() {
    let rt = LuaRuntime::new();
    let script = r#"
        result({
            epoch = _gateway.rfc3339(0),
            y2k = _gateway.rfc3339(946684800),   -- 2000-01-01T00:00:00Z
            future = _gateway.rfc3339(2147483647), -- 2038-01-19T03:14:07Z
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("rfc3339 must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["epoch"], serde_json::json!("1970-01-01T00:00:00Z"));
    assert_eq!(got["y2k"], serde_json::json!("2000-01-01T00:00:00Z"));
    assert_eq!(got["future"], serde_json::json!("2038-01-19T03:14:07Z"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rfc3339_builtin_accepts_float_and_truncates() {
    // Agents might compute a float timestamp with fractional
    // seconds. We truncate toward zero — the RFC3339 shape has
    // no sub-second component anyway, so rounding vs. truncating
    // is indistinguishable for the common "last N minutes" case.
    let rt = LuaRuntime::new();
    let script = r#"
        result(_gateway.rfc3339(1672531200.9))
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("rfc3339 must accept floats");
    assert_eq!(
        expect_direct(outcome),
        serde_json::json!("2023-01-01T00:00:00Z")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rfc3339_non_numeric_arg_errors() {
    let rt = LuaRuntime::new();
    let script = r#"
        result(_gateway.rfc3339("not a number"))
    "#
    .to_string();
    let err = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("string arg must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("rfc3339") && msg.contains("number"),
        "error must name builtin and expected type; got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn now_minus_returns_plausible_recent_timestamp() {
    // Property: `now_minus(0)` returns a timestamp within a few
    // seconds of the test's own read of now. Cannot assert exact
    // equality because clock advances between the host-closure
    // call and the test's `Utc::now()` call, but "bounded drift"
    // is a solid signal the builtin is reading the system clock
    // correctly.
    let rt = LuaRuntime::new();
    let before = chrono::Utc::now().timestamp();
    let script = r#"
        result(_gateway.now_minus(0))
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("now_minus(0) must succeed");
    let after = chrono::Utc::now().timestamp();

    let got = expect_direct(outcome);
    let s = got.as_str().expect("string outcome");
    // Parse back to unix seconds and assert within [before, after].
    let parsed = chrono::DateTime::parse_from_rfc3339(s).expect("output must be valid RFC3339");
    let parsed_secs = parsed.timestamp();
    assert!(
        parsed_secs >= before && parsed_secs <= after,
        "now_minus(0) = {s} ({parsed_secs}) outside [{before}, {after}]"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn now_minus_subtracts_from_now() {
    // `now_minus(3600)` should be an hour-ish behind `now_minus(0)`.
    let rt = LuaRuntime::new();
    let script = r#"
        local a = _gateway.now_minus(0)
        local b = _gateway.now_minus(3600)
        result({ a = a, b = b })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("now_minus comparison must succeed");
    let got = expect_direct(outcome);
    let a = chrono::DateTime::parse_from_rfc3339(got["a"].as_str().expect("string a"))
        .expect("a must be valid RFC3339");
    let b = chrono::DateTime::parse_from_rfc3339(got["b"].as_str().expect("string b"))
        .expect("b must be valid RFC3339");
    let delta = a.timestamp() - b.timestamp();
    // Allow a few-second slack in case there's any scheduling
    // drift between the two calls.
    assert!(
        (3595..=3605).contains(&delta),
        "expected ~3600s between a and b, got {delta}s (a={a}, b={b})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn now_minus_accepts_negative_for_future_times() {
    // Deliberate choice: negative `seconds` → future timestamp.
    // Agents might want "now + 10 minutes" for a scheduled query;
    // one builtin handles both directions.
    let rt = LuaRuntime::new();
    let script = r#"
        local future = _gateway.now_minus(-600)
        local now = _gateway.now_minus(0)
        result({ future = future, now = now })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("negative now_minus must succeed");
    let got = expect_direct(outcome);
    let future = chrono::DateTime::parse_from_rfc3339(got["future"].as_str().expect("future"))
        .expect("future must parse");
    let now = chrono::DateTime::parse_from_rfc3339(got["now"].as_str().expect("now"))
        .expect("now must parse");
    assert!(
        future.timestamp() > now.timestamp(),
        "future ({future}) must be after now ({now})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rfc3339_and_now_minus_compose_with_json_encode() {
    // Common use case: build a time-windowed query body as a Lua
    // table, then encode it to send. Pin that the two surfaces
    // play nicely together end-to-end.
    let rt = LuaRuntime::new();
    let script = r#"
        local body = {
            from = _gateway.now_minus(3600),
            to = _gateway.now_minus(0),
            query = "{job=\"test\"}",
        }
        local encoded = _gateway.json_encode(body)
        local decoded = _gateway.json_decode(encoded)
        result({
            from_ends_z = (string.sub(decoded.from, -1) == "Z"),
            to_ends_z = (string.sub(decoded.to, -1) == "Z"),
            query_preserved = (decoded.query == body.query),
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("compose must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["from_ends_z"], serde_json::json!(true));
    assert_eq!(got["to_ends_z"], serde_json::json!(true));
    assert_eq!(got["query_preserved"], serde_json::json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwrap_content_composes_with_json_decode() {
    // The composable use case: upstream returned a JSON-in-text
    // envelope. Agent wants the decoded payload. Pattern is:
    // json_decode(unwrap_content(raw)). Pin that this composes.
    let rt = LuaRuntime::new();
    let script = r#"
        local raw = {
            content = {
                { type = "text", text = '{"ok": true, "count": 3}' },
            },
        }
        local decoded = _gateway.json_decode(_gateway.unwrap_content(raw))
        result({ ok = decoded.ok, count = decoded.count })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("composed unwrap+decode must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["ok"], serde_json::json!(true));
    assert_eq!(got["count"], serde_json::json!(3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_encode_then_decode_is_identity_for_agent_shapes() {
    // The canonical agent use case: got a CallToolResult from
    // an upstream, want to encode+persist or forward+re-decode
    // without losing shape. This tests the full fidelity path
    // for the exact shape from `json_decode_handles_nested_shapes`.
    let rt = LuaRuntime::new();
    let script = r#"
        local original = {
            content = {
                { type = "text", text = "hello" },
                { type = "text", text = "world" },
            },
            isError = false,
        }
        local text = _gateway.json_encode(original)
        local decoded = _gateway.json_decode(text)
        result({
            first = decoded.content[1].text,
            second = decoded.content[2].text,
            err = decoded.isError,
            types_match = (decoded.content[1].type == "text"),
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("envelope round-trip must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["first"], serde_json::json!("hello"));
    assert_eq!(got["second"], serde_json::json!("world"));
    assert_eq!(got["err"], serde_json::json!(false));
    assert_eq!(got["types_match"], serde_json::json!(true));
}

// ----- _gateway.parallel Lua-visible behaviour -----
//
// These tests exercise the builtin from inside the Luau sandbox
// without needing a real upstream. They pin:
// - Empty input round-trips to an empty array.
// - Bad input shapes produce pcall-catchable errors with the
//   task index in the message.
// - An unknown upstream (nothing registered) yields a per-task
//   `{ok=false}` envelope, not a whole-call abort. This is the
//   load-bearing "one bad task doesn't kill the batch" property.
//
// End-to-end parallelism behaviour (two real upstream calls
// running concurrently under join_all) lives in the gateway
// integration test at `crates/gateway/tests/parallel_e2e.rs`.
//
// The sanitized-name resolution step (`resolve_sanitized_among`)
// is unit-tested directly below: neither harness above can reach
// its collision branch (the empty-upstream tests fail earlier at
// "upstream not registered", and the gateway's inspection guard
// rejects uninspectable colliding tools before resolution), so
// the pure function is the only place to pin that behaviour.

fn tool_names(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| s.to_string()).collect()
}

#[test]
fn resolve_sanitized_unique_match_returns_original() {
    let tools = tool_names(&["get-issue", "list-projects"]);
    assert_eq!(
        resolve_sanitized_among(&tools, "get_issue"),
        Some("get-issue".to_string())
    );
}

#[test]
fn resolve_sanitized_collision_resolves_to_none() {
    // `get-issue` and `get.issue` both sanitize to `get_issue`.
    // Neither may be dispatched: returning the first match would
    // let a caller inspect one tool and invoke the other. The
    // unrelated sibling must be unaffected by the collision.
    let tools = tool_names(&["get-issue", "get.issue", "list-projects"]);
    assert_eq!(resolve_sanitized_among(&tools, "get_issue"), None);
    assert_eq!(
        resolve_sanitized_among(&tools, "list_projects"),
        Some("list-projects".to_string())
    );
}

#[test]
fn resolve_sanitized_no_match_returns_none() {
    let tools = tool_names(&["get-issue"]);
    assert_eq!(resolve_sanitized_among(&tools, "does_not_exist"), None);
    assert_eq!(resolve_sanitized_among(&[], "anything"), None);
}

#[test]
fn resolve_sanitized_matches_partition_semantics() {
    // Property: for any requested name, the pure resolver must
    // agree with `partition_unique_by_sanitized` — resolvable iff
    // the sanitized name is in the unique partition.
    let tools = tool_names(&["get-issue", "get.issue", "nil", "_nil", "1foo", "safe-tool"]);
    let (unique, collisions) = partition_unique_by_sanitized(&tools);
    for original in &tools {
        let sanitized = sanitize_lua_identifier(original);
        let resolved = resolve_sanitized_among(&tools, &sanitized);
        if collisions.contains_key(&sanitized) {
            assert_eq!(resolved, None, "colliding '{sanitized}' must not resolve");
        } else {
            assert!(
                unique.contains(original),
                "test invariant: non-colliding tool is in unique set"
            );
            assert_eq!(
                resolved.as_deref(),
                Some(original.as_str()),
                "unique '{sanitized}' must resolve to its original"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_empty_array_returns_empty_array() {
    let rt = LuaRuntime::new();
    let script = r#"
        local results = _gateway.parallel({})
        result({ count = #results, is_table = type(results) == "table" })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("empty parallel call must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["count"], serde_json::json!(0));
    assert_eq!(got["is_table"], serde_json::json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_rejects_non_array_argument() {
    // Property: passing an object (keyed table) instead of an
    // array surfaces as a pcall-able Lua error with the
    // "expected an array table" message from the validator.
    let rt = LuaRuntime::new();
    let script = r#"
        local ok, err = pcall(function()
            return _gateway.parallel({ server = "x", tool = "y" })
        end)
        result({ ok = ok, err = tostring(err) })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("script runs; pcall catches the builtin's error");
    let got = expect_direct(outcome);
    assert_eq!(got["ok"], serde_json::json!(false));
    let err_msg = got["err"].as_str().expect("err is a string");
    assert!(
        err_msg.contains("expected an array table"),
        "error must mention the shape mismatch; got: {err_msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_rejects_task_missing_server_field_with_1_based_index() {
    let rt = LuaRuntime::new();
    let script = r#"
        local ok, err = pcall(function()
            return _gateway.parallel({
                { server = "x", tool = "y" },
                { tool = "z" },   -- missing server at index 2
            })
        end)
        result({ ok = ok, err = tostring(err) })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("script runs; pcall catches the validation error");
    let got = expect_direct(outcome);
    assert_eq!(got["ok"], serde_json::json!(false));
    let err_msg = got["err"].as_str().expect("err is a string");
    assert!(
        err_msg.contains("task #2"),
        "error must carry 1-based task index; got: {err_msg}"
    );
    assert!(
        err_msg.contains("server"),
        "error must name the missing field; got: {err_msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_unknown_upstream_becomes_per_task_error_envelope() {
    // Property: with no upstreams registered, a task referencing
    // `fake_upstream` resolves to a `{ok=false}` envelope rather
    // than aborting the whole parallel call. This is the
    // "batch survives a single bad task" invariant.
    let rt = LuaRuntime::new();
    let script = r#"
        local results = _gateway.parallel({
            { server = "does_not_exist", tool = "also_fake", args = {} },
        })
        result({
            count = #results,
            ok = results[1].ok,
            has_error = type(results[1].error) == "string",
            error = results[1].error,
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("call itself must succeed even though the task fails");
    let got = expect_direct(outcome);
    assert_eq!(got["count"], serde_json::json!(1));
    assert_eq!(got["ok"], serde_json::json!(false));
    assert_eq!(got["has_error"], serde_json::json!(true));
    let err_text = got["error"].as_str().expect("error is a string");
    assert!(
        err_text.contains("not registered") || err_text.contains("does_not_exist"),
        "error should identify the missing upstream; got: {err_text}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_inspection_guard_rejection_aborts_entire_call() {
    // Property: when the guard rejects any task, the whole
    // _gateway.parallel call surfaces a NotInspected error —
    // not a per-task envelope. Inspection enforcement is
    // intentionally not gameable via the parallel dispatch path.
    let rt = LuaRuntime::new();
    // A guard that rejects everything; this mirrors "nothing
    // inspected this session".
    let guard: ToolCallGuard = Arc::new(|_server: &str, _tool: &str| false);

    let script = r#"
        local ok, err = pcall(function()
            return _gateway.parallel({
                { server = "x", tool = "y" },
            })
        end)
        result({ ok = ok, err = tostring(err) })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            Some(guard),
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("script runs; pcall catches the NotInspected error");
    let got = expect_direct(outcome);
    assert_eq!(
        got["ok"],
        serde_json::json!(false),
        "guard rejection must surface as a caught error, not a success"
    );
    let err_msg = got["err"].as_str().expect("err is a string");
    assert!(
        err_msg.contains("cannot be called without prior inspection")
            || err_msg.contains("NotInspected")
            || err_msg.contains("tool-details"),
        "error must identify as an inspection failure; got: {err_msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_preserves_input_order_across_mixed_outcomes() {
    // Property: results[i] corresponds to input[i], regardless
    // of which completed first. Without real upstreams, we test
    // this by having every task resolve to the "upstream not
    // registered" envelope — the script then verifies each
    // envelope names the server it was supposed to hit.
    let rt = LuaRuntime::new();
    let script = r#"
        local results = _gateway.parallel({
            { server = "alpha", tool = "a" },
            { server = "bravo", tool = "b" },
            { server = "charlie", tool = "c" },
        })
        result({
            count = #results,
            -- Each error string should mention the distinct server name.
            a_matches = string.find(results[1].error, "alpha") ~= nil,
            b_matches = string.find(results[2].error, "bravo") ~= nil,
            c_matches = string.find(results[3].error, "charlie") ~= nil,
        })
    "#
    .to_string();
    let outcome = rt
        .execute(
            script,
            empty_upstreams(),
            None,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("parallel call of three bad tasks must succeed");
    let got = expect_direct(outcome);
    assert_eq!(got["count"], serde_json::json!(3));
    assert_eq!(got["a_matches"], serde_json::json!(true));
    assert_eq!(got["b_matches"], serde_json::json!(true));
    assert_eq!(got["c_matches"], serde_json::json!(true));
}

// ----- mlua FFI panic-safety (F-13) -----
//
// Document what mlua does when a host closure panics.
//
// DISCOVERY (2026-04-22): with mlua 0.11 + `send` feature + Luau backend,
// panics in host closures are NOT caught by mlua. The panic propagates
// through exec() and unwinds the calling thread. This means:
//
//   - Any .unwrap() or .expect() that panics inside a `create_function`
//     closure will abort the spawn_blocking thread, causing a JoinError
//     at the execute() call site and ultimately a runtime-internal error
//     for the MCP client.
//   - The spawn_blocking thread survives (Tokio catches the panic), but
//     the individual `execute()` call fails.
//
// Current mitigation: all host closures in this crate use LuaError::external
// rather than panic/unwrap for expected failure modes. The only .expect()
// calls inside closures are on invariants that are genuinely unreachable
// (e.g. "result slot mutex poisoned"). This is acceptable: a poisoned mutex
// implies a prior panic on the same thread, so the process is already in a
// degraded state.
//
// Future: if closures start using .unwrap() for brevity, add a
// std::panic::catch_unwind wrapper in install_builtin/install_upstream_globals
// and convert panics to LuaExecutionError::Internal("panic in host: …").
//
// The test below pins the current (propagating) behaviour so a future mlua
// version that changes this is detected.

#[test]
#[should_panic(expected = "intentional test panic")]
fn panic_in_host_callback_propagates_through_mlua_ffi() {
    // Pins the observed mlua behaviour: host-closure panics propagate
    // rather than being caught. If mlua ever starts catching panics
    // internally, this test will fail (no panic observed), and the
    // comment above should be updated to reflect the new safety guarantee.
    let lua = Lua::new();
    lua.globals()
        .set(
            "panicking_fn",
            lua.create_function(|_lua, _: Value| -> mlua::Result<Value> {
                panic!("intentional test panic")
            })
            .expect("create_function must succeed"),
        )
        .expect("set global must succeed");

    // This line panics — mlua does NOT catch_unwind host closures.
    let _ = lua.load("panicking_fn()").exec();
}
