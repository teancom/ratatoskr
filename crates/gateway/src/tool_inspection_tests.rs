use super::*;

fn tool(server: &str, name: &str) -> InspectedTool {
    InspectedTool::new(server, name)
}

#[test]
fn unknown_session_reports_not_inspected() {
    let store = ToolInspectionStore::new();
    assert!(
        !store.is_inspected("missing-session", &tool("gitlab", "download_job_log")),
        "unknown sessions must default to not-inspected (fail closed)"
    );
}

#[test]
fn mark_then_is_inspected_is_true() {
    let store = ToolInspectionStore::new();
    let t = tool("datadog_us", "search_datadog_logs");
    store.mark("session-a", t.clone());
    assert!(store.is_inspected("session-a", &t));
}

#[test]
fn mark_is_idempotent() {
    // Marking twice must leave exactly one entry. If the set grew
    // on every call, long sessions would leak memory proportional
    // to how often the agent re-inspected.
    let store = ToolInspectionStore::new();
    let t = tool("gitlab", "list_pipeline_jobs");
    store.mark("s", t.clone());
    store.mark("s", t.clone());
    store.mark("s", t.clone());
    // We don't expose a per-session-tool-count publicly, but we
    // can verify behavioral idempotence: the tool is inspected,
    // forget clears it in one shot, and it's back to not-inspected.
    assert!(store.is_inspected("s", &t));
    store.forget_session("s");
    assert!(!store.is_inspected("s", &t));
}

#[test]
fn sessions_are_isolated() {
    // Critical security property: inspection in session A must
    // not grant access in session B. Otherwise a multi-client
    // gateway leaks capability between users.
    let store = ToolInspectionStore::new();
    let t = tool("atlassian", "jira_get_issue");
    store.mark("alice", t.clone());

    assert!(store.is_inspected("alice", &t));
    assert!(
        !store.is_inspected("bob", &t),
        "session bob must not see session alice's inspections"
    );
}

#[test]
fn different_tools_are_independent() {
    let store = ToolInspectionStore::new();
    store.mark("s", tool("gitlab", "list_pipeline_jobs"));
    assert!(store.is_inspected("s", &tool("gitlab", "list_pipeline_jobs")));
    assert!(
        !store.is_inspected("s", &tool("gitlab", "download_job_log")),
        "inspecting one tool must not grant access to a sibling tool"
    );
}

#[test]
fn same_tool_different_servers_are_independent() {
    // Two upstreams might each expose a `search` tool; inspecting
    // `datadog_us.search` must not grant `glean.search`.
    let store = ToolInspectionStore::new();
    store.mark("s", tool("datadog_us", "search"));
    assert!(store.is_inspected("s", &tool("datadog_us", "search")));
    assert!(
        !store.is_inspected("s", &tool("glean", "search")),
        "same tool name on a different server must require its own inspection"
    );
}

#[test]
fn forget_session_is_scoped() {
    let store = ToolInspectionStore::new();
    let t = tool("gitlab", "list_pipeline_jobs");
    store.mark("keep", t.clone());
    store.mark("drop", t.clone());
    assert_eq!(store.session_count(), 2);

    store.forget_session("drop");
    assert_eq!(store.session_count(), 1);
    assert!(store.is_inspected("keep", &t));
    assert!(!store.is_inspected("drop", &t));
}

#[test]
fn forget_unknown_session_is_not_an_error() {
    // Defensive: the session-ended hook might fire after a session
    // that never inspected anything. That must be a silent no-op,
    // not a failure.
    let store = ToolInspectionStore::new();
    store.forget_session("never-seen-you-before");
}

#[test]
fn concurrent_mark_and_forget_is_bounded() {
    // Race scenario: `forget_session` (session closed hook) fires
    // while a `spawn_blocking` task for tool-details is mid-flight
    // and about to call `mark` for the same session. The RwLock
    // serializes the writers, so there is no data corruption — but
    // depending on interleaving, `mark` may land *after* `forget`
    // and recreate a row for a session that will never be closed
    // again. That ghost row is the bounded consequence we're
    // pinning: at most one entry for this session, never unbounded
    // growth.
    //
    // We loop to make the interleaving actually happen sometimes;
    // the assertion holds regardless of which thread wins.
    for _ in 0..100 {
        let store = ToolInspectionStore::new();
        store.mark("racer", tool("gitlab", "list_pipeline_jobs"));

        let s1 = store.clone();
        let s2 = store.clone();
        let t1 = std::thread::spawn(move || {
            s1.forget_session("racer");
        });
        let t2 = std::thread::spawn(move || {
            s2.mark("racer", tool("gitlab", "list_pipeline_jobs"));
        });
        t1.join().expect("forget thread panicked");
        t2.join().expect("mark thread panicked");

        // Whichever thread won, the session row count is bounded:
        // 0 if forget ran last, 1 if mark ran last. Never more.
        assert!(
            store.session_count() <= 1,
            "concurrent mark+forget produced {} session rows; expected <=1",
            store.session_count()
        );
    }
}
