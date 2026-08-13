// The osascript shell-out itself is untestable I/O (it would raise a
// real banner on the build machine), and the anti-spam edge logic is
// tested where it lives — `UpstreamHealth::enter_needs_login` in
// `health.rs`. What we CAN pin here is the AppleScript-literal
// sanitizer, since a quote slipping through would produce a malformed
// script that silently fails to notify.
#[cfg(target_os = "macos")]
#[test]
fn sanitize_strips_applescript_metacharacters() {
    use super::sanitize_for_applescript;
    assert_eq!(sanitize_for_applescript("Datadog-Acme"), "Datadog-Acme");
    assert_eq!(
        sanitize_for_applescript(r#"weird"name\with-meta"#),
        "weirdnamewith-meta"
    );
    // A name that's all metacharacters degrades to empty rather than
    // producing a broken literal.
    assert_eq!(sanitize_for_applescript(r#""\"#), "");
}

// NOTE: `notify()` itself is deliberately NOT unit-tested — invoking it
// on a dev machine would raise a real desktop banner during `cargo
// test`. Its only branching logic (the edge dedup) lives in
// `UpstreamHealth::enter_needs_login` and is tested there; the rest is
// a detached `osascript` spawn with errors ignored by design.
