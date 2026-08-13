//! Session-resolution and header-gating tests. These exercise the
//! pure inner (`resolve_session_from_parts` / `require_session`)
//! without a full `RequestContext` — adding a fake `http::request::Parts`
//! directly is all that's needed.
use super::*;

fn parts_with_headers(pairs: &[(&str, &str)]) -> http::request::Parts {
    let mut req = http::Request::builder();
    for (k, v) in pairs {
        req = req.header(*k, *v);
    }
    let req = req.body(()).expect("test request builds");
    let (parts, _) = req.into_parts();
    parts
}

/// Tiny label helper for assertion failure messages — `SessionResolution`
/// doesn't derive Debug so we hand-roll it for the panic strings only.
fn other_label(r: &SessionResolution) -> &'static str {
    match r {
        SessionResolution::HttpSession(_) => "HttpSession",
        SessionResolution::NoTransportContext => "NoTransportContext",
        SessionResolution::MissingHeader => "MissingHeader",
    }
}

#[test]
fn no_parts_means_stdio_transport() {
    // Property: requests with no http::request::Parts in extensions
    // come from a non-HTTP transport (stdio in production, the
    // bridge unit tests in dev). This must NOT be conflated with
    // "HTTP request that forgot the header" — stdio has nothing to
    // gate on, so we let it through; HTTP-without-header is a
    // protocol violation we reject.
    match resolve_session_from_parts(None) {
        SessionResolution::NoTransportContext => {}
        other => panic!("expected NoTransportContext; got {:?}", other_label(&other)),
    }
}

#[test]
fn http_with_session_header_resolves_to_session() {
    let parts = parts_with_headers(&[(MCP_SESSION_ID_HEADER, "session-abc-123")]);
    match resolve_session_from_parts(Some(&parts)) {
        SessionResolution::HttpSession(id) => assert_eq!(id, "session-abc-123"),
        other => panic!("expected HttpSession; got {:?}", other_label(&other)),
    }
}

#[test]
fn http_without_session_header_fails_closed() {
    // The load-bearing test for Commit B: an HTTP request that omits
    // the Mcp-Session-Id header must NOT silently fall through to
    // the permissive stdio branch. Otherwise any header-skipping
    // client would bypass the inspection gate entirely.
    let parts = parts_with_headers(&[("X-Other-Header", "irrelevant")]);
    match resolve_session_from_parts(Some(&parts)) {
        SessionResolution::MissingHeader => {}
        other => panic!("expected MissingHeader; got {:?}", other_label(&other)),
    }
}

#[test]
fn require_session_passes_through_http_session() {
    let resolved = SessionResolution::HttpSession("session-xyz".to_string());
    let out = require_session(resolved).expect("HttpSession is Ok");
    assert_eq!(out.as_deref(), Some("session-xyz"));
}

#[test]
fn require_session_passes_through_stdio_as_none() {
    let resolved = SessionResolution::NoTransportContext;
    let out = require_session(resolved).expect("NoTransportContext is Ok");
    assert!(out.is_none(), "stdio resolves to Ok(None), got {out:?}");
}

#[test]
fn require_session_rejects_missing_header_with_invalid_params() {
    // Verifies the JSON-RPC error shape: a missing required header
    // is a malformed request at the protocol level, not a script
    // failure or an internal gateway bug. invalid_params is the
    // right code (-32602).
    let resolved = SessionResolution::MissingHeader;
    let err = require_session(resolved).expect_err("MissingHeader must be Err");
    assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    let msg = err.message.to_string();
    assert!(
        msg.contains("Mcp-Session-Id"),
        "error must name the missing header so a client developer can find it: {msg}"
    );
}
