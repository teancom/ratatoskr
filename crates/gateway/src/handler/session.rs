// Session-resolution helpers for the gateway handler.
//
// These pure functions translate rmcp RequestContext shapes into the
// session-ID / transport-kind information that the inspection gate and
// subscription machinery need. All logic here is free of handler state;
// `resolve_session_from_parts` is the testable pure inner so tests don't
// need a full RequestContext.

use rmcp::{
    ErrorData,
    model::CallToolRequestParams,
    service::{RequestContext, RoleServer},
};

use super::MCP_SESSION_ID_HEADER;

/// Result of resolving the session ID from a `RequestContext`.
///
/// Three distinct cases the inspection-touching tools need to handle
/// differently:
///
///   - `HttpSession(id)`: a normal HTTP request carrying the
///     `Mcp-Session-Id` header. Inspection state is keyed by `id`.
///   - `NoTransportContext`: no `http::request::Parts` in extensions.
///     This is the stdio transport, where there is no header to read
///     and no shared multi-client surface to protect against. The
///     inspection gate intentionally permits all tool calls in this
///     mode — there's no way to inspect via HTTP from stdio anyway,
///     and unit tests exercise the bridge through this path.
///   - `MissingHeader`: HTTP request without `Mcp-Session-Id`. The
///     MCP spec requires the header on initialized streamable-HTTP
///     sessions, so a missing one means a misbehaving (or malicious)
///     client. We fail closed: deny inspection-related operations
///     rather than silently fall through to the "no session" allow
///     branch, which would let any header-skipping client bypass the
///     gate entirely.
pub(super) enum SessionResolution {
    HttpSession(String),
    NoTransportContext,
    MissingHeader,
}

/// Diagnostic returned by every inspection-touching tool when the
/// session header is missing on an HTTP transport. Sentence-fragment
/// lowercase per house style; references the header by its on-the-wire
/// name so a client developer can pattern-match against it.
pub(super) const MISSING_SESSION_ERROR: &str = "request is missing the 'Mcp-Session-Id' header; this header is required for any \
     inspection-related call on the streamable-http transport";

/// Resolve the session for a request.
///
/// Distinguishes "stdio transport, no session header to read"
/// (deliberately permissive) from "HTTP transport, header missing"
/// (fail closed). See [`SessionResolution`] for the rationale.
pub(super) fn session_id_from_context(context: &RequestContext<RoleServer>) -> SessionResolution {
    let parts = context.extensions.get::<http::request::Parts>();
    resolve_session_from_parts(parts)
}

/// Pure inner of `session_id_from_context`. Separated so it's testable
/// without constructing a full `RequestContext` (whose shape is
/// rmcp-internal and verbose to fake).
pub(super) fn resolve_session_from_parts(
    parts: Option<&http::request::Parts>,
) -> SessionResolution {
    let Some(parts) = parts else {
        return SessionResolution::NoTransportContext;
    };
    match parts
        .headers
        .get(MCP_SESSION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        Some(id) => SessionResolution::HttpSession(id.to_string()),
        None => SessionResolution::MissingHeader,
    }
}

/// Convert a [`SessionResolution`] into the `Option<String>` shape the
/// inspection-touching methods consume, failing closed on the
/// HTTP-without-header case.
///
/// The two `Ok` shapes:
/// - `Ok(Some(id))` — normal HTTP session.
/// - `Ok(None)` — stdio / no transport context. Methods treat this as
///   "no session to mark, no guard to enforce" — the historical
///   permissive path.
///
/// The `Err` shape: HTTP request missing the `Mcp-Session-Id` header.
/// Surfaced as `invalid_params` because the request itself is
/// non-conformant with the MCP streamable-HTTP spec, not because the
/// agent's tool call is wrong.
pub(super) fn require_session(resolution: SessionResolution) -> Result<Option<String>, ErrorData> {
    match resolution {
        SessionResolution::HttpSession(id) => Ok(Some(id)),
        SessionResolution::NoTransportContext => Ok(None),
        SessionResolution::MissingHeader => Err(ErrorData::invalid_params(
            MISSING_SESSION_ERROR.to_string(),
            None,
        )),
    }
}

/// Extract a required string argument from a `CallToolRequestParams`,
/// returning a clean `invalid_params` error with the offending field
/// name if it's missing or the wrong type.
pub(super) fn parse_string_arg(
    request: &CallToolRequestParams,
    field: &str,
) -> Result<String, ErrorData> {
    request
        .arguments
        .as_ref()
        .and_then(|m| m.get(field))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            ErrorData::invalid_params(
                format!("'{field}' argument is required and must be a string"),
                None,
            )
        })
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
