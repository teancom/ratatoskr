// pattern: Functional Core
//
// Codec for the gateway's namespaced resource URI scheme.
//
// Why this exists
// ---------------
// Upstream MCP servers expose resources under arbitrary URI schemes
// (`rootly://incidents`, `datadog://mcp/toolsets`, `file:///…`). When
// the gateway aggregates them for a downstream client it has to keep
// two upstreams with the same bare URI distinguishable and remember
// which upstream a downstream `resources/read` should route to.
//
// The gateway answers both with a single namespaced URI shape:
//
//     mcp-proxy://<lua_name>/<percent-encoded-upstream-uri>
//
// - Scheme `mcp-proxy` is the gateway's marker. Every namespaced URI the
//   gateway hands downstream starts with exactly this prefix.
// - Authority `<lua_name>` is the sanitized upstream identifier
//   (`sanitize_lua_identifier` output). It's the same key that
//   `UpstreamManager` uses, so routing is a single HashMap lookup.
// - The path, after the single leading `/`, is the upstream URI
//   percent-encoded with `NON_ALPHANUMERIC`. Aggressive encoding keeps
//   the path component free of any character that could be misread as
//   a URI delimiter (`?`, `#`, `/`, `%`, …), and handles arbitrary
//   UTF-8 in upstream URIs cleanly.
//
// Encoding policy note
// --------------------
// We deliberately use `NON_ALPHANUMERIC` (encode everything except
// `[A-Za-z0-9]`) rather than a looser set that preserves, say, `{`
// and `}` literal. A gsub-friendly fallback for Level-1 templates was
// considered and rejected: the primary template path is
// `_gateway.expand_template`, and the cost of a second "which chars
// are safe here?" maintenance point outweighs the fallback's value.
// If Level-1 template gsub becomes load-bearing later, loosening the
// set is a one-line change plus a roundtrip property extension.
//
// Shape
// -----
// Two pure functions and one error enum:
//   - `encode(lua_name, upstream_uri) -> String`
//   - `decode(namespaced) -> Result<(String, String), DecodeError>`
//   - `DecodeError`: every way the decoder can reject ill-formed input
//
// The primary property is the roundtrip: for any well-formed
// `(lua_name, upstream_uri)`, `decode(encode(lua_name, upstream_uri))`
// returns `(lua_name, upstream_uri)`. Property exercises all
// UTF-8-safe inputs.

use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use thiserror::Error;

/// Scheme prefix every namespaced resource URI starts with. Exported as
/// a constant so handler code that needs to match without invoking
/// `decode` (e.g. "does this look like one of ours?") has a single
/// source of truth.
pub const SCHEME_PREFIX: &str = "mcp-proxy://";

/// Percent-encoding set: encode EVERY byte that isn't an ASCII
/// alphanumeric. Equivalent to `NON_ALPHANUMERIC` but spelled out so
/// the intent is obvious next to the decoder's tolerance rules.
///
/// We start from `CONTROLS` (all bytes 0x00–0x1F and 0x7F) and add each
/// non-alphanumeric printable ASCII byte individually. The bytes
/// listed are: space and every ASCII punctuation character.
///
/// Non-ASCII bytes (0x80–0xFF) are encoded unconditionally by the crate's
/// `AsciiSet::should_percent_encode` regardless of set membership; no
/// explicit `.add()` calls are needed for them.
const ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'-')
    .add(b'.')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'_')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}')
    .add(b'~');

/// Every way the decoder can reject a candidate namespaced URI.
///
/// Variants carry enough context that the handler can surface a
/// meaningful diagnostic without re-parsing. The `Display` text is
/// a lowercase sentence fragment so it composes naturally as
/// `"failed to decode namespaced resource URI: {err}"`.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("namespaced resource URI is missing the `mcp-proxy://` scheme prefix")]
    MissingScheme,

    #[error("namespaced resource URI has empty authority (upstream lua name)")]
    MissingAuthority,

    #[error(
        "namespaced resource URI is missing the `/` separating the authority from the upstream URI"
    )]
    MissingPath,

    #[error(
        "namespaced resource URI has invalid percent-encoding in the upstream URI (not valid UTF-8)"
    )]
    BadPercentEncoding,
}

/// Build a namespaced resource URI from an upstream identifier and an
/// upstream URI.
///
/// `lua_name` is assumed to already be a valid Lua identifier
/// (`sanitize_lua_identifier` output), so it's passed through verbatim
/// as the authority. The upstream URI is percent-encoded with
/// [`ENCODE_SET`] so arbitrary punctuation and UTF-8 roundtrip safely.
pub fn encode(lua_name: &str, upstream_uri: &str) -> String {
    let encoded = utf8_percent_encode(upstream_uri, ENCODE_SET).to_string();
    format!("{SCHEME_PREFIX}{lua_name}/{encoded}")
}

/// Parse a namespaced resource URI back into `(lua_name, upstream_uri)`.
///
/// Returns a `DecodeError` if the input is not in the expected shape.
/// See [`DecodeError`] for the rejection reasons.
///
/// # Examples
///
/// ```
/// use mcp_client::resource_uri::{encode, decode};
///
/// let namespaced = encode("rootly", "rootly://incidents");
/// let (name, uri) = decode(&namespaced).expect("roundtrip");
/// assert_eq!(name, "rootly");
/// assert_eq!(uri, "rootly://incidents");
/// ```
pub fn decode(namespaced: &str) -> Result<(String, String), DecodeError> {
    let remainder = namespaced
        .strip_prefix(SCHEME_PREFIX)
        .ok_or(DecodeError::MissingScheme)?;

    // `split_once('/')` requires BOTH an authority and a path-separator
    // to be present. A bare `mcp-proxy://rootly` (no `/`) hits
    // `MissingPath`; a leading `/` (empty authority, which reads as
    // `mcp-proxy:///path`) hits `MissingAuthority`.
    let Some((lua_name, encoded_path)) = remainder.split_once('/') else {
        return Err(DecodeError::MissingPath);
    };

    if lua_name.is_empty() {
        return Err(DecodeError::MissingAuthority);
    }

    let upstream_uri = percent_decode_str(encoded_path)
        .decode_utf8()
        .map_err(|_| DecodeError::BadPercentEncoding)?
        .into_owned();

    Ok((lua_name.to_string(), upstream_uri))
}

#[cfg(test)]
#[path = "resource_uri_tests.rs"]
mod tests;
