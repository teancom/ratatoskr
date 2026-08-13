use super::*;

// ------------------------------------------------------------------
// Happy-path / primary property: roundtrip
// ------------------------------------------------------------------

#[test]
fn roundtrip_simple_rootly_resource() {
    // Rootly's real resource URI shape, used as a representative example.
    let encoded = encode("rootly", "rootly://incidents");
    assert_eq!(
        encoded, "mcp-proxy://rootly/rootly%3A%2F%2Fincidents",
        "encoded shape must be stable for downstream clients"
    );
    let (name, uri) = decode(&encoded).expect("roundtrip");
    assert_eq!(name, "rootly");
    assert_eq!(uri, "rootly://incidents");
}

#[test]
fn roundtrip_datadog_nested_path() {
    // Datadog ships `datadog://mcp/toolsets`; the embedded `/`
    // must not confuse the path split on the decode side.
    let raw_uri = "datadog://mcp/toolsets";
    let encoded = encode("datadog_eu", raw_uri);
    let (name, uri) = decode(&encoded).expect("roundtrip");
    assert_eq!(name, "datadog_eu");
    assert_eq!(uri, raw_uri);
}

#[test]
fn roundtrip_instantiated_template() {
    // After `_gateway.expand_template(...)` fills a template, the
    // result still has to roundtrip. `incident://42` is the
    // post-expansion shape of Rootly's `incident://{incident_id}`.
    let encoded = encode("rootly", "incident://42");
    let (name, uri) = decode(&encoded).expect("roundtrip");
    assert_eq!(name, "rootly");
    assert_eq!(uri, "incident://42");
}

#[test]
fn roundtrip_preserves_query_and_fragment() {
    // `?` and `#` are URI delimiters — they MUST be percent-encoded
    // so the outer `mcp-proxy://…` shape isn't reparsed as having
    // its own query/fragment.
    let raw = "https://example.com/path?q=1&r=2#frag";
    let encoded = encode("upstream", raw);
    assert!(
        !encoded.contains('?') && !encoded.contains('#'),
        "delimiters must be percent-encoded in the namespaced URI: {encoded}"
    );
    let (_, uri) = decode(&encoded).expect("roundtrip");
    assert_eq!(uri, raw);
}

#[test]
fn roundtrip_preserves_literal_percent() {
    // A literal `%` in the upstream URI must not be mistaken for a
    // percent-encoding escape on the way out.
    let raw = "weird://100%good";
    let encoded = encode("u", raw);
    let (_, uri) = decode(&encoded).expect("roundtrip");
    assert_eq!(uri, raw);
}

#[test]
fn roundtrip_preserves_utf8() {
    // Multi-byte UTF-8 in the upstream URI must survive. No byte
    // slicing anywhere in the codec.
    let raw = "café://emoji-🔍/path";
    let encoded = encode("u", raw);
    let (_, uri) = decode(&encoded).expect("roundtrip");
    assert_eq!(uri, raw);
}

#[test]
fn roundtrip_empty_upstream_uri() {
    // Boundary: empty path. Not expected in practice (MCP requires a
    // URI on every resource) but the codec should handle it cleanly
    // rather than producing something that can't be decoded.
    let encoded = encode("u", "");
    let (name, uri) = decode(&encoded).expect("roundtrip");
    assert_eq!(name, "u");
    assert_eq!(uri, "");
}

#[test]
fn roundtrip_upstream_uri_containing_slash_only() {
    // Single `/`, which would be ambiguous if we naively split on
    // the LAST `/` instead of the first after the authority.
    let raw = "/";
    let encoded = encode("u", raw);
    let (_, uri) = decode(&encoded).expect("roundtrip");
    assert_eq!(uri, raw);
}

#[test]
fn roundtrip_property_over_example_inputs() {
    // Battery of realistic inputs. If any future change breaks the
    // roundtrip, at least one of these will fail with a clear
    // message about which case regressed.
    let cases: &[(&str, &str)] = &[
        ("rootly", "rootly://incidents"),
        ("rootly", "incident://42"),
        ("rootly", "team://dev-infra"),
        ("datadog_eu", "datadog://mcp/toolsets"),
        ("datadog_us", "datadog://mcp/whoami"),
        ("gitlab", "file:///etc/passwd"),
        ("u", ""),
        ("u", "/"),
        ("u", "//"),
        ("u", "no-scheme-here"),
        ("u", "has space"),
        ("u", "a?b=c&d=e#frag"),
        ("u", "100%now"),
        ("u", "café://emoji-🔍/path"),
        ("_while", "reserved-word-lua-name://ok"),
    ];
    for (name, uri) in cases {
        let encoded = encode(name, uri);
        let (d_name, d_uri) =
            decode(&encoded).unwrap_or_else(|e| panic!("decode failed for {name:?}/{uri:?}: {e}"));
        assert_eq!(&d_name, name, "name roundtrip broke for {name:?}/{uri:?}");
        assert_eq!(&d_uri, uri, "uri roundtrip broke for {name:?}/{uri:?}");
    }
}

// ------------------------------------------------------------------
// Encoding shape guarantees (structural properties the decoder
// relies on — pin them so a well-meaning "readability" change to
// the encode set doesn't silently break routing).
// ------------------------------------------------------------------

#[test]
fn encoded_form_always_starts_with_scheme_prefix() {
    let inputs = [("a", "x"), ("a", ""), ("_", "/"), ("abc", "café")];
    for (name, uri) in inputs {
        let encoded = encode(name, uri);
        assert!(
            encoded.starts_with(SCHEME_PREFIX),
            "encoded form missing scheme for {name:?}/{uri:?}: {encoded}"
        );
    }
}

#[test]
fn encoded_form_has_exactly_one_unencoded_slash_after_authority() {
    // The decoder splits on the first `/` after the authority. If
    // encode ever leaves an unencoded `/` in the path it would
    // move the split point and corrupt the upstream URI.
    //
    // The `/` between authority and path is the first one; no
    // more should appear after it.
    let encoded = encode("u", "has/many/slashes/in/it");
    let after_prefix = encoded
        .strip_prefix(SCHEME_PREFIX)
        .expect("starts with scheme");
    // Authority-path separator is the only unencoded '/' in the
    // remainder.
    assert_eq!(
        after_prefix.matches('/').count(),
        1,
        "expected exactly one unencoded '/' after scheme, got {encoded}"
    );
}

#[test]
fn encoded_form_contains_no_uri_delimiters_in_path() {
    // The path portion must be free of characters that a naive
    // URI parser might treat as delimiters. Catches accidental
    // loosening of the ENCODE_SET.
    let encoded = encode("u", "scheme://host/path?q=1#frag");
    let (_, path) = encoded
        .strip_prefix(SCHEME_PREFIX)
        .expect("scheme")
        .split_once('/')
        .expect("authority/path split");
    for forbidden in ['?', '#', '/', ':', '@', '[', ']'] {
        assert!(
            !path.contains(forbidden),
            "path component contains forbidden delimiter {forbidden:?}: {path}"
        );
    }
}

// ------------------------------------------------------------------
// Decoder rejections
// ------------------------------------------------------------------

#[test]
fn decode_rejects_missing_scheme() {
    assert_eq!(
        decode("rootly://incidents"),
        Err(DecodeError::MissingScheme),
        "upstream-native URIs are not namespaced"
    );
    assert_eq!(decode(""), Err(DecodeError::MissingScheme));
    assert_eq!(
        decode("http://example.com/path"),
        Err(DecodeError::MissingScheme)
    );
}

#[test]
fn decode_rejects_wrong_scheme_prefix() {
    // Close-but-wrong schemes. `mcp-proxy:` (missing `//`) and
    // `mcp-proxy//` (missing `:`) both lack the full prefix.
    assert_eq!(
        decode("mcp-proxy:rootly/x"),
        Err(DecodeError::MissingScheme)
    );
    assert_eq!(
        decode("mcp-proxy//rootly/x"),
        Err(DecodeError::MissingScheme)
    );
}

#[test]
fn decode_rejects_empty_authority() {
    // `mcp-proxy:///some/path` - authority is empty. Routing has
    // no upstream to dispatch to.
    assert_eq!(
        decode("mcp-proxy:///some%2Fpath"),
        Err(DecodeError::MissingAuthority)
    );
}

#[test]
fn decode_rejects_missing_path_separator() {
    // `mcp-proxy://rootly` - authority present but no `/` before
    // a path. Can't distinguish from a bare-upstream URI shape.
    assert_eq!(decode("mcp-proxy://rootly"), Err(DecodeError::MissingPath));
}

#[test]
fn decode_rejects_invalid_percent_encoding() {
    // Valid percent-encoding must be `%` followed by two hex
    // digits AND must decode to a UTF-8 byte sequence. The
    // `percent_decode_str::decode_utf8` branch fails on invalid
    // UTF-8 after byte decoding; malformed hex is silently passed
    // through by the library (treated as literal `%` bytes) and
    // is therefore accepted, but non-UTF-8 byte sequences are not.
    //
    // `%FF` alone is an invalid UTF-8 start byte. A standalone
    // 0xFF byte cannot begin a valid UTF-8 character.
    assert_eq!(
        decode("mcp-proxy://u/%FF"),
        Err(DecodeError::BadPercentEncoding)
    );
}

#[test]
fn decode_accepts_malformed_percent_hex_as_literal() {
    // percent_decode_str passes `%` followed by non-hex chars through as
    // literal bytes. Document the observable behavior so a library upgrade
    // that changes this is immediately visible.
    let (_, uri) = decode("mcp-proxy://u/%zz-suffix").expect("malformed hex is accepted");
    assert_eq!(uri, "%zz-suffix");
}

#[test]
fn decode_accepts_empty_path() {
    // `mcp-proxy://u/` — empty path, corresponds to an upstream
    // URI of "". Round-trips because encode("u", "") produces
    // exactly this shape.
    assert_eq!(
        decode("mcp-proxy://u/"),
        Ok(("u".to_string(), String::new()))
    );
}

#[test]
fn decode_error_display_is_lowercase_fragment() {
    // House rule: error messages are lowercase sentence fragments
    // that compose as "failed to X: {err}". Easy to regress if
    // someone adds a new variant; pin it.
    for err in [
        DecodeError::MissingScheme,
        DecodeError::MissingAuthority,
        DecodeError::MissingPath,
        DecodeError::BadPercentEncoding,
    ] {
        let msg = err.to_string();
        let first = msg.chars().next().expect("non-empty message");
        assert!(
            first.is_ascii_lowercase() || !first.is_alphabetic(),
            "error message must start lowercase: {msg:?}"
        );
        assert!(
            !msg.ends_with('.'),
            "error message must be a fragment (no trailing period): {msg:?}"
        );
    }
}
