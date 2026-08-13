// pattern: Functional Core
//
// Pure glue between RFC 6570 URI template expansion (via the
// `uri-template` crate) and the gateway's namespaced-URI codec
// (`mcp_client::resource_uri`). Exists as its own module so the
// conversion from "JSON-shaped params the user wrote in Lua" to
// "`uri_template::HashValues` the expander understands" can be
// unit-tested without spinning up a Lua VM.
//
// The public entry point is `expand_and_namespace`: given a server
// (upstream lua_name), an RFC 6570 template, and a params JSON value,
// it produces a `mcp-proxy://<server>/<percent-encoded-expansion>`
// string ready to be handed to the downstream client as the argument
// of a `resources/read` request.
//
// Why JSON-shaped params
// ----------------------
// The Lua host function has a convenient `lua_value_to_json` helper
// that already handles the table->object/array heuristic and rejects
// unsupported types (functions, threads, userdata). Re-using it means
// the Functional Core doesn't have to know about `mlua::Value` at all
// — it takes a `serde_json::Value` and every test can construct
// inputs as `serde_json::json!({...})` literals.
//
// Why we go through serde to build `uri_template::Value`
// ------------------------------------------------------
// `uri-template` re-exports `UriTemplate`, `Value`, `Values`,
// `HashValues`, and `BTreeValues` — but NOT `Scalar`. `Value::List`
// and `Value::Keys` both carry `Cow<[Scalar]>` payloads, so there's
// no way to construct them at runtime without naming `Scalar`, which
// is crate-private.
//
// The escape hatch: with the `serde` feature enabled, `Value`
// implements `Deserialize`. We construct each top-level param as a
// `serde_json::Value`, then go through `serde_json::from_value` to
// get a `uri_template::Value`. The crate's own `Deserialize` impl
// handles the scalar/list/keys dispatch and enforces the
// "no nested composites" rule by rejecting non-scalar children during
// visit_seq / visit_map. Our job shrinks to: validate the top-level
// shape, call into the upstream deserializer, surface any errors with
// enough context to blame the right key.
//
// RFC 6570 levels supported
// -------------------------
// All four. The crate accepts any valid template and expands whatever
// we hand it; our responsibility is only to shape the values correctly.
//   - Level 1 (`{var}`)                    — scalar string
//   - Level 2 (`{+var}`, `{#var}`)          — scalar string, different encoding
//   - Level 3 (`{?x,y}`, `{;x}`, `{x,y}`)   — multiple scalars
//   - Level 4 (`{var*}`, `{list}`, `{?q*}`) — list / associative array
//
// JSON shape mapping (enforced by `uri_template::Value`'s own serde impl)
// ----------------------------------------------------------------------
//   Null            -> variable omitted from the values set (RFC 6570
//                      "undefined" semantics; template expands to
//                      nothing for that placeholder)
//   Bool / Number   -> stringified into a Scalar
//   String          -> Scalar
//   Array           -> List (each element must be a JSON scalar; nested
//                      arrays/objects are rejected by the crate's
//                      ScalarValueVisitor)
//   Object          -> Keys (each value must be a JSON scalar; insertion
//                      order preserved by serde_json::Map)

use crate::json_shape::describe_json_shape;
use mcp_client::resource_uri;
use serde::Deserialize;
use serde::de::IntoDeserializer;
use thiserror::Error;
use uri_template::{HashValues, UriTemplate, Value as UriValue};

/// Every way `expand_and_namespace` can fail, with enough detail that
/// the error surfaces a usable diagnostic downstream.
///
/// Messages are lowercase sentence fragments so they compose as
/// `"_gateway.expand_template: {err}"` from the Lua host closure.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExpandTemplateError {
    #[error("server name is empty; must be a non-empty upstream lua_name")]
    EmptyServer,

    #[error("template is empty; must be a non-empty RFC 6570 URI template")]
    EmptyTemplate,

    #[error("failed to parse template {template:?}: {reason}")]
    ParseFailed { template: String, reason: String },

    #[error("failed to expand template {template:?}: {reason}")]
    ExpandFailed { template: String, reason: String },

    #[error(
        "unsupported param value for {key:?}: {reason}. RFC 6570 values may be scalars, \
         arrays of scalars, or associative arrays of scalar values — no nested structures"
    )]
    UnsupportedParamValue { key: String, reason: String },
}

/// Expand an RFC 6570 URI template with `params`, then wrap the result
/// in the gateway's `mcp-proxy://<server>/<encoded>` namespace.
///
/// `server` is the upstream's sanitized Lua identifier (same key
/// `UpstreamManager` uses). `template` is the raw upstream template
/// text — do NOT pass a template that's already been wrapped by the
/// gateway's codec; the agent is expected to have pulled the bare
/// template out of `list_resource_templates` via a
/// `resource_uri::decode` step before calling this.
///
/// `params` is a JSON object whose top-level keys name template
/// variables. A `null` at any top-level key removes that variable
/// from the expansion set (RFC 6570 "undefined" semantics).
///
/// Returns a namespaced URI string. Never panics.
pub fn expand_and_namespace(
    server: &str,
    template: &str,
    params: &serde_json::Value,
) -> Result<String, ExpandTemplateError> {
    if server.is_empty() {
        return Err(ExpandTemplateError::EmptyServer);
    }
    if template.is_empty() {
        return Err(ExpandTemplateError::EmptyTemplate);
    }

    let parsed =
        UriTemplate::new(template.to_string()).map_err(|e| ExpandTemplateError::ParseFailed {
            template: template.to_string(),
            reason: e.to_string(),
        })?;

    let values = json_to_uri_template_values(params)?;

    let expanded = parsed
        .expand(&values)
        .map_err(|e| ExpandTemplateError::ExpandFailed {
            template: template.to_string(),
            reason: e.to_string(),
        })?;

    Ok(resource_uri::encode(server, &expanded))
}

/// Convert a JSON object of template parameters into
/// `uri_template::HashValues`.
///
/// Top-level rules:
///   - `Null` or empty `Object` — no variables bound (templates with
///     `{var}` placeholders expand the placeholder to empty per RFC 6570)
///   - non-object — rejected with a clear error naming `<root>`
///
/// Per-key rules: delegated to `serde_json::from_value` on
/// `uri_template::Value`, whose serde impl handles the scalar/list/keys
/// dispatch AND rejects nested composites inside lists/objects with
/// serde's own error type. We remap those errors to
/// `UnsupportedParamValue` with the offending top-level key so agents
/// know which parameter was wrong.
///
/// `None`-valued keys skip the variable entirely — the unbound
/// placeholder gets RFC 6570's "undefined" treatment at expansion time
/// (empty expansion). This is how a script can conditionally omit a
/// query parameter without branching the template string.
pub(crate) fn json_to_uri_template_values(
    params: &serde_json::Value,
) -> Result<HashValues<'static, 'static, String>, ExpandTemplateError> {
    let mut values: HashValues<'static, 'static, String> = HashValues::new();

    let obj = match params {
        serde_json::Value::Null => return Ok(values),
        serde_json::Value::Object(m) => m,
        other => {
            return Err(ExpandTemplateError::UnsupportedParamValue {
                key: "<root>".to_string(),
                reason: format!(
                    "params must be an object (Lua table with string keys); got {}",
                    describe_json_shape(other)
                ),
            });
        }
    };

    for (key, value) in obj {
        // Null → skip. RFC 6570 treats an unbound variable as
        // "expand to nothing"; omitting from the values set gets us
        // that behaviour for free.
        if value.is_null() {
            continue;
        }

        // `uri_template::Value<'v, 's>`'s `Deserialize` impl is
        // `impl<'de: 'v + 's, 'v, 's: 'v> Deserialize<'de> for Value<'v, 's>`,
        // which doesn't satisfy `DeserializeOwned`'s `for<'de> Deserialize<'de>`
        // bound. `serde_json::from_value` wants owned deserialization,
        // so we deserialize into a `Value<'_, '_>` borrowed from an
        // `IntoDeserializer` on the cloned JSON, then normalize via
        // `into_static` (`Value` exposes that publicly). This mirrors
        // the crate's own `uri_template_core::deserialize_owned_value`
        // helper, which isn't re-exported from `uri-template`.
        let borrowed = UriValue::deserialize(value.clone().into_deserializer()).map_err(
            |e: serde_json::Error| ExpandTemplateError::UnsupportedParamValue {
                key: key.clone(),
                reason: format!(
                    "{} (expected string, number, boolean, list of scalars, \
                     or map of scalars)",
                    e
                ),
            },
        )?;

        values.insert(key.clone(), borrowed.into_static());
    }

    Ok(values)
}

#[cfg(test)]
#[path = "expand_template_tests.rs"]
mod tests;
