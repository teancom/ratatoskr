// pattern: Imperative Shell
//
// Resources family: `resources/list`, `resources/templates/list`,
// `resources/read`. Aggregates every registered upstream's resources
// into the gateway's namespaced `mcp-proxy://<lua_name>/<encoded-uri>`
// scheme; per-upstream failures log and are skipped rather than
// failing the whole listing.
//
// URI routing is centralized: callers use `resource_uri::encode` on
// the way out and `resource_uri::decode` on the way in. The handler
// doesn't try to rewrite URIs inside resource content — that's a
// content-vs-routing distinction the gateway deliberately stays out of.

use super::*;

impl GatewayHandler {
    /// Handle `resources/read`: decode the namespaced URI, route to
    /// the right upstream, forward the read verbatim. Errors from the
    /// upstream propagate with a `lua_name: …` prefix so the agent can
    /// tell which upstream rejected the read.
    ///
    /// URIs inside the returned content are NOT rewritten: a
    /// `datadog://mcp/toolsets` string inside the payload may be a URI
    /// or an incidental string — distinguishing them is heuristics,
    /// not grunt work. Staying out of it is the pinned design decision.
    pub(super) async fn call_read_resource(
        &self,
        request: ReadResourceRequestParams,
    ) -> Result<ReadResourceResult, ErrorData> {
        // Decode the gateway's namespaced URI back into (lua_name,
        // upstream_uri). Malformed input gets `invalid_params` so a
        // compliant client can see the specific failure (wrong scheme,
        // missing authority, bad percent-encoding) rather than a
        // generic NotFound.
        let (lua_name, upstream_uri) = resource_uri::decode(&request.uri).map_err(|e| {
            ErrorData::invalid_params(
                format!("failed to decode resource URI '{}': {e}", request.uri),
                None,
            )
        })?;

        let handle = self.require_upstream_by_lua_name(&lua_name).await?;

        let params = ReadResourceRequestParams::new(upstream_uri);
        handle.read_resource(params).await.map_err(|e| {
            ErrorData::internal_error(format!("upstream '{lua_name}' read failed: {e}"), None)
        })
    }

    /// Aggregate `resources/list` across every registered upstream.
    ///
    /// Per-upstream failures log a warning and are skipped — consistent
    /// with the bootstrap-best-effort posture the gateway already uses
    /// for connect/list failures. A single flaky upstream must not
    /// remove every other upstream's resources from the downstream
    /// catalog.
    ///
    /// Each upstream's resources are namespaced into the gateway's
    /// `mcp-proxy://<lua_name>/<encoded-uri>` scheme so a downstream
    /// `resources/read` can route back to the right upstream. Ordering
    /// within the catalog is by upstream display name, then by the
    /// upstream's own list order; downstream clients should not depend
    /// on it but it's stable across invocations.
    pub(super) async fn aggregate_resources(&self) -> Vec<Resource> {
        let configs = self.state.ctx.upstreams.list().await;
        let mut out: Vec<Resource> = Vec::new();
        for config in configs {
            let Some(handle) = self.state.ctx.upstreams.handle_by_name(&config.name).await else {
                continue;
            };
            match self.state.ctx.resource_cache.get(&handle).await {
                Ok(listing) => {
                    for resource in listing.resources.iter() {
                        let mut raw = resource.raw.clone();
                        raw.uri = resource_uri::encode(&config.lua_name, &raw.uri);
                        out.push(Resource::new(raw, resource.annotations.clone()));
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        upstream = %config.name,
                        error = %e,
                        "failed to list resources; skipping upstream in aggregation"
                    );
                }
            }
        }
        out
    }

    /// Aggregate `resources/templates/list` across every registered
    /// upstream. Same error policy as `aggregate_resources`.
    ///
    /// The template's `uri_template` field is encoded via
    /// `resource_uri::encode` exactly like a concrete URI — the
    /// `{var}` placeholders get percent-encoded to `%7B...%7D`, which
    /// is the pinned decision from the codec design. Agents should
    /// instantiate templates via `_gateway.expand_template(...)` in
    /// Lua rather than trying to do gsub on the encoded form.
    pub(super) async fn aggregate_resource_templates(&self) -> Vec<ResourceTemplate> {
        let configs = self.state.ctx.upstreams.list().await;
        let mut out: Vec<ResourceTemplate> = Vec::new();
        for config in configs {
            let Some(handle) = self.state.ctx.upstreams.handle_by_name(&config.name).await else {
                continue;
            };
            match self.state.ctx.resource_cache.get(&handle).await {
                Ok(listing) => {
                    for template in listing.templates.iter() {
                        let mut raw = template.raw.clone();
                        raw.uri_template =
                            resource_uri::encode(&config.lua_name, &raw.uri_template);
                        out.push(ResourceTemplate::new(raw, template.annotations.clone()));
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        upstream = %config.name,
                        error = %e,
                        "failed to list resource templates; skipping upstream in aggregation"
                    );
                }
            }
        }
        out
    }
}
