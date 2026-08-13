//! Catalog-family tests. The tool-details description pin lives
//! here because the response-shape tag set it advertises is
//! resolved by `resolve_response_shape` in this module; keeping
//! the test next to the resolver means adding a new
//! `ResponseShape` variant lands one drift catcher in one file.

use crate::response_shape::ResponseShape;
use crate::tools_schema;

// ----- tool-details description cross-reference pin -----
//
// The `tool-details` tool description advertises which `responseShape`
// tags it may emit ("one of `text-prose`, `text-json`, `multi-block`,
// `structured`, `mixed`"). Those must match the exhaustive set of
// `ResponseShape::tag()` values, or agents will see a tag they have no
// documentation for.

#[test]
fn tool_details_description_names_every_response_shape_tag() {
    // Enumerate every ResponseShape variant via .tag(). The
    // `MultiBlock { len }` variant's tag is independent of len, so any
    // length suffices here. Match on the backtick-wrapped form used
    // in the description prose so a typo like `text-jsonX` doesn't
    // accidentally satisfy a naked substring check for `text-json`.
    let all_tags = [
        ResponseShape::TextProse.tag(),
        ResponseShape::TextJson.tag(),
        ResponseShape::MultiBlock { len: 1 }.tag(),
        ResponseShape::Structured.tag(),
        ResponseShape::Mixed.tag(),
    ];

    let description = tools_schema::tool_details_tool()
        .description
        .unwrap_or_default();
    for tag in all_tags {
        let needle = format!("`{tag}`");
        assert!(
            description.contains(&needle),
            "tool-details description must name the response-shape tag \
             `{tag}` (as `{needle}`) — add a new variant here when \
             ResponseShape grows"
        );
    }
}
