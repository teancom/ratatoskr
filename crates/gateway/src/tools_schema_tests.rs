use super::*;

// Self-enforcing cross-reference: each tool constructor's `Tool::new`
// name literal must match the entry in `BASE_TOOL_NAMES`.
// If someone renames a tool in the constructor but forgets the constant
// (or vice versa), list_tools and the dispatch match will drift silently.
// The tests below catch that at compile-time-of-tests.

#[test]
fn list_servers_description_steers_away_from_duplicate_auth() {
    // Point-of-action pin. The competing surface for upstream discovery
    // is a same-named, unauthenticated connector in the agent's flat
    // tool list (e.g. account-level `claude.ai *` integrations) whose
    // `authenticate` tool invites OAuth at the exact moment the agent is
    // choosing what to call. The server `instructions` string is read
    // once at init and loses to that; the tool *description* the agent
    // reads while deciding is the surface that competes on equal
    // footing. So list-servers' description must (a) give a reason to
    // list BEFORE authenticating, and (b) stay category-level — no
    // service name, since instance examples only help one literal
    // string and don't scale to the rest of the connector family.
    let desc = list_servers_tool()
        .description
        .expect("list-servers must carry a description")
        .to_string();

    assert!(
        desc.to_lowercase().contains("authenticat") || desc.to_lowercase().contains("oauth"),
        "list-servers' description must steer the agent to list before \
         triggering an authentication/OAuth flow on a same-named tool — \
         that's the point-of-action decision this directive exists to win"
    );
    assert!(
        !desc.to_lowercase().contains("datadog"),
        "list-servers' description must stay category-level: naming a \
         specific service only helps that one string and reintroduces the \
         non-scaling fix we deliberately removed"
    );
}

#[test]
fn base_constructor_names_match_base_name_constant() {
    let actual = [
        ping_tool().name,
        list_servers_tool().name,
        server_details_tool().name,
        list_server_tools_tool().name,
        tool_details_tool().name,
        inspect_tool_response_tool().name,
        execute_tool().name,
        reload_config_tool().name,
        reconnect_upstream_tool().name,
        restart_gateway_tool().name,
    ];
    let expected: Vec<String> = BASE_TOOL_NAMES.iter().map(|s| (*s).to_string()).collect();
    let actual_strs: Vec<String> = actual.iter().map(|n| n.to_string()).collect();
    assert_eq!(
        actual_strs, expected,
        "BASE_TOOL_NAMES must match the names produced by the tool constructors \
         (order matters — list_tools builds its vec in this order)"
    );
}
