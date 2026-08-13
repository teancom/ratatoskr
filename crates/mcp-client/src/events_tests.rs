use super::*;

#[test]
fn upstream_name_is_accessible_for_all_variants() {
    let a = UpstreamEvent::ToolListChanged {
        upstream: "a".to_string(),
    };
    let b = UpstreamEvent::ResourceListChanged {
        upstream: "b".to_string(),
    };
    let c = UpstreamEvent::PromptListChanged {
        upstream: "c".to_string(),
    };
    let d = UpstreamEvent::ResourceUpdated {
        upstream: "d".to_string(),
        uri: "incident://42".to_string(),
    };
    assert_eq!(a.upstream_name(), "a");
    assert_eq!(b.upstream_name(), "b");
    assert_eq!(c.upstream_name(), "c");
    assert_eq!(d.upstream_name(), "d");
}

#[test]
fn resource_updated_carries_bare_upstream_uri() {
    // The event-bus carries UPSTREAM-native URIs, not the namespaced
    // `mcp-proxy://` form. Translation happens at fan-out time so
    // the bus doesn't need to know the encoding policy.
    let event = UpstreamEvent::ResourceUpdated {
        upstream: "rootly".to_string(),
        uri: "incident://42".to_string(),
    };
    match event {
        UpstreamEvent::ResourceUpdated { uri, .. } => {
            assert!(
                !uri.starts_with("mcp-proxy://"),
                "bus must carry bare upstream URIs, not namespaced ones"
            );
        }
        _ => panic!("expected ResourceUpdated"),
    }
}
