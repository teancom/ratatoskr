// pattern: Imperative Shell
//
// Diagnostic probe: connect to every upstream in the user's config, ask
// each one for its resources + resource templates, and print the
// results.
//
// Origin: confirming which configured upstreams actually implement
// `resources/list` (Datadog and Rootly ship real resources; Atlassian,
// Glean, and GitLab advertise the capability but return empty lists).
//
// Kept in tree rather than thrown away: new upstreams show up over
// time, upstream MCP servers evolve (GitLab already advertises
// subscribe+listChanged but ships nothing), and "does $upstream do
// resources?" is a question with a shelf life longer than one session.
// Re-run with:
//
//     cargo run --example probe_resources
//
// Honors $RATO_CONFIG_PATH the same way the gateway binary does; falls back
// to the platform-default path otherwise. Never touches the running
// gateway — each probe opens its own upstream connection and tears it
// down cleanly.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use gateway::config;
use mcp_client::{LiveUpstream, SamplingBroker, UpstreamEventBus, connect};
use rmcp::model::ReadResourceRequestParams;

const CONFIG_PATH_ENV: &str = "RATO_CONFIG_PATH";
const CONFIG_MAX_BYTES: u64 = 1024 * 1024;
const PROBE_TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let configs = load_configs()?;
    println!(
        "Probing {} upstream(s) for resource support\n",
        configs.len()
    );

    let bus = Arc::new(UpstreamEventBus::new());
    let broker = Arc::new(SamplingBroker::new());

    for cfg in configs {
        let name = cfg.name.clone();
        println!("=== {name} ===");

        let connect_result =
            tokio::time::timeout(PROBE_TIMEOUT, connect(&cfg, &bus, &broker)).await;
        let live = match connect_result {
            Ok(Ok(live)) => live,
            Ok(Err(e)) => {
                println!("  connect error: {e}\n");
                continue;
            }
            Err(_) => {
                println!("  connect timed out after {PROBE_TIMEOUT:?}\n");
                continue;
            }
        };

        probe_one(&live).await;

        // Close cleanly so stdio children get SIGTERM'd via stdin EOF
        // rather than leaking as orphans. Matches the production shutdown
        // path (`close_all_upstreams`).
        if let Err(e) = live.close().await {
            eprintln!("  close warning: {e}");
        }
        println!();
    }

    Ok(())
}

async fn probe_one(live: &LiveUpstream) {
    let peer = live.peer();

    // Resources.
    match tokio::time::timeout(PROBE_TIMEOUT, peer.list_all_resources()).await {
        Ok(Ok(resources)) if resources.is_empty() => {
            println!("  resources: (empty)");
        }
        Ok(Ok(resources)) => {
            println!("  resources: {} total", resources.len());
            for r in &resources {
                println!("    - uri:  {}", r.raw.uri);
                println!("      name: {}", r.raw.name);
                if let Some(desc) = &r.raw.description {
                    println!("      desc: {}", single_line(desc));
                }
                if let Some(mime) = &r.raw.mime_type {
                    println!("      mime: {mime}");
                }
            }

            // Try to read the first resource's content so we can see
            // whether it's real data or just an advertised placeholder.
            if let Some(first) = resources.first() {
                println!("    probing read of first resource ({}):", first.raw.uri);
                let params = ReadResourceRequestParams::new(first.raw.uri.clone());
                match tokio::time::timeout(PROBE_TIMEOUT, peer.read_resource(params)).await {
                    Ok(Ok(res)) => {
                        let summary = summarize_contents(&res);
                        println!("      read ok: {summary}");
                    }
                    Ok(Err(e)) => println!("      read error: {e}"),
                    Err(_) => println!("      read timed out"),
                }
            }
        }
        Ok(Err(e)) => println!("  resources: list_all_resources error: {e}"),
        Err(_) => println!("  resources: list timed out"),
    }

    // Resource templates.
    match tokio::time::timeout(PROBE_TIMEOUT, peer.list_all_resource_templates()).await {
        Ok(Ok(templates)) if templates.is_empty() => {
            println!("  templates: (empty)");
        }
        Ok(Ok(templates)) => {
            println!("  templates: {} total", templates.len());
            for t in &templates {
                println!(
                    "    - uri_template: {}  (name: {})",
                    t.raw.uri_template, t.raw.name
                );
                if let Some(desc) = &t.raw.description {
                    println!("      desc: {}", single_line(desc));
                }
            }
        }
        Ok(Err(e)) => println!("  templates: list_all_resource_templates error: {e}"),
        Err(_) => println!("  templates: list timed out"),
    }

    // Only for context — so we know the upstream is genuinely
    // capability-complete vs. silently returning empty for everything.
    match tokio::time::timeout(PROBE_TIMEOUT, peer.list_all_tools()).await {
        Ok(Ok(tools)) => println!(
            "  (sanity) tools: {} total — upstream responds to list methods",
            tools.len()
        ),
        Ok(Err(e)) => println!("  (sanity) tools: list error: {e}"),
        Err(_) => println!("  (sanity) tools: list timed out"),
    }

    if let Some(info) = peer.peer_info() {
        let caps = &info.capabilities;
        println!(
            "  capabilities advertised: resources={} prompts={} tools={}",
            caps.resources.is_some(),
            caps.prompts.is_some(),
            caps.tools.is_some()
        );
        // Flag any explicit sub-capabilities on resources (subscribe,
        // listChanged). That's the hint that the upstream is actually
        // implementing, not just advertising capability: true.
        if let Some(r) = &caps.resources {
            println!("    resources sub-caps: {r:?}");
        }
    }
}

fn summarize_contents(res: &rmcp::model::ReadResourceResult) -> String {
    if res.contents.is_empty() {
        return "(no contents)".to_string();
    }
    let mut parts = Vec::new();
    for item in &res.contents {
        match item {
            rmcp::model::ResourceContents::TextResourceContents { text, uri, .. } => {
                let snippet: String = text.chars().take(80).collect();
                parts.push(format!("text<{}> {}...", uri, snippet));
            }
            rmcp::model::ResourceContents::BlobResourceContents { blob, uri, .. } => {
                parts.push(format!("blob<{}> {}B", uri, blob.len()));
            }
        }
    }
    parts.join(" | ")
}

fn single_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn load_configs() -> Result<Vec<mcp_client::UpstreamConfig>> {
    let path = resolve_config_path().context("no config path resolved")?;
    let metadata = std::fs::metadata(&path)
        .with_context(|| format!("failed to stat config file {}", path.display()))?;
    if metadata.len() > CONFIG_MAX_BYTES {
        anyhow::bail!(
            "config file at {} is {} bytes, over {}B safety limit",
            path.display(),
            metadata.len(),
            CONFIG_MAX_BYTES
        );
    }
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    let parsed = config::parse_full(&contents).context("config file contents are invalid")?;
    Ok(parsed.upstreams)
}

fn resolve_config_path() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os(CONFIG_PATH_ENV) {
        return Some(PathBuf::from(p));
    }
    let home = std::env::var_os("HOME")?;
    Some(
        PathBuf::from(home)
            .join("Library")
            .join("Preferences")
            .join("ratatoskr")
            .join("config.json"),
    )
}
