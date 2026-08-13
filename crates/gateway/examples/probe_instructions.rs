// pattern: Imperative Shell
//
// Diagnostic probe: connect to every upstream in the user's config and
// print whatever each one advertises in `InitializeResult.instructions`.
//
// Origin: an agent confused "I haven't fetched the Datadog tool
// schemas" with "the Datadog MCP isn't available" after scanning
// `list-servers`. The proxy's catalog is compact by design (protecting
// session-start context), but compact came at the cost of not
// surfacing *why* an agent might pull one upstream's full tool list vs.
// another. MCP servers can advertise a human-readable `instructions`
// string at initialize time for exactly that purpose. Before designing
// a feature that forwards it through `list-servers`, we need to know
// what the user's actual upstreams ship today: empty? useful
// one-liners? walls of setup text?
//
// Kept in tree for the same reason `probe_resources` is: upstreams
// evolve, new ones get added, and "does $upstream set instructions?"
// has a shelf life longer than one session. Re-run with:
//
//     cargo run --example probe_instructions
//
// Honors $RATO_CONFIG_PATH, falling back to the macOS preference path
// (~/Library/Preferences/ratatoskr/config.json). Like the gateway
// binary, a missing config is a hard error — a diagnostic probe run
// against anything but the real upstream roster would print misleading
// output. Never touches the running
// gateway — each probe opens its own upstream connection and tears it
// down cleanly. Sequential, not parallel — some stdio upstreams don't
// tolerate being spawned twice concurrently (auth token races,
// lockfiles), and we don't want to investigate that here.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use gateway::config;
use mcp_client::{LiveUpstream, SamplingBroker, UpstreamEventBus, connect};

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
        "Probing {} upstream(s) for initialize.instructions\n",
        configs.len()
    );

    let bus = Arc::new(UpstreamEventBus::new());
    let broker = Arc::new(SamplingBroker::new());

    // Aggregate so we can print a compact summary table at the end,
    // separate from the per-upstream detail blocks. Easier to eyeball
    // "who ships what" without scrolling.
    let mut summary: Vec<InstructionsReport> = Vec::with_capacity(configs.len());

    for cfg in configs {
        let name = cfg.name.clone();
        println!("=== {name} ===");

        let connect_result =
            tokio::time::timeout(PROBE_TIMEOUT, connect(&cfg, &bus, &broker)).await;
        let live = match connect_result {
            Ok(Ok(live)) => live,
            Ok(Err(e)) => {
                println!("  connect error: {e}\n");
                summary.push(InstructionsReport::failed(&name, format!("connect: {e}")));
                continue;
            }
            Err(_) => {
                println!("  connect timed out after {PROBE_TIMEOUT:?}\n");
                summary.push(InstructionsReport::failed(
                    &name,
                    "connect timeout".to_string(),
                ));
                continue;
            }
        };

        let report = probe_one(&name, &live);
        summary.push(report);

        if let Err(e) = live.close().await {
            eprintln!("  close warning: {e}");
        }
        println!();
    }

    print_summary(&summary);

    Ok(())
}

#[derive(Debug)]
struct InstructionsReport {
    name: String,
    status: ReportStatus,
}

#[derive(Debug)]
enum ReportStatus {
    Present { chars: usize, lines: usize },
    EmptyString,
    Absent,
    Failed(String),
}

impl InstructionsReport {
    fn failed(name: &str, reason: String) -> Self {
        Self {
            name: name.to_string(),
            status: ReportStatus::Failed(reason),
        }
    }
}

fn probe_one(name: &str, live: &LiveUpstream) -> InstructionsReport {
    let peer = live.peer();
    let Some(info) = peer.peer_info() else {
        println!("  (no peer_info available — upstream may not have completed handshake)");
        return InstructionsReport::failed(name, "no peer_info".to_string());
    };

    println!(
        "  server: {} v{}",
        info.server_info.name, info.server_info.version
    );

    match &info.instructions {
        None => {
            println!("  instructions: (not set)");
            InstructionsReport {
                name: name.to_string(),
                status: ReportStatus::Absent,
            }
        }
        Some(text) if text.is_empty() => {
            println!("  instructions: (present but empty string)");
            InstructionsReport {
                name: name.to_string(),
                status: ReportStatus::EmptyString,
            }
        }
        Some(text) => {
            let chars = text.chars().count();
            let lines = text.lines().count();
            println!("  instructions: {chars} chars, {lines} line(s)");
            println!("  ---");
            for line in text.lines() {
                println!("  | {line}");
            }
            println!("  ---");
            InstructionsReport {
                name: name.to_string(),
                status: ReportStatus::Present { chars, lines },
            }
        }
    }
}

fn print_summary(reports: &[InstructionsReport]) {
    println!("\n=== Summary ===");
    let name_width = reports
        .iter()
        .map(|r| r.name.len())
        .max()
        .unwrap_or(8)
        .max(8);
    for r in reports {
        let status = match &r.status {
            ReportStatus::Present { chars, lines } => {
                format!("PRESENT  {chars:>6} chars  {lines:>3} line(s)")
            }
            ReportStatus::EmptyString => "empty string".to_string(),
            ReportStatus::Absent => "absent".to_string(),
            ReportStatus::Failed(reason) => format!("FAILED   {reason}"),
        };
        println!("  {:<width$}  {}", r.name, status, width = name_width);
    }
}

fn load_configs() -> Result<Vec<mcp_client::UpstreamConfig>> {
    let path = resolve_config_path().context(
        "no config path resolved; set $RATO_CONFIG_PATH or create ~/Library/Preferences/ratatoskr/config.json",
    )?;
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
