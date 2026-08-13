// pattern: Imperative Shell
//
// Binary entry point. The actual implementation lives in the gateway lib;
// this file exists solely to give Cargo a [[bin]] target.
//
// Args/flags here are intentionally hand-parsed rather than depending on
// clap. The gateway has a tiny surface: the `login <upstream>` subcommand and
// the `--log-file[=PATH]` flags. Adding a CLI-parser crate for that would be
// pure overhead. If the surface ever grows past a few items, switch to clap.

use std::path::PathBuf;

use anyhow::{Context, Result};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Default log directory on macOS. Matches the platform convention —
/// `Console.app` picks up files here automatically and `newsyslog` knows
/// how to manage them. Linux/Windows get a clear error pointing at
/// `--log-file=PATH` instead of silently dropping files in a weird spot.
#[cfg(target_os = "macos")]
fn default_log_dir() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").context("$HOME not set; cannot resolve default log dir")?;
    Ok(PathBuf::from(home).join("Library/Logs/ratatoskr"))
}

#[cfg(not(target_os = "macos"))]
fn default_log_dir() -> Result<PathBuf> {
    anyhow::bail!(
        "--log-file without a path is only supported on macOS; \
         pass --log-file=PATH explicitly"
    )
}

/// Rotation policy. Daily rolling gives human-readable filenames
/// (`gateway.log.2026-04-23`) that are easy to reach for when
/// investigating "what happened on Tuesday". Keep 14 — two weeks is
/// plenty for single-user debugging, and the
/// `tracing_appender::rolling` builder auto-prunes older files so disk
/// growth stays bounded.
const LOG_FILE_PREFIX: &str = "gateway.log";
const KEEP_LOG_FILES: usize = 14;

/// Hand-parsed flag state. See module header for why no clap.
struct CliArgs {
    /// `None` → stdout-only (default). `Some(None)` → log-file flag
    /// passed without a path, use platform default. `Some(Some(path))`
    /// → explicit path.
    log_file: Option<Option<PathBuf>>,
}

fn parse_args() -> Result<CliArgs> {
    let mut log_file: Option<Option<PathBuf>> = None;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--log-file" => {
                // Peek at the next arg. If it looks like a value (no
                // leading `--`), consume it as the path; otherwise
                // treat the flag as bare (use default).
                match iter.clone().next() {
                    Some(next) if !next.starts_with("--") => {
                        let path = iter.next().expect("just peeked").clone();
                        log_file = Some(Some(PathBuf::from(path)));
                    }
                    _ => {
                        log_file = Some(None);
                    }
                }
            }
            flag if flag.starts_with("--log-file=") => {
                let path = flag.trim_start_matches("--log-file=");
                log_file = Some(Some(PathBuf::from(path)));
            }
            "--help" | "-h" => {
                eprintln!(
                    "rato\n\n\
                     USAGE:\n  \
                         rato [--log-file[=PATH]]\n\n\
                     FLAGS:\n  \
                         --log-file         Write logs to daily-rotated file (default: \
                                            ~/Library/Logs/ratatoskr/gateway.log on macOS).\n  \
                         --log-file=PATH    Write logs to PATH with daily rotation.\n\n\
                     When --log-file is absent, logs go to stdout only (default; never touches disk).\n\n\
                     SUBCOMMANDS:\n  \
                         login <upstream>   Interactively authorize an `http-oauth` upstream via\n  \
                         \x20                 your browser (one-time; credentials are persisted and\n  \
                         \x20                 refreshed silently thereafter).\n\n\
                     ENVIRONMENT:\n  \
                         RUST_LOG           tracing filter (default: \"info,gateway=debug\")\n  \
                         RATO_CONFIG_PATH path to gateway config JSON\n  \
                         RATO_STATE_DIR   override the OAuth credential state directory\n",
                );
                std::process::exit(0);
            }
            unknown => {
                anyhow::bail!("unknown flag: {unknown} (try --help)");
            }
        }
    }

    Ok(CliArgs { log_file })
}

/// Resolve the log-file target from the parsed flag state. Returns a
/// (directory, filename-prefix) pair that `tracing_appender::rolling`
/// uses to write `gateway.log.YYYY-MM-DD` files.
///
/// If the caller passed `--log-file=PATH`, the final filename component
/// becomes the prefix and the rest of the path is the directory. This
/// mirrors how people actually invoke it: `--log-file=/tmp/proxy.log`
/// means "write /tmp/proxy.log.2026-04-23 etc.".
fn resolve_log_target(flag: Option<Option<PathBuf>>) -> Result<Option<(PathBuf, String)>> {
    match flag {
        None => Ok(None),
        Some(None) => {
            let dir = default_log_dir()?;
            Ok(Some((dir, LOG_FILE_PREFIX.to_string())))
        }
        Some(Some(path)) => {
            let file_name = path
                .file_name()
                .context("--log-file path must not end in / (needs a filename component)")?
                .to_string_lossy()
                .into_owned();
            let dir = path
                .parent()
                .map(PathBuf::from)
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| PathBuf::from("."));
            Ok(Some((dir, file_name)))
        }
    }
}

/// Install tracing subscribers based on CLI flags. Returns a `WorkerGuard`
/// that MUST be held for the process lifetime when file logging is active —
/// the non-blocking writer drops buffered lines on Drop.
fn init_tracing(flag: Option<Option<PathBuf>>) -> Result<Option<WorkerGuard>> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|e| {
        eprintln!("WARN: invalid RUST_LOG, using default filter: {e}");
        "info,gateway=debug".into()
    });

    match resolve_log_target(flag)? {
        // No --log-file flag: stdout-only. The unconfigured case never
        // touches disk — this single-user gateway requires operator
        // opt-in before it allocates disk.
        None => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
            Ok(None)
        }
        Some((dir, prefix)) => {
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("creating log dir {}", dir.display()))?;

            let file_appender = RollingFileAppender::builder()
                .rotation(Rotation::DAILY)
                .filename_prefix(&prefix)
                // Bounded retention is load-bearing: without this, daily
                // rotation is just a slower disk-space footgun.
                .max_log_files(KEEP_LOG_FILES)
                .build(&dir)
                .with_context(|| format!("building rolling appender in {}", dir.display()))?;

            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

            // Dual output: stdout for whoever's tailing a live session,
            // file for forensic review. Both get the same JSON
            // formatting so parsing scripts don't branch on source.
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer().json())
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(non_blocking)
                        // ANSI off for file output: the stdout layer above
                        // already handles terminal colors if a tty is
                        // attached. File output must be plain bytes so
                        // grep/jq don't choke on escape codes.
                        .with_ansi(false),
                )
                .init();

            tracing::info!(
                log_dir = %dir.display(),
                log_prefix = %prefix,
                keep = KEEP_LOG_FILES,
                "file logging enabled (daily rotation)"
            );

            Ok(Some(guard))
        }
    }
}

/// Install a panic hook that reports via `tracing::error!` so panics land
/// in the same JSON log stream as everything else. Without this, a panic
/// in a blocking worker would print `thread '...' panicked at '...'` to
/// stderr — visible in a terminal session, but unstructured and easy to
/// miss in a file-based log pipeline.
fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info.payload();
        let payload_str = if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            "non-string panic payload".to_string()
        };

        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown".to_string());

        let backtrace = std::backtrace::Backtrace::capture();

        tracing::error!(
            panic.payload = %payload_str,
            panic.location = %location,
            panic.backtrace = %backtrace,
            "panic in gateway process"
        );

        // Still fall through to the default hook so the process dies
        // with the usual stderr banner. We want belt AND suspenders
        // here: structured for log pipelines, unstructured for a human
        // watching the terminal.
        default_hook(info);
    }));
}

/// Dispatch the `login <upstream>` subcommand if it's the first positional
/// argument. Returns `Some(result)` when login was invoked (the caller should
/// return it), `None` when no subcommand was present (fall through to the
/// normal gateway run). Kept separate from `parse_args` because login is an
/// interactive CLI mode with its own (stderr) logging, not a server flag.
async fn maybe_run_subcommand() -> Option<Result<()>> {
    let mut args = std::env::args().skip(1);
    let first = args.next()?;
    if first != "login" {
        return None;
    }

    let Some(upstream) = args.next() else {
        return Some(Err(anyhow::anyhow!("usage: rato login <upstream-name>")));
    };

    // Interactive mode: human-readable logs to stderr, never the JSON file
    // pipeline. Best-effort — if a global subscriber is somehow already set,
    // proceed without one.
    let _ = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn,gateway=info".into()))
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .try_init();

    Some(gateway::login::run(&upstream).await)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Subcommands are checked before flag parsing and tracing setup so an
    // interactive `login` doesn't spin up the server's file-logging pipeline.
    if let Some(result) = maybe_run_subcommand().await {
        return result;
    }

    let args = parse_args()?;
    // `_guard` MUST stay alive for the whole process. Dropping it would
    // drop the non-blocking writer's worker thread and flush behavior
    // silently degrades.
    let _guard = init_tracing(args.log_file)?;

    install_panic_hook();

    tracing::info!("rato starting");
    gateway::run().await
}
