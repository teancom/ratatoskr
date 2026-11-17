// pattern: Imperative Shell
//
// Scope bounds: drain in fixed chunks, retain at most 16 KiB of complete lines,
// and omit oversized lines rather than exposing a truncated credential. A pipe
// inherited by a descendant must never make connection failure wait for EOF.
// Invariant: successful connections keep draining, but retain no diagnostics.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinHandle;

const MAX_BYTES: usize = 16 * 1024;
const MAX_LINE: usize = 4096;

#[derive(Default)]
struct Lines {
    lines: VecDeque<String>,
    bytes: usize,
    discarded: bool,
}

impl Lines {
    fn push(&mut self, line: String) {
        if self.discarded {
            return;
        }
        // Rendering can replace even a short line with the redaction marker.
        self.bytes += line.len().max(48) + 1;
        self.lines.push_back(line);
        while self.bytes > MAX_BYTES {
            if let Some(line) = self.lines.pop_front() {
                self.bytes -= line.len().max(48) + 1;
            }
        }
    }
}

/// Drains a child's stderr for the lifetime of its connection.
pub struct StartupStderr {
    lines: Arc<Mutex<Lines>>,
    task: JoinHandle<()>,
}

impl StartupStderr {
    pub(crate) fn spawn(reader: impl AsyncRead + Unpin + Send + 'static) -> Self {
        let lines = Arc::new(Mutex::new(Lines::default()));
        let sink = Arc::clone(&lines);
        let task = tokio::spawn(async move { drain(reader, sink).await });
        Self { lines, task }
    }

    pub(crate) fn connected(&self) {
        let mut lines = self.lines.lock().expect("stderr buffer lock");
        lines.lines.clear();
        lines.bytes = 0;
        lines.discarded = true;
    }

    pub(crate) async fn diagnostic(&mut self, secrets: &[String]) -> String {
        // A launcher or descendant can hold stderr open after stdout closes.
        let _ = tokio::time::timeout(Duration::from_millis(100), &mut self.task).await;
        let lines = self.lines.lock().expect("stderr buffer lock");
        let mut text = String::new();
        for line in &lines.lines {
            text.push_str(&redact(line, secrets));
            text.push('\n');
        }
        text.trim().to_string()
    }
}

impl Drop for StartupStderr {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn drain(mut reader: impl AsyncRead + Unpin, sink: Arc<Mutex<Lines>>) {
    let mut chunk = [0; 1024];
    let mut line = Vec::new();
    let mut oversized = false;
    loop {
        let count = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(count) => count,
        };
        for &byte in &chunk[..count] {
            if byte == b'\n' {
                let text = if oversized {
                    "[oversized stderr line omitted]".to_string()
                } else {
                    String::from_utf8_lossy(&line).into_owned()
                };
                sink.lock().expect("stderr buffer lock").push(text);
                line.clear();
                oversized = false;
            } else if line.len() < MAX_LINE {
                line.push(byte);
            } else {
                oversized = true;
            }
        }
    }
    if oversized || !line.is_empty() {
        let text = if oversized {
            "[oversized stderr line omitted]".to_string()
        } else {
            String::from_utf8_lossy(&line).into_owned()
        };
        sink.lock().expect("stderr buffer lock").push(text);
    }
}

fn sensitive_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "passwd",
        "authorization",
        "cookie",
        "api_key",
        "api-key",
        "apikey",
        "credential",
        "private_key",
        "private key",
        "bearer",
    ]
    .iter()
    .any(|word| name.contains(word))
}

fn redact(line: &str, secrets: &[String]) -> String {
    // Control removal can join separated fragments into a secret, sensitive
    // field name, or credential-bearing URL. Check both forms: raw secrets can
    // themselves contain controls and must not be missed by sanitizing first.
    let sanitized: String = line
        .chars()
        .filter(|c| !c.is_control() || *c == '\t')
        .collect();
    let sensitive = |text: &str| {
        sensitive_name(text)
            || secrets
                .iter()
                .any(|secret| !secret.is_empty() && text.contains(secret))
            || (text.contains("://") && (text.contains('@') || text.contains('?')))
    };
    if sensitive(line) || sensitive(&sanitized) {
        "[credential-bearing stderr line redacted]".to_string()
    } else {
        sanitized
    }
}

pub(crate) fn secrets(
    env: &std::collections::HashMap<String, String>,
    args: &[String],
) -> Vec<String> {
    let mut values: Vec<String> = env.values().filter(|v| !v.is_empty()).cloned().collect();
    values.extend(
        std::env::vars()
            .filter(|(key, value)| sensitive_name(key) && !value.is_empty())
            .map(|(_, value)| value),
    );
    for pair in args.windows(2) {
        if sensitive_name(&pair[0]) {
            values.push(pair[1].clone());
        }
    }
    for arg in args {
        if sensitive_name(arg)
            && let Some((_, value)) = arg.split_once('=')
        {
            values.push(value.to_string());
        }
    }
    values
}

/// Bound diagnostics before retaining them. HTTP transport errors may contain
/// response bodies or OAuth credentials unknown to the config; expose no raw
/// HTTP error text. Stdio shares the launcher's conservative line policy.
pub(crate) fn connection_diagnostic(config: &crate::UpstreamConfig, error: &str) -> String {
    let crate::UpstreamKind::Stdio { env, args, .. } = &config.kind else {
        return "HTTP connection failed (transport details withheld to protect credentials); retry reconnect-upstream for the current status".to_string();
    };
    let secrets = secrets(env, args);
    let mut out = String::new();
    for line in error.lines() {
        let line = if line.len() > MAX_LINE {
            "[oversized diagnostic line omitted]".to_string()
        } else {
            redact(line, &secrets)
        };
        if out.len() + line.len() + 1 > MAX_BYTES {
            break;
        }
        out.push_str(&line);
        out.push('\n');
    }
    out.trim().to_string()
}

#[cfg(test)]
#[path = "startup_stderr_tests.rs"]
mod tests;
