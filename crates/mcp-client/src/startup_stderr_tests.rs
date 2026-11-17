use super::*;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn bounded_tail_drains_large_output_and_keeps_actionable_last_line() {
    let (reader, mut writer) = tokio::io::duplex(1024);
    let mut capture = StartupStderr::spawn(reader);
    tokio::time::timeout(Duration::from_secs(3), async {
        for _ in 0..1000 {
            writer
                .write_all(b"ordinary startup chatter\n")
                .await
                .expect("write chatter");
        }
        writer
            .write_all(&vec![b'x'; MAX_LINE * 2])
            .await
            .expect("write oversized line");
        writer
            .write_all(b"\nlauncher: run login to renew your session\n")
            .await
            .expect("write hint");
        drop(writer);
    })
    .await
    .expect("stderr must continuously drain");
    let text = capture.diagnostic(&[]).await;
    assert!(text.len() <= MAX_BYTES);
    assert!(text.contains("launcher: run login to renew your session"));
    assert!(text.contains("[oversized stderr line omitted]"));
}

#[tokio::test]
async fn diagnostic_does_not_wait_for_pipe_eof() {
    let (reader, _writer) = tokio::io::duplex(1024);
    let mut capture = StartupStderr::spawn(reader);
    let text = tokio::time::timeout(Duration::from_secs(1), capture.diagnostic(&[]))
        .await
        .expect("open stderr cannot block error reporting");
    assert_eq!(text, "");
}

#[tokio::test]
async fn successful_connection_keeps_draining_without_retaining_output() {
    let (reader, mut writer) = tokio::io::duplex(1024);
    let mut capture = StartupStderr::spawn(reader);
    capture.connected();
    tokio::time::timeout(Duration::from_secs(3), async {
        writer
            .write_all(&vec![b'x'; MAX_BYTES * 8])
            .await
            .expect("drain after connect");
        drop(writer);
    })
    .await
    .expect("successful stderr must not fill pipe");
    assert_eq!(capture.diagnostic(&[]).await, "");
}

#[test]
fn secret_inventory_covers_configured_environment_and_argument_values() {
    let env = std::collections::HashMap::from([(
        "NORMAL_NAME".to_string(),
        "opaque-env-value".to_string(),
    )]);
    let args = vec![
        "--api-key=opaque-inline-value".to_string(),
        "--password".to_string(),
        "opaque-next-value".to_string(),
    ];
    let values = secrets(&env, &args);
    for value in [
        "opaque-env-value",
        "opaque-inline-value",
        "opaque-next-value",
    ] {
        assert!(values.iter().any(|candidate| candidate == value));
        assert_eq!(
            redact(&format!("debug: {value}"), &values),
            "[credential-bearing stderr line redacted]"
        );
    }
}

#[tokio::test]
async fn redaction_marker_expansion_remains_bounded() {
    let (reader, mut writer) = tokio::io::duplex(1024);
    let mut capture = StartupStderr::spawn(reader);
    for _ in 0..2000 {
        writer.write_all(b"token=x\n").await.expect("write");
    }
    drop(writer);
    let text = capture.diagnostic(&[]).await;
    assert!(text.len() <= MAX_BYTES);
    assert!(!text.contains("token=x"));
}

#[test]
fn control_removal_cannot_reconstruct_sensitive_diagnostics() {
    let secret = "synthetic-opaque-välue";
    let secrets = vec![secret.to_string()];
    for (boundary, _) in secret.char_indices().skip(1) {
        for control in (0..=0x9f)
            .filter_map(char::from_u32)
            .filter(|c| c.is_control() && *c != '\t')
        {
            let line = format!("{}{}{}", &secret[..boundary], control, &secret[boundary..]);
            assert!(
                !redact(&line, &secrets).contains(secret),
                "boundary {boundary}, control {control:?}"
            );
        }
    }
    for line in [
        "pass\u{1b}word=hunter2",
        "https:/\u{1b}/user:pass@example.com",
    ] {
        assert_eq!(
            redact(line, &[]),
            "[credential-bearing stderr line redacted]"
        );
    }
    assert_eq!(
        redact("opaque\u{1b}value", &["opaque\u{1b}value".to_string()]),
        "[credential-bearing stderr line redacted]"
    );
    assert_eq!(
        redact("ordinary\u{1b} diagnostic\ttext", &[]),
        "ordinary diagnostic\ttext"
    );
}

#[tokio::test]
async fn both_diagnostic_paths_hide_secrets_reconstructed_by_control_removal() {
    let secret = "synthetic-opaque-value";
    let line = "synthetic-opaque-\u{1b}value";
    let (reader, mut writer) = tokio::io::duplex(1024);
    let mut capture = StartupStderr::spawn(reader);
    writer
        .write_all(line.as_bytes())
        .await
        .expect("write interrupted secret");
    drop(writer);
    let captured = capture.diagnostic(&[secret.to_string()]).await;
    assert!(!captured.contains(secret));
    assert_eq!(captured, "[credential-bearing stderr line redacted]");

    let config = crate::UpstreamConfig::new(
        "launcher",
        "launcher",
        crate::UpstreamKind::Stdio {
            command: "launcher".to_string(),
            args: vec![],
            env: std::collections::HashMap::from([("VALUE".to_string(), secret.to_string())]),
        },
    );
    let diagnostic = connection_diagnostic(&config, line);
    assert!(!diagnostic.contains(secret));
    assert_eq!(diagnostic, "[credential-bearing stderr line redacted]");
}

#[test]
fn credential_lines_are_omitted_without_hiding_launcher_hint() {
    let secrets = vec!["opaque-config-value".to_string()];
    for line in [
        "Authorization: Bearer abcdef",
        "{\"access_token\":\"abc\"}",
        "PASSWORD=hunter2",
        "https://user:pass@example.com",
        "https://example.com?key=abc",
        "debug opaque-config-value",
    ] {
        let rendered = redact(line, &secrets);
        assert_eq!(rendered, "[credential-bearing stderr line redacted]");
        assert!(!rendered.contains("hunter2"));
        assert!(!rendered.contains("opaque-config-value"));
    }
    assert_eq!(
        redact("Run ride-cli login to renew your session", &secrets),
        "Run ride-cli login to renew your session"
    );
}
