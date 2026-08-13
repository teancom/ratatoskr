// pattern: Imperative Shell
//
// Regression test for the second bug surfaced by the first real-config
// smoke run: UpstreamKind::Http.headers was parsed into the config but
// discarded in connect(). Any HTTP upstream requiring auth (e.g. Glean
// with an OAuth bearer token) would fail with "Auth required" before
// the gateway could even connect.
//
// Strategy: run a bare-TCP listener that parrots a plausible-looking MCP
// error response, and captures the Authorization header from the incoming
// POST. We don't need a full MCP handshake to work — we just need the
// connect attempt to get far enough to send one request. That request's
// headers prove whether rmcp's transport received our custom_headers map.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mcp_client::{SamplingBroker, UpstreamConfig, UpstreamEventBus, UpstreamKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// A one-shot HTTP listener that accepts a single connection, reads the
/// request bytes up to the end-of-headers marker, stashes the raw header
/// block, and replies with a 400 so the client stops cleanly.
async fn spawn_header_capture_listener() -> (String, Arc<Mutex<Option<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().expect("local addr");
    let url = format!("http://{addr}/mcp");
    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let captured_for_task = Arc::clone(&captured);
    tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = Vec::with_capacity(2048);
            // Read until we see \r\n\r\n (end of HTTP headers). Cap at 64
            // KiB to avoid spinning on a client that never sends the
            // terminator.
            let mut chunk = [0u8; 512];
            while buf.len() < 64 * 1024 {
                match socket.read(&mut chunk).await {
                    Ok(0) => break,
                    Ok(n) => {
                        buf.extend_from_slice(&chunk[..n]);
                        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            let headers_only = buf
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|end| String::from_utf8_lossy(&buf[..end]).to_string())
                .unwrap_or_else(|| String::from_utf8_lossy(&buf).to_string());

            *captured_for_task.lock().await = Some(headers_only);

            // Send back something that causes rmcp to fail fast. 400 with
            // plain text is enough — rmcp surfaces it as
            // UnexpectedServerResponse and the test doesn't care about the
            // specific failure mode.
            let _ = socket
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\n\
                      Content-Length: 0\r\n\
                      Connection: close\r\n\
                      \r\n",
                )
                .await;
            let _ = socket.shutdown().await;
        }
    });

    (url, captured)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorization_header_reaches_the_wire() {
    // Property: every key-value pair in UpstreamKind::Http.headers must be
    // present in the outgoing HTTP request. Regression guard for the bug
    // where connect() discarded the headers map before passing it to rmcp's
    // streamable-http transport. Two headers (Authorization and X-Custom-Tag)
    // verify the full map is forwarded, not just the first entry.
    let (url, captured) = spawn_header_capture_listener().await;

    let mut headers = HashMap::new();
    headers.insert(
        "Authorization".to_string(),
        "Bearer test-token-42".to_string(),
    );
    headers.insert("X-Custom-Tag".to_string(), "gateway-smoke".to_string());

    let config = UpstreamConfig::new(
        "captured",
        "captured",
        UpstreamKind::Http {
            url,
            headers: headers.clone(),
        },
    );
    let bus = UpstreamEventBus::new();
    let broker = Arc::new(SamplingBroker::new());

    // Connect is expected to fail — our listener returns a 400. The
    // captured-headers side effect is what we're testing.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        mcp_client::connect(&config, &bus, &broker),
    )
    .await
    .expect("connect should not hang");
    assert!(
        result.is_err(),
        "test server replies 400 so connect must fail, but it succeeded"
    );

    // Give the listener task a moment to finalize the capture after the
    // client hung up. The request was fully received before we replied,
    // so this is just scheduling slack — 500ms is plenty.
    //
    // Why poll rather than a tokio::sync::oneshot or Notify? The harness
    // intentionally uses only an Arc<Mutex<Option<String>>> to keep the
    // spawn_header_capture_listener helper minimal and self-contained.
    // Adding a notification primitive would complicate the return type and
    // the spawned task. The poll loop is legitimate here because we are
    // waiting on a positive event (capture arrives) with a bounded window,
    // and 50 × 10 ms = 500 ms is far more than enough scheduling slack on
    // any CI environment.
    for _ in 0..50 {
        if captured.lock().await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let headers_block = captured
        .lock()
        .await
        .clone()
        .expect("listener should have captured a request");

    // reqwest lowercases header names on the wire, so match
    // case-insensitively. The value must be exact.
    let lower = headers_block.to_ascii_lowercase();
    assert!(
        lower.contains("authorization: bearer test-token-42"),
        "captured headers did not include Authorization:\n{headers_block}"
    );
    assert!(
        lower.contains("x-custom-tag: gateway-smoke"),
        "captured headers did not include X-Custom-Tag:\n{headers_block}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_header_name_surfaces_as_connect_error() {
    // Defense-in-depth: nonsense in the config shouldn't explode deep in
    // the worker — it should fail at connect time with a clear message.
    let mut headers = HashMap::new();
    headers.insert("has space".to_string(), "v".to_string()); // invalid HTTP header name

    let config = UpstreamConfig::new(
        "bad",
        "bad",
        UpstreamKind::Http {
            url: "http://127.0.0.1:1/mcp".to_string(), // unreachable, doesn't matter
            headers,
        },
    );
    let bus = UpstreamEventBus::new();
    let broker = Arc::new(SamplingBroker::new());

    let err = match mcp_client::connect(&config, &bus, &broker).await {
        Ok(_) => panic!("invalid header should fail connect, but it succeeded"),
        Err(e) => e,
    };
    // Match the variant directly rather than asserting on a Debug substring,
    // so refactors to the Debug impl don't affect this test. Display is the
    // stable public contract; Debug is an implementation detail.
    let mcp_client::UpstreamError::Connect { source, .. } = &err else {
        panic!("expected UpstreamError::Connect, got {err:?}");
    };
    let msg = source.to_string();
    assert!(
        msg.contains("invalid header name"),
        "expected message to mention 'invalid header name'; got: {msg}"
    );
    assert!(
        msg.contains("has space"),
        "expected message to identify the bad header 'has space'; got: {msg}"
    );
}
