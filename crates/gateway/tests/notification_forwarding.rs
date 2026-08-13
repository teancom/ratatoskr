// pattern: Imperative Shell
//
// End-to-end test: upstream -> bus -> downstream notification
// forwarding.

mod common;
//
// Strategy: spawn the real gateway (via serve_on) on a random port, connect
// a real rmcp client with a handler that counts received notifications,
// then publish events to the bus and verify they arrive downstream.
//
// This deliberately uses NO real upstream MCP server — we publish synthetic
// events directly to the bus. That isolates the test to the gateway's
// internal fan-out path and keeps it fast (no npx cold start).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use mcp_client::{
    SamplingBroker, SharedSamplingBroker, UpstreamEvent, UpstreamEventBus, UpstreamManager,
};
use rmcp::{
    ClientHandler, ServiceExt,
    model::{ClientCapabilities, ClientInfo, Implementation},
    service::NotificationContext,
    transport::StreamableHttpClientTransport,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
struct NotificationCounter {
    tool_changes: Arc<AtomicU32>,
    resource_changes: Arc<AtomicU32>,
    prompt_changes: Arc<AtomicU32>,
}

impl ClientHandler for NotificationCounter {
    async fn on_tool_list_changed(&self, _context: NotificationContext<rmcp::RoleClient>) {
        self.tool_changes.fetch_add(1, Ordering::SeqCst);
    }
    async fn on_resource_list_changed(&self, _context: NotificationContext<rmcp::RoleClient>) {
        self.resource_changes.fetch_add(1, Ordering::SeqCst);
    }
    async fn on_prompt_list_changed(&self, _context: NotificationContext<rmcp::RoleClient>) {
        self.prompt_changes.fetch_add(1, Ordering::SeqCst);
    }
}

/// Wait for `cond` to become true, polling every 25ms up to `timeout`.
/// Returns Ok(elapsed) on success, Err(elapsed) on timeout.
async fn eventually<F>(
    timeout: Duration,
    mut cond: F,
) -> Result<std::time::Duration, std::time::Duration>
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return Ok(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(start.elapsed())
}

struct GatewayFixture {
    bus: Arc<UpstreamEventBus>,
    url: String,
    ct: CancellationToken,
    server_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl GatewayFixture {
    async fn start() -> Self {
        // Bind port 0 → OS picks a free one.
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind to random port");
        let addr = listener.local_addr().expect("get local addr");
        let url = format!("http://{addr}/mcp");

        let bus = Arc::new(UpstreamEventBus::new());
        let upstreams = Arc::new(UpstreamManager::new());
        let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
        let ct = CancellationToken::new();
        let shutdown_ct = ct.clone();

        let bus_clone = Arc::clone(&bus);
        let upstreams_clone = Arc::clone(&upstreams);
        let broker_clone = broker.clone();
        let ct_for_serve = ct.clone();
        let supervisor_ct = ct.child_token();
        let server_handle = tokio::spawn(async move {
            gateway::serve_on(
                listener,
                common::test_app_context(upstreams_clone, bus_clone, broker_clone).await,
                Vec::new(),
                ct_for_serve,
                supervisor_ct,
                async move {
                    shutdown_ct.cancelled().await;
                },
            )
            .await
        });

        Self {
            bus,
            url,
            ct,
            server_handle,
        }
    }

    async fn shutdown(self) {
        // Give axum a moment to finish graceful shutdown.
        match common::cancel_and_join_server(&self.ct, self.server_handle).await {
            common::ServerShutdown::TimedOut => panic!("gateway did not shut down within 3 s"),
            common::ServerShutdown::Panicked(join_err) => {
                panic!("gateway task panicked: {join_err}")
            }
            common::ServerShutdown::ServeErr(serve_err) => {
                panic!("gateway returned error on shutdown: {serve_err}")
            }
            common::ServerShutdown::Clean => {}
        }
    }
}

async fn connect_counting_client(
    url: &str,
) -> (
    rmcp::service::RunningService<rmcp::RoleClient, CountingClientHandler>,
    NotificationCounter,
) {
    let counter = NotificationCounter::default();
    let transport = StreamableHttpClientTransport::from_uri(url);
    let client_info = ClientInfo::new(
        ClientCapabilities::default(),
        Implementation::new("gate3-test-client", "0.0.1"),
    );
    let handler = CountingClientHandler {
        info: client_info,
        counter: counter.clone(),
    };
    let service = handler.serve(transport).await.expect("client must connect");
    (service, counter)
}

/// Custom ClientHandler that pairs the counter with a known ClientInfo.
#[derive(Clone)]
struct CountingClientHandler {
    info: ClientInfo,
    counter: NotificationCounter,
}

impl ClientHandler for CountingClientHandler {
    fn get_info(&self) -> ClientInfo {
        self.info.clone()
    }
    async fn on_tool_list_changed(&self, context: NotificationContext<rmcp::RoleClient>) {
        self.counter.on_tool_list_changed(context).await
    }
    async fn on_resource_list_changed(&self, context: NotificationContext<rmcp::RoleClient>) {
        self.counter.on_resource_list_changed(context).await
    }
    async fn on_prompt_list_changed(&self, context: NotificationContext<rmcp::RoleClient>) {
        self.counter.on_prompt_list_changed(context).await
    }
}

#[tokio::test]
async fn resource_list_changed_is_forwarded_downstream() {
    let fixture = GatewayFixture::start().await;
    let (_client_service, counter) = connect_counting_client(&fixture.url).await;

    // Give the downstream session's forwarder a beat to attach.
    // (on_initialized runs asynchronously; without a small grace window the
    // publish can race with the forwarder's subscribe() call.)
    eventually(Duration::from_millis(500), || {
        fixture.bus.publish(UpstreamEvent::ResourceListChanged {
            upstream: "fake".to_string(),
        }) > 0
    })
    .await
    .expect("forwarder should subscribe to the bus within 500ms of client connect");

    let reap = eventually(Duration::from_secs(2), || {
        counter.resource_changes.load(Ordering::SeqCst) >= 1
    })
    .await
    .unwrap_or_else(|elapsed| panic!("resource_list_changed was not forwarded within {elapsed:?}"));
    eprintln!("resource_list_changed propagated in {reap:?}");

    fixture.shutdown().await;
}

#[tokio::test]
async fn prompt_list_changed_is_forwarded_downstream() {
    let fixture = GatewayFixture::start().await;
    let (_client_service, counter) = connect_counting_client(&fixture.url).await;

    eventually(Duration::from_millis(500), || {
        fixture.bus.publish(UpstreamEvent::PromptListChanged {
            upstream: "fake".to_string(),
        }) > 0
    })
    .await
    .expect("forwarder subscribes within 500ms");

    let elapsed = eventually(Duration::from_secs(2), || {
        counter.prompt_changes.load(Ordering::SeqCst) >= 1
    })
    .await
    .unwrap_or_else(|elapsed| panic!("prompt_list_changed was not forwarded within {elapsed:?}"));
    eprintln!("prompt_list_changed propagated in {elapsed:?}");

    fixture.shutdown().await;
}

#[tokio::test]
async fn forwarder_exits_promptly_after_client_transport_disconnects() {
    // Property: when the downstream client disconnects its transport
    // abruptly (no formal session close), the session forwarder task
    // must detect the failed notify_* and break out of its loop rather
    // than log-and-continue forever.
    //
    // Observation channel: the bus's broadcast-subscriber count, read
    // via publish()'s return value. The gateway always runs two
    // subscribers of its own (tool_cache invalidator, resource_cache
    // invalidator), so we compute deltas rather than absolute counts.
    // Before the client connects we capture the baseline (should be 2);
    // after it connects we expect baseline + 1 (the session forwarder).
    // After abrupt client disconnect the forwarder's next notify_* fails
    // with a transport error, the forwarder breaks out of its loop, rx
    // drops, and the count returns to baseline.
    let fixture = GatewayFixture::start().await;

    // Baseline: gateway-owned subscribers only, no downstream session.
    // `serve_on` spawns two invalidators (tool-list cache, resource-list
    // cache); wait for them to call bus.subscribe() before we connect a
    // client, so the baseline we record includes both.
    eventually(Duration::from_millis(500), || {
        fixture.bus.publish(UpstreamEvent::ResourceListChanged {
            upstream: "baseline".to_string(),
        }) >= 2
    })
    .await
    .expect("gateway-owned invalidators should subscribe within 500ms");
    let baseline = fixture.bus.publish(UpstreamEvent::ResourceListChanged {
        upstream: "baseline".to_string(),
    });

    let (client_service, counter) = connect_counting_client(&fixture.url).await;

    // Wait until the session forwarder has subscribed (+1 subscriber)
    // AND the forwarder is alive end-to-end by seeing a publish reach
    // the client.
    eventually(Duration::from_millis(500), || {
        fixture.bus.publish(UpstreamEvent::ResourceListChanged {
            upstream: "fake".to_string(),
        }) > baseline
    })
    .await
    .expect("forwarder should subscribe (count > baseline) within 500ms");

    eventually(Duration::from_secs(2), || {
        counter.resource_changes.load(Ordering::SeqCst) >= 1
    })
    .await
    .expect("baseline publish should reach the client");

    // Abruptly tear down the client side — cancel() closes the transport
    // without waiting for a graceful protocol-level session shutdown,
    // which is the scenario the forwarder's transport-closed handling
    // exists to cover.
    let _ = client_service.cancel().await;

    // Now publish events repeatedly. The first post-disconnect publish
    // pushes an event into the forwarder's receiver; the forwarder's
    // notify_* then fails with TransportClosed/TransportSend, so it
    // breaks out of its loop and drops its rx. Subsequent publish counts
    // must fall back to the baseline (gateway-owned subscribers only).
    let terminated = eventually(Duration::from_secs(3), || {
        fixture.bus.publish(UpstreamEvent::ResourceListChanged {
            upstream: "fake".to_string(),
        }) <= baseline
    })
    .await;

    assert!(
        terminated.is_ok(),
        "forwarder task should exit within 3s of client transport disconnect; \
         subscriber count never fell back to the pre-connect baseline of {baseline} \
         (indicating the task kept looping and holding its rx)"
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn tool_list_changed_is_logged_but_not_forwarded() {
    // Design decision: the gateway's downstream-facing tool list is fixed
    // (gateway tools never change due to upstream churn), so forwarding
    // tool list_changed would lie to the client. Matches TS behavior.
    let fixture = GatewayFixture::start().await;
    let (_client_service, counter) = connect_counting_client(&fixture.url).await;

    eventually(Duration::from_millis(500), || {
        fixture.bus.publish(UpstreamEvent::ToolListChanged {
            upstream: "fake".to_string(),
        }) > 0
    })
    .await
    .expect("forwarder subscribes within 500ms");

    // Wait a generous window; if tools/list_changed *were* forwarded, it
    // would arrive well within 500ms.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        counter.tool_changes.load(Ordering::SeqCst),
        0,
        "tool_list_changed must NOT propagate downstream"
    );

    fixture.shutdown().await;
}
