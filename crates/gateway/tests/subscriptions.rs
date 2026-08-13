// pattern: Imperative Shell
//
// End-to-end tests for the resources/subscribe → notifications/resources/updated
// path. In-process upstream + real gateway + real downstream client.

mod common;
//
// Properties this suite pins:
//   (a) Two sessions subscribing to the same (upstream, uri) key produce
//       EXACTLY ONE upstream `resources/subscribe` call. Naive per-session
//       forwarding would call N times and break the upstream's idempotent-
//       subscribe contract.
//   (b) After one session unsubscribes, the upstream remains subscribed
//       (other session still cares). Only when the LAST session leaves
//       does the gateway call upstream unsubscribe.
//   (c) An upstream `resources/updated` notification reaches every
//       subscribed downstream session — with the namespaced URI, not
//       the bare upstream URI.
//   (d) A session that isn't subscribed does NOT receive another
//       session's update. The per-session forwarder filter is
//       doing its job.
//   (e) Session close tears down the upstream subscription if that
//       session was the last subscriber.
//
// The fake upstream captures its downstream-facing peer on subscribe
// so individual tests can push `notify_resource_updated` at will.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mcp_client::{
    ResourceListCache, SamplingBroker, SharedSamplingBroker, ToolListCache, UpstreamEventBus,
    UpstreamHandle, UpstreamManager,
};
use rmcp::{
    ClientHandler, ErrorData, RoleServer, ServerHandler, ServiceExt,
    model::{
        ClientCapabilities, ClientInfo, Implementation, ResourceUpdatedNotificationParam,
        ServerCapabilities, ServerInfo, SubscribeRequestParams, UnsubscribeRequestParams,
    },
    service::{NotificationContext, Peer, RequestContext, RoleClient},
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

// -------- Fake upstream: counts subs/unsubs, lets us inject updates --------

#[derive(Default)]
struct UpstreamState {
    subscribe_calls: AtomicUsize,
    unsubscribe_calls: AtomicUsize,
    /// Last subscribe URI observed, for assertions.
    last_subscribe_uri: Mutex<Option<String>>,
    /// The rmcp peer we answer to, captured on first subscribe. We use
    /// it to emit `resources/updated` from test code.
    peer: Mutex<Option<Peer<RoleServer>>>,
}

#[derive(Clone)]
struct FakeSubscribableServer {
    state: Arc<UpstreamState>,
}

impl ServerHandler for FakeSubscribableServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_resources()
                .enable_resources_subscribe()
                .build(),
        )
        .with_server_info(Implementation::new("fake-subscribable", "0.0.1"))
    }

    async fn subscribe(
        &self,
        request: SubscribeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.state.subscribe_calls.fetch_add(1, Ordering::SeqCst);
        *self
            .state
            .last_subscribe_uri
            .lock()
            .expect("mutex not poisoned") = Some(request.uri.clone());
        // Capture peer on first call so tests can emit updates.
        let mut slot = self.state.peer.lock().expect("mutex not poisoned");
        if slot.is_none() {
            *slot = Some(context.peer.clone());
        }
        Ok(())
    }

    async fn unsubscribe(
        &self,
        _request: UnsubscribeRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.state.unsubscribe_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl UpstreamState {
    fn subscribe_count(&self) -> usize {
        self.subscribe_calls.load(Ordering::SeqCst)
    }
    fn unsubscribe_count(&self) -> usize {
        self.unsubscribe_calls.load(Ordering::SeqCst)
    }
    async fn emit_update(&self, uri: &str) -> Result<(), rmcp::ServiceError> {
        // Wait briefly for peer to be set (subscribe call captures it).
        // Tests that call this have already awaited subscribe so the
        // slot should be populated by the time we get here.
        let peer = self
            .peer
            .lock()
            .expect("mutex not poisoned")
            .clone()
            .expect("emit_update called before any subscribe");
        peer.notify_resource_updated(ResourceUpdatedNotificationParam {
            uri: uri.to_string(),
        })
        .await
    }
}

// -------- Downstream client that records resource_updated notifications --------

#[derive(Clone, Default)]
struct RecordingClient {
    received: Arc<Mutex<Vec<String>>>,
}

impl RecordingClient {
    fn received_uris(&self) -> Vec<String> {
        self.received.lock().expect("mutex not poisoned").clone()
    }
}

impl ClientHandler for RecordingClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("subscriptions-test-client", "0.0.1"),
        )
    }

    async fn on_resource_updated(
        &self,
        params: ResourceUpdatedNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        self.received
            .lock()
            .expect("mutex not poisoned")
            .push(params.uri);
    }
}

// -------- Harness --------

struct Harness {
    upstream_handle: Option<Arc<UpstreamHandle>>,
    fake_upstream_task: tokio::task::JoinHandle<()>,
    upstream_state: Arc<UpstreamState>,
    downstream_url: String,
    ct: CancellationToken,
    server_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn shutdown(mut self) {
        match common::cancel_and_join_server(&self.ct, self.server_handle).await {
            common::ServerShutdown::Clean => {}
            common::ServerShutdown::ServeErr(e) => panic!("gateway server returned error: {e:#}"),
            common::ServerShutdown::Panicked(join_err) => {
                panic!("gateway server task panicked: {join_err}")
            }
            common::ServerShutdown::TimedOut => {
                eprintln!("warning: gateway server did not shut down within 3 s")
            }
        }
        // Drop the gateway-side upstream handle BEFORE awaiting the
        // fake upstream task. The fake upstream's `service.waiting()`
        // returns only when its peer (the gateway-side duplex half
        // owned by `UpstreamHandle`) disconnects. Holding the Arc
        // across the join would deadlock: the fake task waits for its
        // peer to close, and its peer is still alive in the harness.
        self.upstream_handle.take();
        if let Err(e) = self.fake_upstream_task.await {
            panic!("fake upstream task panicked: {e}");
        }
    }
}

async fn start_harness() -> Harness {
    let bus = Arc::new(UpstreamEventBus::new());
    let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
    let manager = Arc::new(UpstreamManager::new());
    let tool_cache = Arc::new(ToolListCache::new());
    let resource_cache = Arc::new(ResourceListCache::new());

    let upstream_state = Arc::new(UpstreamState::default());
    let server = FakeSubscribableServer {
        state: Arc::clone(&upstream_state),
    };

    let (handle, fake_task) =
        common::spawn_fake_upstream("rootly", "rootly", server, &bus, &broker).await;
    manager.register(Arc::clone(&handle)).await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().expect("local addr");
    let downstream_url = format!("http://{addr}/mcp");

    let ct = CancellationToken::new();
    let shutdown_ct = ct.clone();
    let ct_for_serve = ct.clone();
    let supervisor_ct = ct.child_token();

    let server_handle = tokio::spawn(async move {
        gateway::serve_on(
            listener,
            gateway::AppContext {
                tool_cache,
                resource_cache,
                ..common::test_app_context(manager, bus, broker).await
            },
            Vec::new(),
            ct_for_serve,
            supervisor_ct,
            async move {
                shutdown_ct.cancelled().await;
            },
        )
        .await
    });

    Harness {
        upstream_handle: Some(handle),
        fake_upstream_task: fake_task,
        upstream_state,
        downstream_url,
        ct,
        server_handle,
    }
}

async fn connect_client(
    url: &str,
) -> (
    RecordingClient,
    rmcp::service::RunningService<RoleClient, RecordingClient>,
) {
    let client = RecordingClient::default();
    let transport = rmcp::transport::StreamableHttpClientTransport::from_uri(url.to_string());
    let service = client
        .clone()
        .serve(transport)
        .await
        .expect("downstream client connect");
    (client, service)
}

/// Poll-wait helper: returns Ok when `cond` becomes true, Err on timeout.
/// Keeps tests condition-based (per writing-good-tests) rather than
/// guessing sleep durations.
async fn wait_until<F: FnMut() -> bool>(mut cond: F, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {what}");
}

const INCIDENT_42_BARE: &str = "incident://42";
const INCIDENT_42_NAMESPACED: &str = "mcp-proxy://rootly/incident%3A%2F%2F42";

/// Template `incident://{id}` in namespaced form. `{` and `}` encode
/// to `%7B` and `%7D` under the gateway's aggressive encoding policy.
const INCIDENT_TEMPLATE_NAMESPACED: &str = "mcp-proxy://rootly/incident%3A%2F%2F%7Bid%7D";
const INCIDENT_TEMPLATE_BARE: &str = "incident://{id}";

// -------- Tests --------

#[tokio::test]
async fn two_sessions_subscribing_produce_one_upstream_subscribe_call() {
    // Property (a): ref-counting collapses multiple downstream subs on
    // the same key into a single upstream subscribe. Without this we'd
    // be hammering upstreams and relying on their idempotency.
    let harness = start_harness().await;

    let (_client_a, service_a) = connect_client(&harness.downstream_url).await;
    let (_client_b, service_b) = connect_client(&harness.downstream_url).await;

    service_a
        .peer()
        .subscribe(SubscribeRequestParams::new(INCIDENT_42_NAMESPACED))
        .await
        .expect("session A subscribe");
    service_b
        .peer()
        .subscribe(SubscribeRequestParams::new(INCIDENT_42_NAMESPACED))
        .await
        .expect("session B subscribe");

    assert_eq!(
        harness.upstream_state.subscribe_count(),
        1,
        "two downstream sessions must coalesce into one upstream subscribe"
    );
    assert_eq!(
        harness.upstream_state.unsubscribe_count(),
        0,
        "no unsubscribe should have happened yet"
    );

    let _ = service_a.cancel().await;
    let _ = service_b.cancel().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn unsubscribing_one_of_two_sessions_keeps_upstream_subscribed() {
    // Property (b): the upstream sub survives as long as ANY downstream
    // session wants it. Only when the last reference drops do we tear
    // it down upstream.
    let harness = start_harness().await;

    let (_client_a, service_a) = connect_client(&harness.downstream_url).await;
    let (_client_b, service_b) = connect_client(&harness.downstream_url).await;

    service_a
        .peer()
        .subscribe(SubscribeRequestParams::new(INCIDENT_42_NAMESPACED))
        .await
        .expect("session A subscribe");
    service_b
        .peer()
        .subscribe(SubscribeRequestParams::new(INCIDENT_42_NAMESPACED))
        .await
        .expect("session B subscribe");

    service_a
        .peer()
        .unsubscribe(UnsubscribeRequestParams::new(INCIDENT_42_NAMESPACED))
        .await
        .expect("session A unsubscribe");

    assert_eq!(
        harness.upstream_state.unsubscribe_count(),
        0,
        "one-of-two unsubscribe must NOT tear down the upstream sub"
    );

    service_b
        .peer()
        .unsubscribe(UnsubscribeRequestParams::new(INCIDENT_42_NAMESPACED))
        .await
        .expect("session B unsubscribe");

    assert_eq!(
        harness.upstream_state.unsubscribe_count(),
        1,
        "last session's unsubscribe must trigger the upstream teardown"
    );

    let _ = service_a.cancel().await;
    let _ = service_b.cancel().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn subscribed_session_receives_upstream_update_with_namespaced_uri() {
    // Properties (c) + (d): the subscribed session receives the update,
    // and the URI on the wire is the gateway's `mcp-proxy://...` form,
    // not the bare upstream URI. Clients subscribed via the namespaced
    // URI expect the notifications to carry it back.
    let harness = start_harness().await;

    let (client_sub, service_sub) = connect_client(&harness.downstream_url).await;
    let (client_other, service_other) = connect_client(&harness.downstream_url).await;

    service_sub
        .peer()
        .subscribe(SubscribeRequestParams::new(INCIDENT_42_NAMESPACED))
        .await
        .expect("subscriber session subscribe");

    // Wait for the upstream to have observed the subscribe and captured
    // its peer — only then can we emit updates. Condition-based, not
    // timing-based.
    let state = Arc::clone(&harness.upstream_state);
    wait_until(
        || state.subscribe_count() >= 1,
        "upstream saw subscribe call",
    )
    .await;

    // Emit an upstream update. This goes through ProxyUpstreamHandler's
    // on_resource_updated -> UpstreamEvent::ResourceUpdated -> every
    // session forwarder -> registry filter -> only the subscribed
    // session's peer gets notify_resource_updated.
    harness
        .upstream_state
        .emit_update(INCIDENT_42_BARE)
        .await
        .expect("upstream emits update");

    // Wait for the subscribed session to receive exactly one update.
    let client_sub_clone = client_sub.clone();
    wait_until(
        || !client_sub_clone.received_uris().is_empty(),
        "subscriber received an update",
    )
    .await;

    let received = client_sub.received_uris();
    assert_eq!(received.len(), 1, "subscriber received: {received:?}");
    assert_eq!(
        received[0], INCIDENT_42_NAMESPACED,
        "update URI must be namespaced, not bare upstream URI"
    );

    // Property (d): the other (unsubscribed) session must NOT see the
    // update. Give the forwarder a brief beat to drain; the negative
    // assertion is the whole point here so we can't just poll-forever.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        client_other.received_uris().is_empty(),
        "unsubscribed session must not receive updates: {:?}",
        client_other.received_uris()
    );

    let _ = service_sub.cancel().await;
    let _ = service_other.cancel().await;
    harness.shutdown().await;
}

// Note: the "session close tears down upstream subscription" property
// (item (e) in the suite-level comment) is NOT tested here, because
// deterministically invoking `LocalSessionManager::close_session` from
// an integration test requires issuing an HTTP DELETE with the
// rmcp-assigned session ID — which rmcp 1.5's `StreamableHttpClientTransport`
// doesn't expose. We pin that property one level down, in
// `inspecting_session_manager::tests::close_session_drops_subscription_entries_for_that_session`
// (direct call to close_session) and its siblings. Worth the trade:
// the end-to-end lift would require probing rmcp internals, which is
// exactly the test-brittleness the house style avoids.

// -------- Template subscription tests --------

#[tokio::test]
async fn template_subscriber_receives_update_for_matching_concrete_uri() {
    // Core template property: a session subscribed to `incident://{id}`
    // receives updates for concrete `incident://42`, with the URI on
    // the wire being the NAMESPACED CONCRETE form (not the template).
    //
    // This is the "subscribe to the shape, get updates for instances"
    // semantic the MCP spec implies for template subscriptions.
    let harness = start_harness().await;

    let (client, service) = connect_client(&harness.downstream_url).await;

    service
        .peer()
        .subscribe(SubscribeRequestParams::new(INCIDENT_TEMPLATE_NAMESPACED))
        .await
        .expect("template subscribe should succeed");

    // Upstream subscribe carried the bare template string.
    let state = Arc::clone(&harness.upstream_state);
    wait_until(
        || state.subscribe_count() >= 1,
        "upstream observed subscribe",
    )
    .await;
    assert_eq!(
        harness
            .upstream_state
            .last_subscribe_uri
            .lock()
            .expect("mutex ok")
            .as_deref(),
        Some(INCIDENT_TEMPLATE_BARE),
        "upstream subscribe must receive the template string unchanged"
    );

    // Upstream fires a CONCRETE update matching the template.
    harness
        .upstream_state
        .emit_update(INCIDENT_42_BARE)
        .await
        .expect("emit_update");

    let c = client.clone();
    wait_until(
        || !c.received_uris().is_empty(),
        "template subscriber received concrete update",
    )
    .await;

    let received = client.received_uris();
    assert_eq!(received.len(), 1, "received: {received:?}");
    assert_eq!(
        received[0], INCIDENT_42_NAMESPACED,
        "template subscriber must receive the NAMESPACED CONCRETE URI, not the template URI"
    );

    let _ = service.cancel().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn template_subscriber_does_not_receive_non_matching_updates() {
    // Scope: subscriber to `incident://{id}` gets `incident://42`
    // but not `team://42`. The matcher's job is to say no.
    let harness = start_harness().await;

    let (client, service) = connect_client(&harness.downstream_url).await;

    service
        .peer()
        .subscribe(SubscribeRequestParams::new(INCIDENT_TEMPLATE_NAMESPACED))
        .await
        .expect("template subscribe");

    let state = Arc::clone(&harness.upstream_state);
    wait_until(
        || state.subscribe_count() >= 1,
        "upstream observed subscribe",
    )
    .await;

    // Fire a non-matching update. The forwarder's registry lookup
    // should drop it without ever reaching the client.
    harness
        .upstream_state
        .emit_update("team://42")
        .await
        .expect("emit_update");

    // Give the forwarder room to not-deliver. Negative assertion so
    // this is the rare case where a bounded sleep is the tool.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        client.received_uris().is_empty(),
        "non-matching update must not reach template subscriber: {:?}",
        client.received_uris()
    );

    let _ = service.cancel().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn both_concrete_and_template_subscribers_receive_one_update_each() {
    // Property: per-session deduplication. A session subscribed to
    // BOTH `incident://42` (concrete) AND `incident://{id}` (template)
    // should get ONE update for `incident://42`, not two.
    //
    // (Today the forwarder short-circuits: if the concrete hit is
    // true, we don't even consult the template index. Pin the
    // per-session-one-update property so a future refactor that
    // walks both indices independently doesn't regress it.)
    let harness = start_harness().await;

    let (client, service) = connect_client(&harness.downstream_url).await;

    service
        .peer()
        .subscribe(SubscribeRequestParams::new(INCIDENT_42_NAMESPACED))
        .await
        .expect("concrete subscribe");
    service
        .peer()
        .subscribe(SubscribeRequestParams::new(INCIDENT_TEMPLATE_NAMESPACED))
        .await
        .expect("template subscribe");

    let state = Arc::clone(&harness.upstream_state);
    wait_until(
        || state.subscribe_count() >= 2,
        "two upstream subscribes observed",
    )
    .await;

    harness
        .upstream_state
        .emit_update(INCIDENT_42_BARE)
        .await
        .expect("emit_update");

    let c = client.clone();
    wait_until(|| !c.received_uris().is_empty(), "client received update").await;

    // Give the forwarder time to erroneously send a second update
    // if the dedup property were broken.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        client.received_uris().len(),
        1,
        "one update per session even when BOTH concrete AND template subs would match: {:?}",
        client.received_uris()
    );

    let _ = service.cancel().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn level_3_template_subscription_is_rejected_with_invalid_params() {
    // Level 3 uses `{?q}` form-style operators. Our matcher doesn't
    // support them, and the subscribe handler surfaces the compile
    // error as `invalid_params` so a client sees a deterministic,
    // typed failure rather than silent no-delivery.
    //
    // Percent-encode the template: `search://{?q}` becomes
    // `search%3A%2F%2F%7B%3Fq%7D` under the gateway's encoding set.
    const LEVEL3_NAMESPACED: &str = "mcp-proxy://rootly/search%3A%2F%2F%7B%3Fq%7D";

    let harness = start_harness().await;
    let (_client, service) = connect_client(&harness.downstream_url).await;

    let err = service
        .peer()
        .subscribe(SubscribeRequestParams::new(LEVEL3_NAMESPACED))
        .await
        .expect_err("Level 3 template must not be accepted");

    // Error should be JSON-RPC invalid_params (-32602), with a
    // message identifying the offending construct.
    let msg = format!("{err}");
    assert!(
        msg.contains("Level 1-2") || msg.contains("RFC 6570") || msg.contains("not support"),
        "error message should explain the Level 1-2 restriction, got: {msg}"
    );

    // No upstream subscribe fired — the failure happened before we
    // reached the upstream RPC.
    assert_eq!(
        harness.upstream_state.subscribe_count(),
        0,
        "Level 3 template rejection must happen before any upstream call"
    );

    let _ = service.cancel().await;
    harness.shutdown().await;
}
