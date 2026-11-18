//! Deterministic transaction schedules over a real in-process MCP connection.
mod common;

use std::collections::BTreeSet;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use gateway::subscription_registry::{SubscriptionRegistry, UpstreamSubKey};
use gateway::subscription_service::{SubscribeError, SubscriptionService, UnsubscribeError};
use mcp_client::{
    SamplingBroker, SharedSamplingBroker, UpstreamEventBus, UpstreamManager, resource_uri,
};
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{ServerCapabilities, ServerInfo, SubscribeRequestParams, UnsubscribeRequestParams},
    service::RequestContext,
};
use tokio::sync::Semaphore;

const KEYS: [&str; 2] = ["res://a/0", "res://a/{id}"];

struct State {
    fail: AtomicBool,
    pause: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
    actions: Mutex<Vec<(bool, String, bool)>>,
    active: Mutex<BTreeSet<String>>,
}
impl Default for State {
    fn default() -> Self {
        Self {
            fail: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            actions: Mutex::default(),
            active: Mutex::default(),
        }
    }
}
impl State {
    async fn call(&self, subscribe: bool, uri: String) -> Result<(), ErrorData> {
        let fail = self.fail.swap(false, Ordering::SeqCst);
        self.actions
            .lock()
            .expect("actions lock")
            .push((subscribe, uri.clone(), fail));
        self.entered.add_permits(1);
        if self.pause.load(Ordering::SeqCst) {
            self.release
                .acquire()
                .await
                .expect("release remains open")
                .forget();
        }
        if fail {
            return Err(ErrorData::internal_error("scheduled rejection", None));
        }
        let mut active = self.active.lock().expect("active lock");
        if subscribe {
            active.insert(uri);
        } else {
            active.remove(&uri);
        }
        Ok(())
    }

    async fn wait_entered(&self) {
        bounded(self.entered.acquire())
            .await
            .expect("entry semaphore open")
            .forget();
    }

    fn resume(&self) {
        self.pause.store(false, Ordering::SeqCst);
        self.release.add_permits(1);
    }

    fn assert_active(&self, key: usize, expected: bool) {
        assert_eq!(
            self.active.lock().expect("active lock").contains(KEYS[key]),
            expected
        );
    }

    fn assert_actions(&self, key: usize, expected: &[(bool, bool)]) {
        let expected: Vec<_> = expected
            .iter()
            .map(|&(subscribe, fail)| (subscribe, KEYS[key].to_string(), fail))
            .collect();
        assert_eq!(*self.actions.lock().expect("actions lock"), expected);
    }
}

#[derive(Clone)]
struct Fake(Arc<State>);
impl ServerHandler for Fake {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_resources()
                .enable_resources_subscribe()
                .build(),
        )
    }
    async fn subscribe(
        &self,
        p: SubscribeRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.0.call(true, p.uri).await
    }
    async fn unsubscribe(
        &self,
        p: UnsubscribeRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        self.0.call(false, p.uri).await
    }
}

struct Harness {
    service: SubscriptionService,
    registry: Arc<SubscriptionRegistry>,
    state: Arc<State>,
    task: tokio::task::JoinHandle<()>,
}
impl Harness {
    async fn new() -> Self {
        let state = Arc::new(State::default());
        let bus = Arc::new(UpstreamEventBus::new());
        let broker: SharedSamplingBroker = Arc::new(SamplingBroker::new());
        let (handle, task) =
            common::spawn_fake_upstream("fault", "fault", Fake(state.clone()), &bus, &broker).await;
        let manager = Arc::new(UpstreamManager::new());
        manager.register(handle).await;
        let registry = Arc::new(SubscriptionRegistry::new());
        Self {
            service: SubscriptionService::new(registry.clone(), manager),
            registry,
            state,
            task,
        }
    }

    fn assert_members(&self, key: usize, expected: &[&str]) {
        let actual: BTreeSet<_> = if key == 1 {
            self.registry
                .template_sessions_for("fault", "res://a/probe")
                .into_iter()
                .collect()
        } else {
            self.registry
                .sessions_for(&UpstreamSubKey::new("fault", KEYS[key]))
                .into_iter()
                .collect()
        };
        assert_eq!(actual, expected.iter().map(|s| s.to_string()).collect());
    }

    async fn shutdown(self) {
        drop(self.service);
        bounded(self.task).await.expect("fake task succeeds");
    }
}

fn uri(key: usize) -> String {
    resource_uri::encode("fault", KEYS[key])
}

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("operation completes")
}

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "operation must wait for the pending transaction"
        );
        Poll::Ready(())
    })
    .await;
}

fn subscribe_rejected(result: Result<(), SubscribeError>) {
    match result.expect_err("scheduled subscribe rejection") {
        SubscribeError::UpstreamCall { upstream, source } => {
            assert_eq!(upstream, "fault");
            assert!(
                source.to_string().contains("scheduled rejection"),
                "{source}"
            );
        }
        other => panic!("expected upstream rejection, got {other:?}"),
    }
}

fn unsubscribe_rejected(result: Result<(), UnsubscribeError>) {
    match result.expect_err("scheduled unsubscribe rejection") {
        UnsubscribeError::UpstreamCall { upstream, source } => {
            assert_eq!(upstream, "fault");
            assert!(
                source.to_string().contains("scheduled rejection"),
                "{source}"
            );
        }
        other => panic!("expected upstream rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn follower_waits_for_failed_first_subscribe_and_retries_upstream() {
    // Property: a follower cannot mistake an unaccepted upstream subscription for an established one.
    for k in 0..KEYS.len() {
        let h = Harness::new().await;
        h.state.fail.store(true, Ordering::SeqCst);
        h.state.pause.store(true, Ordering::SeqCst);
        let service = h.service.clone();
        let first = tokio::spawn(async move { service.subscribe("first", &uri(k)).await });
        h.state.wait_entered().await;
        let name = uri(k);
        let mut follower = Box::pin(h.service.subscribe("second", &name));
        assert_pending(follower.as_mut()).await;
        h.state.resume();
        subscribe_rejected(bounded(first).await.expect("first task"));
        bounded(follower).await.expect("follower retries upstream");
        h.assert_members(k, &["second"]);
        h.state.assert_active(k, true);
        h.state.assert_actions(k, &[(true, true), (true, false)]);
        h.shutdown().await;
    }
}

#[tokio::test]
async fn cancelled_subscribe_before_rejection_does_not_wedge_retry() {
    // Property: dropping the caller cannot skip rollback or turn a retry into false success.
    for k in 0..KEYS.len() {
        let h = Harness::new().await;
        h.state.fail.store(true, Ordering::SeqCst);
        h.state.pause.store(true, Ordering::SeqCst);
        let service = h.service.clone();
        let request = tokio::spawn(async move { service.subscribe("s", &uri(k)).await });
        h.state.wait_entered().await;
        request.abort();
        assert!(request.await.expect_err("aborted caller").is_cancelled());
        h.state.resume();
        bounded(h.service.subscribe("s", &uri(k)))
            .await
            .expect("retry succeeds");
        h.state.assert_actions(k, &[(true, true), (true, false)]);
        h.assert_members(k, &["s"]);
        h.state.assert_active(k, true);
        h.shutdown().await;
    }
}

#[tokio::test]
async fn failed_unsubscribe_cannot_restore_a_closed_session() {
    // Property: session close follows rollback and removes ownership permanently.
    for k in 0..KEYS.len() {
        let h = Harness::new().await;
        h.service
            .subscribe("closing", &uri(k))
            .await
            .expect("initial subscribe");
        h.state.wait_entered().await;
        h.state.fail.store(true, Ordering::SeqCst);
        h.state.pause.store(true, Ordering::SeqCst);
        let service = h.service.clone();
        let request = tokio::spawn(async move { service.unsubscribe("closing", &uri(k)).await });
        h.state.wait_entered().await;
        let mut close = Box::pin(h.service.drop_session("closing"));
        assert_pending(close.as_mut()).await;
        h.state.resume();
        unsubscribe_rejected(bounded(request).await.expect("unsubscribe task"));
        bounded(close).await;
        h.assert_members(k, &[]);
        h.state.assert_active(k, false);
        h.state
            .assert_actions(k, &[(true, false), (false, true), (false, false)]);
        h.shutdown().await;
    }
}

#[tokio::test]
async fn successful_unsubscribe_precedes_new_follower_subscribe() {
    // Property: a new subscriber reestablishes upstream state after pending last-owner teardown.
    for k in 0..KEYS.len() {
        let h = Harness::new().await;
        h.service
            .subscribe("first", &uri(k))
            .await
            .expect("initial subscribe");
        h.state.wait_entered().await;
        h.state.pause.store(true, Ordering::SeqCst);
        let service = h.service.clone();
        let request = tokio::spawn(async move { service.unsubscribe("first", &uri(k)).await });
        h.state.wait_entered().await;
        let name = uri(k);
        let mut follower = Box::pin(h.service.subscribe("second", &name));
        assert_pending(follower.as_mut()).await;
        h.state.resume();
        bounded(request)
            .await
            .expect("unsubscribe task")
            .expect("unsubscribe succeeds");
        bounded(follower).await.expect("follower succeeds");
        h.assert_members(k, &["second"]);
        h.state.assert_active(k, true);
        h.state
            .assert_actions(k, &[(true, false), (false, false), (true, false)]);
        h.shutdown().await;
    }
}

#[tokio::test]
async fn cancelled_unsubscribe_finishes_success_or_rollback_before_retry() {
    // Property: caller cancellation preserves both successful teardown and rejected teardown rollback.
    for k in 0..KEYS.len() {
        for fail in [false, true] {
            let h = Harness::new().await;
            h.service
                .subscribe("s", &uri(k))
                .await
                .expect("initial subscribe");
            h.state.wait_entered().await;
            h.state.fail.store(fail, Ordering::SeqCst);
            h.state.pause.store(true, Ordering::SeqCst);
            let service = h.service.clone();
            let request = tokio::spawn(async move { service.unsubscribe("s", &uri(k)).await });
            h.state.wait_entered().await;
            request.abort();
            assert!(request.await.expect_err("aborted caller").is_cancelled());
            h.state.resume();
            bounded(h.service.unsubscribe("s", &uri(k)))
                .await
                .expect("unsubscribe retry");
            h.assert_members(k, &[]);
            h.state.assert_active(k, false);
            if fail {
                h.state
                    .assert_actions(k, &[(true, false), (false, true), (false, false)]);
            } else {
                h.state.assert_actions(k, &[(true, false), (false, false)]);
            }
            h.shutdown().await;
        }
    }
}
