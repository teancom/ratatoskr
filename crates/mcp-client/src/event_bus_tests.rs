use super::*;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;

use crate::events::UpstreamEvent;

#[tokio::test]
async fn slow_consumer_receives_lagged_error_when_ring_buffer_overflows() {
    // Property: a receiver that does not drain will see RecvError::Lagged
    // once the publisher has written past the ring-buffer capacity. This
    // locks in the contract that the bus uses a *lossy* channel that reports
    // lag rather than blocking the publisher or silently discarding without
    // notification. If BUS_CAPACITY or the channel type ever changes in a
    // way that removes the lag signal, this test trips immediately.
    let bus = UpstreamEventBus::new();
    let mut rx = bus.subscribe();

    // Publish one more than the ring-buffer can hold without draining.
    for i in 0..=BUS_CAPACITY {
        bus.publish(UpstreamEvent::ToolListChanged {
            upstream: format!("u{i}"),
        });
    }

    let result = rx.recv().await;
    match result {
        Err(RecvError::Lagged(_)) => {} // expected: lag was reported
        other => panic!("expected RecvError::Lagged, got {other:?}"),
    }
}

#[tokio::test]
async fn publish_fans_out_to_multiple_subscribers() {
    let bus = UpstreamEventBus::new();
    let mut rx1 = bus.subscribe();
    let mut rx2 = bus.subscribe();

    let sent = bus.publish(UpstreamEvent::ToolListChanged {
        upstream: "u1".to_string(),
    });
    assert_eq!(sent, 2, "both subscribers should receive");

    let e1 = tokio::time::timeout(Duration::from_millis(100), rx1.recv())
        .await
        .expect("rx1 didn't receive in time")
        .expect("rx1 channel closed");
    let e2 = tokio::time::timeout(Duration::from_millis(100), rx2.recv())
        .await
        .expect("rx2 didn't receive in time")
        .expect("rx2 channel closed");

    assert_eq!(e1, e2);
    assert_eq!(e1.upstream_name(), "u1");
}

#[tokio::test]
async fn publish_with_no_subscribers_does_not_error() {
    let bus = UpstreamEventBus::new();
    let sent = bus.publish(UpstreamEvent::ResourceListChanged {
        upstream: "ghost".to_string(),
    });
    assert_eq!(sent, 0);
}

#[tokio::test]
async fn unsubscribed_receiver_no_longer_counted() {
    // Property: unsubscribing (dropping the receiver) removes the
    // listener from the next publish's fan-out count. Guards against
    // memory leaks if sessions churn.
    let bus = UpstreamEventBus::new();
    let rx = bus.subscribe();
    drop(rx);
    let sent = bus.publish(UpstreamEvent::PromptListChanged {
        upstream: "x".to_string(),
    });
    assert_eq!(sent, 0);
}

#[tokio::test]
async fn resource_updated_round_trip_preserves_both_fields() {
    // Property: the broadcast channel preserves the full payload of the
    // two-field ResourceUpdated variant. Both `upstream` and `uri` must
    // survive the send/receive cycle unchanged. Guards against a future
    // refactor of UpstreamEvent that accidentally strips or renames the
    // `uri` field (the fan-out logic in session_forwarder uses it to
    // filter subscriptions, so silent field-loss would silently break
    // resource-update routing).
    let bus = UpstreamEventBus::new();
    let mut rx = bus.subscribe();

    bus.publish(UpstreamEvent::ResourceUpdated {
        upstream: "s".to_string(),
        uri: "incident://42".to_string(),
    });

    let event = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("recv timed out")
        .expect("channel closed");

    match event {
        UpstreamEvent::ResourceUpdated { upstream, uri } => {
            assert_eq!(upstream, "s");
            assert_eq!(uri, "incident://42");
        }
        other => panic!("expected ResourceUpdated, got {other:?}"),
    }
}
