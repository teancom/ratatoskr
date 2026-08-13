// pattern: Imperative Shell
//
// Broadcast channel that fans upstream events out to downstream sessions.
//
// `tokio::sync::broadcast` is the right fit here: one producer per upstream,
// many consumers (one per downstream session), bounded capacity so a slow
// consumer can't OOM us. Lag is reported back to the consumer as a specific
// error variant so they can react (most likely: log + continue).

use tokio::sync::broadcast;

use crate::events::UpstreamEvent;

/// Bounded broadcast capacity per event bus. At 256, even a spectacularly
/// chatty upstream emitting list_changed every 10ms gives consumers ~2.5s
/// of catch-up room. Consumers that fall further behind see `RecvError::Lagged`
/// — which for our use case is recoverable: re-subscribing is cheap.
const BUS_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
pub struct UpstreamEventBus {
    tx: broadcast::Sender<UpstreamEvent>,
}

impl UpstreamEventBus {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(BUS_CAPACITY);
        Self { tx }
    }

    /// Publish an event. Returns the number of active subscribers that
    /// received it. Not an error if zero: during early boot no downstream
    /// session has subscribed yet.
    pub fn publish(&self, event: UpstreamEvent) -> usize {
        // send() errors only when zero receivers exist; not an error for us.
        self.tx.send(event).unwrap_or(0)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<UpstreamEvent> {
        self.tx.subscribe()
    }
}

impl Default for UpstreamEventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "event_bus_tests.rs"]
mod tests;
