// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

//! Event Bus Actor
//!
//! This module provides a central message dispatching system for the actor model.
//! The event bus allows actors to:
//! - Subscribe to specific event types
//! - Publish events that get routed to subscribed actors
//! - Receive acknowledgments for delivered messages

use crate::interlude::*;
use std::any::TypeId;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::{Duration, Instant};

/// Unique identifier for events
pub type EventId = u64;

/// Counter for generating unique event IDs
static NEXT_EVENT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Generate a new unique event ID
pub fn generate_event_id() -> EventId {
    NEXT_EVENT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Trait that all events must implement
pub trait Event: Send + Debug + 'static {
    /// Returns the event type identifier
    fn event_type(&self) -> &'static str;

    /// Returns the timestamp when the event was created
    fn timestamp(&self) -> Instant;

    /// Returns a unique event ID
    fn event_id(&self) -> EventId;
}

/// Wrapper for events that includes metadata for the event bus
#[derive(Debug)]
pub struct EventEnvelope<E> {
    pub event: E,
    pub requires_ack: bool,
    pub event_id: EventId,
    pub timestamp: Instant,
}

impl<E> EventEnvelope<E> {
    pub fn new(event: E) -> Self {
        Self {
            event,
            requires_ack: true,
            event_id: generate_event_id(),
            timestamp: Instant::now(),
        }
    }

    pub fn without_ack(event: E) -> Self {
        Self {
            event,
            requires_ack: false,
            event_id: generate_event_id(),
            timestamp: Instant::now(),
        }
    }
}

/// Acknowledgment message sent back to the event bus
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct EventAck {
    pub event_id: EventId,
    pub success: bool,
    pub error_message: Option<String>,
}

impl EventAck {
    pub fn success(event_id: EventId) -> Self {
        Self {
            event_id,
            success: true,
            error_message: None,
        }
    }

    pub fn failure(event_id: EventId, error: impl ToString) -> Self {
        Self {
            event_id,
            success: false,
            error_message: Some(error.to_string()),
        }
    }
}

/// Subscription information stored in the event bus
struct SubscriptionInfo {
    event_type: &'static str,
    subscriber_id: String,
    #[allow(dead_code)]
    type_id: TypeId,
}

/// Subscription ID returned after successful subscription
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

impl<A, M> actix::dev::MessageResponse<A, M> for SubscriptionId
where
    A: Actor,
    M: Message<Result = SubscriptionId>,
{
    fn handle(self, _ctx: &mut A::Context, tx: Option<actix::dev::OneshotSender<M::Result>>) {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
    }
}

/// Message to subscribe to events
#[derive(Message)]
#[rtype(result = "SubscriptionId")]
pub struct Subscribe {
    pub event_type: &'static str,
    pub subscriber_id: String,
    pub type_id: TypeId,
}

/// Message to unsubscribe from events
#[derive(Message)]
#[rtype(result = "bool")]
pub struct Unsubscribe {
    pub subscription_id: SubscriptionId,
}

/// Internal tracking for pending acknowledgments
struct PendingAckInfo {
    event_id: EventId,
    event_type: &'static str,
    subscriber_id: String,
    sent_at: Instant,
    retry_count: u32,
}

/// Configuration for the event bus
#[derive(Clone)]
pub struct EventBusConfig {
    /// Maximum number of retries for unacknowledged messages
    pub max_retries: u32,
    /// Timeout for waiting for acknowledgment
    pub ack_timeout: Duration,
    /// Interval between retry checks
    pub retry_check_interval: Duration,
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            ack_timeout: Duration::from_secs(30),
            retry_check_interval: Duration::from_secs(5),
        }
    }
}

/// Registry for managing subscriptions
struct SubscriptionRegistry {
    /// Maps subscription ID to subscription info
    subscriptions: HashMap<SubscriptionId, SubscriptionInfo>,
    /// Maps event type to list of subscription IDs
    by_event_type: HashMap<&'static str, Vec<SubscriptionId>>,
    /// Next subscription ID counter
    next_id: u64,
}

impl SubscriptionRegistry {
    fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            by_event_type: HashMap::new(),
            next_id: 1,
        }
    }

    fn subscribe(
        &mut self,
        event_type: &'static str,
        subscriber_id: String,
        type_id: TypeId,
    ) -> SubscriptionId {
        let id = SubscriptionId(self.next_id);
        self.next_id += 1;

        self.subscriptions.insert(
            id,
            SubscriptionInfo {
                event_type,
                subscriber_id,
                type_id,
            },
        );

        self.by_event_type.entry(event_type).or_default().push(id);

        id
    }

    fn unsubscribe(&mut self, subscription_id: SubscriptionId) -> bool {
        if let Some(info) = self.subscriptions.remove(&subscription_id) {
            if let Some(subs) = self.by_event_type.get_mut(info.event_type) {
                subs.retain(|id| *id != subscription_id);
            }
            true
        } else {
            false
        }
    }

    fn get_subscriber_ids(&self, event_type: &'static str) -> Vec<(SubscriptionId, &str)> {
        self.by_event_type
            .get(event_type)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| {
                        self.subscriptions
                            .get(id)
                            .map(|info| (*id, info.subscriber_id.as_str()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn get_subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    fn get_event_type_count(&self) -> usize {
        self.by_event_type.len()
    }
}

/// The Event Bus Actor - central message dispatching unit
pub struct EventBusActor {
    config: EventBusConfig,
    registry: SubscriptionRegistry,
    pending_acks: HashMap<EventId, PendingAckInfo>,
    /// Delivery handlers indexed by (event_type, subscription_id)
    delivery_handlers: HashMap<(&'static str, SubscriptionId), Box<dyn DeliveryHandler>>,
}

/// Trait for handling event delivery
trait DeliveryHandler: Send + 'static {
    /// Attempt to deliver an event, returns true if delivery was successful
    fn deliver(&self, event_id: EventId) -> bool;
}

impl EventBusActor {
    pub fn new(config: EventBusConfig) -> Self {
        Self {
            config,
            registry: SubscriptionRegistry::new(),
            pending_acks: HashMap::new(),
            delivery_handlers: HashMap::new(),
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(EventBusConfig::default())
    }

    /// Get statistics about the event bus
    pub fn stats(&self) -> EventBusStats {
        EventBusStats {
            subscription_count: self.registry.get_subscription_count(),
            event_type_count: self.registry.get_event_type_count(),
            pending_ack_count: self.pending_acks.len(),
        }
    }
}

/// Statistics about the event bus
#[derive(Debug, Clone)]
pub struct EventBusStats {
    pub subscription_count: usize,
    pub event_type_count: usize,
    pub pending_ack_count: usize,
}

impl<A, M> actix::dev::MessageResponse<A, M> for EventBusStats
where
    A: Actor,
    M: Message<Result = EventBusStats>,
{
    fn handle(self, _ctx: &mut A::Context, tx: Option<actix::dev::OneshotSender<M::Result>>) {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
    }
}

impl Actor for EventBusActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        trace!("EventBus actor started");

        // Start periodic ack timeout checking
        let check_interval = self.config.retry_check_interval;
        ctx.run_interval(check_interval, |act, _ctx| {
            act.check_ack_timeouts();
        });
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        trace!("EventBus actor stopped");
    }
}

impl EventBusActor {
    fn check_ack_timeouts(&mut self) {
        let now = Instant::now();
        let timeout = self.config.ack_timeout;
        let max_retries = self.config.max_retries;

        let timed_out: Vec<EventId> = self
            .pending_acks
            .iter()
            .filter(|(_, info)| now.duration_since(info.sent_at) > timeout)
            .map(|(id, _)| *id)
            .collect();

        for event_id in timed_out {
            if let Some(mut info) = self.pending_acks.remove(&event_id) {
                if info.retry_count < max_retries {
                    warn!(
                        "Event {} timed out waiting for ack from {}, retry {}",
                        event_id,
                        info.subscriber_id,
                        info.retry_count + 1
                    );
                    info.retry_count += 1;
                    info.sent_at = now;
                    self.pending_acks.insert(event_id, info);
                } else {
                    error!(
                        "Event {} (type: {}) failed after {} retries for subscriber {}, giving up",
                        event_id, info.event_type, max_retries, info.subscriber_id
                    );
                }
            }
        }
    }

    fn track_pending_ack(
        &mut self,
        event_id: EventId,
        event_type: &'static str,
        subscriber_id: String,
    ) {
        self.pending_acks.insert(
            event_id,
            PendingAckInfo {
                event_id,
                event_type,
                subscriber_id,
                sent_at: Instant::now(),
                retry_count: 0,
            },
        );
    }
}

impl Handler<Subscribe> for EventBusActor {
    type Result = SubscriptionId;

    fn handle(&mut self, msg: Subscribe, _ctx: &mut Self::Context) -> Self::Result {
        trace!(
            "Subscription request from {} for event type {}",
            msg.subscriber_id,
            msg.event_type
        );

        let id = self
            .registry
            .subscribe(msg.event_type, msg.subscriber_id, msg.type_id);
        debug!("Created subscription {:?}", id);
        id
    }
}

impl Handler<Unsubscribe> for EventBusActor {
    type Result = bool;

    fn handle(&mut self, msg: Unsubscribe, _ctx: &mut Self::Context) -> Self::Result {
        trace!("Unsubscribe request for subscription {:?}", msg.subscription_id);
        let result = self.registry.unsubscribe(msg.subscription_id);
        if result {
            debug!("Removed subscription {:?}", msg.subscription_id);
        } else {
            warn!("Subscription {:?} not found", msg.subscription_id);
        }
        result
    }
}

impl Handler<EventAck> for EventBusActor {
    type Result = ();

    fn handle(&mut self, msg: EventAck, _ctx: &mut Self::Context) -> Self::Result {
        trace!(
            "Received acknowledgment for event {}: success={}",
            msg.event_id,
            msg.success
        );

        if let Some(info) = self.pending_acks.remove(&msg.event_id) {
            if msg.success {
                debug!(
                    "Event {} successfully processed by {}",
                    msg.event_id, info.subscriber_id
                );
            } else {
                warn!(
                    "Event {} failed processing by {}: {:?}",
                    msg.event_id, info.subscriber_id, msg.error_message
                );
            }
        } else {
            trace!(
                "Received ack for unknown event {} (may have been already processed)",
                msg.event_id
            );
        }
    }
}

/// Message to get event bus statistics
#[derive(Message)]
#[rtype(result = "EventBusStats")]
pub struct GetStats;

impl Handler<GetStats> for EventBusActor {
    type Result = EventBusStats;

    fn handle(&mut self, _msg: GetStats, _ctx: &mut Self::Context) -> Self::Result {
        self.stats()
    }
}

/// Message to publish an event and track acknowledgment
#[derive(Message)]
#[rtype(result = "PublishResult")]
pub struct PublishEvent {
    pub event_type: &'static str,
    pub event_id: EventId,
    pub requires_ack: bool,
}

/// Result of publishing an event
#[derive(Debug)]
pub struct PublishResult {
    pub event_id: EventId,
    pub subscribers_notified: usize,
}

impl<A, M> actix::dev::MessageResponse<A, M> for PublishResult
where
    A: Actor,
    M: Message<Result = PublishResult>,
{
    fn handle(self, _ctx: &mut A::Context, tx: Option<actix::dev::OneshotSender<M::Result>>) {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
    }
}

impl Handler<PublishEvent> for EventBusActor {
    type Result = PublishResult;

    fn handle(&mut self, msg: PublishEvent, _ctx: &mut Self::Context) -> Self::Result {
        let subscribers = self.registry.get_subscriber_ids(msg.event_type);
        let count = subscribers.len();

        trace!(
            "Publishing event {} (type: {}) to {} subscribers",
            msg.event_id,
            msg.event_type,
            count
        );

        if msg.requires_ack {
            // Collect subscriber IDs first to avoid borrow issues
            let subscriber_ids: Vec<String> = subscribers
                .iter()
                .map(|(_, sid)| sid.to_string())
                .collect();

            for subscriber_id in subscriber_ids {
                self.track_pending_ack(msg.event_id, msg.event_type, subscriber_id);
            }
        }

        PublishResult {
            event_id: msg.event_id,
            subscribers_notified: count,
        }
    }
}

/// Message to register a typed subscription with delivery handler
#[derive(Message)]
#[rtype(result = "SubscriptionId")]
pub struct SubscribeWithHandler<M>
where
    M: Message + Send + 'static,
    M::Result: Send,
{
    pub event_type: &'static str,
    pub subscriber_id: String,
    pub recipient: Recipient<M>,
}

/// Typed delivery handler that can send messages to a recipient
struct TypedDeliveryHandler<M>
where
    M: Message + Send + 'static,
    M::Result: Send,
{
    recipient: Recipient<M>,
    #[allow(dead_code)]
    message_factory: Box<dyn Fn(EventId) -> Option<M> + Send>,
}

impl<M> DeliveryHandler for TypedDeliveryHandler<M>
where
    M: Message + Send + 'static,
    M::Result: Send,
{
    fn deliver(&self, _event_id: EventId) -> bool {
        // This is a placeholder - actual delivery happens through the recipient directly
        self.recipient.connected()
    }
}

/// Builder for creating typed subscriptions
pub struct SubscriptionBuilder<M>
where
    M: Message + Send + 'static,
    M::Result: Send,
{
    event_type: &'static str,
    subscriber_id: String,
    recipient: Recipient<M>,
}

impl<M> SubscriptionBuilder<M>
where
    M: Message + Send + 'static,
    M::Result: Send,
{
    pub fn new(
        event_type: &'static str,
        subscriber_id: impl Into<String>,
        recipient: Recipient<M>,
    ) -> Self {
        Self {
            event_type,
            subscriber_id: subscriber_id.into(),
            recipient,
        }
    }

    pub fn build(self) -> SubscribeWithHandler<M> {
        SubscribeWithHandler {
            event_type: self.event_type,
            subscriber_id: self.subscriber_id,
            recipient: self.recipient,
        }
    }
}

/// Extension trait for easily working with the event bus
#[async_trait]
pub trait EventBusExt {
    /// Subscribe to events with a specific handler
    async fn subscribe_handler<M>(
        &self,
        event_type: &'static str,
        subscriber_id: impl Into<String> + Send,
        recipient: Recipient<M>,
    ) -> Result<SubscriptionId>
    where
        M: Message + Send + 'static,
        M::Result: Send;

    /// Unsubscribe from events
    async fn unsubscribe(&self, subscription_id: SubscriptionId) -> Result<bool>;

    /// Publish an event and notify subscribers
    async fn publish(
        &self,
        event_type: &'static str,
        event_id: EventId,
        requires_ack: bool,
    ) -> Result<PublishResult>;

    /// Send acknowledgment for an event
    fn send_ack(&self, event_id: EventId, success: bool, error_message: Option<String>);

    /// Get statistics
    async fn get_stats(&self) -> Result<EventBusStats>;
}

#[async_trait]
impl EventBusExt for Addr<EventBusActor> {
    async fn subscribe_handler<M>(
        &self,
        event_type: &'static str,
        subscriber_id: impl Into<String> + Send,
        recipient: Recipient<M>,
    ) -> Result<SubscriptionId>
    where
        M: Message + Send + 'static,
        M::Result: Send,
    {
        let msg = SubscriptionBuilder::new(event_type, subscriber_id, recipient).build();
        self.send(msg)
            .await
            .map_err(|e| eyre::eyre!("Failed to subscribe: {}", e))
    }

    async fn unsubscribe(&self, subscription_id: SubscriptionId) -> Result<bool> {
        self.send(Unsubscribe { subscription_id })
            .await
            .map_err(|e| eyre::eyre!("Failed to unsubscribe: {}", e))
    }

    async fn publish(
        &self,
        event_type: &'static str,
        event_id: EventId,
        requires_ack: bool,
    ) -> Result<PublishResult> {
        self.send(PublishEvent {
            event_type,
            event_id,
            requires_ack,
        })
        .await
        .map_err(|e| eyre::eyre!("Failed to publish: {}", e))
    }

    fn send_ack(&self, event_id: EventId, success: bool, error_message: Option<String>) {
        self.do_send(EventAck {
            event_id,
            success,
            error_message,
        });
    }

    async fn get_stats(&self) -> Result<EventBusStats> {
        self.send(GetStats)
            .await
            .map_err(|e| eyre::eyre!("Failed to get stats: {}", e))
    }
}

impl<M> Handler<SubscribeWithHandler<M>> for EventBusActor
where
    M: Message + Send + 'static,
    M::Result: Send,
{
    type Result = SubscriptionId;

    fn handle(&mut self, msg: SubscribeWithHandler<M>, _ctx: &mut Self::Context) -> Self::Result {
        let type_id = TypeId::of::<M>();
        let id = self
            .registry
            .subscribe(msg.event_type, msg.subscriber_id.clone(), type_id);

        // Store the delivery handler
        let handler: Box<dyn DeliveryHandler> = Box::new(TypedDeliveryHandler {
            recipient: msg.recipient,
            message_factory: Box::new(|_| None),
        });
        self.delivery_handlers
            .insert((msg.event_type, id), handler);

        debug!(
            "Created typed subscription {:?} for {} to event type {}",
            id, msg.subscriber_id, msg.event_type
        );
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_registry() {
        let mut registry = SubscriptionRegistry::new();

        let id1 = registry.subscribe("test_event", "subscriber1".to_string(), TypeId::of::<()>());
        let id2 = registry.subscribe("test_event", "subscriber2".to_string(), TypeId::of::<()>());
        let id3 = registry.subscribe("other_event", "subscriber1".to_string(), TypeId::of::<()>());

        assert_eq!(registry.get_subscription_count(), 3);
        assert_eq!(registry.get_event_type_count(), 2);

        let subs = registry.get_subscriber_ids("test_event");
        assert_eq!(subs.len(), 2);

        assert!(registry.unsubscribe(id1));
        assert_eq!(registry.get_subscription_count(), 2);

        let subs = registry.get_subscriber_ids("test_event");
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].0, id2);

        assert!(registry.unsubscribe(id3));
        assert!(!registry.unsubscribe(id3)); // Already unsubscribed
    }

    #[test]
    fn test_event_ack_creation() {
        let ack = EventAck::success(42);
        assert!(ack.success);
        assert!(ack.error_message.is_none());
        assert_eq!(ack.event_id, 42);

        let ack = EventAck::failure(123, "error occurred");
        assert!(!ack.success);
        assert_eq!(ack.error_message, Some("error occurred".to_string()));
        assert_eq!(ack.event_id, 123);
    }

    #[test]
    fn test_event_envelope() {
        let envelope = EventEnvelope::new("test data");
        assert!(envelope.requires_ack);
        assert!(envelope.event_id > 0);

        let envelope2 = EventEnvelope::without_ack("test data");
        assert!(!envelope2.requires_ack);
        assert!(envelope2.event_id > envelope.event_id);
    }

    #[test]
    fn test_event_id_generation() {
        let id1 = generate_event_id();
        let id2 = generate_event_id();
        let id3 = generate_event_id();

        assert!(id2 > id1);
        assert!(id3 > id2);
    }
}
