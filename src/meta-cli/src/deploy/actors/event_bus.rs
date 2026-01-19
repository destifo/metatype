// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

use crate::interlude::*;
use actix::{Message, MessageResult, WeakRecipient};
use std::any::Any;
use std::time::{Duration, SystemTime};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EventType(&'static str);

impl EventType {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct EventMetadata {
    pub timestamp: SystemTime,
    pub source: Option<String>,
}

impl EventMetadata {
    pub fn new(source: Option<String>) -> Self {
        Self {
            timestamp: SystemTime::now(),
            source,
        }
    }
}

pub trait Event: Send + Sync + std::fmt::Debug + 'static {
    fn event_type(&self) -> EventType;
    fn metadata(&self) -> &EventMetadata;
    fn timestamp(&self) -> SystemTime {
        self.metadata().timestamp
    }
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone, Message)]
#[rtype(result = "EventAck")]
pub struct EventEnvelope {
    id: EventId,
    event: Arc<dyn Event>,
    subscription_id: SubscriptionId,
}

impl EventEnvelope {
    pub fn new(event: Arc<dyn Event>, id: EventId, subscription_id: SubscriptionId) -> Self {
        Self {
            id,
            event,
            subscription_id,
        }
    }

    pub fn event_id(&self) -> EventId {
        self.id
    }

    pub fn event(&self) -> &dyn Event {
        self.event.as_ref()
    }

    pub fn event_arc(&self) -> Arc<dyn Event> {
        self.event.clone()
    }

    pub fn downcast_ref<E: Event>(&self) -> Option<&E> {
        self.event.as_any().downcast_ref::<E>()
    }

    pub fn subscription_id(&self) -> SubscriptionId {
        self.subscription_id
    }
}

pub type EventFilter = Arc<dyn Fn(&EventEnvelope) -> bool + Send + Sync>;

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            delay: Duration::from_secs(1),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EventId(u64);

#[derive(Clone, Debug)]
pub struct EventAck {
    event_id: EventId,
}

impl EventAck {
    pub fn new(event_id: EventId) -> Self {
        Self { event_id }
    }

    pub fn event_id(&self) -> EventId {
        self.event_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId(u64);

#[derive(Message)]
#[rtype(result = "SubscriptionId")]
pub struct EventSubscription {
    pub event_type: EventType,
    pub recipient: Recipient<EventEnvelope>,
    pub filter: Option<EventFilter>,
    pub retry_policy: RetryPolicy,
}

impl EventSubscription {
    pub fn new(
        event_type: EventType,
        recipient: Recipient<EventEnvelope>,
        filter: Option<EventFilter>,
    ) -> Self {
        Self {
            event_type,
            recipient,
            filter,
            retry_policy: RetryPolicy::default(),
        }
    }

    pub fn with_retry_policy(
        event_type: EventType,
        recipient: Recipient<EventEnvelope>,
        filter: Option<EventFilter>,
        retry_policy: RetryPolicy,
    ) -> Self {
        Self {
            event_type,
            recipient,
            filter,
            retry_policy,
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct EventUnsubscribe {
    pub id: SubscriptionId,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct PublishEvent {
    pub event: Arc<dyn Event>,
}

impl PublishEvent {
    pub fn new<E: Event>(event: E) -> Self {
        Self {
            event: Arc::new(event),
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ActorMessage {
    pub event: Arc<dyn Event>,
}

#[derive(Message)]
#[rtype(result = "()")]
struct RetryDelivery {
    subscription: Subscription,
    envelope: EventEnvelope,
    event_type: EventType,
    event_id: EventId,
    attempts: u32,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct DeliveryAck {
    pub subscription_id: SubscriptionId,
    pub event_id: EventId,
}

impl DeliveryAck {
    pub fn new(subscription_id: SubscriptionId, event_id: EventId) -> Self {
        Self {
            subscription_id,
            event_id,
        }
    }
}

#[derive(Clone)]
struct Subscription {
    id: SubscriptionId,
    recipient: WeakRecipient<EventEnvelope>,
    filter: Option<EventFilter>,
    retry_policy: RetryPolicy,
}

#[allow(dead_code)]
#[derive(Clone)]
struct PendingDelivery {
    subscription: Subscription,
    envelope: EventEnvelope,
    event_type: EventType,
    attempts: u32,
}

pub struct EventBus {
    subscriptions: HashMap<EventType, Vec<Subscription>>,
    pending_deliveries: HashMap<(SubscriptionId, EventId), PendingDelivery>,
    next_id: u64,
    next_event_id: u64,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            pending_deliveries: HashMap::new(),
            next_id: 1,
            next_event_id: 1,
        }
    }

    fn next_subscription_id(&mut self) -> SubscriptionId {
        let id = SubscriptionId(self.next_id);
        self.next_id += 1;
        id
    }

    fn next_event_id(&mut self) -> EventId {
        let id = EventId(self.next_event_id);
        self.next_event_id += 1;
        id
    }

    fn dispatch_to_subscription(
        &mut self,
        ctx: &mut Context<Self>,
        subscription: Subscription,
        envelope: EventEnvelope,
        event_type: EventType,
        event_id: EventId,
        attempts: u32,
    ) {
        if let Some(filter) = &subscription.filter {
            if !(filter)(&envelope) {
                return;
            }
        }

        let key = (subscription.id, event_id);
        self.pending_deliveries.insert(
            key,
            PendingDelivery {
                subscription: subscription.clone(),
                envelope: envelope.clone(),
                event_type,
                attempts,
            },
        );

        let Some(recipient) = subscription.recipient.upgrade() else {
            return;
        };

        let future_envelope = envelope.clone();
        let future_subscription = subscription.clone();
        let bus_addr = ctx.address();
        let fut = async move {
            match recipient.send(future_envelope.clone()).await {
                Ok(ack) => {
                    if ack.event_id() != event_id {
                        warn!(
                            "event bus ack mismatch: type={} expected={} got={}",
                            event_type,
                            event_id.0,
                            ack.event_id().0
                        );
                    }
                }
                Err(err) => {
                    warn!(
                        "event bus delivery failed: type={} id={} err={}",
                        event_type, event_id.0, err
                    );
                    bus_addr.do_send(RetryDelivery {
                        subscription: future_subscription.clone(),
                        envelope: future_envelope,
                        event_type,
                        event_id,
                        attempts,
                    });
                }
            }
        };
        ctx.spawn(fut.into_actor(self));

        let retry_msg = RetryDelivery {
            subscription: subscription.clone(),
            envelope: envelope.clone(),
            event_type,
            event_id,
            attempts,
        };
        ctx.notify_later(retry_msg, subscription.retry_policy.delay);
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for EventBus {
    type Context = Context<Self>;
}

impl Handler<EventSubscription> for EventBus {
    type Result = MessageResult<EventSubscription>;

    fn handle(&mut self, msg: EventSubscription, _ctx: &mut Context<Self>) -> Self::Result {
        let id = self.next_subscription_id();
        let EventSubscription {
            event_type,
            recipient,
            filter,
            retry_policy,
        } = msg;

        let entry = self.subscriptions.entry(event_type).or_default();
        entry.push(Subscription {
            id,
            recipient: recipient.downgrade(),
            filter,
            retry_policy,
        });

        trace!("event bus subscribed: type={} id={}", event_type, id.0);

        MessageResult(id)
    }
}

impl Handler<EventUnsubscribe> for EventBus {
    type Result = ();

    fn handle(&mut self, msg: EventUnsubscribe, _ctx: &mut Context<Self>) -> Self::Result {
        let mut removed = 0usize;
        for subscriptions in self.subscriptions.values_mut() {
            let before = subscriptions.len();
            subscriptions.retain(|sub| sub.id != msg.id);
            removed += before - subscriptions.len();
        }
        if removed > 0 {
            trace!("event bus unsubscribed: id={}", msg.id.0);
        }
        self.pending_deliveries
            .retain(|(sub_id, _), _| *sub_id != msg.id);
    }
}

impl Handler<PublishEvent> for EventBus {
    type Result = ();

    fn handle(&mut self, msg: PublishEvent, ctx: &mut Context<Self>) -> Self::Result {
        let event_type = msg.event.event_type();
        let subscribers_snapshot = {
            let Some(subscribers) = self.subscriptions.get_mut(&event_type) else {
                trace!("event bus publish: type={} subscribers=0", event_type);
                return;
            };

            subscribers.retain(|sub| sub.recipient.upgrade().is_some());
            trace!(
                "event bus publish: type={} subscribers={}",
                event_type,
                subscribers.len()
            );

            subscribers.clone()
        };

        let event_id = self.next_event_id();
        for sub in subscribers_snapshot.into_iter() {
            let envelope = EventEnvelope::new(msg.event.clone(), event_id, sub.id);
            self.dispatch_to_subscription(ctx, sub, envelope, event_type, event_id, 1);
        }
    }
}

impl Handler<RetryDelivery> for EventBus {
    type Result = ();

    fn handle(&mut self, msg: RetryDelivery, ctx: &mut Context<Self>) -> Self::Result {
        let key = (msg.subscription.id, msg.event_id);
        if !self.pending_deliveries.contains_key(&key) {
            return;
        }
        if msg.attempts >= msg.subscription.retry_policy.max_attempts {
            self.pending_deliveries.remove(&key);
            warn!(
                "event bus retry limit reached: type={} id={} attempts={}",
                msg.event_type, msg.event_id.0, msg.attempts
            );
            return;
        }

        self.dispatch_to_subscription(
            ctx,
            msg.subscription,
            msg.envelope,
            msg.event_type,
            msg.event_id,
            msg.attempts + 1,
        )
    }
}

impl Handler<DeliveryAck> for EventBus {
    type Result = ();

    fn handle(&mut self, msg: DeliveryAck, _ctx: &mut Context<Self>) -> Self::Result {
        self.pending_deliveries
            .remove(&(msg.subscription_id, msg.event_id));
    }
}

impl Handler<ActorMessage> for EventBus {
    type Result = ();

    fn handle(&mut self, msg: ActorMessage, ctx: &mut Context<Self>) -> Self::Result {
        self.handle(PublishEvent { event: msg.event }, ctx);
    }
}

pub trait EventBusExt {
    fn publish<E: Event>(&self, event: E);
    fn publish_envelope(&self, event: EventEnvelope);
    fn send_actor_message<E: Event>(&self, event: E);
    fn send_delivery_ack(&self, ack: DeliveryAck);
}

impl EventBusExt for Addr<EventBus> {
    fn publish<E: Event>(&self, event: E) {
        self.do_send(PublishEvent::new(event));
    }

    fn publish_envelope(&self, event: EventEnvelope) {
        self.do_send(PublishEvent {
            event: event.event_arc(),
        });
    }

    fn send_actor_message<E: Event>(&self, event: E) {
        self.do_send(ActorMessage {
            event: Arc::new(event),
        });
    }

    fn send_delivery_ack(&self, ack: DeliveryAck) {
        self.do_send(ack);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deploy::actors::events::{LogEvent, LogLevel, LOG_EVENT_TYPE};

    #[test]
    fn log_event_implements_event_trait() {
        let event = LogEvent::new(LogLevel::Debug, "msg".to_string(), None);
        assert_eq!(event.event_type(), LOG_EVENT_TYPE);
        let now = SystemTime::now();
        assert!(event.timestamp() <= now);
    }
}
