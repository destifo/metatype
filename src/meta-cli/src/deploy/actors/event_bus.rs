// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

use crate::interlude::*;
use actix::WeakRecipient;
use std::any::Any;
use std::time::SystemTime;

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
}

impl EventEnvelope {
    pub fn new(event: Arc<dyn Event>, id: EventId) -> Self {
        Self { id, event }
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
}

pub type EventFilter = Arc<dyn Fn(&EventEnvelope) -> bool + Send + Sync>;

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

struct Subscription {
    id: SubscriptionId,
    recipient: WeakRecipient<EventEnvelope>,
    filter: Option<EventFilter>,
}

pub struct EventBus {
    subscriptions: HashMap<EventType, Vec<Subscription>>,
    next_id: u64,
    next_event_id: u64,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
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
    type Result = SubscriptionId;

    fn handle(&mut self, msg: EventSubscription, _ctx: &mut Context<Self>) -> Self::Result {
        let id = self.next_subscription_id();
        let EventSubscription {
            event_type,
            recipient,
            filter,
        } = msg;

        let entry = self.subscriptions.entry(event_type).or_default();
        entry.push(Subscription {
            id,
            recipient: recipient.downgrade(),
            filter,
        });

        trace!("event bus subscribed: type={} id={}", event_type, id.0);

        id
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
    }
}

impl Handler<PublishEvent> for EventBus {
    type Result = ();

    fn handle(&mut self, msg: PublishEvent, ctx: &mut Context<Self>) -> Self::Result {
        let event_type = msg.event.event_type();
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

        let event_id = self.next_event_id();
        let base_envelope = EventEnvelope::new(msg.event.clone(), event_id);
        for sub in subscribers.iter() {
            let Some(recipient) = sub.recipient.upgrade() else {
                continue;
            };

            if let Some(filter) = &sub.filter {
                if !(filter)(&base_envelope) {
                    continue;
                }
            }

            let envelope = base_envelope.clone();
            let sub_id = sub.id;
            let event_id = event_id;
            let bus_addr = ctx.address();
            let fut = async move {
                match recipient.send(envelope).await {
                    Ok(ack) => {
                        if ack.event_id() != event_id {
                            warn!(
                                "event bus ack mismatch: type={} expected={} got={}",
                                event_type, event_id.0, ack.event_id().0
                            );
                        }
                    }
                    Err(err) => {
                        warn!(
                            "event bus delivery failed: type={} id={} err={}",
                            event_type, event_id.0, err
                        );
                        bus_addr.do_send(EventUnsubscribe { id: sub_id });
                    }
                }
            };
            ctx.spawn(fut.into_actor(self));
        }
    }
}

pub trait EventBusExt {
    fn publish<E: Event>(&self, event: E);
    fn publish_envelope(&self, event: EventEnvelope);
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
}
