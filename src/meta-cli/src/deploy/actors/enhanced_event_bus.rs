// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

//! Enhanced Event Bus with configurable limits and retry logic
//!
//! Environment Variables:
//! - EVENT_BUS_MAX_QUEUE_SIZE: Maximum number of messages in queue (default: 10000)
//! - EVENT_BUS_MESSAGE_TIMEOUT_SECS: Message expiration timeout (default: 30)
//! - EVENT_BUS_ACK_TIMEOUT_SECS: Acknowledgment timeout (default: 5)
//! - EVENT_BUS_MAX_RETRIES: Maximum retry attempts (default: 3)
//! - EVENT_BUS_RETRY_DELAY_MS: Base retry delay in milliseconds (default: 1000)
//! - EVENT_BUS_EXPONENTIAL_BACKOFF: Enable exponential backoff (default: true)
//! - EVENT_BUS_ENABLE_PRIORITY_QUEUE: Enable priority queue (default: true)
//! - EVENT_BUS_ENABLE_MESSAGE_TRACING: Enable detailed tracing (default: false)

use super::event_bus::{Event, EventBusExt};
use super::message_system::{ActorId, Message, MessageFilter, MessageId, MessagePriority};
use super::unified_message::{EventToMessage, MessageEnvelope, MessageEnvelopeAck, UnifiedMessage};
use crate::interlude::*;
use actix::WeakRecipient;
use std::collections::{BinaryHeap, HashMap};
use std::cmp::Reverse;
use std::time::{Duration, Instant};
use std::env;

pub struct EnhancedEventBus {
    message_subscriptions: HashMap<String, Vec<MessageSubscription>>,
    message_queue: BinaryHeap<Reverse<PriorityMessage>>,
    routing_table: HashMap<ActorId, Vec<WeakRecipient<MessageEnvelope>>>,
    pending_acks: HashMap<MessageId, PendingMessage>,
    retry_queue: BinaryHeap<Reverse<RetryMessage>>,
    stats: EventBusStats,
    config: EventBusConfig,
}

#[derive(Clone)]
struct MessageSubscription {
    id: SubscriptionId,
    recipient: WeakRecipient<MessageEnvelope>,
    filter: Option<MessageFilter>,
    actor_id: ActorId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId(u64);

#[derive(Debug, Clone)]
struct PriorityMessage {
    message: MessageEnvelope,
    priority: MessagePriority,
    created_at: Instant,
}

#[derive(Debug, Clone)]
struct PendingMessage {
    message: MessageEnvelope,
    sent_at: Instant,
    retry_count: usize,
    recipient: WeakRecipient<MessageEnvelope>,
    subscription_id: SubscriptionId,
}

#[derive(Debug, Clone)]
struct RetryMessage {
    message: MessageEnvelope,
    retry_at: Instant,
    retry_count: usize,
    recipient: WeakRecipient<MessageEnvelope>,
    subscription_id: SubscriptionId,
    priority: MessagePriority,
}

impl PartialEq for PriorityMessage {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.created_at == other.created_at
    }
}

impl Eq for PriorityMessage {}

impl PartialOrd for PriorityMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match other.priority.cmp(&self.priority) {
            std::cmp::Ordering::Equal => self.created_at.cmp(&other.created_at),
            other_order => other_order,
        }
    }
}

impl PartialEq for RetryMessage {
    fn eq(&self, other: &Self) -> bool {
        self.retry_at == other.retry_at && self.priority == other.priority
    }
}

impl Eq for RetryMessage {}

impl PartialOrd for RetryMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RetryMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.retry_at.cmp(&other.retry_at) {
            std::cmp::Ordering::Equal => other.priority.cmp(&self.priority),
            time_order => time_order,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventBusStats {
    pub messages_sent: u64,
    pub messages_delivered: u64,
    pub messages_failed: u64,
    pub messages_expired: u64,
    pub messages_retried: u64,
    pub messages_acknowledged: u64,
    pub messages_timed_out: u64,
    pub active_subscriptions: usize,
    pub pending_acks: usize,
}

impl Default for EventBusStats {
    fn default() -> Self {
        Self {
            messages_sent: 0,
            messages_delivered: 0,
            messages_failed: 0,
            messages_expired: 0,
            messages_retried: 0,
            messages_acknowledged: 0,
            messages_timed_out: 0,
            active_subscriptions: 0,
            pending_acks: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventBusConfig {
    pub max_queue_size: usize,
    pub message_timeout: Duration,
    pub enable_priority_queue: bool,
    pub enable_message_tracing: bool,
    pub ack_timeout: Duration,
    pub max_retries: usize,
    pub retry_delay: Duration,
    pub exponential_backoff: bool,
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl EventBusConfig {
    pub fn from_env() -> Self {
        let max_queue_size = env::var("EVENT_BUS_MAX_QUEUE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10000);

        let message_timeout_secs = env::var("EVENT_BUS_MESSAGE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        let ack_timeout_secs = env::var("EVENT_BUS_ACK_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);

        let max_retries = env::var("EVENT_BUS_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);

        let retry_delay_ms = env::var("EVENT_BUS_RETRY_DELAY_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        let enable_priority_queue = env::var("EVENT_BUS_ENABLE_PRIORITY_QUEUE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(true);

        let enable_message_tracing = env::var("EVENT_BUS_ENABLE_MESSAGE_TRACING")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(false);

        let exponential_backoff = env::var("EVENT_BUS_EXPONENTIAL_BACKOFF")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(true);

        Self {
            max_queue_size,
            message_timeout: Duration::from_secs(message_timeout_secs),
            enable_priority_queue,
            enable_message_tracing,
            ack_timeout: Duration::from_secs(ack_timeout_secs),
            max_retries,
            retry_delay: Duration::from_millis(retry_delay_ms),
            exponential_backoff,
        }
    }
}

impl EnhancedEventBus {
    pub fn new() -> Self {
        Self::with_config(EventBusConfig::default())
    }

    pub fn with_config(config: EventBusConfig) -> Self {
        Self {
            message_subscriptions: HashMap::new(),
            message_queue: BinaryHeap::new(),
            routing_table: HashMap::new(),
            pending_acks: HashMap::new(),
            retry_queue: BinaryHeap::new(),
            stats: EventBusStats::default(),
            config,
        }
    }

    pub fn subscribe_to_messages(
        &mut self,
        message_type: String,
        actor_id: ActorId,
        recipient: Recipient<MessageEnvelope>,
        filter: Option<MessageFilter>,
    ) -> SubscriptionId {
        let id = SubscriptionId(self.stats.messages_sent + 1);
        
        let subscription = MessageSubscription {
            id,
            recipient: recipient.downgrade(),
            filter,
            actor_id: actor_id.clone(),
        };

        self.message_subscriptions
            .entry(message_type)
            .or_default()
            .push(subscription);

        self.routing_table
            .entry(actor_id)
            .or_default()
            .push(recipient.downgrade());

        self.stats.active_subscriptions += 1;
        id
    }

    pub fn unsubscribe(&mut self, subscription_id: SubscriptionId) {
        for subscriptions in self.message_subscriptions.values_mut() {
            let before = subscriptions.len();
            subscriptions.retain(|sub| sub.id.0 != subscription_id.0);
            if subscriptions.len() < before {
                self.stats.active_subscriptions -= 1;
                break;
            }
        }
    }

    pub fn publish_message<M: Message>(&mut self, message: M) -> Result<(), String> {
        if self.message_queue.len() >= self.config.max_queue_size {
            return Err("Message queue is full".to_string());
        }

        let envelope = MessageEnvelope::new(message);
        let priority_msg = PriorityMessage {
            priority: envelope.message().priority(),
            created_at: Instant::now(),
            message: envelope,
        };

        if self.config.enable_priority_queue {
            self.message_queue.push(Reverse(priority_msg));
        } else {
            self.process_message(priority_msg);
        }

        self.stats.messages_sent += 1;
        Ok(())
    }

    pub fn publish_event<E: Event>(&mut self, event: E, sender: ActorId, receiver: ActorId) -> Result<(), String> {
        let unified = event.to_unified_message(sender, receiver);
        self.publish_message(unified)
    }

    pub fn process_queued_messages(&mut self) {
        while let Some(Reverse(priority_msg)) = self.message_queue.pop() {
            if let Some(timeout) = priority_msg.message.message().timeout() {
                if priority_msg.created_at.elapsed() > timeout {
                    self.stats.messages_expired += 1;
                    continue;
                }
            }

            self.process_message(priority_msg);
        }
    }

    pub fn process_retry_queue(&mut self) {
        let now = Instant::now();
        
        while let Some(Reverse(retry_msg)) = self.retry_queue.peek() {
            if retry_msg.retry_at > now {
                break;
            }
            
            let retry_msg = self.retry_queue.pop().unwrap().0;
            
            let Some(recipient) = retry_msg.recipient.upgrade() else {
                continue;
            };

            match recipient.try_send(retry_msg.message.clone()) {
                Ok(()) => {
                    self.stats.messages_retried += 1;
                    
                    let pending_msg = PendingMessage {
                        message: retry_msg.message.clone(),
                        sent_at: now,
                        retry_count: retry_msg.retry_count,
                        recipient: retry_msg.recipient,
                        subscription_id: retry_msg.subscription_id,
                    };
                    self.pending_acks.insert(retry_msg.message.message_id(), pending_msg);
                    
                    if self.config.enable_message_tracing {
                        trace!(
                            "Message {} retried (attempt {})",
                            retry_msg.message.message_id(),
                            retry_msg.retry_count + 1
                        );
                    }
                }
                Err(err) => {
                    if retry_msg.retry_count >= self.config.max_retries {
                        self.stats.messages_failed += 1;
                        warn!(
                            "Message {} failed after {} retries: {}",
                            retry_msg.message.message_id(),
                            retry_msg.retry_count,
                            err
                        );
                    } else {
                        self.schedule_retry(
                            retry_msg.message,
                            retry_msg.retry_count + 1,
                            retry_msg.recipient,
                            retry_msg.subscription_id,
                        );
                    }
                }
            }
        }
    }

    pub fn check_pending_acks(&mut self) {
        let now = Instant::now();
        let mut timed_out_messages = Vec::new();

        for (message_id, pending_msg) in &self.pending_acks {
            if now.duration_since(pending_msg.sent_at) > self.config.ack_timeout {
                timed_out_messages.push(*message_id);
            }
        }

        for message_id in timed_out_messages {
            if let Some(pending_msg) = self.pending_acks.remove(&message_id) {
                self.stats.messages_timed_out += 1;
                
                if pending_msg.retry_count < self.config.max_retries {
                    self.schedule_retry(
                        pending_msg.message,
                        pending_msg.retry_count + 1,
                        pending_msg.recipient,
                        pending_msg.subscription_id,
                    );
                } else {
                    self.stats.messages_failed += 1;
                    warn!(
                        "Message {} failed after {} retries (ack timeout)",
                        message_id,
                        pending_msg.retry_count
                    );
                }
            }
        }

        self.stats.pending_acks = self.pending_acks.len();
    }

    fn schedule_retry(
        &mut self,
        message: MessageEnvelope,
        retry_count: usize,
        recipient: WeakRecipient<MessageEnvelope>,
        subscription_id: SubscriptionId,
    ) {
        let delay = if self.config.exponential_backoff {
            self.config.retry_delay * (2_u32.pow(retry_count as u32 - 1))
        } else {
            self.config.retry_delay
        };

        let retry_msg = RetryMessage {
            message: message.clone(),
            retry_at: Instant::now() + delay,
            retry_count,
            recipient,
            subscription_id,
            priority: message.message().priority(),
        };

        self.retry_queue.push(Reverse(retry_msg));
        
        if self.config.enable_message_tracing {
            trace!(
                "Message {} scheduled for retry {} in {:?}",
                message.message_id(),
                retry_count,
                delay
            );
        }
    }

    pub fn acknowledge_message(&mut self, message_id: MessageId) -> bool {
        if let Some(_pending_msg) = self.pending_acks.remove(&message_id) {
            self.stats.messages_acknowledged += 1;
            self.stats.pending_acks = self.pending_acks.len();
            
            if self.config.enable_message_tracing {
                trace!("Message {} acknowledged", message_id);
            }
            true
        } else {
            false
        }
    }

    fn process_message(&mut self, priority_msg: PriorityMessage) {
        let message = &priority_msg.message;
        let message_type = message.message().message_type();

        let subscribers = match self.message_subscriptions.get(message_type) {
            Some(subs) => subs,
            None => {
                if self.config.enable_message_tracing {
                    trace!("No subscribers for message type: {}", message_type);
                }
                return;
            }
        };

        let mut delivered = 0;
        let mut failed = 0;

        for subscription in subscribers {
            let Some(recipient) = subscription.recipient.upgrade() else {
                continue;
            };

            if !message.is_for_actor(&subscription.actor_id) {
                continue;
            }

            if let Some(ref filter) = subscription.filter {
                if !filter.matches(message.message()) {
                    continue;
                }
            }

            match recipient.try_send(message.clone()) {
                Ok(()) => {
                    delivered += 1;
                    
                    // Track message for acknowledgment
                    let pending_msg = PendingMessage {
                        message: message.clone(),
                        sent_at: Instant::now(),
                        retry_count: 0,
                        recipient: subscription.recipient.clone(),
                        subscription_id: subscription.id,
                    };
                    self.pending_acks.insert(message.message_id(), pending_msg);
                    
                    if self.config.enable_message_tracing {
                        trace!(
                            "Message {} delivered to {}, waiting for ack",
                            message.message_id(),
                            subscription.actor_id
                        );
                    }
                }
                Err(err) => {
                    failed += 1;
                    warn!(
                        "Failed to deliver message {} to {}: {}",
                        message.message_id(),
                        subscription.actor_id,
                        err
                    );
                }
            }
        }

        self.stats.messages_delivered += delivered;
        self.stats.messages_failed += failed;

        if self.config.enable_message_tracing {
            trace!(
                "Message {} processed: {} delivered, {} failed",
                message.message_id(),
                delivered,
                failed
            );
        }
    }

    pub fn stats(&self) -> &EventBusStats {
        &self.stats
    }

    pub fn cleanup_subscriptions(&mut self) {
        for subscriptions in self.message_subscriptions.values_mut() {
            let before = subscriptions.len();
            subscriptions.retain(|sub| sub.recipient.upgrade().is_some());
            let removed = before - subscriptions.len();
            self.stats.active_subscriptions = self.stats.active_subscriptions.saturating_sub(removed);
        }

        for recipients in self.routing_table.values_mut() {
            recipients.retain(|recipient| recipient.upgrade().is_some());
        }
    }

    pub fn get_message_type_stats(&self) -> HashMap<String, usize> {
        self.message_subscriptions
            .iter()
            .map(|(msg_type, subs)| (msg_type.clone(), subs.len()))
            .collect()
    }
}

impl Default for EnhancedEventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for EnhancedEventBus {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(60), |act, _ctx| {
            act.cleanup_subscriptions();
        });

        if self.config.enable_priority_queue {
            ctx.run_interval(Duration::from_millis(10), |act, _ctx| {
                act.process_queued_messages();
            });
        }

        ctx.run_interval(Duration::from_millis(100), |act, _ctx| {
            act.process_retry_queue();
        });

        ctx.run_interval(Duration::from_secs(1), |act, _ctx| {
            act.check_pending_acks();
        });
    }
}

pub mod messages {
    use super::*;

    #[derive(Message)]
    #[rtype(result = "SubscriptionId")]
    pub struct SubscribeToMessages {
        pub message_type: String,
        pub actor_id: ActorId,
        pub recipient: Recipient<MessageEnvelope>,
        pub filter: Option<MessageFilter>,
    }

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct UnsubscribeFromMessages {
        pub subscription_id: SubscriptionId,
    }

    #[derive(Message)]
    #[rtype(result = "Result<(), String>")]
    pub struct PublishMessage {
        pub message: MessageEnvelope,
    }

    #[derive(Message)]
    #[rtype(result = "EventBusStats")]
    pub struct GetStats;

    #[derive(Message)]
    #[rtype(result = "bool")]
    pub struct AcknowledgeMessage {
        pub message_id: MessageId,
    }
}

use messages::*;

impl Handler<SubscribeToMessages> for EnhancedEventBus {
    type Result = SubscriptionId;

    fn handle(&mut self, msg: SubscribeToMessages, _ctx: &mut Context<Self>) -> Self::Result {
        self.subscribe_to_messages(msg.message_type, msg.actor_id, msg.recipient, msg.filter)
    }
}

impl Handler<UnsubscribeFromMessages> for EnhancedEventBus {
    type Result = ();

    fn handle(&mut self, msg: UnsubscribeFromMessages, _ctx: &mut Context<Self>) -> Self::Result {
        self.unsubscribe(msg.subscription_id);
    }
}

impl Handler<PublishMessage> for EnhancedEventBus {
    type Result = Result<(), String>;

    fn handle(&mut self, msg: PublishMessage, _ctx: &mut Context<Self>) -> Self::Result {
        if let Some(unified) = msg.message.downcast_message::<UnifiedMessage>() {
            let cloned = unified.clone();
            self.publish_message(cloned)
        } else {
            Err("Message is not a UnifiedMessage".to_string())
        }
    }
}

impl Handler<GetStats> for EnhancedEventBus {
    type Result = EventBusStats;

    fn handle(&mut self, _msg: GetStats, _ctx: &mut Context<Self>) -> Self::Result {
        self.stats.clone()
    }
}

impl Handler<AcknowledgeMessage> for EnhancedEventBus {
    type Result = bool;

    fn handle(&mut self, msg: AcknowledgeMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.acknowledge_message(msg.message_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::events::{LogEvent, LogLevel};

    #[test]
    fn test_enhanced_event_bus_creation() {
        let bus = EnhancedEventBus::new();
        assert_eq!(bus.stats().active_subscriptions, 0);
        assert_eq!(bus.stats().messages_sent, 0);
        assert_eq!(bus.stats().pending_acks, 0);
        assert_eq!(bus.stats().messages_retried, 0);
    }

    #[test]
    fn test_config_from_env() {
        std::env::set_var("EVENT_BUS_MAX_QUEUE_SIZE", "5000");
        std::env::set_var("EVENT_BUS_MAX_RETRIES", "5");
        std::env::set_var("EVENT_BUS_EXPONENTIAL_BACKOFF", "false");
        
        let config = EventBusConfig::from_env();
        assert_eq!(config.max_queue_size, 5000);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.exponential_backoff, false);
        
        // Clean up
        std::env::remove_var("EVENT_BUS_MAX_QUEUE_SIZE");
        std::env::remove_var("EVENT_BUS_MAX_RETRIES");
        std::env::remove_var("EVENT_BUS_EXPONENTIAL_BACKOFF");
    }

    #[test]
    fn test_message_priority_ordering() {
        let msg1 = PriorityMessage {
            message: MessageEnvelope::new(UnifiedMessage::from_event(
                LogEvent::new(LogLevel::Info, "test1".to_string(), None),
                ActorId::instance("sender"),
                ActorId::actor_type("receiver"),
            )),
            priority: MessagePriority::Low,
            created_at: Instant::now(),
        };

        let msg2 = PriorityMessage {
            message: MessageEnvelope::new(UnifiedMessage::from_event(
                LogEvent::new(LogLevel::Error, "test2".to_string(), None),
                ActorId::instance("sender"),
                ActorId::actor_type("receiver"),
            )),
            priority: MessagePriority::High,
            created_at: Instant::now(),
        };

        assert!(msg2 < msg1);
    }
}