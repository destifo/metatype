// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

use crate::interlude::*;
use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

static MESSAGE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MessageId(u64);

impl MessageId {
    pub fn new() -> Self {
        Self(MESSAGE_ID_COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "msg_{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl Default for MessagePriority {
    fn default() -> Self {
        MessagePriority::Normal
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ActorId {
    Instance(String),
    Type(String),
    Broadcast(String),
    System,
}

impl ActorId {
    pub fn instance(name: impl Into<String>) -> Self {
        Self::Instance(name.into())
    }

    pub fn actor_type(name: impl Into<String>) -> Self {
        Self::Type(name.into())
    }

    pub fn broadcast(actor_type: impl Into<String>) -> Self {
        Self::Broadcast(actor_type.into())
    }

    pub fn system() -> Self {
        Self::System
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActorId::Instance(name) => write!(f, "instance:{}", name),
            ActorId::Type(name) => write!(f, "type:{}", name),
            ActorId::Broadcast(name) => write!(f, "broadcast:{}", name),
            ActorId::System => write!(f, "system"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MessageMetadata {
    pub id: MessageId,
    pub timestamp: SystemTime,
    pub sender: ActorId,
    pub receiver: ActorId,
    pub priority: MessagePriority,
    pub correlation_id: Option<MessageId>,
    pub timeout: Option<Duration>,
}

impl MessageMetadata {
    pub fn new(sender: ActorId, receiver: ActorId) -> Self {
        Self {
            id: MessageId::new(),
            timestamp: SystemTime::now(),
            sender,
            receiver,
            priority: MessagePriority::default(),
            correlation_id: None,
            timeout: None,
            tags: Vec::new(),
        }
    }

    pub fn with_priority(mut self, priority: MessagePriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: MessageId) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

}

pub trait Message: Send + Sync + std::fmt::Debug + 'static {
    fn metadata(&self) -> &MessageMetadata;
    fn message_type(&self) -> &'static str;

    fn message_id(&self) -> MessageId {
        self.metadata().id
    }

    fn timestamp(&self) -> SystemTime {
        self.metadata().timestamp
    }

    fn sender(&self) -> &ActorId {
        &self.metadata().sender
    }

    fn receiver(&self) -> &ActorId {
        &self.metadata().receiver
    }

    fn priority(&self) -> MessagePriority {
        self.metadata().priority
    }

    fn correlation_id(&self) -> Option<MessageId> {
        self.metadata().correlation_id
    }

    fn timeout(&self) -> Option<Duration> {
        self.metadata().timeout
    }

    fn is_for_actor(&self, actor_id: &ActorId) -> bool {
        match (&self.metadata().receiver, actor_id) {
            (receiver, target) if receiver == target => true,
            (ActorId::Broadcast(broadcast_type), ActorId::Instance(instance)) => {
                instance.starts_with(&format!("{}:", broadcast_type))
            }
            (ActorId::System, _) => true,
            _ => false,
        }
    }

    fn is_expired(&self) -> bool {
        if let Some(timeout) = self.timeout() {
            if let Ok(elapsed) = self.timestamp().elapsed() {
                return elapsed > timeout;
            }
        }
        false
    }

    fn as_any(&self) -> &dyn Any;

    fn create_response<R: Message>(&self, response_data: R, sender: ActorId) -> R {
        response_data
    }
}

#[derive(Clone, Debug)]
pub struct MessageAck {
    metadata: MessageMetadata,
    pub acknowledged_message_id: MessageId,
    pub success: bool,
    pub error: Option<String>,
}

impl MessageAck {
    pub fn success(original_message: &dyn Message, sender: ActorId) -> Self {
        Self {
            metadata: MessageMetadata::new(sender, original_message.sender().clone())
                .with_correlation_id(original_message.message_id()),
            acknowledged_message_id: original_message.message_id(),
            success: true,
            error: None,
        }
    }

    pub fn error(original_message: &dyn Message, sender: ActorId, error: String) -> Self {
        Self {
            metadata: MessageMetadata::new(sender, original_message.sender().clone())
                .with_correlation_id(original_message.message_id()),
            acknowledged_message_id: original_message.message_id(),
            success: false,
            error: Some(error),
        }
    }
}

impl Message for MessageAck {
    fn metadata(&self) -> &MessageMetadata {
        &self.metadata
    }

    fn message_type(&self) -> &'static str {
        "system.message_ack"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub trait FilterableMessage: Message {
    fn matches_filter(&self, filter: &MessageFilter) -> bool;
}
#[derive(Clone, Debug)]
pub struct MessageFilter {
    pub message_types: Option<Vec<String>>,
    pub senders: Option<Vec<ActorId>>,
    pub receivers: Option<Vec<ActorId>>,
    pub min_priority: Option<MessagePriority>,
    pub custom_filter: Option<Arc<dyn Fn(&dyn Message) -> bool + Send + Sync>>,
}

impl MessageFilter {
    pub fn new() -> Self {
        Self {
            message_types: None,
            senders: None,
            receivers: None,
            min_priority: None,
            custom_filter: None,
        }
    }

    pub fn with_message_types(mut self, types: Vec<String>) -> Self {
        self.message_types = Some(types);
        self
    }

    pub fn with_senders(mut self, senders: Vec<ActorId>) -> Self {
        self.senders = Some(senders);
        self
    }

    pub fn with_receivers(mut self, receivers: Vec<ActorId>) -> Self {
        self.receivers = Some(receivers);
        self
    }

    pub fn with_min_priority(mut self, priority: MessagePriority) -> Self {
        self.min_priority = Some(priority);
        self
    }

    pub fn with_custom_filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&dyn Message) -> bool + Send + Sync + 'static,
    {
        self.custom_filter = Some(Arc::new(filter));
        self
    }

    pub fn matches(&self, message: &dyn Message) -> bool {
        if let Some(ref types) = self.message_types {
            if !types.contains(&message.message_type().to_string()) {
                return false;
            }
        }

        if let Some(ref senders) = self.senders {
            if !senders.contains(message.sender()) {
                return false;
            }
        }

        if let Some(ref receivers) = self.receivers {
            if !receivers.contains(message.receiver()) {
                return false;
            }
        }

        if let Some(min_priority) = self.min_priority {
            if message.priority() < min_priority {
                return false;
            }
        }

        if let Some(ref custom_filter) = self.custom_filter {
            if !custom_filter(message) {
                return false;
            }
        }

        true
    }
}

impl Default for MessageFilter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestMessage {
        metadata: MessageMetadata,
        content: String,
    }

    impl TestMessage {
        fn new(sender: ActorId, receiver: ActorId, content: String) -> Self {
            Self {
                metadata: MessageMetadata::new(sender, receiver),
                content,
            }
        }
    }

    impl Message for TestMessage {
        fn metadata(&self) -> &MessageMetadata {
            &self.metadata
        }

        fn message_type(&self) -> &'static str {
            "test.message"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_message_id_generation() {
        let id1 = MessageId::new();
        let id2 = MessageId::new();
        assert_ne!(id1, id2);
        assert!(id2.as_u64() > id1.as_u64());
    }

    #[test]
    fn test_actor_id_targeting() {
        let sender = ActorId::instance("test_sender");
        let receiver = ActorId::actor_type("TaskManager");
        let message = TestMessage::new(sender, receiver.clone(), "test".to_string());

        assert!(message.is_for_actor(&receiver));
        assert!(!message.is_for_actor(&ActorId::actor_type("WatcherActor")));
    }

    #[test]
    fn test_broadcast_targeting() {
        let sender = ActorId::instance("test_sender");
        let receiver = ActorId::broadcast("TaskActor");
        let message = TestMessage::new(sender, receiver, "test".to_string());

        assert!(message.is_for_actor(&ActorId::instance("TaskActor:file1.py")));
        assert!(message.is_for_actor(&ActorId::instance("TaskActor:file2.py")));
        assert!(!message.is_for_actor(&ActorId::instance("WatcherActor:main")));
    }

    #[test]
    fn test_message_filter() {
        let sender = ActorId::instance("test_sender");
        let receiver = ActorId::actor_type("TaskManager");
        let message = TestMessage::new(sender.clone(), receiver, "test".to_string());

        let filter = MessageFilter::new()
            .with_message_types(vec!["test.message".to_string()])
            .with_senders(vec![sender]);

        assert!(filter.matches(&message));

        let filter2 = MessageFilter::new()
            .with_message_types(vec!["other.message".to_string()]);

        assert!(!filter2.matches(&message));
    }
}