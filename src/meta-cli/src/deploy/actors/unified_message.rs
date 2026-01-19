// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

use super::event_bus::Event;
use super::message_system::{ActorId, Message, MessageMetadata, MessagePriority};
use crate::interlude::*;
use std::any::Any;

#[derive(Clone, Debug)]
pub struct UnifiedMessage {
    message_metadata: MessageMetadata,
    event: Arc<dyn Event>,
}

impl UnifiedMessage {
    pub fn from_event<E: Event>(event: E, sender: ActorId, receiver: ActorId) -> Self {
        let priority = Self::determine_priority(&event);
        Self {
            message_metadata: MessageMetadata::new(sender, receiver).with_priority(priority),
            event: Arc::new(event),
        }
    }

    pub fn from_event_with_metadata<E: Event>(
        event: E,
        message_metadata: MessageMetadata,
    ) -> Self {
        Self {
            message_metadata,
            event: Arc::new(event),
        }
    }

    pub fn event(&self) -> &dyn Event {
        self.event.as_ref()
    }

    pub fn event_arc(&self) -> Arc<dyn Event> {
        self.event.clone()
    }

    pub fn downcast_event<E: Event>(&self) -> Option<&E> {
        self.event.as_any().downcast_ref::<E>()
    }

    fn determine_priority(event: &dyn Event) -> MessagePriority {
        let event_type = event.event_type().as_str();
        match event_type {
            "log" => {
                if let Some(log_event) = event.as_any().downcast_ref::<super::events::LogEvent>() {
                    match log_event.level {
                        super::events::LogLevel::Error => MessagePriority::High,
                        super::events::LogLevel::Warning => MessagePriority::Normal,
                        super::events::LogLevel::Info => MessagePriority::Normal,
                        super::events::LogLevel::Debug => MessagePriority::Low,
                    }
                } else {
                    MessagePriority::Normal
                }
            }
            "task.finished" => MessagePriority::High,
            "task.progress" => MessagePriority::Normal,
            "discovery.done" => MessagePriority::High,
            "watcher.event" => MessagePriority::Normal,
            "typegate.state" => MessagePriority::High,
            "task.manager.command" => MessagePriority::High,
            "actor.communication" => MessagePriority::High,
            "watcher.file" => MessagePriority::Normal,
            "task.manager.next" => MessagePriority::Normal,
            _ => MessagePriority::Normal,
        }
    }
}

impl Message for UnifiedMessage {
    fn metadata(&self) -> &MessageMetadata {
        &self.message_metadata
    }

    fn message_type(&self) -> &'static str {
        self.event.event_type().as_str()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub trait EventToMessage {
    fn to_unified_message(self, sender: ActorId, receiver: ActorId) -> UnifiedMessage;
}

impl<E: Event> EventToMessage for E {
    fn to_unified_message(self, sender: ActorId, receiver: ActorId) -> UnifiedMessage {
        UnifiedMessage::from_event(self, sender, receiver)
    }
}

#[derive(Clone, Debug)]
pub struct MessageEnvelope {
    message: Arc<dyn Message>,
}

impl MessageEnvelope {
    pub fn new<M: Message>(message: M) -> Self {
        Self {
            message: Arc::new(message),
        }
    }

    pub fn from_unified(unified: UnifiedMessage) -> Self {
        Self {
            message: Arc::new(unified),
        }
    }

    pub fn message(&self) -> &dyn Message {
        self.message.as_ref()
    }

    pub fn message_arc(&self) -> Arc<dyn Message> {
        self.message.clone()
    }

    pub fn downcast_message<M: Message>(&self) -> Option<&M> {
        self.message.as_any().downcast_ref::<M>()
    }

    pub fn downcast_event<E: Event>(&self) -> Option<&E> {
        if let Some(unified) = self.downcast_message::<UnifiedMessage>() {
            unified.downcast_event::<E>()
        } else {
            None
        }
    }

    pub fn contains_event_type(&self, event_type: &str) -> bool {
        self.message.message_type() == event_type
    }

    pub fn message_id(&self) -> super::message_system::MessageId {
        self.message.message_id()
    }

    pub fn is_for_actor(&self, actor_id: &ActorId) -> bool {
        self.message.is_for_actor(actor_id)
    }
}

#[derive(Clone, Debug)]
pub struct MessageEnvelopeAck {
    message_id: super::message_system::MessageId,
    success: bool,
    error: Option<String>,
}

impl MessageEnvelopeAck {
    pub fn success(envelope: &MessageEnvelope) -> Self {
        Self {
            message_id: envelope.message_id(),
            success: true,
            error: None,
        }
    }

    pub fn error(envelope: &MessageEnvelope, error: String) -> Self {
        Self {
            message_id: envelope.message_id(),
            success: false,
            error: Some(error),
        }
    }

    pub fn message_id(&self) -> super::message_system::MessageId {
        self.message_id
    }

    pub fn is_success(&self) -> bool {
        self.success
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

pub mod actor_ids {
    use super::ActorId;

    pub fn task_manager() -> ActorId {
        ActorId::actor_type("TaskManager")
    }

    pub fn console_actor() -> ActorId {
        ActorId::actor_type("ConsoleActor")
    }

    pub fn watcher_actor() -> ActorId {
        ActorId::actor_type("WatcherActor")
    }

    pub fn discovery_actor() -> ActorId {
        ActorId::actor_type("DiscoveryActor")
    }

    pub fn task_actor(path: &str) -> ActorId {
        ActorId::instance(format!("TaskActor:{}", path))
    }

    pub fn all_task_actors() -> ActorId {
        ActorId::broadcast("TaskActor")
    }

    pub fn system() -> ActorId {
        ActorId::system()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::events::{LogEvent, LogLevel};

    #[test]
    fn test_unified_message_creation() {
        let log_event = LogEvent::new(
            LogLevel::Info,
            "Test message".to_string(),
            Some("test_actor".to_string()),
        );

        let unified = UnifiedMessage::from_event(
            log_event,
            ActorId::instance("test_sender"),
            ActorId::actor_type("ConsoleActor"),
        );

        assert_eq!(unified.message_type(), "log");
        assert_eq!(unified.priority(), MessagePriority::Normal);
    }

    #[test]
    fn test_message_envelope() {
        let log_event = LogEvent::new(
            LogLevel::Error,
            "Error message".to_string(),
            Some("test_actor".to_string()),
        );

        let unified = log_event.to_unified_message(
            ActorId::instance("test_sender"),
            ActorId::actor_type("ConsoleActor"),
        );

        let envelope = MessageEnvelope::from_unified(unified);

        assert!(envelope.contains_event_type("log"));
        assert!(envelope.downcast_event::<LogEvent>().is_some());

        if let Some(log) = envelope.downcast_event::<LogEvent>() {
            assert_eq!(log.level, LogLevel::Error);
            assert_eq!(log.message, "Error message");
        }
    }

    #[test]
    fn test_actor_targeting() {
        let log_event = LogEvent::new(
            LogLevel::Info,
            "Test message".to_string(),
            Some("test_actor".to_string()),
        );

        let unified = log_event.to_unified_message(
            ActorId::instance("test_sender"),
            ActorId::actor_type("ConsoleActor"),
        );

        let envelope = MessageEnvelope::from_unified(unified);

        assert!(envelope.is_for_actor(&ActorId::actor_type("ConsoleActor")));
        assert!(!envelope.is_for_actor(&ActorId::actor_type("TaskManager")));
    }
}