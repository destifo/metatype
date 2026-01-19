// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

#[cfg(test)]
mod tests {
    use super::super::central_bus::{CentralEventBus, CentralEventBusAccess};
    use super::super::enhanced_event_bus::{EnhancedEventBus, messages::*};
    use super::super::events::{LogEvent, LogLevel};
    use super::super::message_system::{ActorId, Message, MessageFilter, MessagePriority};
    use super::super::unified_message::{EventToMessage, MessageEnvelope, actor_ids};
    use actix::prelude::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[actix::test]
    async fn test_message_trait_implementation() {
        let log_event = LogEvent::new(
            LogLevel::Error,
            "Test error message".to_string(),
            Some("test_actor".to_string()),
        );

        let unified = log_event.to_unified_message(
            ActorId::instance("test_sender"),
            ActorId::actor_type("ConsoleActor"),
        );

        // Test Message trait methods
        assert_eq!(unified.message_type(), "log");
        assert_eq!(unified.priority(), MessagePriority::High); // Error should be high priority
        assert_eq!(unified.sender(), &ActorId::instance("test_sender"));
        assert_eq!(unified.receiver(), &ActorId::actor_type("ConsoleActor"));
        assert!(!unified.is_expired()); // Should not be expired immediately
    }

    #[actix::test]
    async fn test_message_metadata_extraction() {
        let sender = ActorId::instance("TaskActor:file.py");
        let receiver = ActorId::actor_type("TaskManager");
        
        let log_event = LogEvent::new(
            LogLevel::Info,
            "Task completed".to_string(),
            Some("TaskActor".to_string()),
        );

        let unified = log_event.to_unified_message(sender.clone(), receiver.clone());

        // Test common message values
        assert_eq!(unified.sender(), &sender);
        assert_eq!(unified.receiver(), &receiver);
        assert_eq!(unified.priority(), MessagePriority::Normal);
        assert!(unified.message_id().as_u64() > 0);
        assert!(unified.timestamp().elapsed().unwrap() < Duration::from_secs(1));
        assert_eq!(unified.correlation_id(), None);
        assert_eq!(unified.timeout(), None);
    }

    #[actix::test]
    async fn test_actor_id_targeting() {
        let log_event = LogEvent::new(
            LogLevel::Warning,
            "Test warning".to_string(),
            Some("test".to_string()),
        );

        // Test specific actor targeting
        let unified = log_event.to_unified_message(
            ActorId::instance("sender"),
            ActorId::instance("TaskActor:specific_file.py"),
        );

        assert!(unified.is_for_actor(&ActorId::instance("TaskActor:specific_file.py")));
        assert!(!unified.is_for_actor(&ActorId::instance("TaskActor:other_file.py")));

        // Test broadcast targeting
        let broadcast_unified = LogEvent::new(
            LogLevel::Info,
            "Broadcast message".to_string(),
            Some("test".to_string()),
        ).to_unified_message(
            ActorId::instance("sender"),
            ActorId::broadcast("TaskActor"),
        );

        assert!(broadcast_unified.is_for_actor(&ActorId::instance("TaskActor:file1.py")));
        assert!(broadcast_unified.is_for_actor(&ActorId::instance("TaskActor:file2.py")));
        assert!(!broadcast_unified.is_for_actor(&ActorId::instance("WatcherActor:main")));

        // Test system broadcast
        let system_unified = LogEvent::new(
            LogLevel::Critical,
            "System message".to_string(),
            Some("system".to_string()),
        ).to_unified_message(
            ActorId::system(),
            ActorId::system(),
        );

        assert!(system_unified.is_for_actor(&ActorId::instance("TaskActor:file.py")));
        assert!(system_unified.is_for_actor(&ActorId::actor_type("TaskManager")));
        assert!(system_unified.is_for_actor(&ActorId::system()));
    }

    #[actix::test]
    async fn test_message_filtering() {
        let log_event = LogEvent::new(
            LogLevel::Error,
            "Error message".to_string(),
            Some("test_sender".to_string()),
        );

        let unified = log_event.to_unified_message(
            ActorId::instance("test_sender"),
            ActorId::actor_type("ConsoleActor"),
        );

        // Test message type filter
        let type_filter = MessageFilter::new()
            .with_message_types(vec!["log".to_string()]);
        assert!(type_filter.matches(&unified));

        let wrong_type_filter = MessageFilter::new()
            .with_message_types(vec!["task.finished".to_string()]);
        assert!(!wrong_type_filter.matches(&unified));

        // Test sender filter
        let sender_filter = MessageFilter::new()
            .with_senders(vec![ActorId::instance("test_sender")]);
        assert!(sender_filter.matches(&unified));

        let wrong_sender_filter = MessageFilter::new()
            .with_senders(vec![ActorId::instance("other_sender")]);
        assert!(!wrong_sender_filter.matches(&unified));

        // Test priority filter
        let priority_filter = MessageFilter::new()
            .with_min_priority(MessagePriority::Normal);
        assert!(priority_filter.matches(&unified)); // Error is High priority

        let high_priority_filter = MessageFilter::new()
            .with_min_priority(MessagePriority::Critical);
        assert!(!high_priority_filter.matches(&unified)); // Error is High, not Critical

        // Test custom filter
        let custom_filter = MessageFilter::new()
            .with_custom_filter(|msg| msg.message_type() == "log");
        assert!(custom_filter.matches(&unified));
    }

    #[actix::test]
    async fn test_enhanced_event_bus() {
        let mut bus = EnhancedEventBus::new();
        
        // Test initial state
        assert_eq!(bus.stats().active_subscriptions, 0);
        assert_eq!(bus.stats().messages_sent, 0);
        assert_eq!(bus.stats().pending_acks, 0);

        // Test message publishing
        let log_event = LogEvent::new(
            LogLevel::Info,
            "Test message".to_string(),
            Some("test".to_string()),
        );

        let unified = log_event.to_unified_message(
            ActorId::instance("test_sender"),
            ActorId::actor_type("ConsoleActor"),
        );

        let result = bus.publish_message(unified);
        assert!(result.is_ok());
        assert_eq!(bus.stats().messages_sent, 1);
    }

    #[actix::test]
    async fn test_central_event_bus_with_messages() {
        // Initialize the central event bus
        let _bus = CentralEventBus::initialize();

        // Create a test actor that implements the new message system
        let test_actor = MessageTestActor::new().start();

        // Subscribe to messages
        let subscription_id = CentralEventBus::get()
            .send(SubscribeToMessages {
                message_type: "log".to_string(),
                actor_id: actor_ids::console_actor(),
                recipient: test_actor.recipient(),
                filter: None,
            })
            .await
            .expect("Failed to subscribe");

        // Publish a message using the new system
        let log_event = LogEvent::new(
            LogLevel::Info,
            "Central bus message test".to_string(),
            Some("test_sender".to_string()),
        );

        let unified = log_event.to_unified_message(
            ActorId::instance("test_sender"),
            actor_ids::console_actor(),
        );

        let envelope = MessageEnvelope::from_unified(unified);
        
        let result = CentralEventBus::get()
            .send(PublishMessage { message: envelope })
            .await
            .expect("Failed to send publish message");

        assert!(result.is_ok());

        // Wait for message processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check that the message was received
        let received_count = test_actor
            .send(GetReceivedCount)
            .await
            .expect("Failed to get received count");

        assert_eq!(received_count, 1);

        // Unsubscribe
        CentralEventBus::get()
            .send(UnsubscribeFromMessages { subscription_id })
            .await
            .expect("Failed to unsubscribe");
    }

    #[actix::test]
    async fn test_message_priority_system() {
        let mut bus = EnhancedEventBus::new();

        // Create messages with different priorities
        let low_priority = LogEvent::new(
            LogLevel::Debug,
            "Low priority".to_string(),
            Some("test".to_string()),
        ).to_unified_message(
            ActorId::instance("sender"),
            ActorId::actor_type("receiver"),
        );

        let high_priority = LogEvent::new(
            LogLevel::Error,
            "High priority".to_string(),
            Some("test".to_string()),
        ).to_unified_message(
            ActorId::instance("sender"),
            ActorId::actor_type("receiver"),
        );

        // Publish in reverse priority order
        bus.publish_message(low_priority).unwrap();
        bus.publish_message(high_priority).unwrap();

        assert_eq!(bus.stats().messages_sent, 2);
    }

    #[actix::test]
    async fn test_message_acknowledgment() {
        let bus = EnhancedEventBus::new().start();
        
        let test_actor = MessageTestActor::new().start();

        let subscription_id = bus
            .send(SubscribeToMessages {
                message_type: "log".to_string(),
                actor_id: actor_ids::console_actor(),
                recipient: test_actor.recipient(),
                filter: None,
            })
            .await
            .expect("Failed to subscribe");

        let log_event = LogEvent::new(
            LogLevel::Info,
            "Ack test message".to_string(),
            Some("test_sender".to_string()),
        );

        let unified = log_event.to_unified_message(
            ActorId::instance("test_sender"),
            actor_ids::console_actor(),
        );

        let envelope = MessageEnvelope::from_unified(unified);
        let message_id = envelope.message_id();
        
        let result = bus
            .send(PublishMessage { message: envelope })
            .await
            .expect("Failed to send publish message");

        assert!(result.is_ok());

        tokio::time::sleep(Duration::from_millis(50)).await;

        let ack_result = bus
            .send(AcknowledgeMessage { message_id })
            .await
            .expect("Failed to send ack");

        assert!(ack_result);

        let stats = bus.send(GetStats).await.expect("Failed to get stats");
        assert_eq!(stats.messages_acknowledged, 1);

        bus.send(UnsubscribeFromMessages { subscription_id })
            .await
            .expect("Failed to unsubscribe");
    }

    // Test actor for message system testing
    struct MessageTestActor {
        received_count: usize,
    }

    impl MessageTestActor {
        fn new() -> Self {
            Self { received_count: 0 }
        }
    }

    impl Actor for MessageTestActor {
        type Context = Context<Self>;
    }

    #[derive(Message)]
    #[rtype(result = "usize")]
    struct GetReceivedCount;

    impl Handler<GetReceivedCount> for MessageTestActor {
        type Result = usize;

        fn handle(&mut self, _msg: GetReceivedCount, _ctx: &mut Context<Self>) -> Self::Result {
            self.received_count
        }
    }

    impl Handler<MessageEnvelope> for MessageTestActor {
        type Result = ();

        fn handle(&mut self, _msg: MessageEnvelope, _ctx: &mut Context<Self>) -> Self::Result {
            self.received_count += 1;
        }
    }
}