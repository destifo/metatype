// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

#[cfg(test)]
mod tests {
    use super::super::central_bus::{CentralEventBus, CentralEventBusAccess};
    use super::super::event_bus::*;
    use super::super::events::*;
    use actix::prelude::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[actix::test]
    async fn test_central_event_bus_singleton() {
        // Initialize the central event bus
        let bus1 = CentralEventBus::initialize();
        
        // Get the same instance
        let bus2 = CentralEventBus::get();
        
        // They should be the same address (same underlying actor)
        assert!(CentralEventBus::is_initialized());
        
        // Both should be able to publish events
        bus1.publish(LogEvent::new(
            LogLevel::Info,
            "Test message 1".to_string(),
            Some("test".to_string()),
        ));
        
        bus2.publish(LogEvent::new(
            LogLevel::Info,
            "Test message 2".to_string(),
            Some("test".to_string()),
        ));
        
        // Wait a bit for message processing
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[actix::test]
    async fn test_central_event_bus_communication() {
        // Initialize the central event bus
        let _bus = CentralEventBus::initialize();
        
        // Create a test actor that will receive events
        let test_actor = TestActor::new().start();
        
        // Subscribe to log events through the central bus
        let subscription_id = CentralEventBus::get()
            .send(EventSubscription::new(
                LOG_EVENT_TYPE,
                test_actor.recipient(),
                None,
            ))
            .await
            .expect("Failed to subscribe");

        // Publish a log event through the central bus
        let test_message = "Central bus test message".to_string();
        CentralEventBus::get().publish(LogEvent::new(
            LogLevel::Info,
            test_message.clone(),
            Some("test".to_string()),
        ));

        // Wait a bit for message processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check that the test actor received the message
        let received_messages = test_actor
            .send(GetReceivedMessages)
            .await
            .expect("Failed to get messages");

        assert_eq!(received_messages.len(), 1);
        if let Some(event) = received_messages[0].downcast_ref::<LogEvent>() {
            assert_eq!(event.message, test_message);
            assert_eq!(event.level, LogLevel::Info);
        } else {
            panic!("Expected LogEvent");
        }

        // Unsubscribe
        CentralEventBus::get().do_send(EventUnsubscribe { id: subscription_id });
    }

    #[actix::test]
    async fn test_actor_communication_through_central_bus() {
        // Initialize the central event bus
        let _bus = CentralEventBus::initialize();
        
        // Create a test actor that will receive communication events
        let test_actor = TestActor::new().start();
        
        // Subscribe to actor communication events
        let subscription_id = CentralEventBus::get()
            .send(EventSubscription::new(
                ACTOR_COMMUNICATION_EVENT_TYPE,
                test_actor.recipient(),
                Some(Arc::new(|envelope| {
                    if let Some(event) = envelope.downcast_ref::<ActorCommunicationEvent>() {
                        event.target_actor == "TestActor"
                    } else {
                        false
                    }
                })),
            ))
            .await
            .expect("Failed to subscribe");

        // Publish an actor communication event
        CentralEventBus::get().publish(ActorCommunicationEvent::new(
            "TestActor".to_string(),
            ActorCommunicationCommand::Stop,
            Some("test".to_string()),
        ));

        // Wait a bit for message processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check that the test actor received the message
        let received_messages = test_actor
            .send(GetReceivedMessages)
            .await
            .expect("Failed to get messages");

        assert_eq!(received_messages.len(), 1);
        if let Some(event) = received_messages[0].downcast_ref::<ActorCommunicationEvent>() {
            assert_eq!(event.target_actor, "TestActor");
            matches!(event.command, ActorCommunicationCommand::Stop);
        } else {
            panic!("Expected ActorCommunicationEvent");
        }

        // Unsubscribe
        CentralEventBus::get().do_send(EventUnsubscribe { id: subscription_id });
    }

    // Test actor to receive events
    struct TestActor {
        received_events: Vec<Arc<dyn Event>>,
    }

    impl TestActor {
        fn new() -> Self {
            Self {
                received_events: Vec::new(),
            }
        }
    }

    impl Actor for TestActor {
        type Context = Context<Self>;
    }

    #[derive(Message)]
    #[rtype(result = "Vec<Arc<dyn Event>>")]
    struct GetReceivedMessages;

    impl Handler<GetReceivedMessages> for TestActor {
        type Result = Vec<Arc<dyn Event>>;

        fn handle(&mut self, _msg: GetReceivedMessages, _ctx: &mut Context<Self>) -> Self::Result {
            self.received_events.clone()
        }
    }

    impl Handler<EventEnvelope> for TestActor {
        type Result = EventAck;

        fn handle(&mut self, msg: EventEnvelope, _ctx: &mut Context<Self>) -> Self::Result {
            self.received_events.push(msg.event_arc());
            EventAck::new(msg.event_id())
        }
    }
}