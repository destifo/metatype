// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

#[cfg(test)]
mod tests {
    use super::super::central_bus::{CentralEventBus, CentralEventBusAccess};
    use super::super::enhanced_event_bus::{EnhancedEventBus, messages::*};
    use super::super::events::{LogEvent, LogLevel};
    use super::super::message_system::{ActorId, Message};
    use super::super::two_channel_bus::{TwoChannelEventBus, TwoChannelEventBusAccess, MessageHandler, MessageConfirmation};
    use super::super::unified_message::{EventToMessage, MessageEnvelope, actor_ids};
    use actix::prelude::*;
    use std::time::Duration;

    #[actix::test]
    async fn test_two_channel_handshake_system() {
        let (outbound, inbound) = TwoChannelEventBus::initialize();
        
        let test_actor = HandshakeTestActor::new().start();

        let subscription_id = outbound
            .send(SubscribeToMessages {
                message_type: "log".to_string(),
                actor_id: ActorId::actor_type("TestActor"),
                recipient: test_actor.recipient(),
                filter: None,
            })
            .await
            .expect("Failed to subscribe");

        let log_event = LogEvent::new(
            LogLevel::Info,
            "Handshake test message".to_string(),
            Some("test_sender".to_string()),
        );

        let unified = log_event.to_unified_message(
            ActorId::instance("test_sender"),
            ActorId::actor_type("TestActor"),
        );

        let envelope = MessageEnvelope::from_unified(unified);
        let message_id = envelope.message_id();
        
        let result = outbound
            .send(PublishMessage { message: envelope })
            .await
            .expect("Failed to send publish message");

        assert!(result.is_ok());

        tokio::time::sleep(Duration::from_millis(100)).await;

        let received_count = test_actor
            .send(GetReceivedCount)
            .await
            .expect("Failed to get received count");

        assert_eq!(received_count, 1);

        let confirmations_sent = test_actor
            .send(GetConfirmationsSent)
            .await
            .expect("Failed to get confirmations sent");

        assert_eq!(confirmations_sent, 1);

        let outbound_stats = outbound.send(GetStats).await.expect("Failed to get outbound stats");
        let inbound_stats = inbound.send(GetStats).await.expect("Failed to get inbound stats");
        
        assert_eq!(outbound_stats.messages_sent, 1);
        assert!(outbound_stats.messages_acknowledged >= 1);

        outbound.send(UnsubscribeFromMessages { subscription_id })
            .await
            .expect("Failed to unsubscribe");
    }

    #[actix::test]
    async fn test_message_handler_flow() {
        let (_outbound, _inbound) = TwoChannelEventBus::initialize();
        
        let actor_id = ActorId::instance("TestActor");
        let handler = MessageHandler::new(actor_id.clone());
        let message_id = super::super::message_system::MessageId::new();
        
        handler.confirm_success(message_id);
        
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        let handler2 = MessageHandler::new(actor_id);
        let message_id2 = super::super::message_system::MessageId::new();
        
        handler2.confirm_error(message_id2, "Test error".to_string());
        
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    #[actix::test]
    async fn test_confirmation_processing() {
        let (_outbound, inbound) = TwoChannelEventBus::initialize();
        
        let message_id = super::super::message_system::MessageId::new();
        let actor_id = ActorId::instance("TestActor");
        
        let success_confirmation = MessageConfirmation::success(
            message_id,
            actor_id.clone(),
            Duration::from_millis(25),
        );
        
        inbound.do_send(ProcessConfirmation { 
            confirmation: success_confirmation 
        });
        
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        let error_confirmation = MessageConfirmation::error(
            message_id,
            actor_id,
            "Processing failed".to_string(),
            Duration::from_millis(50),
        );
        
        inbound.do_send(ProcessConfirmation { 
            confirmation: error_confirmation 
        });
        
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    struct HandshakeTestActor {
        received_count: usize,
        confirmations_sent: usize,
        actor_id: ActorId,
    }

    impl HandshakeTestActor {
        fn new() -> Self {
            Self { 
                received_count: 0,
                confirmations_sent: 0,
                actor_id: ActorId::actor_type("TestActor"),
            }
        }
    }

    impl Actor for HandshakeTestActor {
        type Context = Context<Self>;
    }

    #[derive(Message)]
    #[rtype(result = "usize")]
    struct GetReceivedCount;

    #[derive(Message)]
    #[rtype(result = "usize")]
    struct GetConfirmationsSent;

    impl Handler<GetReceivedCount> for HandshakeTestActor {
        type Result = usize;

        fn handle(&mut self, _msg: GetReceivedCount, _ctx: &mut Context<Self>) -> Self::Result {
            self.received_count
        }
    }

    impl Handler<GetConfirmationsSent> for HandshakeTestActor {
        type Result = usize;

        fn handle(&mut self, _msg: GetConfirmationsSent, _ctx: &mut Context<Self>) -> Self::Result {
            self.confirmations_sent
        }
    }

    impl Handler<MessageEnvelope> for HandshakeTestActor {
        type Result = ();

        fn handle(&mut self, msg: MessageEnvelope, _ctx: &mut Context<Self>) -> Self::Result {
            let handler = self.create_message_handler(self.actor_id.clone());
            let message_id = msg.message_id();
            
            self.received_count += 1;
            self.confirmations_sent += 1;
            
            if msg.downcast_event::<LogEvent>().is_some() {
                handler.confirm_success(message_id);
            } else {
                handler.confirm_error(message_id, "Unknown event type".to_string());
            }
        }
    }

    impl TwoChannelEventBusAccess for HandshakeTestActor {}
}