// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

use super::enhanced_event_bus::{EnhancedEventBus, EventBusConfig, EventBusStats, messages::*};
use super::message_system::{ActorId, Message, MessageFilter, MessageId};
use super::unified_message::{MessageEnvelope, MessageEnvelopeAck};
use crate::interlude::*;
use std::sync::OnceLock;

static CENTRAL_OUTBOUND_BUS: OnceLock<Addr<EnhancedEventBus>> = OnceLock::new();
static CENTRAL_INBOUND_BUS: OnceLock<Addr<EnhancedEventBus>> = OnceLock::new();

pub struct TwoChannelEventBus;

impl TwoChannelEventBus {
    pub fn initialize() -> (Addr<EnhancedEventBus>, Addr<EnhancedEventBus>) {
        let outbound = CENTRAL_OUTBOUND_BUS
            .get_or_init(|| {
                trace!("Initializing central outbound event bus");
                EnhancedEventBus::new().start()
            })
            .clone();

        let inbound = CENTRAL_INBOUND_BUS
            .get_or_init(|| {
                trace!("Initializing central inbound event bus");
                EnhancedEventBus::new().start()
            })
            .clone();

        (outbound, inbound)
    }

    pub fn get_outbound() -> Addr<EnhancedEventBus> {
        CENTRAL_OUTBOUND_BUS
            .get()
            .expect("Two-channel event bus not initialized")
            .clone()
    }

    pub fn get_inbound() -> Addr<EnhancedEventBus> {
        CENTRAL_INBOUND_BUS
            .get()
            .expect("Two-channel event bus not initialized")
            .clone()
    }

    pub fn is_initialized() -> bool {
        CENTRAL_OUTBOUND_BUS.get().is_some() && CENTRAL_INBOUND_BUS.get().is_some()
    }
}

#[derive(Clone, Debug)]
pub struct MessageConfirmation {
    pub message_id: MessageId,
    pub actor_id: ActorId,
    pub success: bool,
    pub error: Option<String>,
    pub processing_time: std::time::Duration,
}

impl MessageConfirmation {
    pub fn success(message_id: MessageId, actor_id: ActorId, processing_time: std::time::Duration) -> Self {
        Self {
            message_id,
            actor_id,
            success: true,
            error: None,
            processing_time,
        }
    }

    pub fn error(message_id: MessageId, actor_id: ActorId, error: String, processing_time: std::time::Duration) -> Self {
        Self {
            message_id,
            actor_id,
            success: false,
            error: Some(error),
            processing_time,
        }
    }
}

pub trait TwoChannelEventBusAccess {
    fn outbound_bus() -> Addr<EnhancedEventBus> {
        TwoChannelEventBus::get_outbound()
    }

    fn inbound_bus() -> Addr<EnhancedEventBus> {
        TwoChannelEventBus::get_inbound()
    }

    fn publish_outbound<M: Message>(&self, message: M) {
        let envelope = MessageEnvelope::new(message);
        Self::outbound_bus().do_send(PublishMessage { message: envelope });
    }

    fn send_confirmation(&self, confirmation: MessageConfirmation) {
        Self::inbound_bus().do_send(ProcessConfirmation { confirmation });
    }

    fn acknowledge_message(&self, message_id: MessageId) {
        Self::outbound_bus().do_send(AcknowledgeMessage { message_id });
    }
}

impl<T> TwoChannelEventBusAccess for T {}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ProcessConfirmation {
    pub confirmation: MessageConfirmation,
}

impl Handler<ProcessConfirmation> for EnhancedEventBus {
    type Result = ();

    fn handle(&mut self, msg: ProcessConfirmation, _ctx: &mut Context<Self>) -> Self::Result {
        let confirmation = msg.confirmation;
        
        if confirmation.success {
            self.acknowledge_message(confirmation.message_id);
            if self.config.enable_message_tracing {
                trace!(
                    "Message {} confirmed by {} in {:?}",
                    confirmation.message_id,
                    confirmation.actor_id,
                    confirmation.processing_time
                );
            }
        } else {
            warn!(
                "Message {} failed at {}: {}",
                confirmation.message_id,
                confirmation.actor_id,
                confirmation.error.unwrap_or_else(|| "Unknown error".to_string())
            );
        }
    }
}

pub struct MessageHandler {
    actor_id: ActorId,
    start_time: std::time::Instant,
}

impl MessageHandler {
    pub fn new(actor_id: ActorId) -> Self {
        Self {
            actor_id,
            start_time: std::time::Instant::now(),
        }
    }

    pub fn confirm_success(self, message_id: MessageId) {
        let processing_time = self.start_time.elapsed();
        let confirmation = MessageConfirmation::success(message_id, self.actor_id, processing_time);
        TwoChannelEventBus::get_inbound().do_send(ProcessConfirmation { confirmation });
    }

    pub fn confirm_error(self, message_id: MessageId, error: String) {
        let processing_time = self.start_time.elapsed();
        let confirmation = MessageConfirmation::error(message_id, self.actor_id, error, processing_time);
        TwoChannelEventBus::get_inbound().do_send(ProcessConfirmation { confirmation });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::events::{LogEvent, LogLevel};
    use super::super::unified_message::{EventToMessage, actor_ids};

    #[actix::test]
    async fn test_two_channel_initialization() {
        let (outbound, inbound) = TwoChannelEventBus::initialize();
        
        assert!(TwoChannelEventBus::is_initialized());
        
        let outbound2 = TwoChannelEventBus::get_outbound();
        let inbound2 = TwoChannelEventBus::get_inbound();
        
        assert_eq!(format!("{:?}", outbound), format!("{:?}", outbound2));
        assert_eq!(format!("{:?}", inbound), format!("{:?}", inbound2));
    }

    #[actix::test]
    async fn test_message_confirmation_flow() {
        let (_outbound, inbound) = TwoChannelEventBus::initialize();
        
        let message_id = MessageId::new();
        let actor_id = ActorId::instance("TestActor");
        
        let confirmation = MessageConfirmation::success(
            message_id,
            actor_id,
            std::time::Duration::from_millis(50),
        );
        
        assert!(confirmation.success);
        assert_eq!(confirmation.message_id, message_id);
        assert_eq!(confirmation.processing_time, std::time::Duration::from_millis(50));
        
        inbound.do_send(ProcessConfirmation { confirmation });
        
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    #[test]
    fn test_message_handler() {
        let actor_id = ActorId::instance("TestActor");
        let handler = MessageHandler::new(actor_id.clone());
        let message_id = MessageId::new();
        
        handler.confirm_success(message_id);
    }
}