// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

use super::enhanced_event_bus::EnhancedEventBus;
use super::event_bus::{Event, EventBusExt};
use super::message_system::{ActorId, Message};
use super::two_channel_bus::{TwoChannelEventBus, TwoChannelEventBusAccess, MessageHandler, MessageConfirmation};
use super::unified_message::{EventToMessage, MessageEnvelope};
use crate::interlude::*;

pub struct CentralEventBus;

impl CentralEventBus {
    pub fn initialize() -> (Addr<EnhancedEventBus>, Addr<EnhancedEventBus>) {
        TwoChannelEventBus::initialize()
    }

    pub fn get_outbound() -> Addr<EnhancedEventBus> {
        TwoChannelEventBus::get_outbound()
    }

    pub fn get_inbound() -> Addr<EnhancedEventBus> {
        TwoChannelEventBus::get_inbound()
    }

    pub fn is_initialized() -> bool {
        TwoChannelEventBus::is_initialized()
    }
}

pub trait CentralEventBusAccess: TwoChannelEventBusAccess {
    fn publish_to_central<E: Event>(&self, event: E) {
        let sender = ActorId::instance("unknown");
        let receiver = ActorId::system();
        
        let unified = event.to_unified_message(sender, receiver);
        self.publish_outbound(unified);
    }

    fn publish_message_to_central<E: Event>(&self, event: E, sender: ActorId, receiver: ActorId) {
        let unified = event.to_unified_message(sender, receiver);
        self.publish_outbound(unified);
    }

    fn publish_raw_message<M: Message>(&self, message: M) {
        self.publish_outbound(message);
    }

    fn create_message_handler(&self, actor_id: ActorId) -> MessageHandler {
        MessageHandler::new(actor_id)
    }
}

// Implement for all types by default
impl<T> CentralEventBusAccess for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_central_bus_two_channel() {
        let (outbound, inbound) = CentralEventBus::initialize();
        
        let outbound2 = CentralEventBus::get_outbound();
        let inbound2 = CentralEventBus::get_inbound();
        
        assert_eq!(format!("{:?}", outbound), format!("{:?}", outbound2));
        assert_eq!(format!("{:?}", inbound), format!("{:?}", inbound2));
        assert!(CentralEventBus::is_initialized());
    }
}