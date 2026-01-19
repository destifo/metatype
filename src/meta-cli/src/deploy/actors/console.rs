// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

pub mod input;

use crate::config::Config;
use crate::deploy::actors::enhanced_event_bus::EnhancedEventBus;
use crate::deploy::actors::event_bus::{
    EventAck, EventBusExt, EventEnvelope, EventSubscription, EventUnsubscribe,
    SubscriptionId,
};
use crate::deploy::actors::events::{LogEvent, LogLevel, LOG_EVENT_TYPE};
use crate::deploy::actors::message_system::ActorId;
use crate::deploy::actors::two_channel_bus::{TwoChannelEventBusAccess, MessageHandler};
use crate::deploy::actors::unified_message::MessageEnvelope as NewMessageEnvelope;
use crate::interlude::*;
use std::io::BufRead;
use tokio::sync::oneshot;

enum Mode {
    Input {
        output_buffer: Vec<Box<dyn OutputMessage + 'static>>,
    },
    Output,
}

pub struct ConsoleActor {
    #[allow(dead_code)]
    config: Arc<Config>,
    event_bus: Addr<EnhancedEventBus>,
    mode: Mode,
    input_tx: std::sync::mpsc::Sender<oneshot::Sender<String>>,
    subscription_id: Option<SubscriptionId>,
    actor_id: ActorId,
}

impl ConsoleActor {
    pub fn new(config: Arc<Config>, event_bus: Addr<EnhancedEventBus>) -> Self {
        let (input_tx, input_rx) = std::sync::mpsc::channel();

        Self::create_input_thread(input_rx);

        Self {
            config,
            event_bus,
            mode: Mode::Output,
            input_tx,
            subscription_id: None,
            actor_id: ActorId::actor_type("ConsoleActor"),
        }
    }

    fn handle_output(&mut self, output: impl OutputMessage + 'static) {
        match self.mode {
            Mode::Input {
                ref mut output_buffer,
                ..
            } => {
                output_buffer.push(Box::new(output));
            }
            Mode::Output => {
                output.send();
            }
        }
    }

    fn emit_log(&mut self, level: LogLevel, message: String) {
        match level {
            LogLevel::Debug => self.handle_output(Debug(message)),
            LogLevel::Info => self.handle_output(Info(message)),
            LogLevel::Warning => self.handle_output(Warning(message)),
            LogLevel::Error => self.handle_output(Error(message)),
        }
    }

    fn create_input_thread(rx: std::sync::mpsc::Receiver<oneshot::Sender<String>>) {
        std::thread::spawn(move || {
            let mut stdin = std::io::stdin().lock();

            while let Ok(tx) = rx.recv() {
                let mut input = String::new();
                stdin.read_line(&mut input).unwrap();
                tx.send(input).unwrap();
            }

            log::trace!("Input thread stopped.");
        });
    }
}

impl Actor for ConsoleActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let bus = self.event_bus.clone();
        let recipient = ctx.address().recipient();
        let fut = async move { bus.send(EventSubscription::new(LOG_EVENT_TYPE, recipient, None)).await };
        ctx.spawn(
            fut.into_actor(self).map(|res, actor, _ctx| match res {
                Ok(id) => actor.subscription_id = Some(id),
                Err(err) => log::error!("failed to subscribe console to event bus: {err}"),
            }),
        );
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(id) = self.subscription_id.take() {
            self.event_bus.do_send(EventUnsubscribe { id });
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Debug(pub String);

#[derive(Message)]
#[rtype(result = "()")]
struct Info(pub String);

#[derive(Message)]
#[rtype(result = "()")]
struct Warning(pub String);

#[derive(Message)]
#[rtype(result = "()")]
struct Error(pub String);

trait OutputMessage {
    fn send(&self);
}

impl OutputMessage for Debug {
    fn send(&self) {
        log::debug!("{}", self.0);
    }
}

impl OutputMessage for Info {
    fn send(&self) {
        log::info!("{}", self.0);
    }
}

impl OutputMessage for Warning {
    fn send(&self) {
        log::warn!("{}", self.0);
    }
}

impl OutputMessage for Error {
    fn send(&self) {
        log::error!("{}", self.0);
    }
}

impl<T> Handler<T> for ConsoleActor
where
    T: OutputMessage + Sized + actix::Message<Result = ()> + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: T, _ctx: &mut Context<Self>) -> Self::Result {
        self.handle_output(msg);
    }
}

impl Handler<EventEnvelope> for ConsoleActor {
    type Result = EventAck;

    fn handle(&mut self, msg: EventEnvelope, _ctx: &mut Context<Self>) -> Self::Result {
        if let Some(event) = msg.downcast_ref::<LogEvent>() {
            self.emit_log(event.level, event.message.clone());
        }

        EventAck::new(msg.event_id())
    }
}

impl Handler<NewMessageEnvelope> for ConsoleActor {
    type Result = ();

    fn handle(&mut self, msg: NewMessageEnvelope, _ctx: &mut Context<Self>) -> Self::Result {
        let handler = self.create_message_handler(self.actor_id.clone());
        let message_id = msg.message_id();
        
        if let Some(event) = msg.downcast_event::<LogEvent>() {
            self.emit_log(event.level, event.message.clone());
            handler.confirm_success(message_id);
        } else {
            handler.confirm_error(message_id, "Unknown message type".to_string());
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct StartInput(oneshot::Sender<String>);

impl Handler<StartInput> for ConsoleActor {
    type Result = ();

    fn handle(&mut self, msg: StartInput, _ctx: &mut Context<Self>) -> Self::Result {
        let StartInput(tx) = msg;
        self.mode = Mode::Input {
            output_buffer: Vec::new(),
        };
        self.input_tx.send(tx).unwrap();
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct EndInput;

impl Handler<EndInput> for ConsoleActor {
    type Result = ();

    fn handle(&mut self, _msg: EndInput, ctx: &mut Context<Self>) -> Self::Result {
        match std::mem::replace(&mut self.mode, Mode::Output) {
            Mode::Input { output_buffer } => {
                for output in output_buffer.into_iter() {
                    output.send();
                }
            }
            Mode::Output => {
                ctx.address()
                    .error("EndInput received while not in input mode.".to_string());
            }
        }
    }
}

#[derive(Clone)]
pub struct ConsoleHandle {
    console: Addr<ConsoleActor>,
    event_bus: Addr<EnhancedEventBus>,
}

impl ConsoleHandle {
    pub fn new(console: Addr<ConsoleActor>, event_bus: Addr<EnhancedEventBus>) -> Self {
        Self { console, event_bus }
    }

    pub fn event_bus(&self) -> Addr<EnhancedEventBus> {
        self.event_bus.clone()
    }
}

#[async_trait::async_trait]
pub trait Console {
    fn debug(&self, msg: String);
    fn info(&self, msg: String);
    fn warning(&self, msg: String);
    fn error(&self, msg: String);
    async fn read_line(&self) -> String;
}

#[async_trait::async_trait]
impl Console for ConsoleHandle {
    fn debug(&self, msg: String) {
        self.event_bus
            .publish(LogEvent::new(LogLevel::Debug, msg, None));
    }

    fn info(&self, msg: String) {
        self.event_bus
            .publish(LogEvent::new(LogLevel::Info, msg, None));
    }

    fn warning(&self, msg: String) {
        self.event_bus
            .publish(LogEvent::new(LogLevel::Warning, msg, None));
    }

    fn error(&self, msg: String) {
        self.event_bus
            .publish(LogEvent::new(LogLevel::Error, msg, None));
    }

    async fn read_line(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.console.do_send(StartInput(tx));
        let line = rx.await.unwrap();
        self.console.do_send(EndInput);
        line
    }
}
