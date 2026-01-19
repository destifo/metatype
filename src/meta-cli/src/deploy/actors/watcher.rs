// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

use super::central_bus::{CentralEventBus, CentralEventBusAccess};
use super::console::{Console, ConsoleHandle};
use super::event_bus::{
    EventAck, EventBusExt, EventEnvelope, EventSubscription, EventUnsubscribe, SubscriptionId,
};
use super::message_system::ActorId;
use super::two_channel_bus::{TwoChannelEventBusAccess, MessageHandler};
use super::unified_message::MessageEnvelope as NewMessageEnvelope;
use super::events::{
    ActorCommunicationCommand, ActorCommunicationEvent, TaskManagerCommand, TaskManagerEvent, 
    WatcherFileEvent, WatcherUpdateEvent, WatcherUpdateKind, ACTOR_COMMUNICATION_EVENT_TYPE,
    WATCHER_FILE_EVENT_TYPE, WATCHER_UPDATE_EVENT_TYPE,
};
use super::task::action::TaskAction;
use super::task_manager::{TaskGenerator, TaskReason};
use crate::config::Config;
use crate::deploy::actors::task::deploy::TypegraphData;
use crate::deploy::push::pusher::RetryManager;
use crate::interlude::*;
use crate::typegraph::dependency_graph::DependencyGraph;
use crate::typegraph::loader::discovery::FileFilter;
use notify_debouncer_mini::notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_mini::{new_debouncer, notify, DebounceEventResult, Debouncer};
use pathdiff::diff_paths;
use std::path::{Path, PathBuf};
use std::{sync::Arc, time::Duration};

pub mod message {
    use super::*;

    // TODO remove
    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct Stop;

    #[derive(Message)]
    #[rtype(result = "()")]
    pub(super) struct File(pub PathBuf);

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct RemoveTypegraph(pub PathBuf);
}

use message::*;

#[derive(Debug)]
pub enum Event {
    DependencyChanged {
        typegraph_module: PathBuf,
        dependency_path: PathBuf,
    },
    TypegraphModuleChanged {
        typegraph_module: PathBuf,
    },
    TypegraphModuleDeleted {
        typegraph_module: PathBuf,
    },
    ConfigChanged,
}

pub struct WatcherActor<A: TaskAction + 'static> {
    config: Arc<Config>,
    directory: Arc<Path>,
    task_generator: TaskGenerator,
    console: ConsoleHandle,
    debouncer: Option<Debouncer<RecommendedWatcher>>,
    dependency_graph: DependencyGraph,
    file_filter: FileFilter,
    subscriptions: Vec<SubscriptionId>,
    actor_id: ActorId,
}

impl<A: TaskAction> Actor for WatcherActor<A> {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        if let Err(e) = self.start_watcher(ctx) {
            self.console
                .error(format!("Failed to start watcher: {}", e));
            ctx.stop();
        }
        let bus = self.console.event_bus();
        let recipient = ctx.address().recipient();
        let fut = async move {
            let mut ids = Vec::new();
            ids.push(
                bus.send(EventSubscription::new(
                    WATCHER_UPDATE_EVENT_TYPE,
                    recipient.clone(),
                    None,
                ))
                .await?,
            );
            ids.push(
                bus.send(EventSubscription::new(
                    ACTOR_COMMUNICATION_EVENT_TYPE,
                    recipient.clone(),
                    Some(Arc::new(|envelope| {
                        // Only handle events targeted at WatcherActor
                        if let Some(event) = envelope.downcast_ref::<ActorCommunicationEvent>() {
                            event.target_actor == "WatcherActor"
                        } else {
                            false
                        }
                    })),
                ))
                .await?,
            );
            ids.push(
                bus.send(EventSubscription::new(
                    WATCHER_FILE_EVENT_TYPE,
                    recipient,
                    None,
                ))
                .await?,
            );
            Result::<Vec<SubscriptionId>>::Ok(ids)
        };
        ctx.spawn(
            fut.into_actor(self).map(|res, actor, _ctx| match res {
                Ok(ids) => actor.subscriptions = ids,
                Err(err) => actor
                    .console
                    .error(format!("failed to subscribe watcher: {err}")),
            }),
        );
        log::trace!("Watcher actor started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        let _ = self.debouncer.take();
        for id in self.subscriptions.drain(..) {
            self.console.event_bus().do_send(EventUnsubscribe { id });
        }
        log::trace!("Watcher actor stopped");
    }
}

impl<A: TaskAction> WatcherActor<A> {
    pub fn new(
        config: Arc<Config>,
        directory: Arc<Path>,
        task_generator: TaskGenerator,
        console: ConsoleHandle,
    ) -> Result<Self> {
        let file_filter = FileFilter::new(&config)?;
        Ok(Self {
            config,
            directory,
            task_generator,
            console,
            debouncer: None,
            dependency_graph: DependencyGraph::default(),
            file_filter,
            subscriptions: Vec::new(),
            actor_id: ActorId::actor_type("WatcherActor"),
        })
    }

    fn start_watcher(
        &mut self,
        ctx: &mut <WatcherActor<A> as actix::Actor>::Context,
    ) -> Result<()> {
        let self_addr = ctx.address();
        let mut debouncer =
            new_debouncer(Duration::from_secs(1), move |res: DebounceEventResult| {
                let events = res.unwrap();
                for path in events.into_iter().map(|e| e.path) {
                    CentralEventBus::get().publish(WatcherFileEvent::new(path, Some("WatcherActor".to_string())));
                }
            })?;
        debouncer.watcher().configure(
            notify::Config::default()
                .with_poll_interval(Duration::from_secs(1))
                .with_compare_contents(false), // TODO configurable?
        )?;

        let watcher = debouncer.watcher();
        self.console
            .info(format!("Watching {path:?}...", path = self.directory));
        watcher
            .watch(&self.directory, RecursiveMode::Recursive)
            .with_context(|| format!("Watching {path:?}", path = self.directory))?;

        self.debouncer = Some(debouncer);

        Ok(())
    }
}

impl<A: TaskAction + 'static> Handler<Stop> for WatcherActor<A> {
    type Result = ();

    fn handle(&mut self, _msg: Stop, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}

impl<A: TaskAction + 'static> Handler<File> for WatcherActor<A> {
    type Result = ();

    fn handle(&mut self, msg: File, ctx: &mut Self::Context) -> Self::Result {
        let path = msg.0;
        if &path == self.config.path.as_ref().unwrap() {
            self.console
                .warning("metatype configuration file changed".to_owned());
            self.console
                .warning("reloading all the typegraphs".to_owned());
            self.console.event_bus().publish(TaskManagerEvent::new(
                TaskManagerCommand::Restart,
                Some("WatcherActor".to_string()),
            ));
            ctx.stop();
        } else {
            let reverse_deps = self.dependency_graph.get_rdeps(&path);
            if !reverse_deps.is_empty() {
                let rel_path = diff_paths(&path, &self.directory).unwrap();
                self.console
                    .info(format!("File modified: {rel_path:?}; dependency of:"));
                for path in reverse_deps {
                    let dependency_path = path.clone();
                    let rel_path = diff_paths(&path, &self.directory).unwrap();
                    self.console
                        .info(format!("  -> {rel_path}", rel_path = rel_path.display()));

                    RetryManager::clear_counter(&path);
                    self.console.event_bus().publish(TaskManagerEvent::new(
                        TaskManagerCommand::AddTask {
                            task_ref: self.task_generator.generate(rel_path.into(), 0),
                            reason: TaskReason::DependencyChanged(dependency_path),
                        },
                        Some("WatcherActor".to_string()),
                    ));
                }
            } else if path.try_exists().unwrap() {
                if !self.file_filter.is_excluded(&path) {
                    let rel_path = diff_paths(&path, &self.directory).unwrap();
                    self.console.info(format!("File modified: {rel_path:?}"));

                    RetryManager::clear_counter(&path);
                    self.console.event_bus().publish(TaskManagerEvent::new(
                        TaskManagerCommand::AddTask {
                            task_ref: self.task_generator.generate(rel_path.into(), 0),
                            reason: TaskReason::FileChanged,
                        },
                        Some("WatcherActor".to_string()),
                    ));
                }
            } else {
                RetryManager::clear_counter(&path);
                // TODO method call
                self.publish_to_central(ActorCommunicationEvent::new(
                    "WatcherActor".to_string(),
                    ActorCommunicationCommand::RemoveTypegraph { path: path.clone() },
                    Some("WatcherActor".to_string()),
                ));

                // TODO delete typegraph in typegate
                // TODO cancel any eventual active deployment task
            }
        }
    }
}

impl<A: TaskAction + 'static> Handler<RemoveTypegraph> for WatcherActor<A> {
    type Result = ();

    fn handle(&mut self, msg: RemoveTypegraph, _ctx: &mut Self::Context) -> Self::Result {
        self.dependency_graph.remove_typegraph_at(&msg.0)
    }
}

impl<A: TaskAction + 'static> Handler<EventEnvelope> for WatcherActor<A> {
    type Result = EventAck;

    fn handle(&mut self, msg: EventEnvelope, ctx: &mut Self::Context) -> Self::Result {
        if let Some(event) = msg.downcast_ref::<WatcherUpdateEvent>() {
            match &event.update {
                WatcherUpdateKind::Dependencies(data) => {
                    let TypegraphData {
                        path, artifacts, ..
                    } = data;
                    self.dependency_graph
                        .update_typegraph(path.clone(), artifacts);
                }
            }
            return EventAck::new(msg.event_id());
        }

        if let Some(event) = msg.downcast_ref::<ActorCommunicationEvent>() {
            if event.target_actor == "WatcherActor" {
                match &event.command {
                    ActorCommunicationCommand::Stop => {
                        ctx.stop();
                    }
                    ActorCommunicationCommand::RemoveTypegraph { path } => {
                        self.dependency_graph.remove_typegraph_at(path);
                    }
                    _ => {} // Other commands not handled by WatcherActor
                }
            }
            return EventAck::new(msg.event_id());
        }

        if let Some(event) = msg.downcast_ref::<WatcherFileEvent>() {
            ctx.address().do_send(File(event.path.clone()));
            return EventAck::new(msg.event_id());
        }

        EventAck::new(msg.event_id())
    }
}

impl<A: TaskAction + 'static> Handler<NewMessageEnvelope> for WatcherActor<A> {
    type Result = ();

    fn handle(&mut self, msg: NewMessageEnvelope, ctx: &mut Context<Self>) -> Self::Result {
        let handler = self.create_message_handler(self.actor_id.clone());
        let message_id = msg.message_id();
        
        if let Some(event) = msg.downcast_event::<WatcherUpdateEvent>() {
            match &event.update {
                WatcherUpdateKind::Dependencies(data) => {
                    let TypegraphData {
                        path, artifacts, ..
                    } = data;
                    self.dependency_graph
                        .update_typegraph(path.clone(), artifacts);
                }
            }
            handler.confirm_success(message_id);
            return;
        }

        if let Some(event) = msg.downcast_event::<ActorCommunicationEvent>() {
            if event.target_actor == "WatcherActor" {
                match &event.command {
                    ActorCommunicationCommand::Stop => {
                        ctx.stop();
                        handler.confirm_success(message_id);
                    }
                    ActorCommunicationCommand::RemoveTypegraph { path } => {
                        self.dependency_graph.remove_typegraph_at(path);
                        handler.confirm_success(message_id);
                    }
                    _ => {
                        handler.confirm_error(message_id, "Unsupported command".to_string());
                    }
                }
            } else {
                handler.confirm_error(message_id, "Message not for this actor".to_string());
            }
            return;
        }

        if let Some(event) = msg.downcast_event::<WatcherFileEvent>() {
            ctx.address().do_send(File(event.path.clone()));
            handler.confirm_success(message_id);
            return;
        }

        handler.confirm_error(message_id, "Unknown message type".to_string());
    }
}
