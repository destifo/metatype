// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0
use super::central_bus::{CentralEventBus, CentralEventBusAccess};
use super::console::{Console, ConsoleHandle};
use super::discovery::DiscoveryActor;
use super::event_bus::{
    EventAck, EventEnvelope, EventSubscription, EventUnsubscribe, SubscriptionId,
};
use super::message_system::ActorId;
use super::two_channel_bus::{TwoChannelEventBusAccess, MessageHandler};
use super::unified_message::MessageEnvelope as NewMessageEnvelope;
use super::events::{
    ActorCommunicationCommand, ActorCommunicationEvent, DiscoveryDoneEvent, NextTaskEvent, 
    TaskFinishedEvent, TaskManagerCommand, TaskManagerEvent, WatcherEvent,
    WatcherEventKind, ACTOR_COMMUNICATION_EVENT_TYPE, DISCOVERY_DONE_EVENT_TYPE, 
    NEXT_TASK_EVENT_TYPE, TASK_FINISHED_EVENT_TYPE, TASK_MANAGER_EVENT_TYPE, WATCHER_EVENT_TYPE,
};
use super::task::action::{TaskAction, TaskActionGenerator};
use super::task::{self, TaskActor, TaskFinishStatus};
use super::watcher::WatcherActor;
use crate::{config::Config, interlude::*};
use colored::OwoColorize;
use futures::channel::oneshot;
use indexmap::IndexMap;
use pathdiff::diff_paths;
use signal_handler::set_stop_recipient;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use typegraph_core::Lib;

pub mod report;
pub use report::Report;
pub mod signal_handler;

pub mod message {
    use super::*;

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct AddTask {
        pub task_ref: TaskRef,
        pub reason: TaskReason,
    }

    #[derive(Message)]
    #[rtype(result = "()")]
    pub(super) struct NextTask;

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct TaskFinished<A: TaskAction + 'static> {
        pub task_ref: TaskRef,
        pub status: TaskFinishStatus<A>,
    }

    /// manual stop (by CTRL-C handler)
    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct Stop;

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct ForceStop;

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct Restart;

    #[derive(Message)]
    #[rtype(result = "()")]
    pub struct DiscoveryDone;
}

use message::*;

pub enum StopSchedule {
    Manual,
    Automatic,
}

#[derive(Clone, Debug)]
pub enum StopReason {
    Natural,
    Restart,
    Manual,
    ManualForced,
    Error,
}

#[derive(Clone, Debug)]
pub struct TaskGenerator {
    next_task_id: Arc<AtomicUsize>,
}

impl TaskGenerator {
    pub fn generate(&self, path: Arc<Path>, retry_no: usize) -> TaskRef {
        TaskRef {
            path,
            id: TaskId(self.next_task_id.fetch_add(1, Ordering::Relaxed)),
            retry_no,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskId(usize);

#[derive(Clone, Debug)]
pub struct TaskRef {
    pub path: Arc<Path>,
    pub id: TaskId,
    pub retry_no: usize,
}

// enum RetryStatus {
//     Pending,
//     Cancelled,
// }

pub enum TaskSource {
    Static(Vec<PathBuf>),
    Discovery(Arc<Path>),
    DiscoveryAndWatch(Arc<Path>),
}

pub struct TaskManager<A: TaskAction + 'static> {
    init_params: TaskManagerInit<A>,
    task_generator: TaskGenerator,
    active_tasks: HashMap<Arc<Path>, Addr<TaskActor<A>>>,
    task_queue: VecDeque<TaskRef>,
    pending_retries: HashMap<Arc<Path>, TaskId>,
    report_tx: Option<oneshot::Sender<Report<A>>>,
    stop_reason: Option<StopReason>,
    reports: IndexMap<Arc<Path>, TaskFinishStatus<A>>,
    watcher_addr: Option<Addr<WatcherActor<A>>>,
    console: ConsoleHandle,
    seen_tasks: usize,
    subscriptions: Vec<SubscriptionId>,
    actor_id: ActorId,
}

const DEFAULT_INITIAL_RETRY_INTERVAL: Duration = Duration::from_secs(3);

pub struct TaskManagerInit<A: TaskAction> {
    config: Arc<Config>,
    action_generator: A::Generator,
    max_parallel_tasks: usize,
    max_retry_count: usize,
    initial_retry_interval: Duration,
    console: ConsoleHandle,
    task_source: TaskSource,
}

impl<A: TaskAction + 'static> TaskManagerInit<A> {
    pub fn new(
        config: Arc<Config>,
        action_generator: A::Generator,
        console: ConsoleHandle,
        task_source: TaskSource,
    ) -> Self {
        Self {
            config,
            action_generator,
            max_parallel_tasks: num_cpus::get(),
            max_retry_count: 0,
            initial_retry_interval: DEFAULT_INITIAL_RETRY_INTERVAL,
            console,
            task_source,
        }
    }

    pub fn max_parallel_tasks(mut self, max_parallel_tasks: usize) -> Self {
        self.max_parallel_tasks = max_parallel_tasks;
        self
    }

    pub fn retry(mut self, max_retry_count: usize, initial_interval: Option<Duration>) -> Self {
        self.max_retry_count = max_retry_count;
        self.initial_retry_interval = initial_interval.unwrap_or(DEFAULT_INITIAL_RETRY_INTERVAL);
        self
    }

    pub async fn run(self) -> Report<A> {
        let (report_tx, report_rx) = oneshot::channel();

        TaskManager::<A>::create(move |ctx| {
            let addr = ctx.address();

            set_stop_recipient(addr.clone().recipient().downgrade());

            let task_generator = TaskGenerator {
                next_task_id: Arc::new(AtomicUsize::new(1)),
            };

            let watcher_addr = self.start_source(addr, task_generator.clone());

            let console = self.console.clone();

            TaskManager::<A> {
                init_params: self,
                task_generator,
                active_tasks: Default::default(),
                task_queue: Default::default(),
                pending_retries: Default::default(),
                report_tx: Some(report_tx),
                stop_reason: None,
                reports: IndexMap::new(),
                watcher_addr,
                console,
                seen_tasks: 0,
                subscriptions: Vec::new(),
                actor_id: ActorId::actor_type("TaskManager"),
            }
        });

        report_rx.await.expect("task manager has been dropped")
    }

    fn start_source(
        &self,
        addr: Addr<TaskManager<A>>,
        task_generator: TaskGenerator,
    ) -> Option<Addr<WatcherActor<A>>> {
        match &self.task_source {
            TaskSource::Static(paths) => {
                let working_dir = self
                    .action_generator
                    .get_shared_config()
                    .working_dir
                    .clone();
                for path in paths {
                    let relative_path = diff_paths(path, &working_dir);
                    addr.do_send(AddTask {
                        task_ref: task_generator
                            .generate(relative_path.unwrap_or_else(|| path.clone()).into(), 0),
                        reason: TaskReason::User,
                    });
                }
                None
            }
            TaskSource::Discovery(path) => {
                DiscoveryActor::new(
                    self.config.clone(),
                    task_generator.clone(),
                    self.console.clone(),
                    path.clone(),
                )
                .start();
                None
            }
            TaskSource::DiscoveryAndWatch(path) => {
                let path: Arc<Path> = path.clone();
                DiscoveryActor::new(
                    self.config.clone(),
                    task_generator.clone(),
                    self.console.clone(),
                    path.clone(),
                )
                .start();

                let watcher = WatcherActor::new(
                    self.config.clone(),
                    path,
                    task_generator.clone(),
                    self.console.clone(),
                )
                .unwrap_or_log()
                .start();

                Some(watcher)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum TaskReason {
    User, // single file specified with the -f option
    Discovery,
    FileChanged,
    // FileCreated,
    DependencyChanged(PathBuf),
    Retry(usize),
}

impl<A: TaskAction + 'static> Actor for TaskManager<A> {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // this cannot mess with the interactive deployment
        self.console.debug("started task manager".to_string());

        let bus = self.console.event_bus();
        let recipient = ctx.address().recipient();
        let fut = async move {
            let mut ids = Vec::new();
            ids.push(
                bus.send(EventSubscription::new(
                    TASK_FINISHED_EVENT_TYPE,
                    recipient.clone(),
                    None,
                ))
                .await?,
            );
            ids.push(
                bus.send(EventSubscription::new(
                    DISCOVERY_DONE_EVENT_TYPE,
                    recipient.clone(),
                    None,
                ))
                .await?,
            );
            ids.push(
                bus.send(EventSubscription::new(
                    WATCHER_EVENT_TYPE,
                    recipient.clone(),
                    None,
                ))
                .await?,
            );
            ids.push(
                bus.send(EventSubscription::new(
                    TASK_MANAGER_EVENT_TYPE,
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
                        // Only handle events targeted at TaskManager
                        if let Some(event) = envelope.downcast_ref::<ActorCommunicationEvent>() {
                            event.target_actor == "TaskManager"
                        } else {
                            false
                        }
                    })),
                ))
                .await?,
            );
            ids.push(
                bus.send(EventSubscription::new(
                    NEXT_TASK_EVENT_TYPE,
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
                    .error(format!("failed to subscribe task manager: {err}")),
            }),
        );
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        match &self.stop_reason {
            Some(StopReason::Restart) => {
                self.stop_reason.take();
                self.watcher_addr = self
                    .init_params
                    .start_source(ctx.address(), self.task_generator.clone());
                Running::Continue
            }
            Some(_) => Running::Stop,
            None => Running::Continue,
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        trace!("TaskManager stopped");
        for id in self.subscriptions.drain(..) {
            self.console.event_bus().do_send(EventUnsubscribe { id });
        }
        // send report
        let report = Report {
            stop_reason: self
                .stop_reason
                .take()
                .ok_or_else(|| eyre::eyre!("missing stop reason in task manager"))
                .unwrap_or_log(),
            entries: std::mem::take(&mut self.reports)
                .into_iter()
                .map(|(path, status)| report::ReportEntry { path, status })
                .collect(),
        };

        self.report_tx.take().unwrap().send(report).unwrap_or_log();
    }
}

impl<A: TaskAction + 'static> TaskManager<A> {
    fn handle_task_finished(
        &mut self,
        message: TaskFinished<A>,
        ctx: &mut Context<Self>,
    ) {
        self.console.debug("task finished".to_string());
        self.active_tasks.remove(&message.task_ref.path);
        self.publish_to_central(NextTaskEvent::new(Some("TaskManager".to_string())));

        let next_retry_no: Option<usize> =
            if message.task_ref.retry_no < self.init_params.max_retry_count {
                match &message.status {
                    TaskFinishStatus::Error => Some(message.task_ref.retry_no + 1),
                    TaskFinishStatus::Finished(results) => {
                        // TODO partial retry - if multiple typegraphs in a single file
                        if results.iter().any(|r| r.1.is_err()) {
                            Some(message.task_ref.retry_no + 1)
                        } else {
                            None
                        }
                    }
                    TaskFinishStatus::Cancelled => None,
                }
            } else {
                None
            };

        self.reports
            .insert(message.task_ref.path.clone(), message.status);

        if let Some(retry_no) = next_retry_no {
            let path = message.task_ref.path;
            let task_ref = self.task_generator.generate(path.clone(), retry_no);
            let task_manager = ctx.address();
            self.pending_retries.insert(path.clone(), task_ref.id);

            let retry_interval = self.init_params.initial_retry_interval * (retry_no as u32);

            let fut = async move {
                tokio::time::sleep(retry_interval).await;
                CentralEventBus::get().publish(ActorCommunicationEvent::new(
                    "TaskManager".to_string(),
                    ActorCommunicationCommand::AddTask { 
                        task_ref, 
                        reason: TaskReason::Retry(retry_no) 
                    },
                    Some("TaskManager".to_string()),
                ));
            };
            ctx.spawn(fut.in_current_span().into_actor(self));
        }

        if self.task_queue.is_empty() && self.active_tasks.is_empty() {
            if self.watcher_addr.is_none() && self.pending_retries.is_empty() {
                // no watcher, auto stop when all tasks finished
                self.console.debug("all tasks finished".to_string());
                self.stop_reason = Some(StopReason::Natural);
                ctx.stop();
            } else if let Some(StopReason::Manual) = self.stop_reason {
                ctx.stop();
            }
        }
    }

    fn handle_discovery_done(&mut self, ctx: &mut Context<Self>) {
        self.console.debug("discovery done".to_string());

        if self.task_queue.is_empty() && self.active_tasks.is_empty() {
            if self.seen_tasks == 0 {
                self.console.error("no typegraphs discovered".to_string());
                self.stop_reason = Some(StopReason::Error);
                ctx.stop();
            } else if self.watcher_addr.is_none() && self.pending_retries.is_empty() {
                // no watcher, auto stop when all tasks finished
                self.console.debug("all tasks finished".to_string());
                self.stop_reason = Some(StopReason::Natural);
                ctx.stop();
            } else if let Some(StopReason::Manual) = self.stop_reason {
                ctx.stop();
            }
        }
    }

    fn handle_watcher_event(&mut self, event: &WatcherEvent, ctx: &mut Context<Self>) {
        match &event.kind {
            WatcherEventKind::ConfigChanged { .. } => {
                self.publish_to_central(ActorCommunicationEvent::new(
                    "TaskManager".to_string(),
                    ActorCommunicationCommand::Restart,
                    Some("TaskManager".to_string()),
                ));
            }
            WatcherEventKind::DependencyChanged {
                typegraph_module,
                dependency_path,
            } => {
                self.publish_to_central(ActorCommunicationEvent::new(
                    "TaskManager".to_string(),
                    ActorCommunicationCommand::AddTask {
                        task_ref: self.task_generator.generate(typegraph_module.clone().into(), 0),
                        reason: TaskReason::DependencyChanged(dependency_path.clone()),
                    },
                    Some("TaskManager".to_string()),
                ));
            }
            WatcherEventKind::TypegraphModuleChanged { typegraph_module } => {
                self.publish_to_central(ActorCommunicationEvent::new(
                    "TaskManager".to_string(),
                    ActorCommunicationCommand::AddTask {
                        task_ref: self.task_generator.generate(typegraph_module.clone().into(), 0),
                        reason: TaskReason::FileChanged,
                    },
                    Some("TaskManager".to_string()),
                ));
            }
            WatcherEventKind::TypegraphModuleDeleted { .. } => {}
        }
    }
}

impl<A: TaskAction + 'static> Handler<AddTask> for TaskManager<A> {
    type Result = ();

    fn handle(&mut self, msg: AddTask, ctx: &mut Context<Self>) -> Self::Result {
        let pending_retry_id = self.pending_retries.remove(&msg.task_ref.path);

        if msg.task_ref.retry_no > 0 {
            if let Some(retry_task_id) = pending_retry_id {
                if retry_task_id != msg.task_ref.id {
                    // unreachable
                    self.console.warning(
                        "invalid state: different task id for retry; cancelling".to_string(),
                    );
                    return;
                }
                // ok: this task is a retry
            } else {
                self.console
                    .warning("invalid state: unregistered retry; cancelling".to_string());
                return;
            }
        } else if pending_retry_id.is_some() {
            // ok: this task is not a retry; pending retry cancelled
            self.console.warning(format!(
                "pending retry task for {path} cancelled",
                path = msg.task_ref.path.display().to_string().yellow(),
            ));
        } else {
            // ok: new task
        }

        self.seen_tasks += 1;
        self.task_queue.push_back(msg.task_ref);
        self.publish_to_central(NextTaskEvent::new(Some("TaskManager".to_string())));
    }
}

impl<A: TaskAction + 'static> Handler<NextTask> for TaskManager<A> {
    type Result = ();

    fn handle(&mut self, _msg: NextTask, ctx: &mut Context<Self>) -> Self::Result {
        if self.active_tasks.len() >= self.init_params.max_parallel_tasks {
            // too busy
            return;
        }

        let Some(task) = self.task_queue.pop_front() else {
            // nothing to do
            return;
        };

        // FIXME: Temporary workaround until refactoring the global state in core
        Lib::reset();

        if let Some(stop_reason) = &self.stop_reason {
            match stop_reason {
                StopReason::Natural => {}
                _ => {
                    self.console
                        .warning(format!("task cancelled for {:?}", task.path));
                    return;
                }
            }
        }

        let action = self.init_params.action_generator.generate(
            task.clone(),
            Default::default(), // TODO
        );
        let task_addr = TaskActor::new(
            self.init_params.config.clone(),
            self.init_params.action_generator.clone(),
            action,
            ctx.address(),
            self.console.clone(),
            self.init_params.max_retry_count,
        )
        .start();

        self.active_tasks.insert(task.path, task_addr);
    }
}

impl<A: TaskAction + 'static> Handler<TaskFinished<A>> for TaskManager<A> {
    type Result = ();

    fn handle(&mut self, message: TaskFinished<A>, ctx: &mut Context<Self>) -> Self::Result {
        self.handle_task_finished(message, ctx);
    }
}

impl<A: TaskAction + 'static> Handler<DiscoveryDone> for TaskManager<A> {
    type Result = ();

    fn handle(&mut self, _: DiscoveryDone, ctx: &mut Context<Self>) -> Self::Result {
        self.handle_discovery_done(ctx);
    }
}

impl<A: TaskAction + 'static> Handler<EventEnvelope> for TaskManager<A> {
    type Result = EventAck;

    fn handle(&mut self, msg: EventEnvelope, ctx: &mut Context<Self>) -> Self::Result {
        if let Some(event) = msg.downcast_ref::<TaskFinishedEvent<A>>() {
            self.handle_task_finished(
                TaskFinished {
                    task_ref: event.task_ref.clone(),
                    status: event.status.clone(),
                },
                ctx,
            );
            return EventAck::new(msg.event_id());
        }

        if msg.downcast_ref::<DiscoveryDoneEvent>().is_some() {
            self.handle_discovery_done(ctx);
            return EventAck::new(msg.event_id());
        }

        if let Some(event) = msg.downcast_ref::<WatcherEvent>() {
            self.handle_watcher_event(event, ctx);
            return EventAck::new(msg.event_id());
        }

        if let Some(event) = msg.downcast_ref::<TaskManagerEvent>() {
            match &event.command {
                TaskManagerCommand::AddTask { task_ref, reason } => {
                    self.publish_to_central(ActorCommunicationEvent::new(
                        "TaskManager".to_string(),
                        ActorCommunicationCommand::AddTask {
                            task_ref: task_ref.clone(),
                            reason: reason.clone(),
                        },
                        Some("TaskManager".to_string()),
                    ));
                }
                TaskManagerCommand::Restart => {
                    self.publish_to_central(ActorCommunicationEvent::new(
                        "TaskManager".to_string(),
                        ActorCommunicationCommand::Restart,
                        Some("TaskManager".to_string()),
                    ));
                }
            }
            return EventAck::new(msg.event_id());
        }

        if let Some(event) = msg.downcast_ref::<ActorCommunicationEvent>() {
            if event.target_actor == "TaskManager" {
                match &event.command {
                    ActorCommunicationCommand::AddTask { task_ref, reason } => {
                        ctx.address().do_send(AddTask {
                            task_ref: task_ref.clone(),
                            reason: reason.clone(),
                        });
                    }
                    ActorCommunicationCommand::NextTask => {
                        ctx.address().do_send(NextTask);
                    }
                    ActorCommunicationCommand::Restart => {
                        ctx.address().do_send(Restart);
                    }
                    ActorCommunicationCommand::Stop => {
                        ctx.address().do_send(Stop);
                    }
                    ActorCommunicationCommand::ForceStop => {
                        ctx.address().do_send(ForceStop);
                    }
                    _ => {} // Other commands not handled by TaskManager
                }
            }
            return EventAck::new(msg.event_id());
        }

        if msg.downcast_ref::<NextTaskEvent>().is_some() {
            ctx.address().do_send(NextTask);
            return EventAck::new(msg.event_id());
        }

        EventAck::new(msg.event_id())
    }
}

impl<A: TaskAction + 'static> Handler<NewMessageEnvelope> for TaskManager<A> {
    type Result = ();

    fn handle(&mut self, msg: NewMessageEnvelope, ctx: &mut Context<Self>) -> Self::Result {
        let handler = self.create_message_handler(self.actor_id.clone());
        let message_id = msg.message_id();
        
        if let Some(event) = msg.downcast_event::<TaskFinishedEvent<A>>() {
            self.handle_task_finished(
                TaskFinished {
                    task_ref: event.task_ref.clone(),
                    status: event.status.clone(),
                },
                ctx,
            );
            handler.confirm_success(message_id);
            return;
        }

        if msg.downcast_event::<DiscoveryDoneEvent>().is_some() {
            self.handle_discovery_done(ctx);
            handler.confirm_success(message_id);
            return;
        }

        if let Some(event) = msg.downcast_event::<WatcherEvent>() {
            self.handle_watcher_event(event, ctx);
            handler.confirm_success(message_id);
            return;
        }

        if let Some(event) = msg.downcast_event::<TaskManagerEvent>() {
            match &event.command {
                TaskManagerCommand::AddTask { task_ref, reason } => {
                    self.publish_to_central(ActorCommunicationEvent::new(
                        "TaskManager".to_string(),
                        ActorCommunicationCommand::AddTask {
                            task_ref: task_ref.clone(),
                            reason: reason.clone(),
                        },
                        Some("TaskManager".to_string()),
                    ));
                }
                TaskManagerCommand::Restart => {
                    self.publish_to_central(ActorCommunicationEvent::new(
                        "TaskManager".to_string(),
                        ActorCommunicationCommand::Restart,
                        Some("TaskManager".to_string()),
                    ));
                }
            }
            handler.confirm_success(message_id);
            return;
        }

        if let Some(event) = msg.downcast_event::<ActorCommunicationEvent>() {
            if event.target_actor == "TaskManager" {
                match &event.command {
                    ActorCommunicationCommand::AddTask { task_ref, reason } => {
                        ctx.address().do_send(AddTask {
                            task_ref: task_ref.clone(),
                            reason: reason.clone(),
                        });
                    }
                    ActorCommunicationCommand::NextTask => {
                        ctx.address().do_send(NextTask);
                    }
                    ActorCommunicationCommand::Restart => {
                        ctx.address().do_send(Restart);
                    }
                    ActorCommunicationCommand::Stop => {
                        ctx.address().do_send(Stop);
                    }
                    ActorCommunicationCommand::ForceStop => {
                        ctx.address().do_send(ForceStop);
                    }
                    _ => {}
                }
                handler.confirm_success(message_id);
                return;
            }
        }

        if msg.downcast_event::<NextTaskEvent>().is_some() {
            ctx.address().do_send(NextTask);
            handler.confirm_success(message_id);
            return;
        }

        handler.confirm_error(message_id, "Unknown message type".to_string());
    }
}

impl<A: TaskAction + 'static> Handler<Stop> for TaskManager<A> {
    type Result = ();

    fn handle(&mut self, _msg: Stop, ctx: &mut Context<Self>) -> Self::Result {
        if let Some(_watcher) = &self.watcher_addr {
            // Send stop command to watcher through event bus
            self.publish_to_central(ActorCommunicationEvent::new(
                "WatcherActor".to_string(),
                ActorCommunicationCommand::Stop,
                Some("TaskManager".to_string()),
            ));
        }
        match self.stop_reason.clone() {
            Some(reason) => match reason {
                StopReason::Natural | StopReason::Restart => {
                    self.stop_reason = Some(StopReason::Manual);
                }
                StopReason::Manual => {
                    self.stop_reason = Some(StopReason::ManualForced);
                    self.publish_to_central(ActorCommunicationEvent::new(
                        "TaskManager".to_string(),
                        ActorCommunicationCommand::ForceStop,
                        Some("TaskManager".to_string()),
                    ));
                }
                StopReason::ManualForced => {
                    self.console
                        .warning("stopping the task manager".to_string());
                    ctx.stop();
                }
                StopReason::Error => {}
            },
            None => {
                self.stop_reason = Some(StopReason::Manual);
            }
        }
    }
}

impl<A: TaskAction + 'static> Handler<ForceStop> for TaskManager<A> {
    type Result = ();

    fn handle(&mut self, _msg: ForceStop, _ctx: &mut Context<Self>) -> Self::Result {
        self.console
            .warning("force stopping active tasks".to_string());
        for (path, _addr) in self.active_tasks.iter() {
            self.publish_to_central(ActorCommunicationEvent::new(
                format!("TaskActor:{}", path.display()),
                ActorCommunicationCommand::Stop,
                Some("TaskManager".to_string()),
            ));
        }
    }
}

impl<A: TaskAction + 'static> Handler<Restart> for TaskManager<A> {
    type Result = ();

    fn handle(&mut self, _msg: Restart, ctx: &mut Context<Self>) -> Self::Result {
        self.stop_reason = Some(StopReason::Restart);
        self.publish_to_central(ActorCommunicationEvent::new(
            "TaskManager".to_string(),
            ActorCommunicationCommand::ForceStop,
            Some("TaskManager".to_string()),
        ));
    }
}
