// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

use super::event_bus::{Event, EventMetadata, EventType};
use super::message_system::{ActorId, Message, MessageMetadata, MessagePriority};
use super::task::TaskFinishStatus;
use super::task::deploy::TypegraphData;
use super::task_manager::{TaskReason, TaskRef};
use crate::deploy::actors::task::action::TaskAction;
use crate::interlude::*;
use std::any::Any;

pub const LOG_EVENT_TYPE: EventType = EventType::new("log");
pub const TASK_FINISHED_EVENT_TYPE: EventType = EventType::new("task.finished");
pub const DISCOVERY_DONE_EVENT_TYPE: EventType = EventType::new("discovery.done");
pub const WATCHER_EVENT_TYPE: EventType = EventType::new("watcher.event");
pub const WATCHER_UPDATE_EVENT_TYPE: EventType = EventType::new("watcher.update");
pub const TYPEGATE_EVENT_TYPE: EventType = EventType::new("typegate.state");
pub const TASK_PROGRESS_EVENT_TYPE: EventType = EventType::new("task.progress");
pub const TASK_MANAGER_EVENT_TYPE: EventType = EventType::new("task.manager.command");
pub const ACTOR_COMMUNICATION_EVENT_TYPE: EventType = EventType::new("actor.communication");
pub const TASK_ACTOR_EVENT_TYPE: EventType = EventType::new("task.actor.command");
pub const WATCHER_FILE_EVENT_TYPE: EventType = EventType::new("watcher.file");
pub const NEXT_TASK_EVENT_TYPE: EventType = EventType::new("task.manager.next");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug)]
pub struct LogEvent {
    metadata: EventMetadata,
    pub level: LogLevel,
    pub message: String,
}

impl LogEvent {
    pub fn new(level: LogLevel, message: String, source: Option<String>) -> Self {
        Self {
            metadata: EventMetadata::new(source),
            level,
            message,
        }
    }
}

impl Event for LogEvent {
    fn event_type(&self) -> EventType {
        LOG_EVENT_TYPE
    }

    fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct TaskFinishedEvent<A: TaskAction + 'static> {
    metadata: EventMetadata,
    pub task_ref: TaskRef,
    pub status: TaskFinishStatus<A>,
}

impl<A: TaskAction + 'static> TaskFinishedEvent<A> {
    pub fn new(task_ref: TaskRef, status: TaskFinishStatus<A>, source: Option<String>) -> Self {
        Self {
            metadata: EventMetadata::new(source),
            task_ref,
            status,
        }
    }
}

impl<A: TaskAction + 'static> Event for TaskFinishedEvent<A> {
    fn event_type(&self) -> EventType {
        TASK_FINISHED_EVENT_TYPE
    }

    fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub struct DiscoveryDoneEvent {
    metadata: EventMetadata,
    pub directory: Arc<Path>,
}

impl DiscoveryDoneEvent {
    pub fn new(directory: Arc<Path>, source: Option<String>) -> Self {
        Self {
            metadata: EventMetadata::new(source),
            directory,
        }
    }
}

impl Event for DiscoveryDoneEvent {
    fn event_type(&self) -> EventType {
        DISCOVERY_DONE_EVENT_TYPE
    }

    fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub enum WatcherEventKind {
    ConfigChanged { path: PathBuf },
    DependencyChanged {
        typegraph_module: PathBuf,
        dependency_path: PathBuf,
    },
    TypegraphModuleChanged { typegraph_module: PathBuf },
    TypegraphModuleDeleted { typegraph_module: PathBuf },
}

#[derive(Clone, Debug)]
pub struct WatcherEvent {
    metadata: EventMetadata,
    pub kind: WatcherEventKind,
}

impl WatcherEvent {
    pub fn new(kind: WatcherEventKind, source: Option<String>) -> Self {
        Self {
            metadata: EventMetadata::new(source),
            kind,
        }
    }
}

impl Event for WatcherEvent {
    fn event_type(&self) -> EventType {
        WATCHER_EVENT_TYPE
    }

    fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub enum TypegateState {
    Starting,
    Ready { port: u16 },
    Stopped,
    Error { message: String },
}

#[derive(Clone, Debug)]
pub struct TypegateEvent {
    metadata: EventMetadata,
    pub state: TypegateState,
}

impl TypegateEvent {
    pub fn new(state: TypegateState, source: Option<String>) -> Self {
        Self {
            metadata: EventMetadata::new(source),
            state,
        }
    }
}

impl Event for TypegateEvent {
    fn event_type(&self) -> EventType {
        TYPEGATE_EVENT_TYPE
    }

    fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub enum TaskProgressStage {
    Started,
    WaitingForExit,
    Stopping,
}

#[derive(Clone, Debug)]
pub struct TaskProgressEvent {
    metadata: EventMetadata,
    pub task_ref: TaskRef,
    pub stage: TaskProgressStage,
}

impl TaskProgressEvent {
    pub fn new(task_ref: TaskRef, stage: TaskProgressStage, source: Option<String>) -> Self {
        Self {
            metadata: EventMetadata::new(source),
            task_ref,
            stage,
        }
    }
}

impl Event for TaskProgressEvent {
    fn event_type(&self) -> EventType {
        TASK_PROGRESS_EVENT_TYPE
    }

    fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub enum TaskManagerCommand {
    AddTask { task_ref: TaskRef, reason: TaskReason },
    Restart,
}

#[derive(Clone, Debug)]
pub struct TaskManagerEvent {
    metadata: EventMetadata,
    pub command: TaskManagerCommand,
}

impl TaskManagerEvent {
    pub fn new(command: TaskManagerCommand, source: Option<String>) -> Self {
        Self {
            metadata: EventMetadata::new(source),
            command,
        }
    }
}

impl Event for TaskManagerEvent {
    fn event_type(&self) -> EventType {
        TASK_MANAGER_EVENT_TYPE
    }

    fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub enum WatcherUpdateKind {
    Dependencies(TypegraphData),
}

#[derive(Clone, Debug)]
pub struct WatcherUpdateEvent {
    metadata: EventMetadata,
    pub update: WatcherUpdateKind,
}

impl WatcherUpdateEvent {
    pub fn new(update: WatcherUpdateKind, source: Option<String>) -> Self {
        Self {
            metadata: EventMetadata::new(source),
            update,
        }
    }
}

impl Event for WatcherUpdateEvent {
    fn event_type(&self) -> EventType {
        WATCHER_UPDATE_EVENT_TYPE
    }

    fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Event for generic actor-to-actor communication through the central bus
#[derive(Clone, Debug)]
pub enum ActorCommunicationCommand {
    AddTask { task_ref: TaskRef, reason: TaskReason },
    NextTask,
    TaskFinished { task_ref: TaskRef, status: String }, // Simplified for now
    Stop,
    ForceStop,
    Restart,
    FileChanged { path: PathBuf },
    RemoveTypegraph { path: PathBuf },
}

#[derive(Clone, Debug)]
pub struct ActorCommunicationEvent {
    metadata: EventMetadata,
    pub target_actor: String, // Which actor this message is for
    pub command: ActorCommunicationCommand,
}

impl ActorCommunicationEvent {
    pub fn new(target_actor: String, command: ActorCommunicationCommand, source: Option<String>) -> Self {
        Self {
            metadata: EventMetadata::new(source),
            target_actor,
            command,
        }
    }
}

impl Event for ActorCommunicationEvent {
    fn event_type(&self) -> EventType {
        ACTOR_COMMUNICATION_EVENT_TYPE
    }

    fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Event for file system changes from watcher
#[derive(Clone, Debug)]
pub struct WatcherFileEvent {
    metadata: EventMetadata,
    pub path: PathBuf,
}

impl WatcherFileEvent {
    pub fn new(path: PathBuf, source: Option<String>) -> Self {
        Self {
            metadata: EventMetadata::new(source),
            path,
        }
    }
}

impl Event for WatcherFileEvent {
    fn event_type(&self) -> EventType {
        WATCHER_FILE_EVENT_TYPE
    }

    fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Event for requesting next task processing
#[derive(Clone, Debug)]
pub struct NextTaskEvent {
    metadata: EventMetadata,
}

impl NextTaskEvent {
    pub fn new(source: Option<String>) -> Self {
        Self {
            metadata: EventMetadata::new(source),
        }
    }
}

impl Event for NextTaskEvent {
    fn event_type(&self) -> EventType {
        NEXT_TASK_EVENT_TYPE
    }

    fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
