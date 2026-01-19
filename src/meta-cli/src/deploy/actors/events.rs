// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

use super::event_bus::{Event, EventMetadata, EventType};
use super::task::deploy::TypegraphData;
use super::task::TaskFinishStatus;
use super::task_manager::{TaskReason, TaskRef};
use crate::deploy::actors::task::action::TaskAction;
use crate::interlude::*;
use std::any::Any;

pub const LOG_EVENT_TYPE: EventType = EventType::new("log");
pub const DISCOVERY_DONE_EVENT_TYPE: EventType = EventType::new("discovery.done");
pub const WATCHER_EVENT_TYPE: EventType = EventType::new("watcher.event");
pub const WATCHER_UPDATE_EVENT_TYPE: EventType = EventType::new("watcher.update");
pub const TYPEGATE_EVENT_TYPE: EventType = EventType::new("typegate.state");
pub const TASK_PROGRESS_EVENT_TYPE: EventType = EventType::new("task.progress");
pub const TASK_MANAGER_EVENT_TYPE: EventType = EventType::new("task.manager.command");

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
    ConfigChanged {
        path: PathBuf,
    },
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
    AddTask {
        task_ref: TaskRef,
        reason: TaskReason,
    },
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
