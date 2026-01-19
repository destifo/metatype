// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

//! Concrete event types for the actor system
//!
//! This module defines all the events that can flow through the event bus.
//! Events are simple data structures that carry information between actors.

use super::event_bus::{generate_event_id, Event, EventId};
use super::task::deploy::TypegraphData;
use super::task_manager::TaskRef;
use crate::interlude::*;
use std::time::Instant;

/// Event types for the system - used as identifiers for subscription
pub mod event_types {
    pub const ADD_TASK: &str = "add_task";
    pub const TASK_FINISHED: &str = "task_finished";
    pub const STOP: &str = "stop";
    pub const FORCE_STOP: &str = "force_stop";
    pub const RESTART: &str = "restart";
    pub const DISCOVERY_DONE: &str = "discovery_done";
    pub const TYPEGRAPH_DEPLOYED: &str = "typegraph_deployed";
    pub const FILE_CHANGED: &str = "file_changed";
    pub const UPDATE_DEPENDENCIES: &str = "update_dependencies";
    pub const CONSOLE_MESSAGE: &str = "console_message";
    pub const WATCHER_STOP: &str = "watcher_stop";
    pub const TASK_STOP: &str = "task_stop";
}

/// Reason for adding a task
#[derive(Debug, Clone)]
pub enum TaskReasonEvent {
    User,
    Discovery,
    FileChanged,
    DependencyChanged(PathBuf),
    Retry(usize),
}

/// Event for adding a new task to the task manager
#[derive(Debug)]
pub struct AddTaskEvent {
    id: EventId,
    timestamp: Instant,
    pub task_ref: TaskRef,
    pub reason: TaskReasonEvent,
}

impl AddTaskEvent {
    pub fn new(task_ref: TaskRef, reason: TaskReasonEvent) -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
            task_ref,
            reason,
        }
    }
}

impl Event for AddTaskEvent {
    fn event_type(&self) -> &'static str {
        event_types::ADD_TASK
    }

    fn timestamp(&self) -> Instant {
        self.timestamp
    }

    fn event_id(&self) -> EventId {
        self.id
    }
}

/// Event for manual stop (by CTRL-C handler)
#[derive(Debug)]
pub struct StopEvent {
    id: EventId,
    timestamp: Instant,
}

impl StopEvent {
    pub fn new() -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
        }
    }
}

impl Default for StopEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl Event for StopEvent {
    fn event_type(&self) -> &'static str {
        event_types::STOP
    }

    fn timestamp(&self) -> Instant {
        self.timestamp
    }

    fn event_id(&self) -> EventId {
        self.id
    }
}

/// Event for force stop
#[derive(Debug)]
pub struct ForceStopEvent {
    id: EventId,
    timestamp: Instant,
}

impl ForceStopEvent {
    pub fn new() -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
        }
    }
}

impl Default for ForceStopEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl Event for ForceStopEvent {
    fn event_type(&self) -> &'static str {
        event_types::FORCE_STOP
    }

    fn timestamp(&self) -> Instant {
        self.timestamp
    }

    fn event_id(&self) -> EventId {
        self.id
    }
}

/// Event for restart
#[derive(Debug)]
pub struct RestartEvent {
    id: EventId,
    timestamp: Instant,
}

impl RestartEvent {
    pub fn new() -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
        }
    }
}

impl Default for RestartEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl Event for RestartEvent {
    fn event_type(&self) -> &'static str {
        event_types::RESTART
    }

    fn timestamp(&self) -> Instant {
        self.timestamp
    }

    fn event_id(&self) -> EventId {
        self.id
    }
}

/// Event for discovery done
#[derive(Debug)]
pub struct DiscoveryDoneEvent {
    id: EventId,
    timestamp: Instant,
}

impl DiscoveryDoneEvent {
    pub fn new() -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
        }
    }
}

impl Default for DiscoveryDoneEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl Event for DiscoveryDoneEvent {
    fn event_type(&self) -> &'static str {
        event_types::DISCOVERY_DONE
    }

    fn timestamp(&self) -> Instant {
        self.timestamp
    }

    fn event_id(&self) -> EventId {
        self.id
    }
}

/// Event for typegraph deployed
#[derive(Debug)]
pub struct TypegraphDeployedEvent {
    id: EventId,
    timestamp: Instant,
    pub data: TypegraphData,
}

impl TypegraphDeployedEvent {
    pub fn new(data: TypegraphData) -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
            data,
        }
    }
}

impl Event for TypegraphDeployedEvent {
    fn event_type(&self) -> &'static str {
        event_types::TYPEGRAPH_DEPLOYED
    }

    fn timestamp(&self) -> Instant {
        self.timestamp
    }

    fn event_id(&self) -> EventId {
        self.id
    }
}

/// Event for file changed (watcher)
#[derive(Debug)]
pub struct FileChangedEvent {
    id: EventId,
    timestamp: Instant,
    pub path: PathBuf,
}

impl FileChangedEvent {
    pub fn new(path: PathBuf) -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
            path,
        }
    }
}

impl Event for FileChangedEvent {
    fn event_type(&self) -> &'static str {
        event_types::FILE_CHANGED
    }

    fn timestamp(&self) -> Instant {
        self.timestamp
    }

    fn event_id(&self) -> EventId {
        self.id
    }
}

/// Event for updating dependencies
#[derive(Debug)]
pub struct UpdateDependenciesEvent {
    id: EventId,
    timestamp: Instant,
    pub data: TypegraphData,
}

impl UpdateDependenciesEvent {
    pub fn new(data: TypegraphData) -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
            data,
        }
    }
}

impl Event for UpdateDependenciesEvent {
    fn event_type(&self) -> &'static str {
        event_types::UPDATE_DEPENDENCIES
    }

    fn timestamp(&self) -> Instant {
        self.timestamp
    }

    fn event_id(&self) -> EventId {
        self.id
    }
}

/// Console log levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsoleLevel {
    Debug,
    Info,
    Warning,
    Error,
}

/// Console event for logging
#[derive(Debug)]
pub struct ConsoleEvent {
    id: EventId,
    timestamp: Instant,
    pub level: ConsoleLevel,
    pub message: String,
}

impl ConsoleEvent {
    pub fn debug(message: impl Into<String>) -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
            level: ConsoleLevel::Debug,
            message: message.into(),
        }
    }

    pub fn info(message: impl Into<String>) -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
            level: ConsoleLevel::Info,
            message: message.into(),
        }
    }

    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
            level: ConsoleLevel::Warning,
            message: message.into(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
            level: ConsoleLevel::Error,
            message: message.into(),
        }
    }
}

impl Event for ConsoleEvent {
    fn event_type(&self) -> &'static str {
        event_types::CONSOLE_MESSAGE
    }

    fn timestamp(&self) -> Instant {
        self.timestamp
    }

    fn event_id(&self) -> EventId {
        self.id
    }
}

/// Watcher stop event
#[derive(Debug)]
pub struct WatcherStopEvent {
    id: EventId,
    timestamp: Instant,
}

impl WatcherStopEvent {
    pub fn new() -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
        }
    }
}

impl Default for WatcherStopEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl Event for WatcherStopEvent {
    fn event_type(&self) -> &'static str {
        event_types::WATCHER_STOP
    }

    fn timestamp(&self) -> Instant {
        self.timestamp
    }

    fn event_id(&self) -> EventId {
        self.id
    }
}

/// Task stop event
#[derive(Debug)]
pub struct TaskStopEvent {
    id: EventId,
    timestamp: Instant,
}

impl TaskStopEvent {
    pub fn new() -> Self {
        Self {
            id: generate_event_id(),
            timestamp: Instant::now(),
        }
    }
}

impl Default for TaskStopEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl Event for TaskStopEvent {
    fn event_type(&self) -> &'static str {
        event_types::TASK_STOP
    }

    fn timestamp(&self) -> Instant {
        self.timestamp
    }

    fn event_id(&self) -> EventId {
        self.id
    }
}
