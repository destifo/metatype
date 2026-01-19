// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

use crate::deploy::actors::task_manager::TaskReason;
use crate::interlude::*;

use pathdiff::diff_paths;

use crate::{config::Config, typegraph::loader::discovery::Discovery};

use super::console::{Console, ConsoleHandle};
use super::events::{DiscoveryDoneEvent, TaskManagerCommand, TaskManagerEvent};
use super::task_manager::TaskGenerator;
use crate::deploy::actors::event_bus::EventBusExt;

pub struct DiscoveryActor {
    config: Arc<Config>,
    task_generator: TaskGenerator,
    console: ConsoleHandle,
    directory: Arc<Path>,
}

impl DiscoveryActor {
    pub fn new(
        config: Arc<Config>,
        task_generator: TaskGenerator,
        console: ConsoleHandle,
        directory: Arc<Path>,
    ) -> Self {
        Self {
            config,
            task_generator,
            console,
            directory,
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Stop;

impl Actor for DiscoveryActor {
    type Context = Context<Self>;

    #[tracing::instrument(skip(self))]
    fn started(&mut self, ctx: &mut Self::Context) {
        log::trace!("DiscoveryActor started; directory={:?}", self.directory);
        let config = Arc::clone(&self.config);
        let dir = self.directory.clone();
        let console = self.console.clone();
        let event_bus = self.console.event_bus();
        let discovery = ctx.address();
        let task_generator = self.task_generator.clone();

        console.info("starting discovery".to_string());
        console.warning("make sure to exclude all non-typegraph Python and TypeScript/JavaScript files in the metatype.yaml config file using the include/exclude patterns".to_string());

        let fut = async move {
            match Discovery::new(config, dir.to_path_buf())
                .start(|path| match path {
                    Ok(path) => {
                        let rel_path = diff_paths(path, &dir).unwrap();
                        console.debug(format!("discovered typegraph definition at {rel_path:?}"));
                        event_bus.publish(TaskManagerEvent::new(
                            TaskManagerCommand::AddTask {
                                task_ref: task_generator.generate(rel_path.into(), 0),
                                reason: TaskReason::Discovery,
                            },
                            Some("DiscoveryActor".to_string()),
                        ));
                    }
                    Err(err) => console.error(format!("Error while discovering modules: {}", err)),
                })
                .await
            {
                Ok(_) => (),
                Err(err) => console.error(format!("Error while discovering modules: {}", err)),
            }
            event_bus.publish(DiscoveryDoneEvent::new(
                dir.clone(),
                Some("DiscoveryActor".to_string()),
            ));
            discovery.do_send(Stop);
        }
        .in_current_span();
        ctx.spawn(fut.into_actor(self));
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        trace!("DiscoveryActor stopped");
    }
}

impl Handler<Stop> for DiscoveryActor {
    type Result = ();

    fn handle(&mut self, msg: Stop, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            Stop => ctx.stop(),
        }
    }
}
