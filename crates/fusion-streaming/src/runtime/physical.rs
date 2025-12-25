use crate::network::channel::TaskChannel;
use crate::runtime::core::{GraphContext, LuaRow};
use crate::runtime::{EVENT_TYPE_EOF, EVENT_TYPE_START};
use crate::task::types::{TaskCore, UnitTask};
use fusion_unit_sdk::graph::types::{ComputingUnit, Context, EdgeCondition, Watermark};
use fusion_unit_sdk::proto::transfer::Row;
use fusion_unit_sdk::runtime::UnitResult;
use fusion_unit_sdk::runtime::logical::LogicalTask;
use fusion_unit_sdk::units::compute_unit::UnitLifeCycle;
use log::{debug, error, trace, warn};
use mlua::Function;
use std::panic;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, broadcast};
use tokio::task::JoinHandle;

/// Physical task instance.
pub struct PhysicalTask {
    /// logical task instance
    logical: Arc<Mutex<Box<dyn LogicalTask + Send>>>,
    /// running core.
    pub(crate) core: Arc<Mutex<TaskCore>>,
    /// statistic data
    pub(crate) stats: Stats,
    /// execution context
    pub(crate) execution_context: Arc<Mutex<GraphContext>>,
}

#[derive(Default)]
pub struct Stats {
    start_time_millis: u64,
    in_total: u64,
}

/// physical task
///
/// create runtime task by logical task, including thread scheduler, message transformation.
impl PhysicalTask {
    /// create new physical task by provided logical task
    pub fn new(logical: Box<dyn LogicalTask + Send>) -> PhysicalTask {
        let execution_context = Arc::new(Mutex::new(GraphContext::default()));
        Self::new_with_watermark(logical, Watermark::new(16, 13, 8, 0), execution_context)
    }

    /// create new physical task by provided logical task and watermark
    pub fn new_with_watermark(
        logical: Box<dyn LogicalTask + Send>,
        watermark: Watermark,
        execution_context: Arc<Mutex<GraphContext>>,
    ) -> PhysicalTask {
        let s = SystemTime::now();
        let time = s
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_micros();
        PhysicalTask {
            logical: Arc::new(Mutex::new(logical)),
            core: Arc::new(Mutex::new(TaskCore::new(time.to_string(), watermark))),
            stats: Stats::default(),
            execution_context,
        }
    }

    /// update computing unit data into current physical task.
    pub async fn update_unit(&self, unit: ComputingUnit) {
        let mut tc = self.core.lock().await;
        tc.unit.replace(unit);
    }

    /// initialize script environment, current lua script was supported.
    pub async fn init_script_env(&self) {
        let context = self.execution_context.lock().await;
        let lua = context.lua.lock().await;
        let globals = lua.globals();
        let this_table = lua.create_table().expect("failed to create table");
        let scope_name = self.get_lua_scope_name().await;
        globals
            .set(scope_name, this_table)
            .expect("failed to set global table");
    }

    /// get lua script variables context name.
    async fn get_lua_scope_name(&self) -> String {
        let core = self.core.lock().await;
        let id = (&core.get_unit_id()).clone();
        format!("{}", id)
    }

    /// on task END-of-FILE event
    pub async fn on_eof(&self, row: Row, ctx: &Context) -> UnitResult<()> {
        let logical_task = &self.logical.lock().await;
        logical_task.event(EVENT_TYPE_EOF, ctx, row, vec![]).await
    }

    /// on task start event
    pub async fn on_start(&self, row: Row, ctx: &Context) -> UnitResult<()> {
        let logical_task = &self.logical.lock().await;
        logical_task.event(EVENT_TYPE_START, ctx, row, vec![]).await
    }

    /// on task computing
    ///
    /// ## Parameters
    /// * `row` - data row
    /// * `ctx` - Context
    pub async fn compute(&self, row: Row, ctx: Context) -> anyhow::Result<()> {
        let logical_task = &self.logical.lock().await;
        let context_ptr = Box::into_raw(Box::new(ctx));
        let row_ptr = Box::into_raw(Box::new(row));
        logical_task.internal_compute(row_ptr, context_ptr)?.await?;
        Ok(())
    }

    /// connect current physical task to neighbors. edge condition is optional.
    ///
    /// ## Parameters
    /// * `target` - neighbor task unit
    /// * `edge_condition` - edge condition configuration. Optional
    pub fn link(
        &self,
        target: &Arc<Mutex<Self>>,
        edge_condition: Option<EdgeCondition>,
    ) -> Vec<JoinHandle<anyhow::Result<()>>> {
        // create channel from current node to target for send streaming data.
        let target_process_input_handle = self.target_handle_current_sent(target, edge_condition);

        // create channel from target node to current for current node to received internal message.
        let current_process_internal_channel = self.current_handle_internal_sent(target);
        vec![
            target_process_input_handle,
            current_process_internal_channel,
        ]
    }

    /// launch the physical task (only execute when is source unit)
    pub fn launch(&self) -> JoinHandle<anyhow::Result<()>> {
        let self_cloned = self.core.clone();
        let cloned_logical = (&self.logical).clone();
        tokio::task::spawn(async move {
            let ctx = {
                let self_tc = self_cloned.lock().await;
                let watermark = self_tc.get_watermark();

                let self_unit = self_tc.unit.clone().expect("Failed to get unit");
                let self_sender = (&self_tc.channel.internal_channel.0).clone();
                Context::new(self_unit, self_sender, watermark)
            };

            let self_logical = cloned_logical.lock().await;
            let context_ptr = Box::into_raw(Box::new(ctx));
            match self_logical
                .internal_launch(context_ptr)
                .expect("Fail to launch task")
                .await
            {
                Ok(_) => {}
                Err(error) => {
                    eprintln!("fail to launch {:?}", error);
                }
            };
            Ok(())
        })
    }

    pub fn ready(&self) -> JoinHandle<()> {
        let self_cloned = self.core.clone();
        tokio::spawn(async move {
            let self_tc = self_cloned.lock().await;
            let internal_sender = self_tc.channel.internal_sender();
            internal_sender.send(Row::start()).unwrap();
        })
    }

    fn current_handle_internal_sent(
        &self,
        target: &Arc<Mutex<Self>>,
    ) -> JoinHandle<anyhow::Result<()>> {
        let cloned_self_core = self.core.clone();
        let cloned_target = target.clone();
        tokio::task::spawn(async move {
            let watermark = {
                let self_core = cloned_self_core.lock().await;
                self_core.get_watermark()
            };

            let (target_unit_id, mut receive_from_target) = {
                let target_physical = cloned_target.lock().await;
                let target_core = target_physical.core.lock().await;
                (
                    target_core.get_unit_id(),
                    target_core.channel.internal_subscribe(),
                )
            };
            loop {
                match receive_from_target.recv().await {
                    Ok(row) => {
                        if row.is_watermark() {
                            let mut wm = watermark.write().await;
                            wm.recv_offset = row.offset.max(wm.recv_offset);
                        } else if row.is_eof() {
                            #[cfg(feature = "trace-physical")]
                            trace!(
                                "[{}] received internal feedback EOF message from [{target_unit_id}]",
                                &row.source
                            );
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        warn!("feedback consume lagged：{}", count);
                    }
                    Err(err) => {
                        error!("recv error: {}", err);
                        break;
                    }
                }
            }
            Ok(())
        })
    }

    fn target_handle_current_sent(
        &self,
        target: &Arc<Mutex<PhysicalTask>>,
        edge_condition: Option<EdgeCondition>,
    ) -> JoinHandle<anyhow::Result<()>> {
        let self_cloned = self.core.clone();
        let target_cloned = Arc::clone(&target);
        let execution_context = self.execution_context.clone();
        tokio::task::spawn(async move {
            let mut receiver = {
                let self_tc = self_cloned.lock().await;
                let lua_scope_context_name = (&self_tc.get_unit_id()).clone();
                (
                    self_tc.get_unit_id(),
                    self_tc.channel.subscribe(),
                    lua_scope_context_name,
                )
            };

            let ch = {
                let target_physical = target_cloned.lock().await;
                let target_core = target_physical.core.lock().await;
                let internal_sender = target_core.channel.internal_sender();
                let watermark = target_core.get_watermark();

                let target_unit = (&target_core).unit.clone().expect("Failed to get unit");
                (
                    target_unit,
                    target_core.channel.internal_channel.0.clone(),
                    internal_sender.clone(),
                    watermark.clone(),
                )
            };
            let target_id = ch.0.get_id().clone();
            let context = Context::new(ch.0, ch.1, ch.3.clone());
            let internal_sender = ch.2;
            let watermark = ch.3;
            let self_id = receiver.0.clone();

            #[cfg(feature = "trace-physical")]
            trace!("creating channel from {self_id} to {target_id}");

            // create filter if edge configure contain `condition` node.
            let mut filter_fn = Self::init_filter(edge_condition, execution_context).await;
            let mut started = false;

            loop {
                match receiver.1.recv().await {
                    Ok(mut row) => {
                        if !started {
                            let mut target_physical_task = target_cloned.lock().await;
                            if target_physical_task.stats.start_time_millis == 0 {
                                let cts = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Time went backwards")
                                    .as_millis() as u64;
                                target_physical_task.stats.start_time_millis = cts;
                                target_physical_task
                                    .on_start(Row::start(), &context)
                                    .await?;
                            }
                            started = true;
                        }

                        if row.is_eof() {
                            let eof_source = row.source.clone();
                            let finished = {
                                let mut self_wm = watermark.write().await;
                                self_wm.upstream_remain -= 1;
                                self_wm.upstream_remain <= 0
                            };
                            if finished {
                                let cloned = row.clone();
                                {
                                    let target_physical_task = target_cloned.lock().await;
                                    match target_physical_task.on_eof(row, &context).await {
                                        Ok(_) => {}
                                        Err(error) => {
                                            error!(
                                                "Exception occur in task [{}]: {}",
                                                &self_id, error
                                            );
                                        }
                                    };
                                }

                                context.send(cloned).await;
                                internal_sender.send(Row::eof(self_id.clone())).unwrap();
                            }
                            #[cfg(feature = "trace-physical")]
                            debug!(
                                "task [{self_id}] received EOF from [{eof_source}], task finished NORMAL"
                            );
                            break;
                        } else {
                            let recv_offset = row.offset;
                            if filter_fn.is_none() {
                                let target_physical_task = target_cloned.lock().await;
                                target_physical_task
                                    .compute(row, context.clone())
                                    .await
                                    .expect("fail to compute");
                            } else {
                                let filter = filter_fn.as_ref().unwrap();
                                let matched = {
                                    let row_arc = Arc::new(Mutex::new(row));
                                    let lua_row = LuaRow::wrap(row_arc.clone()).await;
                                    row = row_arc.lock().await.to_owned();
                                    filter.call::<bool>(lua_row).unwrap_or(true)
                                };

                                if matched {
                                    let target_physical_task = target_cloned.lock().await;
                                    target_physical_task
                                        .compute(row, context.clone())
                                        .await
                                        .expect("fail to compute");
                                }
                            }
                            internal_sender
                                .send(Row::watermark(self_id.clone(), recv_offset))
                                .unwrap();
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        println!("消费滞后：{}", count);
                    }
                    Err(err) => {
                        println!("recv error: {}", err);
                        break;
                    }
                }
            }
            Ok(())
        })
    }

    async fn init_filter(
        edge_condition: Option<EdgeCondition>,
        execution_context: Arc<Mutex<GraphContext>>,
    ) -> Option<Function> {
        if let Some(condition) = edge_condition {
            if let Some(script) = condition.get_script() {
                let execution_context = execution_context.lock().await;
                let lua = execution_context.lua.lock().await;
                let func = lua
                    .load(format!(
                        r#"
                    function(row)
                    {}
                    end
                    "#,
                        script
                    ))
                    .eval::<mlua::Function>()
                    .expect("");
                Some(func)
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<T> From<T> for PhysicalTask
where
    T: LogicalTask + 'static + std::marker::Send,
{
    fn from(value: T) -> Self {
        PhysicalTask::new(Box::new(value))
    }
}
