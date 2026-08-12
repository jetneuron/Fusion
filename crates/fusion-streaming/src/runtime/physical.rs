use crate::runtime::core::LuaRow;
use crate::runtime::scripts::{GraphLua, LuaScript};
use crate::runtime::{EVENT_TYPE_EOF, EVENT_TYPE_START};
use crate::task::types::{TaskCore, UnitTask};
use fusion_unit_sdk::graph::types::{ComputingUnit, EdgeCondition, TaskContext};
use fusion_unit_sdk::proto::transfer::Row;
use fusion_unit_sdk::runtime::logical::LogicalTask;
use fusion_unit_sdk::runtime::state::GraphStates;
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use fusion_unit_sdk::units::compute_unit::UnitLifeCycle;
use log::{debug, error, trace};
use mlua::Function;
use std::panic;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Physical task instance.
pub struct PhysicalTask {
    /// logical task instance
    logical: Arc<Box<dyn LogicalTask + Send + Sync>>,
    /// running core.
    pub(crate) core: Arc<Mutex<TaskCore>>,
    /// statistic data
    pub(crate) stats: Stats,
    /// states manager
    pub(crate) states: GraphStates,
    /// Compute parallelism for this node. 1 = serial (default).
    pub(crate) parallelism: usize,
    /// Per-worker Lua VMs (parallelism > 1 only).
    pub(crate) worker_luas: Arc<tokio::sync::Mutex<Vec<Arc<Mutex<mlua::Lua>>>>>,
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
    pub fn new(logical: Box<dyn LogicalTask + Send + Sync>) -> PhysicalTask {
        Self::new_with_watermark(logical, 0, GraphStates::default(), 1)
    }

    /// create new physical task with upstream count for EOF tracking.
    pub(crate) fn new_with_watermark(
        logical: Box<dyn LogicalTask + Send + Sync>,
        upstream_remain: i8,
        states: GraphStates,
        parallelism: usize,
    ) -> PhysicalTask {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_micros();
        PhysicalTask {
            logical: Arc::new(logical),
            core: Arc::new(Mutex::new(TaskCore::new(time.to_string(), upstream_remain))),
            stats: Stats::default(),
            states,
            parallelism,
            worker_luas: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    /// update computing unit data into current physical task.
    pub async fn update_unit(&self, unit: ComputingUnit) {
        let mut tc = self.core.lock().await;
        tc.set_unit(unit);
    }

    /// initialize script environment, current lua script was supported.
    pub async fn init_script_env(&self) {
        let scope_name = self.get_lua_scope_name().await;
        if self.parallelism > 1 {
            // Per-worker Lua VMs: one VM + scope table per worker so
            // parallel compute never contends on the global GraphLua.
            let mut luas = Vec::with_capacity(self.parallelism);
            for _ in 0..self.parallelism {
                let lua = Arc::new(Mutex::new(mlua::Lua::new()));
                {
                    let l = lua.lock().await;
                    let globals = l.globals();
                    let this_table = l.create_table().expect("failed to create table");
                    #[cfg(feature = "trace-physical")]
                    trace!(
                        "physical init lua context. scope_name = {} (worker)",
                        scope_name
                    );
                    globals
                        .set(scope_name.clone(), this_table)
                        .expect("failed to set global table");
                }
                luas.push(lua);
            }
            // Build phase is single-threaded; safe to take and replace.
            {
                let mut guard = self.worker_luas.lock().await;
                *guard = luas;
            }
            return;
        }

        // Single-worker: global GraphLua scope (original behavior).
        let lua_ref = self.states.state::<GraphLua>().unwrap();
        let lua_state = lua_ref.inner();
        let lua = lua_state.0.lock().await;
        let globals = lua.globals();
        let this_table = lua.create_table().expect("failed to create table");
        #[cfg(feature = "trace-physical")]
        trace!("physical init lua context. scope_name = {}", scope_name);
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
    pub async fn on_eof(&self, row: Row, ctx: &TaskContext) -> UnitResult<()> {
        let logical_task = &*self.logical;
        logical_task.event(EVENT_TYPE_EOF, ctx, row, vec![]).await?;
        self.shutdown().await
    }

    /// on task start event
    pub async fn on_start(&self, row: Row, ctx: &TaskContext) -> UnitResult<()> {
        let logical_task = &*self.logical;
        logical_task.event(EVENT_TYPE_START, ctx, row, vec![]).await
    }

    /// on task computing
    ///
    /// ## Parameters
    /// * `row` - data row
    /// * `ctx` - Context
    pub async fn compute(&self, row: Row, ctx: TaskContext) -> anyhow::Result<()> {
        let logical_task = &*self.logical;
        let context_ptr = Box::into_raw(Box::new(ctx));
        let row_ptr = Box::into_raw(Box::new(row));
        logical_task.internal_compute(row_ptr, context_ptr)?.await?;
        Ok(())
    }

    /// connect current physical task to neighbors. edge condition is optional.
    pub async fn link(
        &self,
        target: &Arc<Mutex<Self>>,
        edge_condition: Option<EdgeCondition>,
    ) -> Vec<JoinHandle<anyhow::Result<()>>> {
        // Pop one pre-allocated mpsc receiver for this edge.
        let rx = {
            let self_tc = self.core.lock().await;
            self_tc
                .channel
                .take_receiver()
                .expect("no output channel for edge — check prepare_outputs")
        };
        // Parallelism is a property of the TARGET node (who processes
        // this edge's rows), so read it from target, not self.
        let (parallelism, worker_luas) = {
            let t = target.lock().await;
            (t.parallelism, t.worker_luas.lock().await.clone())
        };
        self.target_handle_current_sent(
            target,
            rx,
            edge_condition,
            parallelism,
            worker_luas,
        )
    }

    /// launch the physical task (only execute when is source unit)
    pub fn launch(&self) -> JoinHandle<anyhow::Result<()>> {
        let self_cloned = self.core.clone();
        let cloned_logical = (&self.logical).clone();
        let states = self.states.clone();
        tokio::task::spawn(async move {
            let (unit_id, ctx) = {
                let self_tc = self_cloned.lock().await;
                let unit_id = self_tc.get_unit_id();
                let self_unit = self_tc.unit.clone().expect("Failed to get unit");
                let senders = self_tc.channel.get_senders();
                (unit_id, TaskContext::new(self_unit, senders, states))
            };

            #[cfg(feature = "trace-physical")]
            let src_clock = tokio::time::Instant::now();

            let self_logical = &*cloned_logical;

            // Launch via raw pointer. The context is consumed by the
            // launch future and dropped inside it. We reconstruct the
            // Box immediately to satisfy the Send checker — the raw
            // pointer does not escape this scope.
            let launch_future = {
                let context_ptr = Box::into_raw(Box::new(ctx));
                // SAFETY: context_ptr is the only reference to this Box.
                // internal_launch takes ownership and will drop it.
                self_logical
                    .internal_launch(context_ptr)
                    .expect("Fail to launch task")
            };
            let launch_result = launch_future.await;

            match launch_result {
                Ok(_) => {}
                Err(error) => {
                    return Err(anyhow::anyhow!("fail to launch: {error}"));
                }
            };

            #[cfg(feature = "trace-physical")]
            {
                let elapsed = src_clock.elapsed();
                trace!(
                    "[source:{unit_id}] finished in {elapsed:.2?}",
                );
            }
            Ok(())
        })
    }

    fn target_handle_current_sent(
        &self,
        target: &Arc<Mutex<PhysicalTask>>,
        mut rx: tokio::sync::mpsc::Receiver<Row>,
        edge_condition: Option<EdgeCondition>,
        parallelism: usize,
        worker_luas: Vec<Arc<Mutex<mlua::Lua>>>,
    ) -> Vec<JoinHandle<anyhow::Result<()>>> {
        let self_cloned = self.core.clone();
        let target_cloned = Arc::clone(&target);
        let states = self.states.clone();
        let parallelism = parallelism.max(1);

        // Single-worker path (parallelism == 1): no dispatcher overhead.
        if parallelism == 1 {
            let handle = tokio::task::spawn(async move {
                Self::forward_loop(
                    rx,
                    &self_cloned,
                    &target_cloned,
                    states,
                    edge_condition,
                )
                .await
            });
            return vec![handle];
        }

        // Multi-worker path: dispatcher + N worker tasks.
        let mut worker_txs = Vec::with_capacity(parallelism);
        let mut worker_rxs = Vec::with_capacity(parallelism);
        for _ in 0..parallelism {
            let (tx, rx) = tokio::sync::mpsc::channel::<Row>(64);
            worker_txs.push(tx);
            worker_rxs.push(rx);
        }

        let mut handles = Vec::with_capacity(parallelism + 1);

        // Worker tasks first: parallel compute.
        let mut worker_handles = Vec::with_capacity(parallelism);
        for (i, worker_rx) in worker_rxs.into_iter().enumerate() {
            let tgt = target_cloned.clone();
            let st = states.clone();
            let lua = worker_luas.get(i).cloned();
            let handle = tokio::task::spawn(async move {
                Self::worker_loop(worker_rx, &tgt, st, lua).await
            });
            worker_handles.push(handle);
        }

        // Dispatcher task: recv upstream → round-robin → workers.
        let disp_cloned = target_cloned.clone();
        let disp_self = self_cloned.clone();
        let disp_states = states.clone();
        let dispatch = tokio::task::spawn(async move {
            Self::dispatch_loop(
                rx,
                worker_txs,
                worker_handles,
                &disp_self,
                &disp_cloned,
                disp_states,
                edge_condition,
            )
            .await
        });
        handles.push(dispatch);

        // Note: worker handles are awaited by the dispatcher on EOF
        // (drain). They are not joined here.
        handles
    }

    /// Single-worker forwarding loop (parallelism == 1, existing behavior).
    async fn forward_loop(
        mut rx: tokio::sync::mpsc::Receiver<Row>,
        self_cloned: &Arc<Mutex<TaskCore>>,
        target_cloned: &Arc<Mutex<PhysicalTask>>,
        states: GraphStates,
        edge_condition: Option<EdgeCondition>,
    ) -> anyhow::Result<()> {
        let self_id = self_cloned.lock().await.get_unit_id();
        let (target_id, context, target_logical, upstream_remain) = {
            let target_physical = target_cloned.lock().await;
            let target_core = target_physical.core.lock().await;
            let target_unit = (&target_core).unit.clone().expect("Failed to get unit");
            let target_id = target_unit.get_id().clone();
            let senders = target_core.channel.get_senders();
            let context = TaskContext::new(target_unit, senders, states.clone());
            let logical = target_physical.logical.clone();
            let ur = target_core.get_upstream_remain();
            (target_id, context, logical, ur)
        };

        #[cfg(feature = "trace-physical")]
        trace!("creating channel from {self_id} to {target_id}");

        let edge_filter_fn = Self::init_edge_filter(edge_condition, states).await;
        let mut started = false;

        #[cfg(feature = "trace-physical")]
        let (mut fwd_clock, mut fwd_row_count): (Option<tokio::time::Instant>, u64) =
            (None, 0);

        while let Some(mut row) = rx.recv().await {
            if !started {
                Self::ensure_started(target_cloned, &context).await?;
                started = true;
            }

            // ── Barrier ── (forwarded downstream as-is via context.send)
            if row.is_barrier() {
                context.send(row).await;
                continue;
            }

            // ── EOF ──
            if row.is_eof() {
                let eof_source = row.source.clone();
                let remaining =
                    upstream_remain.fetch_sub(1, Ordering::Relaxed) - 1;
                if remaining <= 0 {
                    Self::handle_eof(
                        row,
                        &self_id,
                        &target_id,
                        target_cloned,
                        &context,
                    )
                    .await;
                }
                #[cfg(feature = "trace-physical")]
                {
                    debug!(
                        "task [{self_id}] received EOF from [{eof_source}], task finished NORMAL"
                    );
                    let elapsed = fwd_clock
                        .map(|c| c.elapsed())
                        .unwrap_or_default();
                    trace!(
                        "[chan:{self_id}→{target_id}] done: {fwd_row_count} rows in {elapsed:.2?} ({:.1} rows/ms)",
                        fwd_row_count as f64 / elapsed.as_millis().max(1) as f64
                    );
                }
                break;
            }

            // ── Data row ──
            #[cfg(feature = "trace-physical")]
            {
                if fwd_clock.is_none() {
                    fwd_clock = Some(tokio::time::Instant::now());
                }
                fwd_row_count += 1;
            }

            Self::process_row(
                row,
                &target_logical,
                &context,
                edge_filter_fn.as_ref(),
            )
            .await?;
        }
        Ok(())
    }

    /// Dispatcher: recv from upstream, edge-filter, round-robin to workers.
    /// On EOF: drain all workers (join handles), then fire on_eof.
    async fn dispatch_loop(
        mut rx: tokio::sync::mpsc::Receiver<Row>,
        worker_txs: Vec<tokio::sync::mpsc::Sender<Row>>,
        mut worker_handles: Vec<JoinHandle<anyhow::Result<()>>>,
        self_cloned: &Arc<Mutex<TaskCore>>,
        target_cloned: &Arc<Mutex<PhysicalTask>>,
        states: GraphStates,
        edge_condition: Option<EdgeCondition>,
    ) -> anyhow::Result<()> {
        let self_id = self_cloned.lock().await.get_unit_id();
        let (target_id, context, upstream_remain) = {
            let target_physical = target_cloned.lock().await;
            let target_core = target_physical.core.lock().await;
            let target_unit = (&target_core).unit.clone().expect("Failed to get unit");
            let target_id = target_unit.get_id().clone();
            let senders = target_core.channel.get_senders();
            let context = TaskContext::new(target_unit, senders, states.clone());
            let ur = target_core.get_upstream_remain();
            (target_id, context, ur)
        };

        #[cfg(feature = "trace-physical")]
        trace!(
            "creating channel from {self_id} to {target_id} (parallelism={})",
            worker_txs.len()
        );

        let edge_filter_fn = Self::init_edge_filter(edge_condition, states).await;
        let mut started = false;
        let n = worker_txs.len();
        let mut next_worker = 0usize;

        #[cfg(feature = "trace-physical")]
        let (mut fwd_clock, mut fwd_row_count): (Option<tokio::time::Instant>, u64) =
            (None, 0);

        while let Some(mut row) = rx.recv().await {
            if !started {
                Self::ensure_started(target_cloned, &context).await?;
                started = true;
            }

            // ── Barrier ── (forwarded downstream as-is)
            if row.is_barrier() {
                context.send(row).await;
                continue;
            }

            // ── EOF ──
            if row.is_eof() {
                let eof_source = row.source.clone();
                // Drain workers: send EOF to each, then join their handles.
                for tx in &worker_txs {
                    let _ = tx.send(Row::eof(self_id.clone())).await;
                }
                drop(worker_txs); // close remaining worker channels
                for handle in worker_handles.drain(..) {
                    match handle.await {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => error!("worker error: {e}"),
                        Err(e) => error!("worker panicked: {e}"),
                    }
                }

                let remaining =
                    upstream_remain.fetch_sub(1, Ordering::Relaxed) - 1;
                if remaining <= 0 {
                    Self::handle_eof(
                        row,
                        &self_id,
                        &target_id,
                        target_cloned,
                        &context,
                    )
                    .await;
                }
                #[cfg(feature = "trace-physical")]
                {
                    debug!(
                        "task [{self_id}] received EOF from [{eof_source}], task finished NORMAL"
                    );
                    let elapsed = fwd_clock
                        .map(|c| c.elapsed())
                        .unwrap_or_default();
                    trace!(
                        "[chan:{self_id}→{target_id}] done: {fwd_row_count} rows in {elapsed:.2?} ({:.1} rows/ms)",
                        fwd_row_count as f64 / elapsed.as_millis().max(1) as f64
                    );
                }
                break;
            }

            // ── Data row ──
            #[cfg(feature = "trace-physical")]
            {
                if fwd_clock.is_none() {
                    fwd_clock = Some(tokio::time::Instant::now());
                }
                fwd_row_count += 1;
            }

            // Edge filter (serial, single point).
            let matched = match edge_filter_fn.as_ref() {
                Some(filter) => {
                    let row_arc = Arc::new(Mutex::new(row));
                    let lua_row = LuaRow::wrap(row_arc.clone()).await;
                    let r = row_arc.lock().await.to_owned();
                    let ok = filter.call::<bool>(lua_row).unwrap_or(true);
                    if ok {
                        Some(r)
                    } else {
                        None
                    }
                }
                None => Some(row),
            };

            if let Some(data_row) = matched {
                let idx = next_worker;
                next_worker = (next_worker + 1) % n;
                if worker_txs[idx].send(data_row).await.is_err() {
                    // Worker closed — ignore.
                }
            }
        }
        Ok(())
    }

    /// Worker: receive rows from dispatcher, run parallel compute.
    async fn worker_loop(
        mut rx: tokio::sync::mpsc::Receiver<Row>,
        target_cloned: &Arc<Mutex<PhysicalTask>>,
        states: GraphStates,
        worker_lua: Option<Arc<Mutex<mlua::Lua>>>,
    ) -> anyhow::Result<()> {
        let (context, target_logical) = {
            let target_physical = target_cloned.lock().await;
            let target_core = target_physical.core.lock().await;
            let target_unit = (&target_core).unit.clone().expect("Failed to get unit");
            let senders = target_core.channel.get_senders();
            let context = match worker_lua {
                Some(lua) => {
                    TaskContext::new(target_unit, senders, states).with_worker_lua(lua)
                }
                None => TaskContext::new(target_unit, senders, states),
            };
            let logical = target_physical.logical.clone();
            (context, logical)
        };

        #[cfg(feature = "trace-physical")]
        let (mut fwd_clock, mut fwd_row_count): (Option<tokio::time::Instant>, u64) =
            (None, 0);

        while let Some(mut row) = rx.recv().await {
            // ── EOF ──
            if row.is_eof() {
                break;
            }

            #[cfg(feature = "trace-physical")]
            {
                if fwd_clock.is_none() {
                    fwd_clock = Some(tokio::time::Instant::now());
                }
                fwd_row_count += 1;
            }

            Self::process_row(row, &target_logical, &context, None).await?;
        }

        #[cfg(feature = "trace-physical")]
        {
            let elapsed = fwd_clock.map(|c| c.elapsed()).unwrap_or_default();
            trace!(
                "[worker] done: {fwd_row_count} rows in {elapsed:.2?}",
            );
        }
        Ok(())
    }

    /// Shared per-row compute (with optional edge filter).
    async fn process_row(
        mut row: Row,
        target_logical: &Arc<Box<dyn LogicalTask + Send + Sync>>,
        context: &TaskContext,
        edge_filter_fn: Option<&mlua::Function>,
    ) -> anyhow::Result<()> {
        // Set barrier_ref from row offset for downstream grouping.
        let recv_barrier_ref = row.barrier_ref;
        let group_key = if recv_barrier_ref > 0 {
            recv_barrier_ref
        } else {
            row.offset
        };
        context.sender.set_barrier_ref(group_key);

        if edge_filter_fn.is_none() {
            let context_ptr = Box::into_raw(Box::new(context.clone()));
            let row_ptr = Box::into_raw(Box::new(row));
            target_logical
                .internal_compute(row_ptr, context_ptr)?
                .await?;
        } else {
            let filter = edge_filter_fn.unwrap();
            let matched = {
                let row_arc = Arc::new(Mutex::new(row));
                let lua_row = LuaRow::wrap(row_arc.clone()).await;
                row = row_arc.lock().await.to_owned();
                filter.call::<bool>(lua_row).unwrap_or(true)
            };

            if matched {
                let context_ptr = Box::into_raw(Box::new(context.clone()));
                let row_ptr = Box::into_raw(Box::new(row));
                target_logical
                    .internal_compute(row_ptr, context_ptr)?
                    .await?;
            }
        }
        Ok(())
    }

    /// Ensure on_start was invoked once for the target.
    async fn ensure_started(
        target_cloned: &Arc<Mutex<PhysicalTask>>,
        context: &TaskContext,
    ) -> anyhow::Result<()> {
        let mut target_physical_task = target_cloned.lock().await;
        if target_physical_task.stats.start_time_millis == 0 {
            let cts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as u64;
            target_physical_task.stats.start_time_millis = cts;
            target_physical_task
                .on_start(Row::start(), context)
                .await?;
        }
        Ok(())
    }

    /// Handle EOF: run on_eof + propagate downstream.
    async fn handle_eof(
        row: Row,
        self_id: &str,
        target_id: &str,
        target_cloned: &Arc<Mutex<PhysicalTask>>,
        context: &TaskContext,
    ) {
        let cloned = row.clone();
        {
            #[cfg(feature = "trace-physical")]
            let eof_clock = tokio::time::Instant::now();
            let target_physical_task = target_cloned.lock().await;
            match target_physical_task.on_eof(row, context).await {
                Ok(_) => {}
                Err(error) => {
                    error!(
                        "Exception occur in task [{}]: {}",
                        self_id, error
                    );
                }
            };
            #[cfg(feature = "trace-physical")]
            trace!(
                "[node:{target_id}] on_eof finished in {:.2?}",
                eof_clock.elapsed()
            );
        }
        // Propagate EOF downstream.
        context.send(cloned).await;
    }

    async fn shutdown(&self) -> UnitResult<()> {
        let scope_name = self.get_lua_scope_name().await;

        // Per-worker Lua VMs: clean each worker's scope.
        if self.parallelism > 1 {
            let luas = self.worker_luas.lock().await;
            for lua in luas.iter() {
                let l = lua.lock().await;
                l.globals().raw_remove(scope_name.clone()).map_err(|err| {
                    UnitError::physical_error(format!(
                        "fail to remove lua scope `{scope_name}`: {err}"
                    ))
                })?;
            }
            #[cfg(feature = "trace-physical")]
            trace!("shutdown: removed lua scope `{scope_name}` ({} workers)", luas.len());
            return Ok(());
        }

        // Single-worker: global GraphLua scope (original behavior).
        let lua_ref = self.states.state::<GraphLua>().unwrap();
        let lua_state = lua_ref.inner();
        let lua = lua_state.0.lock().await;
        let globals = lua.globals();

        // Remove the per-task scope table. Because everything
        // (compiled function, `this` userdata, …) is stored
        // inside this table, a single remove cleans the entire
        // node sandbox.
        globals.raw_remove(scope_name.clone()).map_err(|err| {
            UnitError::physical_error(format!(
                "fail to remove lua scope `{scope_name}`: {err}"
            ))
        })?;

        #[cfg(feature = "trace-physical")]
        trace!("shutdown: removed lua scope `{scope_name}`");
        Ok(())
    }

    async fn init_edge_filter(
        edge_condition: Option<EdgeCondition>,
        states: GraphStates,
    ) -> Option<Function> {
        if let Some(condition) = edge_condition {
            if let Some(script) = condition.get_script() {
                let lua_ref = states.state::<GraphLua>().unwrap();
                let lua_state = lua_ref.inner();
                let lua = lua_state.0.lock().await;
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
    T: LogicalTask + 'static + std::marker::Send + std::marker::Sync,
{
    fn from(value: T) -> Self {
        PhysicalTask::new(Box::new(value))
    }
}
