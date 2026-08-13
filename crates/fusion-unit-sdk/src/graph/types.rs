use crate::proto::transfer::Frame;
use crate::runtime::logical::LogicalExecuteContext;
use crate::runtime::state::GraphStates;
use crate::runtime::UnitResult;
use log::{debug, warn};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use futures::future;
use std::future::Future;
use std::ops::Index;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type UnitIdx = String;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UnitMeta {
    pub(crate) id: UnitIdx,
}

impl UnitMeta {
    pub fn get_id(&self) -> UnitIdx {
        self.id.clone()
    }
    pub fn set_id(&mut self, id: UnitIdx) {
        self.id = id;
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GraphDescription {}

pub type UnitConfig = Value;

pub trait EvaluableConf {
    fn eval(&self) -> Option<String>;
}

impl EvaluableConf for Value {
    fn eval(&self) -> Option<String> {
        if self.is_string() {
            Some(self.as_str().unwrap().to_string())
        } else {
            self.as_str().map(|s| s.to_string())
        }
    }
}

trait TeraConfigParser {
    fn eval(&self, exec_ctx: Arc<Mutex<dyn LogicalExecuteContext>>);
}
impl TeraConfigParser for UnitConfig {
    fn eval(&self, exec_ctx: Arc<Mutex<dyn LogicalExecuteContext>>) {
        todo!()
    }
}

// type HashMap: ;
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct ComputingUnit {
    /// id of the computing unit to be searched and connected
    id: String,
    /// name of the computing unit
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    /// type of the computing unit
    r#type: String,
    /// version of computing unit
    version: Option<String>,
    /// node configuration properties
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<UnitConfig>,
    #[serde(default)]
    outgoing: usize,
    #[serde(default)]
    incoming: usize,

    // --------- Runtime Fields ---------
    #[serde(skip)]
    runtime_states: Option<GraphStates>,
}

pub type EdgeConfig = Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ComputingEdge {
    id: String,
    source: String,
    target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<EdgeConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EdgeCondition {
    script: Option<String>,
}

impl EdgeCondition {
    pub fn get_script(&self) -> Option<String> {
        self.script.clone()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SerializeType {
    Json,
    Yaml,
}

impl ComputingUnit {
    pub fn new(id: &str, r#type: &str) -> Self {
        ComputingUnit {
            id: id.to_string(),
            name: None,
            r#type: r#type.to_string(),
            config: None,
            version: None,
            outgoing: 0,
            incoming: 0,
            runtime_states: None,
        }
    }

    pub fn update_neighbors(&mut self, outgoing: usize, incoming: usize) {
        self.outgoing = outgoing;
        self.incoming = incoming;
    }

    pub fn get_outgoing(&self) -> usize {
        self.outgoing
    }

    pub fn with_config(mut self, conf: UnitConfig) -> Self {
        self.config = Some(conf);
        self
    }

    pub fn replace_config(&mut self, conf: UnitConfig) {
        self.config.replace(conf);
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    pub fn get_id(&self) -> &String {
        &self.id
    }

    pub fn get_config(&self) -> Option<UnitConfig> {
        self.config.clone()
    }

    pub fn get_type(&self) -> &String {
        &self.r#type
    }

    pub fn get_version(&self) -> String {
        let cloned_version = (&self).version.clone();
        cloned_version.unwrap_or("unstable".to_string())
    }

    pub fn is_source(&self) -> bool {
        self.incoming == 0 && self.outgoing > 0
    }

    pub fn is_mapper(&self) -> bool {
        self.incoming > 0 && self.outgoing > 0
    }

    pub fn is_sink(&self) -> bool {
        self.incoming > 0 && self.outgoing == 0
    }

    pub fn with_states(&mut self, states: GraphStates) {
        self.runtime_states.replace(states);
    }

    pub fn get_runtime_states(&self) -> Option<GraphStates> {
        self.runtime_states.clone()
    }
}

impl ComputingEdge {
    pub fn new(source: &str, target: &str) -> Self {
        ComputingEdge {
            id: "uuid".to_string(),
            source: String::from(source),
            target: String::from(target),
            config: Default::default(),
        }
    }

    pub fn with_config(mut self, conf: EdgeConfig) -> Self {
        self.config = Some(conf);
        self
    }

    pub fn get_id(&self) -> &String {
        &self.id
    }

    pub fn get_source(&self) -> String {
        self.source.clone()
    }

    pub fn get_target(&self) -> String {
        self.target.clone()
    }

    pub fn get_config(&self) -> Option<EdgeConfig> {
        self.config.clone()
    }
}

pub trait InitUnit {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        Ok(())
    }

    fn on_start<'life0, 'async_trait>(
        &'life0 self,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {})
    }
}

pub trait SourceUnit: InitUnit {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>;
}

pub trait MapUnit: InitUnit {
    /// internal launch source to emit data.
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        frame: Frame,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait;
    /// end of file.
    fn on_eof<'life0, 'async_trait>(
        &'life0 self,
        frame: Frame,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl std::future::Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Ok(Box::pin(async move { Ok(()) }))
    }
}

#[derive(Default)]
pub struct Stats {
    offset: u64,
}

#[repr(C)]
#[derive(Clone)]
pub struct TaskContext {
    pub unit: ComputingUnit,
    pub sender: BackpressureSender,
    pub states: GraphStates,
    /// Per-worker Lua VM (parallelism > 1). None = use global GraphLua.
    pub worker_lua: Option<Arc<tokio::sync::Mutex<mlua::Lua>>>,
}

pub struct Watermark {
    pub ultimate: bool,
    pub upstream_remain: i8,
}

impl Watermark {
    pub fn new(upstream_remain: i8) -> Self {
        Self {
            ultimate: false,
            upstream_remain,
        }
    }

    pub fn is_ultimate(&self) -> bool {
        self.ultimate
    }
}

#[derive(Clone)]
pub struct BackpressureSender {
    id: String,
    senders: Vec<tokio::sync::mpsc::Sender<Frame>>,
    offset: Arc<std::sync::Mutex<u64>>,
    /// Current barrier_ref — set by engine before compute, applied to every emitted frame.
    barrier_ref: Arc<std::sync::Mutex<u64>>,
}

impl BackpressureSender {
    pub fn new(id: String, senders: Vec<tokio::sync::mpsc::Sender<Frame>>) -> Self {
        BackpressureSender {
            id,
            senders,
            offset: Arc::new(std::sync::Mutex::new(0)),
            barrier_ref: Arc::new(std::sync::Mutex::new(0)),
        }
    }

    /// Set the barrier reference for the next computation batch.
    /// Called by the engine forwarding task before target.compute().
    pub fn set_barrier_ref(&self, r: u64) {
        let mut br = self.barrier_ref.lock().unwrap();
        *br = r;
    }

    /// Return the current frame offset count (for trace logging).
    pub fn sent_count(&self) -> u64 {
        *self.offset.lock().unwrap()
    }

    /// send frame data.
    /// mpsc provides built-in backpressure: `tx.send().await` blocks when
    /// the channel buffer is full. No watermark polling needed.
    pub async fn send(&self, mut frame: Frame) {
        // Stamp source/offset/barrier_ref. Skip for barrier/eof frames —
        // barrier offsets must be preserved for fan-in group tracking.
        if !frame.is_barrier() && !frame.is_eof() {
            frame.source = self.id.clone();
            frame.barrier_ref = {
                let br = self.barrier_ref.lock().unwrap();
                *br
            };
            frame.offset = {
                let mut offset_val = self.offset.lock().unwrap();
                *offset_val += 1;
                *offset_val
            };
        }

        if self.senders.is_empty() {
            return; // sink node — no downstream channels
        }
        // Fan-out to all downstream channels in parallel — a slow
        // consumer does not block fast ones.
        let futures: Vec<_> = self
            .senders
            .iter()
            .map(|tx| tx.send(frame.clone()))
            .collect();
        for result in futures::future::join_all(futures).await {
            // downstream closed — ignore
            let _ = result;
        }
    }
}

impl TaskContext {
    pub fn new(
        unit: ComputingUnit,
        senders: Vec<tokio::sync::mpsc::Sender<Frame>>,
        states: GraphStates,
    ) -> Self {
        let id = unit.id.clone();
        TaskContext {
            unit,
            sender: BackpressureSender::new(id, senders),
            states,
            worker_lua: None,
        }
    }

    /// Attach a per-worker Lua VM (parallelism > 1). Scripts executed
    /// through this context run on the worker's own VM.
    pub fn with_worker_lua(mut self, lua: Arc<tokio::sync::Mutex<mlua::Lua>>) -> Self {
        self.worker_lua = Some(lua);
        self
    }

    pub async fn send(&self, frame: Frame) {
        self.sender.send(frame).await;
    }
}
