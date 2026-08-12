use crate::proto::transfer::Row;
use crate::runtime::logical::LogicalExecuteContext;
use crate::runtime::state::GraphStates;
use crate::runtime::UnitResult;
use log::{debug, warn};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::ops::Index;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::{Mutex, RwLock};

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
        row: Row,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait;
    /// end of file.
    fn on_eof<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
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
}

pub struct Watermark {
    pub send_offset: AtomicU64,
    pub recv_offset: AtomicU64,
    pub high: u64,
    pub low: u64,
    pub max: u64,
    pub ultimate: bool,
    pub upstream_remain: i8,
}

impl Watermark {
    pub fn new(max: u64, high: u64, low: u64, upstream_remain: i8) -> Self {
        Self {
            send_offset: AtomicU64::new(0),
            recv_offset: AtomicU64::new(0),
            high,
            low,
            max,
            ultimate: max == u64::MAX,
            upstream_remain,
        }
    }

    fn should_slow_down(&self) -> bool {
        let send = self.send_offset.load(Ordering::Relaxed);
        let recv = self.recv_offset.load(Ordering::Relaxed);
        if recv >= send {
            return false;
        }
        send - recv >= self.high
    }

    fn should_pause(&self) -> bool {
        let send = self.send_offset.load(Ordering::Relaxed);
        let recv = self.recv_offset.load(Ordering::Relaxed);
        if recv >= send {
            return false;
        }
        send - recv >= self.max
    }

    fn should_resume(&self) -> bool {
        let send = self.send_offset.load(Ordering::Relaxed);
        let recv = self.recv_offset.load(Ordering::Relaxed);
        if recv >= send {
            return true;
        }
        send - recv <= self.low
    }

    pub fn is_ultimate(&self) -> bool {
        self.ultimate
    }
}

#[derive(Clone)]
pub struct BackpressureSender {
    id: String,
    sender: Box<Sender<Row>>,
    offset: Arc<std::sync::Mutex<u64>>,
    watermark: Arc<RwLock<Watermark>>,
    /// Current barrier_ref — set by engine before compute, applied to every emitted row.
    barrier_ref: Arc<std::sync::Mutex<u64>>,
}

impl BackpressureSender {
    pub fn new(id: String, sender: Sender<Row>, watermark: Arc<RwLock<Watermark>>) -> Self {
        BackpressureSender {
            id,
            sender: Box::new(sender),
            offset: Arc::new(std::sync::Mutex::new(0)),
            watermark,
            barrier_ref: Arc::new(std::sync::Mutex::new(0)),
        }
    }

    /// Set the barrier reference for the next computation batch.
    /// Called by the engine forwarding task before target.compute().
    pub fn set_barrier_ref(&self, r: u64) {
        let mut br = self.barrier_ref.lock().unwrap();
        *br = r;
    }

    /// send row data with backpressure.
    pub async fn send(&self, mut row: Row) {
        loop {
            let wm = self.watermark.read().await;
            if wm.should_pause() {
                // Gap >= max — consumer is far behind, sleep to give it CPU time.
                drop(wm);
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                continue;
            }
            if wm.should_slow_down() {
                // Gap >= high — consumer is falling behind, yield to let it run.
                drop(wm);
                tokio::task::yield_now().await;
                continue;
            }

            row.source = self.id.clone();
            row.barrier_ref = {
                let br = self.barrier_ref.lock().unwrap();
                *br
            };
            let offset = {
                let mut offset_val = self.offset.lock().unwrap();
                *offset_val += 1;
                row.offset = *offset_val;
                row.offset
            };

            // Atomic store — no write lock needed.
            wm.send_offset.store(offset, Ordering::Relaxed);
            break;
        }
        match self.sender.send(row) {
            Ok(s) => {}
            Err(_) => {
                println!("Watermark sender dropped");
            }
        };
    }
}

impl TaskContext {
    pub fn new(
        unit: ComputingUnit,
        sender: Sender<Row>,
        watermark: Arc<RwLock<Watermark>>,
        states: GraphStates,
    ) -> Self {
        let id = unit.id.clone();
        TaskContext {
            unit,
            sender: BackpressureSender::new(id, sender, watermark),
            states,
        }
    }

    pub async fn send(&self, row: Row) {
        self.sender.send(row).await;
    }
}
