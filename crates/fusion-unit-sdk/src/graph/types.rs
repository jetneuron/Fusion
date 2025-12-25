use crate::proto::transfer::Row;
use crate::runtime::UnitResult;
use crate::runtime::logical::LogicalExecuteContext;
use log::warn;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::ops::Index;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::{Mutex, RwLock};

pub type UnitIdx = String;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GraphDescription {}

pub type UnitConfig = Value;

#[derive(Default)]
pub struct UnitConf {
    raw: Value,
}

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

impl UnitConf {
    pub fn wrap(value: Value) -> UnitConf {
        UnitConf { raw: value }
    }
}

impl Index<&str> for UnitConf {
    type Output = Value;

    fn index(&self, index: &str) -> &Self::Output {
        static NULL: Value = Value::Null;
        self.raw.get(index).unwrap_or(&NULL)
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
    fn init(&mut self, unit: ComputingUnit) {}

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
        ctx: Arc<Context>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>;
}

pub trait MapUnit: InitUnit {
    /// internal launch source to emit data.
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 Context,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait;
    /// end of file.
    fn on_eof<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 Context,
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
pub struct Context {
    pub unit: ComputingUnit,
    pub sender: BackpressureSender,
}

pub struct Watermark {
    pub send_offset: u64,
    pub recv_offset: u64,
    pub high: u64,
    pub low: u64,
    pub max: u64,
    pub ultimate: bool,
    pub upstream_remain: i8,
}

impl Watermark {
    pub fn new(max: u64, high: u64, low: u64, upstream_remain: i8) -> Self {
        Self {
            send_offset: 0,
            recv_offset: 0,
            high,
            low,
            max,
            ultimate: max == u64::MAX,
            upstream_remain,
        }
    }

    fn should_slow_down(&self) -> bool {
        if self.recv_offset >= self.send_offset {
            return false;
        }
        self.send_offset - self.recv_offset >= self.high
    }

    fn should_pause(&self) -> bool {
        if self.recv_offset >= self.send_offset {
            return false;
        }
        self.send_offset - self.recv_offset >= self.max
    }

    fn should_resume(&self) -> bool {
        if self.recv_offset >= self.send_offset {
            return true;
        }
        self.send_offset - self.recv_offset <= self.low
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
}

impl BackpressureSender {
    pub fn new(id: String, sender: Sender<Row>, watermark: Arc<RwLock<Watermark>>) -> Self {
        BackpressureSender {
            id,
            sender: Box::new(sender),
            offset: Arc::new(std::sync::Mutex::new(0)),
            watermark,
        }
    }

    /// send row data with backpressure.
    pub async fn send(&self, mut row: Row) {
        loop {
            let watermark = self.watermark.read().await;
            if watermark.should_pause() {
                warn!("watermark overflow, should be paused.");
                tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                continue;
            } else if watermark.should_slow_down() {
                warn!("watermark high position, should be slow down.");
                tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            }
            break;
        }

        row.source = self.id.clone();
        let offset = {
            let mut offset_val = self.offset.lock().unwrap();
            *offset_val += 1;
            row.offset = *offset_val;
            row.offset
        };

        {
            let mut wm = self.watermark.write().await;
            wm.send_offset = offset;
        }
        match self.sender.send(row) {
            Ok(s) => {}
            Err(_) => {
                println!("Watermark sender dropped");
            }
        };
    }
}

impl Context {
    pub fn new(
        unit: ComputingUnit,
        sender: Sender<Row>,
        watermark: Arc<RwLock<Watermark>>,
    ) -> Self {
        let id = unit.id.clone();
        Context {
            unit,
            sender: BackpressureSender::new(id, sender, watermark),
        }
    }

    pub async fn send(&self, row: Row) {
        self.sender.send(row).await;
    }
}
