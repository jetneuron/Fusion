use crate::graph::core::LogicalGraph;
use crate::runtime::core::PhysicalGraph;
use async_trait::async_trait;
use lazy_static::lazy_static;
use tera::Function;

pub mod core;
pub mod physical;
pub mod sandbox;
pub mod plugin;

lazy_static! {
    pub static ref PRODUCT_NAME: String = "FusionPro".to_string();
    pub static ref PRODUCT_VER: String = "0.1.0".to_string();
    pub static ref PRODUCT_INFO: String = format!("{} / {}", PRODUCT_NAME.as_str(), PRODUCT_VER.as_str());
}

#[async_trait]
pub trait GraphRuntime {
    fn create<T: Into<LogicalGraph>>(&self, graph: T) -> PhysicalGraph;

    async fn register_tera_function<F: Function + 'static>(&mut self, name: &str, function: F) {}
}


pub(crate) const EVENT_TYPE_EOF: i32 = 1 << 0;
pub(crate) const EVENT_TYPE_START: i32 = 1 << 1;
