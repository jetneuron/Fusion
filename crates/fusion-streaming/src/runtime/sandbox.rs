use crate::graph::core::LogicalGraph;
use crate::runtime::GraphRuntime;
use crate::runtime::core::PhysicalGraph;
use crate::runtime::plugin::PluginManager;
use crate::utils::tera_func;
use crate::utils::tera_func::RegisterTeraBuiltinFunc;
use async_trait::async_trait;
use log::trace;
use mlua::Lua;
use std::sync::Arc;
use tera::{Function, Tera};
use tokio::sync::Mutex;

pub struct SandboxRuntime {
    plugin_manager: Arc<Mutex<PluginManager>>,
    global_lua: Arc<Mutex<Lua>>,
    tera: Arc<Mutex<Tera>>,
}

impl SandboxRuntime {
    pub fn new(plugin_manager: PluginManager) -> SandboxRuntime {
        SandboxRuntime {
            plugin_manager: Arc::new(Mutex::new(plugin_manager)),
            global_lua: Arc::new(Mutex::new(Lua::new())),
            tera: Arc::new(Mutex::new(Tera::default())),
        }
    }
}

#[async_trait]
impl GraphRuntime for SandboxRuntime {
    fn create<T: Into<LogicalGraph>>(&self, graph: T) -> PhysicalGraph {
        let logical_graph = graph.into();
        PhysicalGraph {
            logical_graph,
            plugin_manager: self.plugin_manager.clone(),
            graph_lua: Arc::clone(&self.global_lua),
            tera: Arc::clone(&self.tera),
        }
    }

    async fn register_tera_function<F: Function + 'static>(&mut self, name: &str, function: F) {
        let mut tera = self.tera.lock().await;
        tera.register_function(name, function);
        trace!(
            "register functions to [sandbox] physical environment: {}",
            name
        );
    }
}

#[async_trait]
impl RegisterTeraBuiltinFunc for SandboxRuntime {
    async fn register_builtin_tera_functions(&mut self) {
        self.register_tera_function("yyyyMMdd", tera_func::yyyymmdd)
            .await;
        self.register_tera_function("yyyy_MM_dd", tera_func::yyyy_mm_dd)
            .await;
        self.register_tera_function("now", tera_func::now).await;
        self.register_tera_function("time", tera_func::human_time)
            .await;
    }
}
