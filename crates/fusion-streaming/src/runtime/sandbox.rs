use crate::graph::core::LogicalGraph;
use crate::runtime::GraphRuntime;
use crate::runtime::core::PhysicalGraph;
use crate::runtime::plugin::PluginManager;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SandboxRuntime {
    plugin_manager: Arc<Mutex<PluginManager>>,
}

impl SandboxRuntime {
    pub fn new(plugin_manager: PluginManager) -> SandboxRuntime {
        SandboxRuntime {
            plugin_manager: Arc::new(Mutex::new(plugin_manager)),
        }
    }
}

#[async_trait]
impl GraphRuntime for SandboxRuntime {
    fn create<T: Into<LogicalGraph>>(&self, graph: T) -> PhysicalGraph {
        let logical_graph = graph.into();
        PhysicalGraph::new(logical_graph, self.plugin_manager.clone())
    }
}
