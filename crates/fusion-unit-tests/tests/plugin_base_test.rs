mod datafusion;
mod excel;
mod unit_test;

use fusion_streaming::runtime::physical::PhysicalTask;
use fusion_streaming::runtime::plugin::PluginManager;
use fusion_streaming::runtime::sandbox::SandboxRuntime;
use fusion_streaming::runtime::GraphRuntime;
use fusion_streaming::task::builtin::{DebugInputUnitTask, DebugMapUnitTask, DebugOutputUnitTask};
use fusion_unit_sdk::graph::types::ComputingUnit;
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};

use fusion_streaming::utils::tera_func::RegisterTeraBuiltinFunc;
use fusion_unit_datafusion::datafusion_unit::DataFusionUnit;
use fusion_unit_excel::SpreadSheetUnitTask;
use fusion_unit_ssh::SSHUnitTask;
use fusion_unit_universal_fs::UniversalFsUnitTask;
use libloading::{Library, Symbol};
use std::ops::Deref;

#[derive(Default)]
pub(crate) struct TestPlugin {}

impl GraphUnitPlugin for TestPlugin {
    fn register_units(&self) -> UnitManifest {
        let mut manifest = UnitManifest::default();
        // Debug
        DebugInputUnitTask::register_unit(&mut manifest, self.plugin_version());
        DebugMapUnitTask::register_unit(&mut manifest, self.plugin_version());
        DebugOutputUnitTask::register_unit(&mut manifest, self.plugin_version());

        // Excel
        SpreadSheetUnitTask::register_unit(&mut manifest, self.plugin_version());

        // DataFusion
        DataFusionUnit::register_unit(&mut manifest, self.plugin_version());

        // SSH
        SSHUnitTask::register_unit(&mut manifest, self.plugin_version());

        // Universal FS
        UniversalFsUnitTask::register_unit(&mut manifest, self.plugin_version());
        manifest
    }
}

pub(crate) async fn register_plugin_execute(plugin_paths: Vec<String>, graph: &str) {
    let plugin_manager = PluginManager::new().await;
    plugin_manager
        .add_plugin("test", Box::new(TestPlugin::default()))
        .await;
    for path in plugin_paths.iter() {
        plugin_manager.register_plugin(path).await;
    }
    let mut runtime = SandboxRuntime::new(plugin_manager);
    runtime.register_builtin_tera_functions().await;
    let file = graph;
    let graph_path = format!(
        "file://{}/tests/graphs/{}",
        env!("CARGO_MANIFEST_DIR"),
        file
    );
    let graph = runtime.create(graph_path);

    // execute the physical graph.
    graph.execute().await;
}

pub(crate) async fn execute(graph: &str) {
    let plugin_manager = PluginManager::new().await;
    plugin_manager
        .add_plugin("test", Box::new(TestPlugin::default()))
        .await;
    let mut runtime = SandboxRuntime::new(plugin_manager);
    runtime.register_builtin_tera_functions().await;
    let file = graph;
    let graph_path = format!(
        "file://{}/tests/graphs/{}",
        env!("CARGO_MANIFEST_DIR"),
        file
    );
    let graph = runtime.create(graph_path);

    // execute the physical graph.
    graph.execute().await;
}

#[tokio::test]
pub async fn test() {
    let dylib_path = format!(
        "{}/../graph-unit-example/target/release/libgraph_unit_example.dylib",
        env!("CARGO_MANIFEST_DIR")
    );
    {
        let mut plugin = unsafe {
            let lib = Library::new(dylib_path).expect("Failed to load library");
            let init_plugin: Symbol<unsafe fn() -> Box<dyn GraphUnitPlugin>> =
                lib.get(b"init_plugin").expect("Failed to load symbol");
            init_plugin()
        };

        plugin.register_units();
        let plugin_version = plugin.plugin_version();
        println!("registered plugin version: {}", plugin_version);

        let unit = ComputingUnit::new("id1", "ExampleSourceUnit");
        if let Ok(instance) = plugin.create(unit) {
            let phy_task = PhysicalTask::new(instance);
            println!("created instance");
        }

        let t = plugin.deref();
        println!("----------->>2");
        println!("plugin 尚未离开作用域");
    }
    println!("plugin 离开了作用域")
}
