use crate::task::builtin::{
    DebugInputUnitTask, DebugMapUnitTask, DebugOutputUnitTask, MapUnitTask,
};
use crate::task::http_unit::HttpUnitTask;
use fusion_unit_sdk::graph::types::ComputingUnit;
use fusion_unit_sdk::runtime::logical::LogicalTask;
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};
use libloading::{Library, Symbol};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const DEFAULT_PLUGIN_PATH: &str = "__DEFAULT__";

pub struct PluginManager {
    plugin_map: Arc<Mutex<HashMap<String, String>>>,
    plugins: Arc<Mutex<HashMap<String, Box<dyn GraphUnitPlugin + Send + Sync>>>>,
}

impl PluginManager {
    pub async fn new() -> Self {
        let mut plugin_map = HashMap::new();
        let mut plugins: HashMap<_, Box<(dyn GraphUnitPlugin + Send + Sync)>> = HashMap::new();

        let builtin_plugin = BuiltinPlugin::default();
        let builtin_manifest = builtin_plugin.register_units();
        plugins.insert(DEFAULT_PLUGIN_PATH.to_string(), Box::new(builtin_plugin));
        let keys = builtin_manifest.keys();
        for key in keys {
            plugin_map.insert(key.clone(), DEFAULT_PLUGIN_PATH.to_string());
        }

        PluginManager {
            plugin_map: Arc::new(Mutex::new(plugin_map)),
            plugins: Arc::new(Mutex::new(plugins)),
        }
    }

    pub async fn add_plugin(&self, name: &str, plugin: Box<dyn GraphUnitPlugin + Send + Sync>) {
        let builtin_manifest = plugin.register_units();
        self.plugins.lock().await.insert(name.to_string(), plugin);

        let keys = builtin_manifest.keys();
        let mut plugin_map = self.plugin_map.lock().await;
        for key in keys {
            plugin_map.insert(key.clone(), name.to_string());
        }
    }

    pub async fn register_plugin(&self, path: &String) {
        let dylib_path = path;
        let mut plugin = unsafe {
            let lib = Library::new(dylib_path).expect("Failed to load library");
            let init_plugin: Symbol<unsafe fn() -> Box<dyn GraphUnitPlugin + Send + Sync>> =
                lib.get(b"init_plugin").expect("Failed to load symbol");
            init_plugin()
        };
        let manifest = plugin.register_units();
        let mut plugins = self.plugins.lock().await;
        plugins.insert(path.clone(), plugin);

        let keys = manifest.keys();
        for key in keys {
            self.plugin_map
                .lock()
                .await
                .insert(key.clone(), path.clone());
        }
    }

    pub async fn create_logical_task(
        &self,
        unit: ComputingUnit,
    ) -> Option<Box<dyn LogicalTask + Send>> {
        let version = unit.get_version();
        let key = format!("{}#{}", unit.get_type(), version);

        let plugin_map = self.plugin_map.lock().await;
        let plugin_path = plugin_map
            .get(&key)
            .expect(format!("Could not find plugin in map by key: {}", &key).as_str());

        let plugins = self.plugins.lock().await;
        let plugin = plugins
            .get(plugin_path)
            .expect(format!("Could not found unit provider: {}", key).as_str());
        plugin.create(unit)
    }
}

#[derive(Default)]
pub(crate) struct BuiltinPlugin {}

impl GraphUnitPlugin for BuiltinPlugin {
    fn register_units(&self) -> UnitManifest {
        let mut unit_manifest = UnitManifest::default();
        DebugInputUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());
        DebugMapUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());
        DebugOutputUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());
        MapUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());
        HttpUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());
        unit_manifest
    }

    fn plugin_version(&self) -> &str {
        "builtin"
    }
}
