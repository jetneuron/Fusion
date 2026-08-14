use crate::task::builtin::{
    DebugInputUnitTask, DebugMapUnitTask, DebugOutputUnitTask, MapUnitTask,
};
use crate::task::http_unit::HttpUnitTask;
use fusion_unit_sdk::capability::CapabilityPlugin;
use fusion_unit_sdk::graph::types::ComputingUnit;
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
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
            // The unit lib dir may contain non-unit dylibs — e.g.
            // provider-only crates like `fusion-unit-datafusion-sqlite`
            // export `init_provider_plugin` but no `init_plugin`. A
            // missing symbol means "not a unit plugin"; skip it instead
            // of panicking (its registration would also override the
            // real unit plugin from the same dir).
            let init_plugin: Symbol<unsafe fn() -> Box<dyn GraphUnitPlugin + Send + Sync>> =
                match lib.get(b"init_plugin") {
                    Ok(f) => f,
                    Err(_) => {
                        log::info!("Skipping `{path}`: no `init_plugin` symbol (provider dylib?)");
                        return;
                    }
                };
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

    /// Load an external capability plugin (`.dylib` / `.so`) and register its
    /// capabilities into the global [`CapabilityRegistry`](fusion_unit_sdk::capability::CapabilityRegistry).
    ///
    /// Unlike unit plugins, capability plugins are not stored in the plugin
    /// map — they register directly into the process-global registry and the
    /// loaded library must be kept alive by the caller or manager.
    pub async fn load_capability_plugin(&self, path: &str) -> UnitResult<()> {
        let capability = unsafe {
            let lib = Library::new(path).map_err(|e| {
                UnitError::unknown(format!("Failed to load capability dylib `{path}`: {e}"))
            })?;
            let init: Symbol<unsafe extern "C" fn() -> Box<dyn CapabilityPlugin + Send + Sync>> =
                lib.get(b"init_capability_plugin").map_err(|e| {
                    UnitError::unknown(format!(
                        "Symbol `init_capability_plugin` not found in `{path}`: {e}"
                    ))
                })?;
            // NOTE: `lib` is dropped here, which unloads the dylib.
            // The capability trait objects in the global registry hold
            // vtables pointing into dylib memory — this is a known issue.
            // Future: store `Library` handles in PluginManager to keep
            // capability dylibs alive.
            init()
        };
        capability.register();
        log::info!(
            "Loaded capability plugin v{} from `{path}`",
            capability.version()
        );
        Ok(())
    }

    pub async fn create_logical_task(
        &self,
        unit: ComputingUnit,
    ) -> UnitResult<Box<dyn LogicalTask + Send + Sync>> {
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
