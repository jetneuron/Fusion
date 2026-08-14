use crate::task::builtin::{
    DebugInputUnitTask, DebugMapUnitTask, DebugOutputUnitTask, MapUnitTask,
};
use crate::task::http_unit::HttpUnitTask;
use fusion_unit_sdk::capability::CapabilityPlugin;
use fusion_unit_sdk::config;
use fusion_unit_sdk::graph::types::ComputingUnit;
use fusion_unit_sdk::providers::{ProviderPlugin, TableDataProvider};
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use fusion_unit_sdk::runtime::logical::LogicalTask;
use fusion_unit_sdk::sql_engine_ffi::{HostProviderEntry, HostProviders, SqlEngineFactory};
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};
use libloading::{Library, Symbol};
use std::collections::HashMap;
use std::ffi::{c_char, CString};
use std::sync::Arc;
use tokio::sync::Mutex;

const DEFAULT_PLUGIN_PATH: &str = "__DEFAULT__";

pub struct PluginManager {
    plugin_map: Arc<Mutex<HashMap<String, String>>>,
    plugins: Arc<Mutex<HashMap<String, Box<dyn GraphUnitPlugin + Send + Sync>>>>,
    /// Engine factory collected from the capability dylib
    /// (`init_sql_engine_factory`), injected into unit dylibs via
    /// `set_sql_engine_factory`.
    engine_factory: Arc<Mutex<Option<SqlEngineFactory>>>,
    /// Providers collected from provider dylibs (`init_provider_plugin`),
    /// injected into unit dylibs via `set_host_providers`.
    host_providers: Arc<Mutex<Vec<(String, Arc<dyn TableDataProvider>)>>>,
    /// Keep loaded dylibs alive for the lifetime of the manager. Trait
    /// objects, vtables and injected pointers live in the dylibs' binary
    /// images — unloading them would dangle.
    _libs: Arc<Mutex<Vec<Library>>>,
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
            engine_factory: Arc::new(Mutex::new(None)),
            host_providers: Arc::new(Mutex::new(Vec::new())),
            _libs: Arc::new(Mutex::new(Vec::new())),
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

    /// Serialize the process-global config registry for FFI injection.
    fn serialize_config() -> String {
        let reg = config::read();
        let entries: Vec<config::InjectedConfig> = reg
            .ids()
            .filter_map(|id| {
                reg.entry(id).map(|e| config::InjectedConfig {
                    category: e.category.clone(),
                    config_type: e.config_type.clone(),
                    id: id.clone(),
                    data: e.data.clone(),
                })
            })
            .collect();
        serde_json::to_string(&entries).unwrap_or_default()
    }

    /// Load an external provider plugin (`.dylib` / `.so`) and collect
    /// its `TableDataProvider`s for later injection into unit dylibs.
    ///
    /// Dylibs without an `init_provider_plugin` symbol are skipped (they
    /// may be unit or capability dylibs in the same directory).
    pub async fn load_provider_plugin(&self, path: &str) -> UnitResult<()> {
        let lib = unsafe { Library::new(path) }.map_err(|e| {
            UnitError::unknown(format!("Failed to load provider dylib `{path}`: {e}"))
        })?;
        let mut plugin: Option<Box<dyn ProviderPlugin + Send + Sync>> = None;
        unsafe {
            let init: Symbol<unsafe fn() -> Box<dyn ProviderPlugin + Send + Sync>> =
                match lib.get(b"init_provider_plugin") {
                    Ok(f) => f,
                    Err(_) => {
                        log::info!("`{path}` is not a provider dylib (no `init_provider_plugin`)");
                        return Ok(());
                    }
                };
            // Inject the host config registry so register_providers() can
            // resolve its datasource entries (statics are per-image).
            if let Ok(set_config) =
                lib.get::<unsafe extern "C" fn(*const c_char)>(b"set_config")
            {
                let json = CString::new(Self::serialize_config()).unwrap_or_default();
                set_config(json.as_ptr());
            }
            plugin = Some(init());
            // Keep the dylib alive — provider trait objects live in it.
            self._libs.lock().await.push(lib);
        }
        let plugin = plugin.expect("provider plugin loaded above");
        let providers = plugin.register_providers();
        if providers.is_empty() {
            // Zero providers almost always means the injected config
            // registry has no matching datasource entries — surface it
            // now instead of a confusing `provider not found` at graph
            // execution time.
            log::warn!(
                "Provider plugin from `{path}` registered 0 providers — check \
                 `datasource:` config entries of the matching type (config is \
                 injected into provider dylibs before register_providers())"
            );
        }
        log::info!(
            "Loaded provider plugin v{} from `{path}` ({} providers)",
            plugin.version(),
            providers.len()
        );
        self.host_providers.lock().await.extend(providers);
        Ok(())
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

            // Inject host state into this dylib's binary image: config
            // registry, engine factory and provider objects. SqlUnitTask
            // reads them when graphs execute.
            if let Ok(set_config) =
                lib.get::<unsafe extern "C" fn(*const c_char)>(b"set_config")
            {
                let json = CString::new(Self::serialize_config()).unwrap_or_default();
                set_config(json.as_ptr());
            }
            if let Ok(set_factory) =
                lib.get::<unsafe extern "C" fn(SqlEngineFactory)>(b"set_sql_engine_factory")
            {
                if let Some(factory) = *self.engine_factory.lock().await {
                    set_factory(factory);
                }
            }
            if let Ok(set_providers) =
                lib.get::<unsafe extern "C" fn(HostProviders)>(b"set_host_providers")
            {
                let providers = self.host_providers.lock().await;
                if !providers.is_empty() {
                    let cstrings: Vec<CString> = providers
                        .iter()
                        .map(|(name, _)| CString::new(name.as_str()).unwrap_or_default())
                        .collect();
                    let entries: Vec<HostProviderEntry> = providers
                        .iter()
                        .zip(cstrings.iter())
                        .map(|((_, p), cname)| {
                            // Transfer one Arc<dyn TableDataProvider> fat
                            // pointer per entry (host keeps its Vec).
                            let raw = Arc::into_raw(Arc::clone(p));
                            let (data, vtable): (*const (), *const ()) =
                                unsafe { std::mem::transmute(raw) };
                            HostProviderEntry {
                                name: cname.as_ptr(),
                                provider_data: data,
                                provider_vtable: vtable,
                            }
                        })
                        .collect();
                    set_providers(HostProviders {
                        entries: entries.as_ptr(),
                        len: entries.len(),
                    });
                }
            }

            let plugin = init_plugin();
            // Keep the dylib alive — the plugin's trait object vtable
            // lives in its binary image.
            self._libs.lock().await.push(lib);
            plugin
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
    /// map — they register directly into the process-global registry. The
    /// dylib is kept alive by the manager (trait objects live in it), and
    /// an optional `init_sql_engine_factory` symbol is collected for
    /// injection into unit dylibs.
    pub async fn load_capability_plugin(&self, path: &str) -> UnitResult<()> {
        let lib = unsafe { Library::new(path) }.map_err(|e| {
            UnitError::unknown(format!("Failed to load capability dylib `{path}`: {e}"))
        })?;
        let mut capability: Option<Box<dyn CapabilityPlugin + Send + Sync>> = None;
        unsafe {
            let init: Symbol<unsafe extern "C" fn() -> Box<dyn CapabilityPlugin + Send + Sync>> =
                lib.get(b"init_capability_plugin").map_err(|e| {
                    UnitError::unknown(format!(
                        "Symbol `init_capability_plugin` not found in `{path}`: {e}"
                    ))
                })?;
            // DataFusion capability dylibs export the engine factory.
            if let Ok(factory_sym) =
                lib.get::<unsafe fn() -> SqlEngineFactory>(b"init_sql_engine_factory")
            {
                *self.engine_factory.lock().await = Some(factory_sym());
            }
            capability = Some(init());
            self._libs.lock().await.push(lib);
        }
        let capability = capability.expect("capability plugin loaded above");
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
