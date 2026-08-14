//! # Fusion Runtime
//!
//! The main entry point for the Fusion streaming engine.
//!
//! This crate wires together capability plugins, unit plugins,
//! and graph execution. It is the top-level crate that users
//! depend on to embed Fusion.
//!
//! ## Quick start (singleton / embedded)
//!
//! ```ignore
//! use fusion_runtime::FusionRuntime;
//!
//! let runtime = FusionRuntime::init_app().await?;
//! runtime.execute("graph.yaml", None).await?;
//! ```
//!
//! ## Quick start (server / cluster)
//!
//! ```ignore
//! use fusion_runtime::{FusionRuntimeBuilder, config::FusionConfig};
//!
//! let cfg = FusionConfig::load()?;
//! let runtime = FusionRuntimeBuilder::new()
//!     .with_builtin_units()
//!     .with_all_units()
//!     .with_config(cfg)
//!     .build()
//!     .await?;
//! ```

pub mod config;

use config::FusionConfig;
use fusion_streaming::graph::core::LogicalGraph;
use fusion_streaming::runtime::core::{LaunchEnv, PhysicalGraph};
use fusion_streaming::runtime::plugin::PluginManager;
use fusion_unit_sdk::runtime::UnitResult;
use fusion_unit_sdk::GraphUnitPlugin;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

// ============================================================
// FusionRuntime
// ============================================================

/// The Fusion streaming runtime.
///
/// Owns the plugin manager and capability registry. Script engines
/// (Lua, Tera) are created **per graph execution** inside
/// [`PhysicalGraph::execute`] — concurrent graphs are fully isolated.
/// Created via [`FusionRuntimeBuilder`] or the [`FusionRuntime::init_app()`]
/// convenience method.
pub struct FusionRuntime {
    plugin_manager: Arc<Mutex<PluginManager>>,
    config: FusionConfig,
}

impl FusionRuntime {
    /// Create a new [`FusionRuntimeBuilder`].
    pub fn builder() -> FusionRuntimeBuilder {
        FusionRuntimeBuilder::new()
    }

    /// Initialize for singleton / embedded mode.
    ///
    /// Loads configuration from `config/fusion-conf.yaml` (falling back
    /// to the embedded `config/fusion-conf-app.yaml`), registers built-in
    /// units, and scans configured lib directories for plugins.
    ///
    /// This is the recommended entry point for desktop apps (Tauri) and
    /// other embedded scenarios.
    pub async fn init_app() -> anyhow::Result<Self> {
        let cfg = FusionConfig::load_or_embedded()?;
        FusionRuntimeBuilder::new()
            .with_builtin_units()
            .with_config(cfg)
            .build()
            .await
    }

    /// Access the [`PluginManager`] for dynamic plugin/capability loading.
    pub fn plugin_manager(&self) -> &Arc<Mutex<PluginManager>> {
        &self.plugin_manager
    }

    /// Access the loaded configuration.
    pub fn config(&self) -> &FusionConfig {
        &self.config
    }

    /// Execute a graph.
    ///
    /// `source` may be inline YAML/JSON, or a `file://` URL.
    /// Each call creates its own Lua/Tera engines — concurrent graphs
    /// are isolated.
    pub async fn execute(
        &self,
        source: impl Into<LogicalGraph>,
        env: Option<LaunchEnv>,
    ) -> UnitResult<()> {
        let logical_graph: LogicalGraph = source.into();
        let physical =
            PhysicalGraph::new(logical_graph, self.plugin_manager.clone());
        physical.execute(env).await
    }

    /// Execute a graph from a file path (YAML or JSON).
    pub async fn execute_file(
        &self,
        path: impl AsRef<Path>,
        env: Option<LaunchEnv>,
    ) -> UnitResult<()> {
        let uri = format!("file://{}", path.as_ref().display());
        self.execute(uri, env).await
    }
}

// ============================================================
// FusionRuntimeBuilder
// ============================================================

/// Builder for [`FusionRuntime`].
///
/// Configures which plugins and capabilities to load, then finalizes
/// into a ready-to-use runtime via [`build()`](Self::build).
pub struct FusionRuntimeBuilder {
    /// Paths to capability plugin dylibs (loaded via libloading).
    capability_paths: Vec<String>,
    /// Paths to unit plugin dylibs (loaded via libloading).
    plugin_paths: Vec<String>,
    /// Statically-linked unit plugins.
    unit_plugins: Vec<(String, Box<dyn GraphUnitPlugin + Send + Sync>)>,
    /// Whether to register the built-in debug/map/http units.
    include_builtin: bool,
    /// Configuration (if provided).
    config: Option<FusionConfig>,
    /// Whether config lib dirs have been auto-scanned.
    config_libs_scanned: bool,
}

impl FusionRuntimeBuilder {
    /// Start with an empty configuration.
    pub fn new() -> Self {
        Self {
            capability_paths: Vec::new(),
            plugin_paths: Vec::new(),
            unit_plugins: Vec::new(),
            include_builtin: false,
            config: None,
            config_libs_scanned: false,
        }
    }

    /// Register the built-in unit types that ship with `fusion-streaming`.
    pub fn with_builtin_units(mut self) -> Self {
        self.include_builtin = true;
        self
    }

    /// Apply a [`FusionConfig`].
    ///
    /// Lib directories defined in the config are scanned during
    /// [`build()`](Self::build) and any discovered dylibs are loaded
    /// automatically.
    pub fn with_config(mut self, cfg: FusionConfig) -> Self {
        self.config = Some(cfg);
        self
    }

    /// Load a config file and apply it.
    ///
    /// This is a convenience wrapper around [`FusionConfig::load()`]
    /// and [`with_config()`](Self::with_config).
    pub fn with_config_file(self, path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())?;
        let cfg: FusionConfig = serde_yaml::from_str(&content)?;
        Ok(self.with_config(cfg))
    }

    /// Queue a capability plugin dylib to load during [`build()`](Self::build).
    pub fn with_capability(mut self, path: impl Into<String>) -> Self {
        self.capability_paths.push(path.into());
        self
    }

    /// Queue a unit plugin dylib to load during [`build()`](Self::build).
    pub fn with_plugin(mut self, path: impl Into<String>) -> Self {
        self.plugin_paths.push(path.into());
        self
    }

    /// Register a statically-linked unit plugin.
    pub fn with_unit_plugin(
        mut self,
        name: impl Into<String>,
        plugin: Box<dyn GraphUnitPlugin + Send + Sync>,
    ) -> Self {
        self.unit_plugins.push((name.into(), plugin));
        self
    }

    // ---- Scan config lib dirs for plugins ----

    /// Auto-discover plugins in the configured lib directories and
    /// queue them for loading during [`build()`](Self::build).
    ///
    /// Called automatically by `build()` if a config was provided.
    /// Can also be called explicitly before `build()` to control
    /// discovery timing.
    pub fn scan_config_libs(mut self) -> Self {
        if self.config_libs_scanned {
            return self;
        }
        self.config_libs_scanned = true;

        let cfg = match &self.config {
            Some(c) => c,
            None => return self,
        };

        // Discover capability libraries
        for (_name, path) in cfg.discover_capability_libs() {
            let path_str = path.to_string_lossy().to_string();
            log::info!(
                "Config: queuing capability lib `{}`",
                path.display()
            );
            self.capability_paths.push(path_str);
        }

        // Discover unit libraries
        for (_name, path) in cfg.discover_unit_libs() {
            let path_str = path.to_string_lossy().to_string();
            log::info!(
                "Config: queuing unit lib `{}`",
                path.display()
            );
            self.plugin_paths.push(path_str);
        }

        self
    }

    // ---- Build ----

    /// Finalize configuration and create the [`FusionRuntime`].
    ///
    /// Steps (in order):
    ///
    /// 1. Create [`PluginManager`].
    /// 2. Register statically-linked unit plugins.
    /// 3. Populate the process-global config registry from the YAML
    ///    `config:` section (also injected into unit/provider dylibs).
    /// 4. Auto-scan config lib directories (if config was provided).
    /// 5. Load capability dylibs (collects the SQL engine factory).
    /// 6. Load provider dylibs (collects table providers) — before units,
    ///    because unit dylibs receive the collected providers.
    /// 7. Load unit dylibs (injected with config + factory + providers).
    ///
    /// Script engines are created per graph execution (see
    /// [`FusionRuntime::execute`]), not at build time.
    pub async fn build(mut self) -> anyhow::Result<FusionRuntime> {
        // 1. Plugin manager.
        let plugin_manager = PluginManager::new().await;

        // 2. Statically-linked unit plugins.
        let unit_plugins = std::mem::take(&mut self.unit_plugins);
        for (name, plugin) in unit_plugins {
            plugin_manager.add_plugin(&name, plugin).await;
            log::info!("Registered unit plugin `{name}`");
        }

        // 3. Populate the config registry from the YAML `config:` section.
        if let Some(config_value) = self
            .config
            .as_ref()
            .and_then(|cfg| cfg.config.as_ref())
            .and_then(|c| c.as_object())
        {
            fusion_unit_sdk::config::register(|reg| {
                for (category, types) in config_value {
                    let Some(types) = types.as_object() else {
                        continue;
                    };
                    for (config_type, ids) in types {
                        let Some(ids) = ids.as_object() else {
                            continue;
                        };
                        for (id, data) in ids {
                            reg.insert(
                                category.clone(),
                                config_type.clone(),
                                id.clone(),
                                data.clone(),
                            );
                        }
                    }
                }
            });
            log::info!("Populated config registry from YAML `config:` section");
        }

        // 4. Auto-scan config lib dirs for dynamic plugins.
        self = self.scan_config_libs();

        // 5. Capability plugins (dylib).
        for path in &self.capability_paths {
            plugin_manager.load_capability_plugin(path).await?;
            log::info!("Loaded capability plugin: {path}");
        }

        // 6. Provider plugins (dylib) — must precede unit dylibs so their
        // providers can be injected into the units.
        for path in &self.plugin_paths {
            plugin_manager.load_provider_plugin(path).await?;
        }

        // 7. Unit plugins (dylib).
        for path in &self.plugin_paths {
            plugin_manager.register_plugin(path).await;
            log::info!("Loaded unit plugin: {path}");
        }

        let config = self.config.unwrap_or_default();

        Ok(FusionRuntime {
            plugin_manager: Arc::new(Mutex::new(plugin_manager)),
            config,
        })
    }
}

impl Default for FusionRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================
// Feature-gated unit plugins (statically linked)
// ============================================================

impl FusionRuntimeBuilder {
    #[cfg(feature = "unit-datafusion")]
    pub fn with_datafusion(mut self) -> Self {
        self.unit_plugins.push((
            "datafusion".into(),
            Box::new(fusion_unit_datafusion::SqlUnitPlugin {}),
        ));
        self
    }

    #[cfg(feature = "unit-excel")]
    pub fn with_excel(mut self) -> Self {
        self.unit_plugins.push((
            "excel".into(),
            Box::new(fusion_unit_excel::ExcelUnitPlugin {}),
        ));
        self
    }

    #[cfg(feature = "unit-ssh")]
    pub fn with_ssh(mut self) -> Self {
        self.unit_plugins.push((
            "ssh".into(),
            Box::new(fusion_unit_ssh::SSHUnitPlugin {}),
        ));
        self
    }

    #[cfg(feature = "unit-redis")]
    pub fn with_redis(mut self) -> Self {
        self.unit_plugins.push((
            "redis".into(),
            Box::new(fusion_unit_redis::RedisUnitPlugin {}),
        ));
        self
    }

    #[cfg(feature = "unit-net")]
    pub fn with_net(mut self) -> Self {
        self.unit_plugins.push((
            "net".into(),
            Box::new(fusion_unit_net::NetUnitPlugin {}),
        ));
        self
    }

    #[cfg(feature = "unit-universal-fs")]
    pub fn with_universal_fs(mut self) -> Self {
        self.unit_plugins.push((
            "universal-fs".into(),
            Box::new(fusion_unit_universal_fs::UniversalFsUnitPlugin {}),
        ));
        self
    }

    #[cfg(feature = "all-units")]
    pub fn with_all_units(self) -> Self {
        self.with_datafusion()
            .with_excel()
            .with_ssh()
            .with_redis()
            .with_net()
            .with_universal_fs()
    }
}

// ============================================================
// GraphExecutor — multi-graph lifecycle management
// ============================================================

/// Execution status of a submitted graph.
#[derive(Debug, Clone)]
pub enum GraphStatus {
    /// Graph is queued or running.
    Running,
    /// Graph finished successfully.
    Done,
    /// Graph failed with an error message.
    Failed(String),
}

/// A background executor that runs multiple graphs concurrently.
///
/// Each [`submit`](Self::submit) call spawns an independent tokio task;
/// per-graph script engines guarantee isolation. The shared
/// [`PluginManager`] registry is read-only during execution, so
/// concurrent graphs never interfere.
pub struct GraphExecutor {
    runtime: Arc<FusionRuntime>,
    tasks: Arc<Mutex<HashMap<String, JoinHandle<UnitResult<()>>>>>,
}

impl GraphExecutor {
    pub fn new(runtime: Arc<FusionRuntime>) -> Self {
        Self {
            runtime,
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Submit a graph (YAML/JSON string or file URL) for execution in
    /// the background. Returns the graph id.
    pub async fn submit(
        &self,
        source: impl Into<LogicalGraph>,
        env: Option<LaunchEnv>,
    ) -> String {
        let graph: LogicalGraph = source.into();
        let graph_id = graph.get_id();
        log::info!("GraphExecutor: submit graph `{graph_id}`");
        let runtime = self.runtime.clone();
        let gid = graph_id.clone();
        let handle = tokio::spawn(async move {
            log::info!("GraphExecutor: executing graph `{gid}`");
            let result = runtime.execute(graph, env).await;
            log::info!(
                "GraphExecutor: graph `{gid}` finished: {}",
                match &result {
                    Ok(()) => "ok".to_string(),
                    Err(e) => format!("{e:?}"),
                }
            );
            result
        });
        self.tasks.lock().await.insert(graph_id.clone(), handle);
        graph_id
    }

    /// Check the status of a submitted graph.
    ///
    /// `None` means the id is unknown (never submitted, or already
    /// reaped after completion).
    pub async fn status(&self, id: &str) -> Option<GraphStatus> {
        let mut tasks = self.tasks.lock().await;
        let handle = tasks.remove(id)?;
        if handle.is_finished() {
            match handle.await {
                Ok(Ok(())) => Some(GraphStatus::Done),
                Ok(Err(e)) => Some(GraphStatus::Failed(e.to_string())),
                Err(e) => Some(GraphStatus::Failed(format!("task panicked: {e}"))),
            }
        } else {
            tasks.insert(id.to_string(), handle);
            Some(GraphStatus::Running)
        }
    }

    /// Cancel a running graph. Returns `true` if it was still running.
    pub async fn cancel(&self, id: &str) -> bool {
        let mut tasks = self.tasks.lock().await;
        match tasks.remove(id) {
            Some(handle) => {
                handle.abort();
                true
            }
            None => false,
        }
    }

    /// Block until a submitted graph finishes.
    pub async fn wait(&self, id: &str) -> Option<UnitResult<()>> {
        let mut tasks = self.tasks.lock().await;
        let handle = tasks.remove(id)?;
        match handle.await {
            Ok(result) => Some(result),
            Err(e) => Some(Err(fusion_unit_sdk::runtime::UnitError::unknown(
                format!("task panicked: {e}"),
            ))),
        }
    }

    /// Number of currently running graphs.
    pub async fn running_count(&self) -> usize {
        let tasks = self.tasks.lock().await;
        tasks.values().filter(|h| !h.is_finished()).count()
    }
}
