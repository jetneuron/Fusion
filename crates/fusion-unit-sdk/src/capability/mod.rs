//! Capability plugin system.
//!
//! ## Finding available capabilities
//!
//! All capability traits are prefixed with `Capability` and live in
//! files named `capability_*.rs`. In an IDE, search for `Capability`
//! to see every trait the engine supports.
//!
//! | File | Trait | Purpose |
//! |------|-------|---------|
//! | [`capability_sql_engine`] | [`CapabilitySqlEngine`] | SQL query execution |
//! | [`capability_document_database`] | [`CapabilityDocumentDatabase`] | Document-oriented storage |
//! | [`capability_key_value_store`] | [`CapabilityKeyValueStore`] | Key-value get/set/scan |
//! | [`capability_spreadsheet_engine`] | [`CapabilitySpreadsheetEngine`] | Spreadsheet read/write |
//!
//! Each trait file also contains a [`well_known`] sub-module listing
//! canonical implementation names.
//!
//! ## Architecture: factories + instances
//!
//! Capabilities are created in two layers:
//!
//! 1. **Factory** — registered by capability plugins, bound to a `config_type`
//!    (e.g. `"redis"`). A factory knows how to create an instance from a
//!    [`ConfigEntry`](crate::config::ConfigEntry).
//! 2. **Instance** — created lazily when first requested by instance ID
//!    (e.g. `"redis-cache"`). The instance ID matches a config entry whose
//!    `config_type` triggers the corresponding factory.
//!
//! ```text
//! capability::kv("redis-cache")
//!   → read config entry "redis-cache" → config_type = "redis"
//!   → find factory for "redis"
//!   → factory(&entry) → Arc<dyn CapabilityKeyValueStore>
//!   → cache under "redis-cache"
//! ```
//!
//! ## Lifecycle
//!
//! ```text
//! register factory → create instance (lazy) → init → (use) → shutdown
//! ```
//!
//! ## Adding a new capability
//!
//! 1. Create `capability_<name>.rs` in this directory.
//! 2. Define a trait `Capability<Name>` extending [`Capability`].
//! 3. Add a `well_known` sub-module with canonical name constants.
//! 4. Add a `HashMap<String, Arc<dyn Trait>>` slot in [`CapabilityRegistry`]
//!    with `set_xxx()` / `xxx()` / `init_xxx()` / `shutdown_xxx()` methods.
//! 5. Declare `pub mod capability_<name>;` below and re-export the trait.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::runtime::UnitResult;

// ---- Sub-modules (one `capability_*.rs` per capability trait) --
pub mod capability_sql_engine;
pub mod capability_document_database;
pub mod capability_key_value_store;
pub mod capability_key_value_store_config;
pub mod capability_spreadsheet_engine;

// Re-export all `Capability*` traits for convenient imports.
pub use capability_sql_engine::CapabilitySqlEngine;
pub use capability_document_database::CapabilityDocumentDatabase;
pub use capability_key_value_store::CapabilityKeyValueStore;
pub use capability_spreadsheet_engine::CapabilitySpreadsheetEngine;

// ---- Factory types ------------------------------------------

/// A factory that creates a [`CapabilityKeyValueStore`] instance from a
/// config entry.
pub type KvFactory = Box<
    dyn Fn(&crate::config::ConfigEntry) -> crate::runtime::UnitResult<Arc<dyn CapabilityKeyValueStore>>
        + Send
        + Sync,
>;

/// A factory that creates a [`CapabilitySqlEngine`] instance from a
/// config entry.
pub type SqlFactory = Box<
    dyn Fn(&crate::config::ConfigEntry) -> crate::runtime::UnitResult<Arc<dyn CapabilitySqlEngine>>
        + Send
        + Sync,
>;

/// A factory that creates a [`CapabilityDocumentDatabase`] instance from a
/// config entry.
pub type DocFactory = Box<
    dyn Fn(&crate::config::ConfigEntry) -> crate::runtime::UnitResult<Arc<dyn CapabilityDocumentDatabase>>
        + Send
        + Sync,
>;

/// A factory that creates a [`CapabilitySpreadsheetEngine`] instance from a
/// config entry.
pub type SpreadsheetFactory = Box<
    dyn Fn(&crate::config::ConfigEntry) -> crate::runtime::UnitResult<Arc<dyn CapabilitySpreadsheetEngine>>
        + Send
        + Sync,
>;

// ============================================================
// Base capability trait
// ============================================================

/// Base trait for all capability implementations.
///
/// Capabilities are process-global services that unit plugins consume.
/// Each concrete implementation provides a [`name()`](Capability::name)
/// that serves as its lookup key in the [`CapabilityRegistry`].
///
/// # Lifecycle
///
/// ```text
/// register → init → (use) → shutdown
/// ```
///
/// 1. **register** — the capability is stored in the registry via
///    `set_xxx()`. No resources are allocated yet.
/// 2. **init** — [`init()`](Capability::init) is called to establish
///    connections, open files, allocate pools, etc.
/// 3. **shutdown** — [`shutdown()`](Capability::shutdown) releases
///    resources before the capability is removed from the registry.
///
/// # Example
///
/// ```ignore
/// use fusion_unit_sdk::capability::capability_key_value_store::well_known;
///
/// struct RedisKV { client: Option<redis::Client> }
///
/// impl Capability for RedisKV {
///     fn name(&self) -> &str { well_known::REDIS }
///
///     async fn init(&self) -> UnitResult<()> {
///         // establish connection on first use or at init time
///         Ok(())
///     }
///
///     async fn shutdown(&self) -> UnitResult<()> {
///         // close connections gracefully
///         Ok(())
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait Capability: Send + Sync + 'static {
    /// Unique name for this capability implementation.
    ///
    /// Used as the lookup key in [`CapabilityRegistry`]. Two implementations
    /// of the same capability trait can coexist when they return different names.
    fn name(&self) -> &str;

    /// Initialize this capability.
    ///
    /// Called once after registration. Use this to establish database
    /// connections, open files, allocate thread pools, or perform any
    /// async setup that must complete before the capability is usable.
    ///
    /// The default implementation is a no-op.
    async fn init(&self) -> UnitResult<()> {
        Ok(())
    }

    /// Shut down this capability and release resources.
    ///
    /// Called before the capability is removed from the registry.
    /// Use this to close connections, flush buffers, or gracefully
    /// terminate background tasks.
    ///
    /// The default implementation is a no-op.
    async fn shutdown(&self) -> UnitResult<()> {
        Ok(())
    }
}

// ============================================================
// Global capability registry
// ============================================================

/// Holds capability factories and instances.
///
/// Capabilities are obtained by **instance ID** (e.g. `"redis-cache"`),
/// which matches a config entry's ID. The config entry's `config_type`
/// (e.g. `"redis"`) selects the factory that creates the instance.
///
/// # Two registration modes
///
/// | Method | Use case |
/// |--------|----------|
/// | `set_kv(instance)` | Pre-built instance (e.g. in-memory, testing) |
/// | `set_kv_factory(type, fn)` | Factory that creates instances from config |
///
/// # Lookup
///
/// - `registry.kv(id)` — returns pre-built or cached instance
/// - `capability::kv(id)` — global convenience, does get-or-create via factory
pub struct CapabilityRegistry {
    // ---- Pre-built instances (set_xxx) ----
    kv_stores: HashMap<String, Arc<dyn CapabilityKeyValueStore>>,
    sql_engines: HashMap<String, Arc<dyn CapabilitySqlEngine>>,
    document_dbs: HashMap<String, Arc<dyn CapabilityDocumentDatabase>>,
    spreadsheets: HashMap<String, Arc<dyn CapabilitySpreadsheetEngine>>,

    // ---- Factories (set_xxx_factory) ----
    kv_factories: HashMap<String, KvFactory>,
    sql_factories: HashMap<String, SqlFactory>,
    doc_factories: HashMap<String, DocFactory>,
    spreadsheet_factories: HashMap<String, SpreadsheetFactory>,

    // ---- Lazy instance cache ----
    kv_instances: parking_lot::RwLock<HashMap<String, Arc<dyn CapabilityKeyValueStore>>>,
    sql_instances: parking_lot::RwLock<HashMap<String, Arc<dyn CapabilitySqlEngine>>>,
    doc_instances: parking_lot::RwLock<HashMap<String, Arc<dyn CapabilityDocumentDatabase>>>,
    spreadsheet_instances:
        parking_lot::RwLock<HashMap<String, Arc<dyn CapabilitySpreadsheetEngine>>>,

    /// Escape hatch for capabilities not yet defined as typed slots.
    pub extensions: HashMap<&'static str, Arc<dyn std::any::Any + Send + Sync>>,
}

impl CapabilityRegistry {
    pub fn new() -> Self {
        Self {
            kv_stores: HashMap::new(),
            sql_engines: HashMap::new(),
            document_dbs: HashMap::new(),
            spreadsheets: HashMap::new(),
            kv_factories: HashMap::new(),
            sql_factories: HashMap::new(),
            doc_factories: HashMap::new(),
            spreadsheet_factories: HashMap::new(),
            kv_instances: parking_lot::RwLock::new(HashMap::new()),
            sql_instances: parking_lot::RwLock::new(HashMap::new()),
            doc_instances: parking_lot::RwLock::new(HashMap::new()),
            spreadsheet_instances: parking_lot::RwLock::new(HashMap::new()),
            extensions: HashMap::new(),
        }
    }

    // ================================================================
    // CapabilityKeyValueStore
    // ================================================================

    /// Register a pre-built [`CapabilityKeyValueStore`] instance.
    ///
    /// For config-driven instantiation, use [`set_kv_factory`](Self::set_kv_factory)
    /// and look up by config instance ID via [`kv`](Self::kv).
    pub fn set_kv(&mut self, kv: Arc<dyn CapabilityKeyValueStore>) {
        let name = kv.name().to_string();
        self.kv_stores.insert(name, kv);
    }

    /// Register a factory that creates [`CapabilityKeyValueStore`] instances
    /// from config entries whose `config_type` matches this factory's key.
    ///
    /// # Example
    ///
    /// ```ignore
    /// reg.set_kv_factory("redis", |entry| {
    ///     let cfg: RedisConfig = serde_json::from_value(entry.data.clone())?;
    ///     Ok(Arc::new(RedisCapability::new(&cfg.connection_url())?))
    /// });
    /// ```
    pub fn set_kv_factory(
        &mut self,
        config_type: impl Into<String>,
        factory: impl Fn(&crate::config::ConfigEntry) -> crate::runtime::UnitResult<Arc<dyn CapabilityKeyValueStore>>
            + Send
            + Sync
            + 'static,
    ) {
        self.kv_factories
            .insert(config_type.into(), Box::new(factory));
    }

    /// Get-or-create a [`CapabilityKeyValueStore`] by instance ID.
    ///
    /// 1. Checks pre-built stores (fast path).
    /// 2. Checks the lazy instance cache.
    /// 3. Reads the config entry for this ID, finds the matching factory
    ///    by `config_type`, creates the instance, and caches it.
    pub fn kv(&self, id: &str) -> Option<Arc<dyn CapabilityKeyValueStore>> {
        // Fast path: pre-built stores
        if let Some(inst) = self.kv_stores.get(id) {
            return Some(inst.clone());
        }
        // Lazy cache hit
        if let Some(inst) = self.kv_instances.read().get(id) {
            return Some(inst.clone());
        }
        // Lazy create via factory
        let config_guard = crate::config::read();
        let entry = config_guard.entry(id)?;
        let factory = self.kv_factories.get(&entry.config_type)?;
        let instance = factory(entry).ok()?;
        drop(config_guard);
        self.kv_instances
            .write()
            .insert(id.to_string(), instance.clone());
        Some(instance)
    }

    /// Look up the KV store registered as `"default"`.
    pub fn default_kv(&self) -> Option<Arc<dyn CapabilityKeyValueStore>> {
        self.kv("default")
    }

    /// Iterate all pre-built KV store names.
    pub fn kv_names(&self) -> impl Iterator<Item = &String> {
        self.kv_stores.keys()
    }

    /// Iterate all KV factory names (config types).
    pub fn kv_factory_names(&self) -> impl Iterator<Item = &String> {
        self.kv_factories.keys()
    }

    /// Call [`Capability::init()`] on a pre-built or cached KV instance.
    pub async fn init_kv(&self, name: &str) -> UnitResult<()> {
        match self.kv(name) {
            Some(kv) => kv.init().await,
            None => Err(crate::runtime::UnitError::unknown(format!(
                "kv store `{name}` not registered"
            ))),
        }
    }

    /// Call [`Capability::init()`] on all pre-built + cached KV instances.
    pub async fn init_all_kv(&self) -> UnitResult<()> {
        let names: Vec<String> = self
            .kv_stores
            .keys()
            .chain(self.kv_instances.read().keys())
            .cloned()
            .collect();
        for name in names {
            self.init_kv(&name).await?;
        }
        Ok(())
    }

    /// Call [`Capability::shutdown()`] on the named KV store and remove it.
    pub async fn shutdown_kv(&mut self, name: &str) -> UnitResult<()> {
        if let Some(kv) = self.kv_stores.remove(name) {
            return kv.shutdown().await;
        }
        if let Some(kv) = self.kv_instances.write().remove(name) {
            return kv.shutdown().await;
        }
        Err(crate::runtime::UnitError::unknown(format!(
            "kv store `{name}` not registered"
        )))
    }

    /// Call [`Capability::shutdown()`] on all KV stores and remove them.
    pub async fn shutdown_all_kv(&mut self) -> UnitResult<()> {
        let names: Vec<String> = self
            .kv_stores
            .keys()
            .chain(self.kv_instances.read().keys())
            .cloned()
            .collect();
        for name in names {
            self.shutdown_kv(&name).await?;
        }
        Ok(())
    }

    // ================================================================
    // CapabilitySqlEngine
    // ================================================================

    pub fn set_sql(&mut self, engine: Arc<dyn CapabilitySqlEngine>) {
        let name = engine.name().to_string();
        self.sql_engines.insert(name, engine);
    }

    pub fn set_sql_factory(
        &mut self,
        config_type: impl Into<String>,
        factory: impl Fn(&crate::config::ConfigEntry) -> crate::runtime::UnitResult<Arc<dyn CapabilitySqlEngine>>
            + Send
            + Sync
            + 'static,
    ) {
        self.sql_factories.insert(config_type.into(), Box::new(factory));
    }

    pub fn sql(&self, id: &str) -> Option<Arc<dyn CapabilitySqlEngine>> {
        if let Some(inst) = self.sql_engines.get(id) {
            return Some(inst.clone());
        }
        if let Some(inst) = self.sql_instances.read().get(id) {
            return Some(inst.clone());
        }
        let config_guard = crate::config::read();
        let entry = config_guard.entry(id)?;
        let factory = self.sql_factories.get(&entry.config_type)?;
        let instance = factory(entry).ok()?;
        drop(config_guard);
        self.sql_instances
            .write()
            .insert(id.to_string(), instance.clone());
        Some(instance)
    }

    pub fn default_sql(&self) -> Option<Arc<dyn CapabilitySqlEngine>> {
        self.sql("default")
    }

    pub fn sql_names(&self) -> impl Iterator<Item = &String> {
        self.sql_engines.keys()
    }

    pub fn sql_factory_names(&self) -> impl Iterator<Item = &String> {
        self.sql_factories.keys()
    }

    pub async fn init_sql(&self, name: &str) -> UnitResult<()> {
        match self.sql_engines.get(name) {
            Some(e) => e.init().await,
            None => Err(crate::runtime::UnitError::unknown(format!(
                "sql engine `{name}` not registered"
            ))),
        }
    }

    pub async fn init_all_sql(&self) -> UnitResult<()> {
        for name in self.sql_engines.keys().cloned().collect::<Vec<_>>() {
            self.init_sql(&name).await?;
        }
        Ok(())
    }

    pub async fn shutdown_sql(&mut self, name: &str) -> UnitResult<()> {
        match self.sql_engines.remove(name) {
            Some(e) => e.shutdown().await,
            None => Err(crate::runtime::UnitError::unknown(format!(
                "sql engine `{name}` not registered"
            ))),
        }
    }

    pub async fn shutdown_all_sql(&mut self) -> UnitResult<()> {
        let names: Vec<String> = self.sql_engines.keys().cloned().collect();
        for name in names {
            self.shutdown_sql(&name).await?;
        }
        Ok(())
    }

    // ================================================================
    // CapabilityDocumentDatabase
    // ================================================================

    pub fn set_doc(&mut self, db: Arc<dyn CapabilityDocumentDatabase>) {
        let name = db.name().to_string();
        self.document_dbs.insert(name, db);
    }

    pub fn set_doc_factory(
        &mut self,
        config_type: impl Into<String>,
        factory: impl Fn(&crate::config::ConfigEntry) -> crate::runtime::UnitResult<Arc<dyn CapabilityDocumentDatabase>>
            + Send
            + Sync
            + 'static,
    ) {
        self.doc_factories.insert(config_type.into(), Box::new(factory));
    }

    pub fn doc(&self, id: &str) -> Option<Arc<dyn CapabilityDocumentDatabase>> {
        if let Some(inst) = self.document_dbs.get(id) {
            return Some(inst.clone());
        }
        if let Some(inst) = self.doc_instances.read().get(id) {
            return Some(inst.clone());
        }
        let config_guard = crate::config::read();
        let entry = config_guard.entry(id)?;
        let factory = self.doc_factories.get(&entry.config_type)?;
        let instance = factory(&entry).ok()?;
        self.doc_instances
            .write()
            .insert(id.to_string(), instance.clone());
        Some(instance)
    }

    pub fn default_doc(&self) -> Option<Arc<dyn CapabilityDocumentDatabase>> {
        self.doc("default")
    }

    pub fn doc_names(&self) -> impl Iterator<Item = &String> {
        self.document_dbs.keys()
    }

    pub fn doc_factory_names(&self) -> impl Iterator<Item = &String> {
        self.doc_factories.keys()
    }

    pub async fn init_doc(&self, name: &str) -> UnitResult<()> {
        match self.document_dbs.get(name) {
            Some(db) => db.init().await,
            None => Err(crate::runtime::UnitError::unknown(format!(
                "document db `{name}` not registered"
            ))),
        }
    }

    pub async fn init_all_doc(&self) -> UnitResult<()> {
        for name in self.document_dbs.keys().cloned().collect::<Vec<_>>() {
            self.init_doc(&name).await?;
        }
        Ok(())
    }

    pub async fn shutdown_doc(&mut self, name: &str) -> UnitResult<()> {
        match self.document_dbs.remove(name) {
            Some(db) => db.shutdown().await,
            None => Err(crate::runtime::UnitError::unknown(format!(
                "document db `{name}` not registered"
            ))),
        }
    }

    pub async fn shutdown_all_doc(&mut self) -> UnitResult<()> {
        let names: Vec<String> = self.document_dbs.keys().cloned().collect();
        for name in names {
            self.shutdown_doc(&name).await?;
        }
        Ok(())
    }

    // ================================================================
    // CapabilitySpreadsheetEngine
    // ================================================================

    pub fn set_spreadsheet(&mut self, engine: Arc<dyn CapabilitySpreadsheetEngine>) {
        let name = engine.name().to_string();
        self.spreadsheets.insert(name, engine);
    }

    pub fn set_spreadsheet_factory(
        &mut self,
        config_type: impl Into<String>,
        factory: impl Fn(&crate::config::ConfigEntry) -> crate::runtime::UnitResult<Arc<dyn CapabilitySpreadsheetEngine>>
            + Send
            + Sync
            + 'static,
    ) {
        self.spreadsheet_factories
            .insert(config_type.into(), Box::new(factory));
    }

    pub fn spreadsheet(&self, id: &str) -> Option<Arc<dyn CapabilitySpreadsheetEngine>> {
        if let Some(inst) = self.spreadsheets.get(id) {
            return Some(inst.clone());
        }
        if let Some(inst) = self.spreadsheet_instances.read().get(id) {
            return Some(inst.clone());
        }
        let config_guard = crate::config::read();
        let entry = config_guard.entry(id)?;
        let factory = self.spreadsheet_factories.get(&entry.config_type)?;
        let instance = factory(&entry).ok()?;
        self.spreadsheet_instances
            .write()
            .insert(id.to_string(), instance.clone());
        Some(instance)
    }

    pub fn default_spreadsheet(&self) -> Option<Arc<dyn CapabilitySpreadsheetEngine>> {
        self.spreadsheet("default")
    }

    pub fn spreadsheet_names(&self) -> impl Iterator<Item = &String> {
        self.spreadsheets.keys()
    }

    pub fn spreadsheet_factory_names(&self) -> impl Iterator<Item = &String> {
        self.spreadsheet_factories.keys()
    }

    pub async fn init_spreadsheet(&self, name: &str) -> UnitResult<()> {
        match self.spreadsheets.get(name) {
            Some(e) => e.init().await,
            None => Err(crate::runtime::UnitError::unknown(format!(
                "spreadsheet engine `{name}` not registered"
            ))),
        }
    }

    pub async fn init_all_spreadsheets(&self) -> UnitResult<()> {
        for name in self.spreadsheets.keys().cloned().collect::<Vec<_>>() {
            self.init_spreadsheet(&name).await?;
        }
        Ok(())
    }

    pub async fn shutdown_spreadsheet(&mut self, name: &str) -> UnitResult<()> {
        match self.spreadsheets.remove(name) {
            Some(e) => e.shutdown().await,
            None => Err(crate::runtime::UnitError::unknown(format!(
                "spreadsheet engine `{name}` not registered"
            ))),
        }
    }

    pub async fn shutdown_all_spreadsheets(&mut self) -> UnitResult<()> {
        let names: Vec<String> = self.spreadsheets.keys().cloned().collect();
        for name in names {
            self.shutdown_spreadsheet(&name).await?;
        }
        Ok(())
    }
}

impl Default for CapabilityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---- Global singleton ----------------------------------------

/// Process-global capability registry singleton.
static CAPABILITIES: OnceLock<RwLock<CapabilityRegistry>> = OnceLock::new();

fn registry() -> &'static RwLock<CapabilityRegistry> {
    CAPABILITIES.get_or_init(|| RwLock::new(CapabilityRegistry::new()))
}

/// Register capabilities or factories into the global registry.
///
/// Called by capability plugins at load time.
///
/// # Example (pre-built instance)
///
/// ```ignore
/// capability::register(|reg| {
///     reg.set_kv(Arc::new(InMemoryKV::new()));
/// });
/// ```
///
/// # Example (factory for config-driven creation)
///
/// ```ignore
/// capability::register(|reg| {
///     reg.set_kv_factory("redis", |entry| {
///         let cfg: RedisConfig = serde_json::from_value(entry.data.clone())?;
///         Ok(Arc::new(RedisCapability::new(&cfg.connection_url())?))
///     });
/// });
/// ```
pub fn register(f: impl FnOnce(&mut CapabilityRegistry)) {
    f(&mut registry().write());
}

/// Acquire a read lock on the global capability registry.
pub fn read() -> parking_lot::RwLockReadGuard<'static, CapabilityRegistry> {
    registry().read()
}

/// Acquire a write lock on the global capability registry.
pub fn write() -> parking_lot::RwLockWriteGuard<'static, CapabilityRegistry> {
    registry().write()
}

/// Get-or-create a [`CapabilityKeyValueStore`] by config instance ID.
///
/// This is the primary access point for unit plugins. It handles
/// pre-built instances, cached lazy instances, and factory-based
/// creation transparently.
///
/// # Example
///
/// ```ignore
/// let redis = capability::kv("redis-cache")
///     .ok_or_else(|| UnitError::unknown("redis-cache not available"))?;
/// redis.set("key", b"value").await?;
/// ```
pub fn kv(id: &str) -> Option<Arc<dyn CapabilityKeyValueStore>> {
    registry().read().kv(id)
}

/// Get-or-create a [`CapabilitySqlEngine`] by config instance ID.
pub fn sql(id: &str) -> Option<Arc<dyn CapabilitySqlEngine>> {
    registry().read().sql(id)
}

/// Get-or-create a [`CapabilityDocumentDatabase`] by config instance ID.
pub fn doc(id: &str) -> Option<Arc<dyn CapabilityDocumentDatabase>> {
    registry().read().doc(id)
}

/// Get-or-create a [`CapabilitySpreadsheetEngine`] by config instance ID.
pub fn spreadsheet(id: &str) -> Option<Arc<dyn CapabilitySpreadsheetEngine>> {
    registry().read().spreadsheet(id)
}

// ============================================================
// CapabilityPlugin trait — the dylib-facing interface
// ============================================================

/// Trait implemented by capability plugin crates.
///
/// Each capability plugin is a `cdylib` crate that exports
/// an `init_capability_plugin` symbol returning a `Box<dyn CapabilityPlugin>`.
///
/// This is the capability analogue of [`crate::GraphUnitPlugin`].
pub trait CapabilityPlugin: Send + Sync {
    /// Register all capabilities provided by this plugin
    /// into the global [`CapabilityRegistry`].
    ///
    /// This only registers names — it does not call [`Capability::init()`].
    /// Initialization happens separately via the registry's `init_xxx` methods.
    fn register(&self);

    /// Plugin version string.
    fn version(&self) -> &str {
        "1.0.0"
    }
}
