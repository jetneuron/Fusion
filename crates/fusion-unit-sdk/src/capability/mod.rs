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
//! ## Lifecycle
//!
//! Every capability goes through a defined lifecycle:
//!
//! ```text
//! register → init → (use) → shutdown
//! ```
//!
//! - **register** — stored in [`CapabilityRegistry`] via `set_xxx()`.
//! - **init** — [`Capability::init()`] is called to establish connections,
//!   allocate resources, etc.
//! - **shutdown** — [`Capability::shutdown()`] releases resources before removal.
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

/// Holds all registered capability implementations.
///
/// Each slot is a name → implementation map. Unit plugins look up
/// capabilities by name via the per-trait getter methods.
///
/// # Lifecycle methods
///
/// Beyond `set_xxx()` (register) and `xxx()` (lookup), each slot provides:
///
/// | Method | Purpose |
/// |--------|---------|
/// | `init_xxx(name)` | Call [`Capability::init()`] on one implementation |
/// | `init_all_xxx()` | Call [`Capability::init()`] on all registered |
/// | `shutdown_xxx(name)` | Call [`Capability::shutdown()`] and remove |
/// | `shutdown_all_xxx()` | Shutdown and remove all |
///
/// # Example
///
/// ```ignore
/// // Registration
/// capability::register(|reg| {
///     reg.set_kv(Arc::new(RedisKV::new(...)));
///     reg.set_kv(Arc::new(HBaseKV::new(...)));
/// });
///
/// // Initialize all KV stores (connect to backends)
/// capability::write().init_all_kv().await?;
///
/// // Use
/// let redis = capability::read().kv("redis").unwrap();
/// redis.set("key", b"value").await?;
///
/// // Shutdown one
/// capability::write().shutdown_kv("hbase").await?;
/// ```
pub struct CapabilityRegistry {
    /// SQL query engines by name (e.g., `"datafusion"`, `"duckdb"`).
    sql_engines: HashMap<String, Arc<dyn CapabilitySqlEngine>>,
    /// Document databases by name (e.g., `"mongodb"`, `"couchdb"`).
    document_dbs: HashMap<String, Arc<dyn CapabilityDocumentDatabase>>,
    /// Key-value stores by name (e.g., `"redis"`, `"hbase"`, `"inmemory"`).
    kv_stores: HashMap<String, Arc<dyn CapabilityKeyValueStore>>,
    /// Spreadsheet engines by name (e.g., `"excel"`).
    spreadsheets: HashMap<String, Arc<dyn CapabilitySpreadsheetEngine>>,
    /// Escape hatch for capabilities not yet defined as typed slots.
    pub extensions: HashMap<&'static str, Arc<dyn std::any::Any + Send + Sync>>,
}

impl CapabilityRegistry {
    pub fn new() -> Self {
        Self {
            sql_engines: HashMap::new(),
            document_dbs: HashMap::new(),
            kv_stores: HashMap::new(),
            spreadsheets: HashMap::new(),
            extensions: HashMap::new(),
        }
    }

    // ================================================================
    // CapabilityKeyValueStore
    // ================================================================

    /// Register a [`CapabilityKeyValueStore`] under its [`Capability::name()`].
    ///
    /// Does **not** call [`Capability::init()`] — use [`init_kv()`](Self::init_kv)
    /// or [`init_all_kv()`](Self::init_all_kv) after registration.
    pub fn set_kv(&mut self, kv: Arc<dyn CapabilityKeyValueStore>) {
        let name = kv.name().to_string();
        self.kv_stores.insert(name, kv);
    }

    /// Look up a [`CapabilityKeyValueStore`] by name.
    pub fn kv(&self, name: &str) -> Option<&Arc<dyn CapabilityKeyValueStore>> {
        self.kv_stores.get(name)
    }

    /// Look up the KV store registered as `"default"`.
    pub fn default_kv(&self) -> Option<&Arc<dyn CapabilityKeyValueStore>> {
        self.kv("default")
    }

    /// Iterate all registered KV store names.
    pub fn kv_names(&self) -> impl Iterator<Item = &String> {
        self.kv_stores.keys()
    }

    /// Call [`Capability::init()`] on the named KV store.
    pub async fn init_kv(&self, name: &str) -> UnitResult<()> {
        match self.kv_stores.get(name) {
            Some(kv) => kv.init().await,
            None => Err(crate::runtime::UnitError::unknown(format!(
                "kv store `{name}` not registered"
            ))),
        }
    }

    /// Call [`Capability::init()`] on all registered KV stores.
    pub async fn init_all_kv(&self) -> UnitResult<()> {
        for name in self.kv_stores.keys().cloned().collect::<Vec<_>>() {
            self.init_kv(&name).await?;
        }
        Ok(())
    }

    /// Call [`Capability::shutdown()`] on the named KV store and remove it.
    pub async fn shutdown_kv(&mut self, name: &str) -> UnitResult<()> {
        match self.kv_stores.remove(name) {
            Some(kv) => kv.shutdown().await,
            None => Err(crate::runtime::UnitError::unknown(format!(
                "kv store `{name}` not registered"
            ))),
        }
    }

    /// Call [`Capability::shutdown()`] on all KV stores and remove them.
    pub async fn shutdown_all_kv(&mut self) -> UnitResult<()> {
        let names: Vec<String> = self.kv_stores.keys().cloned().collect();
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

    pub fn sql(&self, name: &str) -> Option<&Arc<dyn CapabilitySqlEngine>> {
        self.sql_engines.get(name)
    }

    pub fn default_sql(&self) -> Option<&Arc<dyn CapabilitySqlEngine>> {
        self.sql("default")
    }

    pub fn sql_names(&self) -> impl Iterator<Item = &String> {
        self.sql_engines.keys()
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

    pub fn doc(&self, name: &str) -> Option<&Arc<dyn CapabilityDocumentDatabase>> {
        self.document_dbs.get(name)
    }

    pub fn default_doc(&self) -> Option<&Arc<dyn CapabilityDocumentDatabase>> {
        self.doc("default")
    }

    pub fn doc_names(&self) -> impl Iterator<Item = &String> {
        self.document_dbs.keys()
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

    pub fn spreadsheet(&self, name: &str) -> Option<&Arc<dyn CapabilitySpreadsheetEngine>> {
        self.spreadsheets.get(name)
    }

    pub fn default_spreadsheet(&self) -> Option<&Arc<dyn CapabilitySpreadsheetEngine>> {
        self.spreadsheet("default")
    }

    pub fn spreadsheet_names(&self) -> impl Iterator<Item = &String> {
        self.spreadsheets.keys()
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

/// Register capabilities into the global registry.
///
/// Called by capability plugins at load time. Does **not** call
/// [`Capability::init()`] — use the global `init_*` functions or
/// [`write()`] → `init_all_xxx()` after registration.
///
/// # Example
///
/// ```ignore
/// capability::register(|reg| {
///     reg.set_kv(Arc::new(RedisKV::new(...)));
///     reg.set_kv(Arc::new(HBaseKV::new(...)));
/// });
/// ```
pub fn register(f: impl FnOnce(&mut CapabilityRegistry)) {
    f(&mut registry().write());
}

/// Acquire a read lock on the global capability registry.
///
/// Use this for capability lookups. For lifecycle operations
/// (init, shutdown), use [`write()`] instead.
pub fn read() -> parking_lot::RwLockReadGuard<'static, CapabilityRegistry> {
    registry().read()
}

/// Acquire a write lock on the global capability registry.
///
/// Use this for lifecycle operations ([`CapabilityRegistry::init_kv`],
/// [`CapabilityRegistry::shutdown_kv`], etc.) that mutate the registry
/// or require exclusive access.
pub fn write() -> parking_lot::RwLockWriteGuard<'static, CapabilityRegistry> {
    registry().write()
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
