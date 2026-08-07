//! DataSource configuration system.
//!
//! ## Architecture
//!
//! ```text
//! fusion-config.yaml ──→ FileConfigProvider ──→ ConfigRegistry ──→ CapabilityManager
//! (or programmatic)        (parses YAML)          (global store)     (creates instances)
//! ```
//!
//! ## Adding a new datasource config type
//!
//! 1. Create a struct implementing [`DataSourceConfig`].
//! 2. Register it with `config_type_registry` so [`FileConfigProvider`] can
//!    deserialize it by `type` field.
//!
//! ## Example (Redis)
//!
//! See [`crate::capability::capability_key_value_store_config`].

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

pub mod providers;

// ============================================================
// DataSourceConfig trait
// ============================================================

/// Common interface for all datasource configurations.
///
/// Each datasource type (redis, postgres, mongodb, …) has a concrete
/// struct implementing this trait. The trait itself is object-safe so
/// configurations can be stored as `Arc<dyn DataSourceConfig>` in the
/// [`ConfigRegistry`].
pub trait DataSourceConfig: Send + Sync + 'static {
    /// Unique identifier for this datasource, e.g. `"redis-cache"`.
    fn id(&self) -> &str;

    /// Datasource type, e.g. `"redis"`, `"postgres"`, `"mongodb"`.
    /// Used by [`FileConfigProvider`] to determine the concrete struct
    /// during deserialization.
    fn source_type(&self) -> &str;

    /// The full configuration as a JSON value.
    ///
    /// Used by [`ConfigRegistry::get_typed`] to deserialize into a
    /// typed config struct on demand.
    fn raw_config(&self) -> &serde_json::Value;

    /// Validate required fields and value ranges.
    ///
    /// Returns a list of validation errors, or `Ok(())` if valid.
    fn validate(&self) -> Result<(), Vec<String>>;

    /// Enable downcasting to concrete types via [`std::any::Any`].
    fn as_any(&self) -> &dyn std::any::Any;
}

// ============================================================
// GenericDataSourceConfig — catch-all for unknown types
// ============================================================

/// A generic datasource config that stores the raw JSON payload.
///
/// This is the default storage format in [`ConfigRegistry`].
/// Typed access is provided by [`ConfigRegistry::get_typed`],
/// which deserializes the raw JSON into a concrete struct on demand.
pub struct GenericDataSourceConfig {
    id: String,
    source_type: String,
    raw: serde_json::Value,
}

impl GenericDataSourceConfig {
    pub fn new(id: impl Into<String>, source_type: impl Into<String>, raw: serde_json::Value) -> Self {
        Self {
            id: id.into(),
            source_type: source_type.into(),
            raw,
        }
    }
}

impl DataSourceConfig for GenericDataSourceConfig {
    fn id(&self) -> &str {
        &self.id
    }

    fn source_type(&self) -> &str {
        &self.source_type
    }

    fn raw_config(&self) -> &serde_json::Value {
        &self.raw
    }

    fn validate(&self) -> Result<(), Vec<String>> {
        Ok(()) // Generic: no schema to validate against.
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ============================================================
// ConfigRegistry — global singleton
// ============================================================

/// Holds all registered datasource configurations.
pub struct ConfigRegistry {
    /// Keyed by datasource id.
    datasources: HashMap<String, Arc<dyn DataSourceConfig>>,
}

impl ConfigRegistry {
    pub fn new() -> Self {
        Self {
            datasources: HashMap::new(),
        }
    }

    /// Register a datasource configuration.
    ///
    /// Replaces any existing config with the same id.
    pub fn register(&mut self, config: Arc<dyn DataSourceConfig>) {
        let id = config.id().to_string();
        self.datasources.insert(id, config);
    }

    /// Look up a datasource by id.
    pub fn get(&self, id: &str) -> Option<&Arc<dyn DataSourceConfig>> {
        self.datasources.get(id)
    }

    /// Remove a datasource by id.
    pub fn remove(&mut self, id: &str) -> Option<Arc<dyn DataSourceConfig>> {
        self.datasources.remove(id)
    }

    /// Iterate all registered datasource ids.
    pub fn ids(&self) -> impl Iterator<Item = &String> {
        self.datasources.keys()
    }

    /// List datasource ids filtered by source type.
    pub fn ids_by_type(&self, source_type: &str) -> Vec<&String> {
        self.datasources
            .iter()
            .filter(|(_, ds)| ds.source_type() == source_type)
            .map(|(id, _)| id)
            .collect()
    }

    /// Deserialize a datasource into a typed config struct.
    ///
    /// Returns `None` if the datasource does not exist or deserialization
    /// fails. The target type must implement [`serde::de::DeserializeOwned`]
    /// and match the structure of the stored JSON.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let redis: RedisDataSourceConfig = registry.get_typed("redis-cache").unwrap();
    /// println!("host={}, port={}", redis.host, redis.port);
    /// ```
    pub fn get_typed<T>(&self, id: &str) -> Option<T>
    where
        T: DataSourceConfig + serde::de::DeserializeOwned,
    {
        let ds = self.datasources.get(id)?;
        serde_json::from_value(ds.raw_config().clone()).ok()
    }

    /// Number of registered datasources.
    pub fn len(&self) -> usize {
        self.datasources.len()
    }

    pub fn is_empty(&self) -> bool {
        self.datasources.is_empty()
    }
}

impl Default for ConfigRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---- Global singleton ----------------------------------------

static CONFIG_REGISTRY: OnceLock<RwLock<ConfigRegistry>> = OnceLock::new();

fn registry() -> &'static RwLock<ConfigRegistry> {
    CONFIG_REGISTRY.get_or_init(|| RwLock::new(ConfigRegistry::new()))
}

/// Register datasource configs into the global registry.
///
/// Used by [`ConfigProvider`](providers::ConfigProvider) implementations
/// and for programmatic registration.
///
/// # Example
///
/// ```ignore
/// config::register(|reg| {
///     reg.register(Arc::new(GenericDataSourceConfig::new(
///         "redis-cache", "redis",
///         serde_json::json!({"host": "localhost", "port": 6379}),
///     )));
/// });
/// ```
pub fn register(f: impl FnOnce(&mut ConfigRegistry)) {
    f(&mut registry().write());
}

/// Acquire a read lock on the global config registry.
pub fn read_config() -> parking_lot::RwLockReadGuard<'static, ConfigRegistry> {
    registry().read()
}

/// Acquire a write lock on the global config registry.
pub fn write_config() -> parking_lot::RwLockWriteGuard<'static, ConfigRegistry> {
    registry().write()
}
