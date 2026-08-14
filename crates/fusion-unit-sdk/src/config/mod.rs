//! Unified configuration system.
//!
//! All configuration is stored in a flat, globally-unique instance-ID-keyed
//! registry. The three-level hierarchy (category → type → instance) is for
//! organization in YAML files and discovery — not for lookup.
//!
//! ## Architecture
//!
//! ```text
//! fusion-conf.yaml
//!   config:
//!     datasource:           ← category
//!       redis:              ← config_type
//!         redis-cache:      ← instance id  }
//!           host: ...       ← data         }  ConfigRegistry entry
//!         redis-session:                   }
//!           host: ...
//!     setting:
//!       pool:
//!         default:
//!           max_size: 16
//! ```
//!
//! Capabilities look up config by instance ID alone:
//!
//! ```ignore
//! let redis: RedisDataSourceConfig = config::get("redis-cache")?;
//! ```
//!
//! ## Adding configuration
//!
//! 1. Define a `Deserialize` struct for your config shape.
//! 2. Add it to a YAML file under the three-level hierarchy.
//! 3. Call `config::get::<YourType>("your-id")` to retrieve it.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

pub mod providers;

// ============================================================
// ConfigEntry
// ============================================================

/// A single configuration entry.
///
/// Stored in [`ConfigRegistry`] keyed by a globally-unique instance ID.
/// The `category` and `config_type` fields are metadata for discovery
/// and YAML organization — lookups use only the instance ID.
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    /// Top-level category: `"datasource"`, `"setting"`, `"metadata"`.
    pub category: String,
    /// Type within the category: `"redis"`, `"postgres"`, `"pool"`.
    pub config_type: String,
    /// The configuration payload as a JSON value.
    pub data: serde_json::Value,
}

// ============================================================
// ConfigRegistry
// ============================================================

/// Holds all configuration entries keyed by globally-unique instance ID.
///
/// # Lookup
///
/// ```ignore
/// let redis: RedisDataSourceConfig = config::get("redis-cache")?;
/// let pool: u32 = config::get::<u32>("redis.pool.size").unwrap_or(16);
/// ```
///
/// # Registration
///
/// ```ignore
/// config::register(|reg| {
///     reg.insert("datasource", "redis", "redis-cache",
///         json!({"host": "localhost", "port": 6379}));
/// });
/// ```
pub struct ConfigRegistry {
    entries: HashMap<String, ConfigEntry>,
}

impl ConfigRegistry {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Insert a configuration entry.
    ///
    /// # Parameters
    /// - `category`: top-level group, e.g. `"datasource"`, `"setting"`, `"metadata"`.
    /// - `config_type`: type within the category, e.g. `"redis"`, `"postgres"`, `"pool"`.
    /// - `id`: globally-unique instance identifier, e.g. `"redis-cache"`.
    /// - `data`: the configuration payload as JSON.
    pub fn insert(
        &mut self,
        category: impl Into<String>,
        config_type: impl Into<String>,
        id: impl Into<String>,
        data: serde_json::Value,
    ) {
        self.entries.insert(
            id.into(),
            ConfigEntry {
                category: category.into(),
                config_type: config_type.into(),
                data,
            },
        );
    }

    /// Look up an entry by instance ID and deserialize it to `T`.
    ///
    /// Returns `None` if the ID is not found or deserialization fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// #[derive(Deserialize)]
    /// struct RedisConfig { host: String, port: u16 }
    ///
    /// let redis: RedisConfig = config::get("redis-cache").unwrap();
    /// ```
    pub fn get<T: serde::de::DeserializeOwned>(&self, id: &str) -> Option<T> {
        let entry = self.entries.get(id)?;
        serde_json::from_value(entry.data.clone()).ok()
    }

    /// Look up the raw JSON value for an entry.
    pub fn get_raw(&self, id: &str) -> Option<&serde_json::Value> {
        self.entries.get(id).map(|e| &e.data)
    }

    /// Look up the full [`ConfigEntry`] metadata.
    pub fn entry(&self, id: &str) -> Option<&ConfigEntry> {
        self.entries.get(id)
    }

    /// Remove an entry by instance ID.
    pub fn remove(&mut self, id: &str) -> Option<ConfigEntry> {
        self.entries.remove(id)
    }

    /// Iterate all instance IDs.
    pub fn ids(&self) -> impl Iterator<Item = &String> {
        self.entries.keys()
    }

    /// Count of registered entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// List instance IDs filtered by category and config type.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let redis_ids = config::read().ids_by("datasource", "redis");
    /// // → ["redis-cache", "redis-session"]
    /// ```
    pub fn ids_by(&self, category: &str, config_type: &str) -> Vec<&String> {
        self.entries
            .iter()
            .filter(|(_, e)| e.category == category && e.config_type == config_type)
            .map(|(id, _)| id)
            .collect()
    }

    /// List instance IDs within a category (any type).
    pub fn ids_in_category(&self, category: &str) -> Vec<&String> {
        self.entries
            .iter()
            .filter(|(_, e)| e.category == category)
            .map(|(id, _)| id)
            .collect()
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

/// Register configuration entries into the global registry.
///
/// Called by [`ConfigProvider`](providers::ConfigProvider) implementations
/// and for programmatic registration.
///
/// # Example
///
/// ```ignore
/// config::register(|reg| {
///     reg.insert("datasource", "redis", "redis-cache",
///         serde_json::json!({"host": "localhost", "port": 6379}));
/// });
/// ```
pub fn register(f: impl FnOnce(&mut ConfigRegistry)) {
    f(&mut registry().write());
}

/// Typed lookup: retrieve a config entry by instance ID and deserialize to `T`.
///
/// # Example
///
/// ```ignore
/// #[derive(Deserialize)]
/// struct RedisConfig { host: String, port: u16 }
///
/// let redis: RedisConfig = config::get("redis-cache").unwrap();
/// ```
pub fn get<T: serde::de::DeserializeOwned>(id: &str) -> Option<T> {
    registry().read().get(id)
}

/// Acquire a read lock on the global config registry.
pub fn read() -> parking_lot::RwLockReadGuard<'static, ConfigRegistry> {
    registry().read()
}

/// Acquire a write lock on the global config registry.
pub fn write() -> parking_lot::RwLockWriteGuard<'static, ConfigRegistry> {
    registry().write()
}

// ============================================================
// FFI injection (host → dylib)
// ============================================================

/// One config registry entry serialized across a binary-image boundary.
///
/// Rust statics are per-binary-image: entries registered in the host
/// process are invisible to unit/provider dylibs. The host serializes
/// its registry as `Vec<InjectedConfig>` JSON and hands it to dylibs
/// through a `set_config` C symbol, which calls [`inject_entries`].
#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub struct InjectedConfig {
    pub category: String,
    pub config_type: String,
    pub id: String,
    pub data: serde_json::Value,
}

/// Install config entries injected from the host (another binary image).
pub fn inject_entries(entries: Vec<InjectedConfig>) {
    let mut reg = registry().write();
    for e in entries {
        reg.insert(e.category, e.config_type, e.id, e.data);
    }
}
