use super::Capability;
use crate::runtime::UnitResult;

/// Key-value store capability.
///
/// Implementations provide simple get/set/delete/scan semantics.
///
/// # Well-known names
///
/// Use the constants in [`well_known`] to refer to implementations:
///
/// ```ignore
/// use fusion_unit_sdk::capability::capability_key_value_store::well_known;
/// let redis = capability::read().kv(well_known::REDIS);
/// ```
///
/// See [`well_known`] for the complete catalog.
#[async_trait::async_trait]
pub trait CapabilityKeyValueStore: Capability {
    /// Get a value by key. Returns `None` if the key does not exist.
    async fn get(&self, key: &str) -> UnitResult<Option<Vec<u8>>>;

    /// Set a value for a key.
    async fn set(&self, key: &str, value: &[u8]) -> UnitResult<()>;

    /// Delete a key.
    async fn delete(&self, key: &str) -> UnitResult<()>;

    /// Scan keys by prefix. Returns `(key, value)` pairs.
    async fn scan(&self, prefix: &str) -> UnitResult<Vec<(String, Vec<u8>)>>;
}

/// Well-known `CapabilityKeyValueStore` capability names.
///
/// This module serves as the **central catalog** of recognized KV store
/// implementations. Third-party capability plugins SHOULD use these
/// constants to name their implementations so that unit plugins can
/// discover them consistently.
///
/// # Usage (unit plugin)
///
/// ```ignore
/// use fusion_unit_sdk::capability::capability_key_value_store::well_known;
///
/// let redis = capability::read().kv(well_known::REDIS);
/// let hbase = capability::read().kv(well_known::HBASE);
/// ```
///
/// # Usage (capability plugin)
///
/// ```ignore
/// impl Capability for RedisKV {
///     fn name(&self) -> &str { well_known::REDIS }
/// }
/// ```
pub mod well_known {
    /// Redis — `"redis"`
    pub const REDIS: &str = "redis";
    /// Apache HBase — `"hbase"`
    pub const HBASE: &str = "hbase";
    /// In-memory store (development / testing) — `"inmemory"`
    pub const IN_MEMORY: &str = "inmemory";
    /// RocksDB embedded key-value store — `"rocksdb"`
    pub const ROCKSDB: &str = "rocksdb";
    /// Default / unspecified implementation — `"default"`
    pub const DEFAULT: &str = "default";
}
