//! # Fusion Capability Redis
//!
//! Redis-backed implementation of [`CapabilityKeyValueStore`].
//!
//! Registered as a **factory** for config type `"redis"`. Each config
//! entry with `config_type: redis` (e.g. `redis-cache`, `redis-session`)
//! creates its own connection pool.
//!
//! ## Usage (from a unit plugin)
//!
//! ```ignore
//! let redis = capability::kv("redis-cache")
//!     .ok_or_else(|| UnitError::unknown("redis kv not available"))?;
//! redis.set("mykey", b"myvalue").await?;
//! ```

use deadpool_redis::{Config, Pool, Runtime};
use fusion_unit_sdk::capability::capability_key_value_store::{ScanOptions, ScanResult, well_known};
use fusion_unit_sdk::capability::{self, Capability, CapabilityKeyValueStore, CapabilityPlugin};
use fusion_unit_sdk::ffi::config_ffi::HostConfigApi;
use fusion_unit_sdk::runtime::UnitResult;
use std::sync::Arc;

// ============================================================
// RedisCapabilityConfig
// ============================================================

/// Configuration deserialized from a config entry whose `config_type` is `"redis"`.
///
/// Fields match the YAML structure under the three-level config hierarchy
/// (category: `datasource`, type: `redis`, instance: e.g. `redis-cache`).
#[derive(serde::Deserialize)]
pub struct RedisCapabilityConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

fn default_host() -> String {
    "localhost".into()
}
fn default_port() -> u16 {
    6379
}
fn default_pool_size() -> u32 {
    8
}

impl RedisCapabilityConfig {
    /// Build a Redis connection URL from the config fields.
    pub fn connection_url(&self) -> String {
        match &self.password {
            Some(pwd) => format!("redis://:{}@{}:{}", pwd, self.host, self.port),
            None => format!("redis://{}:{}", self.host, self.port),
        }
    }
}

// ============================================================
// RedisCapability
// ============================================================

/// Redis-backed key-value store capability.
///
/// Created from a [`RedisCapabilityConfig`] by the factory registered
/// via [`RedisCapabilityPlugin`]. Not constructed directly — use
/// `capability::kv("redis-cache")` to obtain an instance.
pub struct RedisCapability {
    pool: Pool,
}

impl RedisCapability {
    /// Create from typed config (called by the factory, not directly).
    fn from_config(cfg: &RedisCapabilityConfig) -> anyhow::Result<Self> {
        let pool_cfg = Config::from_url(&cfg.connection_url());
        let pool = pool_cfg.create_pool(Some(Runtime::Tokio1))?;
        Ok(Self { pool })
    }

    fn err<E: std::fmt::Display>(e: E) -> fusion_unit_sdk::runtime::UnitError {
        fusion_unit_sdk::runtime::UnitError::unknown(e.to_string())
    }
}

// ============================================================
// Capability trait
// ============================================================

#[async_trait::async_trait]
impl Capability for RedisCapability {
    fn name(&self) -> &str {
        well_known::REDIS
    }

    /// No-op: the pool is already created. Connections are established
    /// lazily on first use.
    async fn init(&self) -> UnitResult<()> {
        Ok(())
    }

    /// Close the connection pool.
    async fn shutdown(&self) -> UnitResult<()> {
        self.pool.close();
        Ok(())
    }
}

// ============================================================
// CapabilityKeyValueStore implementation
// ============================================================

#[async_trait::async_trait]
impl CapabilityKeyValueStore for RedisCapability {
    // ---- Required methods ----

    async fn get(&self, key: &str) -> UnitResult<Option<Vec<u8>>> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        redis::cmd("GET")
            .arg(key)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)
    }

    async fn set(&self, key: &str, value: &[u8]) -> UnitResult<()> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)
    }

    async fn delete(&self, key: &str) -> UnitResult<()> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        redis::cmd("DEL")
            .arg(key)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)
    }

    async fn mget(&self, keys: &[&str]) -> UnitResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        // MGET returns values; missing keys return nil → None.
        let raw: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)?;
        Ok(raw)
    }

    async fn scan_page(
        &self,
        cursor: Option<&str>,
        opts: &ScanOptions,
    ) -> UnitResult<ScanResult> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;

        let redis_cursor = cursor.unwrap_or("0");
        let mut cmd = redis::cmd("SCAN");
        cmd.arg(redis_cursor);

        if let Some(pattern) = &opts.pattern {
            cmd.arg("MATCH").arg(pattern.clone());
        }
        if let Some(count) = opts.page_size {
            cmd.arg("COUNT").arg(count);
        }

        // SCAN returns (next_cursor, keys)
        let (next_cursor, keys): (String, Vec<String>) =
            cmd.query_async(&mut *conn).await.map_err(Self::err)?;

        // "0" cursor means iteration complete in Redis.
        let next = if next_cursor == "0" {
            None
        } else {
            Some(next_cursor)
        };

        // Fetch values for this page.
        let entries: Vec<(String, Vec<u8>)> = if keys.is_empty() {
            Vec::new()
        } else {
            let values: Vec<Vec<u8>> = redis::cmd("MGET")
                .arg(&keys)
                .query_async(&mut *conn)
                .await
                .map_err(Self::err)?;
            keys.into_iter().zip(values.into_iter()).collect()
        };

        Ok(ScanResult {
            entries,
            next_cursor: next,
        })
    }

    // ---- Overrides with native Redis commands ----

    async fn exists(&self, key: &str) -> UnitResult<bool> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        let n: u32 = redis::cmd("EXISTS")
            .arg(key)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)?;
        Ok(n > 0)
    }

    async fn mset(&self, pairs: &[(&str, &[u8])]) -> UnitResult<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        let mut cmd = redis::cmd("MSET");
        for (key, value) in pairs {
            cmd.arg(key).arg(*value);
        }
        cmd.query_async(&mut *conn).await.map_err(Self::err)
    }

    async fn mdelete(&self, keys: &[&str]) -> UnitResult<u64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        let count: u64 = redis::cmd("DEL")
            .arg(keys)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)?;
        Ok(count)
    }

    async fn set_nx(&self, key: &str, value: &[u8]) -> UnitResult<bool> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        // SET key value NX returns nil (None) if key exists, OK if set.
        let result: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg("NX")
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)?;
        Ok(result.is_some())
    }

    async fn get_set(&self, key: &str, value: &[u8]) -> UnitResult<Option<Vec<u8>>> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        let old: Option<Vec<u8>> = redis::cmd("GETSET")
            .arg(key)
            .arg(value)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)?;
        Ok(old)
    }

    async fn incr_by(&self, key: &str, delta: i64) -> UnitResult<i64> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        let new_val: i64 = redis::cmd("INCRBY")
            .arg(key)
            .arg(delta)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)?;
        Ok(new_val)
    }

    async fn set_ex(&self, key: &str, value: &[u8], ttl_ms: u64) -> UnitResult<()> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        redis::cmd("PSETEX")
            .arg(key)
            .arg(ttl_ms)
            .arg(value)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)
    }

    async fn expire(&self, key: &str, ttl_ms: u64) -> UnitResult<bool> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        // PEXPIRE returns 1 if the timeout was set, 0 if key doesn't exist.
        let n: u32 = redis::cmd("PEXPIRE")
            .arg(key)
            .arg(ttl_ms)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)?;
        Ok(n > 0)
    }

    async fn ttl(&self, key: &str) -> UnitResult<Option<u64>> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        // PTTL returns -2 if key doesn't exist, -1 if no expiry, positive ms.
        let ms: i64 = redis::cmd("PTTL")
            .arg(key)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)?;
        Ok(match ms {
            -2 => None,           // key does not exist
            -1 => Some(0),        // exists, no expiry
            n if n > 0 => Some(n as u64),
            _ => None,
        })
    }

    async fn persist(&self, key: &str) -> UnitResult<bool> {
        let mut conn = self.pool.get().await.map_err(Self::err)?;
        let n: u32 = redis::cmd("PERSIST")
            .arg(key)
            .query_async(&mut *conn)
            .await
            .map_err(Self::err)?;
        Ok(n > 0)
    }

}

// ============================================================
// CapabilityPlugin
// ============================================================

/// Plugin entry point that registers a factory for config type `"redis"`.
///
/// Each config entry with `config_type: redis` triggers this factory
/// when `capability::kv("<instance-id>")` is called.
pub struct RedisCapabilityPlugin;

impl CapabilityPlugin for RedisCapabilityPlugin {
    fn register(&self) {
        capability::register(|reg| {
            reg.set_kv_factory("redis", |entry| {
                let cfg: RedisCapabilityConfig =
                    serde_json::from_value(entry.data.clone()).map_err(|e| {
                        fusion_unit_sdk::runtime::UnitError::unknown(e.to_string())
                    })?;
                let cap = RedisCapability::from_config(&cfg)
                    .map_err(|e| fusion_unit_sdk::runtime::UnitError::unknown(e.to_string()))?;
                Ok(Arc::new(cap))
            });
        });
    }

    fn version(&self) -> &str {
        "0.1.0"
    }
}

// ============================================================
// FFI export
// ============================================================

/// Exported symbol for dynamic loading.
///
/// Registers a factory for config type `"redis"`. Actual connections
/// are created lazily when `capability::kv("<instance-id>")` is called.
#[unsafe(no_mangle)]
pub extern "C" fn init_capability_plugin() -> Box<dyn CapabilityPlugin + Send + Sync> {
    Box::new(RedisCapabilityPlugin)
}

/// Install the host's live config query API — config-driven capability
/// factories (e.g. Redis connection pools) read config through this
/// image's own registry.
#[unsafe(no_mangle)]
pub extern "C" fn set_host_config(api: HostConfigApi) {
    fusion_unit_sdk::ffi::config_ffi::set_host_api(api);
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fusion_unit_sdk::capability::capability_key_value_store::ScanOptions;

    /// Construction succeeds from typed config.
    #[test]
    fn test_pool_construction_from_config() {
        let cfg = RedisCapabilityConfig {
            host: "127.0.0.1".into(),
            port: 6379,
            password: None,
            pool_size: 8,
        };
        let cap = RedisCapability::from_config(&cfg);
        assert!(cap.is_ok());
    }

    /// Factory is discoverable after registration.
    #[tokio::test]
    async fn test_factory_discoverable() {
        capability::register(|reg| {
            reg.set_kv_factory("redis", |entry| {
                let cfg: RedisCapabilityConfig =
                    serde_json::from_value(entry.data.clone()).unwrap();
                Ok(Arc::new(RedisCapability::from_config(&cfg).unwrap()))
            });
        });

        let names: Vec<_> = capability::read().kv_factory_names().cloned().collect();
        assert!(names.contains(&"redis".to_string()));
    }

    /// Full three-layer flow:
    ///   1. Config entry (datasource → redis → test-redis-main)
    ///   2. Factory (config_type = "redis")
    ///   3. `capability::kv("test-redis-main")` → instance → use
    ///
    /// Requires a Redis server at localhost:6379. Set `FUSION_TEST_REDIS=1`
    /// to enable, or run `#[ignore]` by default.
    #[tokio::test]
    #[ignore = "requires Redis at localhost:6379; set FUSION_TEST_REDIS=1 to run"]
    async fn test_three_layer_config_to_capability() {
        // ── Layer 1: register config entry ──
        fusion_unit_sdk::config::register(|reg| {
            reg.insert(
                "datasource",
                "redis",
                "test-redis-main",
                serde_json::json!({
                    "host": "127.0.0.1",
                    "port": 6379,
                    "pool_size": 4
                }),
            );
        });

        // ── Layer 2: register factory ──
        capability::register(|reg| {
            reg.set_kv_factory("redis", |entry| {
                let cfg: RedisCapabilityConfig =
                    serde_json::from_value(entry.data.clone()).unwrap();
                Ok(Arc::new(RedisCapability::from_config(&cfg).unwrap()))
            });
        });

        // ── Layer 3: get instance by config ID ──
        let redis = capability::kv("test-redis-main")
            .expect("instance should be created from config entry + factory");

        assert_eq!(redis.name(), well_known::REDIS);

        // ── Use the instance ──
        redis.set("fusion:greeting", b"hello from redis").await.unwrap();
        let val = redis.get("fusion:greeting").await.unwrap();
        assert_eq!(val, Some(b"hello from redis".to_vec()));

        redis.delete("fusion:greeting").await.unwrap();
        assert!(redis.get("fusion:greeting").await.unwrap().is_none());

        // ── Batch operations ──
        redis.mset(&[
            ("fusion:a", b"1".as_ref()),
            ("fusion:b", b"2".as_ref()),
        ]).await.unwrap();
        let vals = redis.mget(&["fusion:a", "fusion:b"]).await.unwrap();
        assert_eq!(vals[0], Some(b"1".to_vec()));
        assert_eq!(vals[1], Some(b"2".to_vec()));
        redis.mdelete(&["fusion:a", "fusion:b"]).await.unwrap();

        // ── Paginated scan ──
        redis.set("fusion:scan:x", b"100").await.unwrap();
        redis.set("fusion:scan:y", b"200").await.unwrap();
        let opts = ScanOptions::new().pattern("fusion:scan:*").page_size(10);
        let page = redis.scan_page(None, &opts).await.unwrap();
        assert_eq!(page.entries.len(), 2);
        redis.mdelete(&["fusion:scan:x", "fusion:scan:y"]).await.unwrap();
    }
}
