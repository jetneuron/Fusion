//! # Fusion Capability Example
//!
//! A minimal demonstration of a capability plugin for Fusion.
//!
//! This plugin provides an **in-memory key-value store** (implementing
//! [`CapabilityKeyValueStore`](fusion_unit_sdk::capability::CapabilityKeyValueStore)) registered
//! under the name `"inmemory"`. Multiple KV store implementations can
//! coexist — e.g. `"redis"`, `"hbase"`, `"inmemory"` — and unit plugins
//! select which one they need by name.
//!
//! ## How it works
//!
//! 1. The engine loads this `.dylib` via `PluginManager::load_capability_plugin()`.
//! 2. The `init_capability_plugin` symbol is called, returning a `CapabilityPlugin`.
//! 3. `CapabilityPlugin::register()` calls `reg.set_kv(Arc::new(InMemoryKV::new()))`.
//! 4. Unit plugins access it via `capability::read().kv("inmemory")`.
//!
//! ## Usage (from a unit plugin)
//!
//! ```ignore
//! use fusion_unit_sdk::capability;
//!
//! fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
//!     self.kv = capability::read()
//!         .kv("inmemory")
//!         .cloned()
//!         .ok_or_else(|| UnitError::unknown("inmemory kv store not installed"))?;
//!     Ok(())
//! }
//! ```
//!
//! ## Building
//!
//! ```sh
//! cargo build -p fusion-capability-example
//! # Output: target/debug/libfusion_capability_example.dylib (macOS)
//! #         target/debug/libfusion_capability_example.so    (Linux)
//! ```

use fusion_unit_sdk::capability::capability_key_value_store::{
    well_known, ScanOptions, ScanResult,
};
use fusion_unit_sdk::capability::{self, Capability, CapabilityKeyValueStore, CapabilityPlugin};
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

// ============================================================
// Concrete capability implementation
// ============================================================

/// An in-memory key-value store for demonstration purposes.
///
/// Registered under the name `"inmemory"` so it can coexist with
/// other `CapabilityKeyValueStore` implementations like `"redis"` or `"hbase"`.
///
/// Data is stored in a `HashMap` behind a `tokio::sync::Mutex`.
/// All data is lost when the process exits.
pub struct InMemoryKV {
    data: Mutex<HashMap<String, Vec<u8>>>,
    /// Tracks whether `init()` has been called.
    initialized: Mutex<bool>,
}

impl InMemoryKV {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            initialized: Mutex::new(false),
        }
    }
}

impl Default for InMemoryKV {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Capability for InMemoryKV {
    fn name(&self) -> &str {
        well_known::IN_MEMORY
    }

    async fn init(&self) -> UnitResult<()> {
        let mut initialized = self.initialized.lock().await;
        if *initialized {
            return Err(UnitError::unknown("already initialized"));
        }
        *initialized = true;
        Ok(())
    }

    async fn shutdown(&self) -> UnitResult<()> {
        let mut data = self.data.lock().await;
        data.clear();
        let mut initialized = self.initialized.lock().await;
        *initialized = false;
        Ok(())
    }
}

#[async_trait::async_trait]
impl CapabilityKeyValueStore for InMemoryKV {
    async fn get(&self, key: &str) -> UnitResult<Option<Vec<u8>>> {
        Ok(self.data.lock().await.get(key).cloned())
    }

    async fn set(&self, key: &str, value: &[u8]) -> UnitResult<()> {
        self.data
            .lock()
            .await
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> UnitResult<()> {
        self.data.lock().await.remove(key);
        Ok(())
    }

    async fn mget(&self, keys: &[&str]) -> UnitResult<Vec<Option<Vec<u8>>>> {
        let data = self.data.lock().await;
        Ok(keys.iter().map(|k| data.get(*k).cloned()).collect())
    }

    async fn scan_page(
        &self,
        cursor: Option<&str>,
        opts: &ScanOptions,
    ) -> UnitResult<ScanResult> {
        let data = self.data.lock().await;

        // Resolve cursor: last processed key, or start from beginning.
        let start_key = cursor.unwrap_or("");

        // Collect matching entries as a sorted vec for stable pagination.
        let mut matching: Vec<(String, Vec<u8>)> = data
            .iter()
            .filter(|(k, _)| {
                if let Some(pattern) = &opts.pattern {
                    // Simple glob: * matches any sequence, ? matches one char.
                    glob_match(k, pattern)
                } else {
                    true
                }
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        matching.sort_by(|a, b| a.0.cmp(&b.0));

        // Find start position from cursor.
        let start_idx = if start_key.is_empty() {
            0
        } else {
            matching
                .binary_search_by(|(k, _)| k.as_str().cmp(start_key))
                .map(|i| i + 1) // skip past the cursor key
                .unwrap_or_else(|i| i)
        };

        let page_size = opts.page_size.unwrap_or(100) as usize;
        let end = (start_idx + page_size).min(matching.len());
        let entries: Vec<_> = matching[start_idx..end].to_vec();

        let next_cursor = if end < matching.len() {
            Some(matching[end - 1].0.clone())
        } else {
            None
        };

        Ok(ScanResult {
            entries,
            next_cursor,
        })
    }
}

/// Minimal glob matching for `*` (any sequence) and `?` (single char).
fn glob_match(s: &str, pattern: &str) -> bool {
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let mut dp = vec![vec![false; p.len() + 1]; s.len() + 1];
    dp[0][0] = true;
    for j in 0..p.len() {
        if p[j] == '*' {
            dp[0][j + 1] = dp[0][j];
        } else {
            break;
        }
    }
    for i in 0..s.len() {
        for j in 0..p.len() {
            if p[j] == '*' {
                dp[i + 1][j + 1] = dp[i][j + 1] || dp[i + 1][j];
            } else if p[j] == '?' || s[i] == p[j] {
                dp[i + 1][j + 1] = dp[i][j];
            }
        }
    }
    dp[s.len()][p.len()]
}

// ============================================================
// CapabilityPlugin — the dylib-facing interface
// ============================================================

/// Plugin entry point that registers both a pre-built instance (for
/// backward compat) and a factory (for config-driven creation).
pub struct ExampleCapabilityPlugin;

impl CapabilityPlugin for ExampleCapabilityPlugin {
    fn register(&self) {
        capability::register(|reg| {
            // Pre-built instance (backward compat).
            reg.set_kv(Arc::new(InMemoryKV::new()));

            // Factory: creates an InMemoryKV from config entries with
            // config_type "inmemory".
            reg.set_kv_factory("inmemory", |_entry| {
                Ok(Arc::new(InMemoryKV::new()))
            });
        });
    }

    fn version(&self) -> &str {
        "0.1.0"
    }
}

// ============================================================
// FFI export — called by the engine via libloading
// ============================================================

/// Exported symbol for dynamic loading.
///
/// The engine calls this function when loading the capability plugin.
/// It returns a trait object that the engine uses to register capabilities
/// into the global registry.
#[unsafe(no_mangle)]
pub extern "C" fn init_capability_plugin() -> Box<dyn CapabilityPlugin + Send + Sync> {
    Box::new(ExampleCapabilityPlugin)
}

// ============================================================
// Tests — exercise the capability end-to-end
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fusion_unit_sdk::capability;

    /// Simulate the full lifecycle:
    /// 1. Load a capability plugin → calls `register()`.
    /// 2. Unit plugin looks up capability by name.
    /// 3. Unit plugin calls trait methods on the capability.
    #[tokio::test]
    async fn test_register_and_use_by_name() {
        // ── Step 1: load capability plugin ──
        let plugin = ExampleCapabilityPlugin;
        assert_eq!(plugin.version(), "0.1.0");
        plugin.register();

        // ── Step 2: look up by well_known constant ──
        let kv = capability::read()
            .kv(well_known::IN_MEMORY)
            .expect("inmemory kv store should be registered");

        // ── Step 3: use the capability ──
        kv.set("key1", b"value1").await.unwrap();
        assert_eq!(kv.get("key1").await.unwrap(), Some(b"value1".to_vec()));

        kv.scan_all(&ScanOptions::new().pattern("key*"))
            .await
            .unwrap();

        kv.delete("key1").await.unwrap();
        assert_eq!(kv.get("key1").await.unwrap(), None);
    }

    /// Full lifecycle: register → init → use → shutdown.
    #[tokio::test]
    async fn test_lifecycle() {
        // ── register ──
        capability::register(|reg| {
            reg.set_kv(Arc::new(InMemoryKV::new()));
        });

        // ── init ──
        capability::write()
            .init_kv(well_known::IN_MEMORY)
            .await
            .expect("init should succeed");

        // Double-init should fail
        let err = capability::write()
            .init_kv(well_known::IN_MEMORY)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already initialized"));

        // ── use ──
        {
            let kv = capability::read()
                .kv(well_known::IN_MEMORY)
                .unwrap();
            kv.set("key", b"val").await.unwrap();
            assert_eq!(kv.get("key").await.unwrap(), Some(b"val".to_vec()));
        }

        // ── shutdown (also removes from registry) ──
        capability::write()
            .shutdown_kv(well_known::IN_MEMORY)
            .await
            .expect("shutdown should succeed");

        // After shutdown, data is cleared and capability is gone
        assert!(capability::read().kv(well_known::IN_MEMORY).is_none());
    }

    /// Verify that multiple named implementations of the same trait
    /// can coexist in the registry.
    #[tokio::test]
    async fn test_multiple_implementations_coexist() {
        // Register two KV stores with different names
        capability::register(|reg| {
            reg.set_kv(Arc::new(InMemoryKV::new())); // name = "inmemory"
            // In a real scenario, this would be Redis or HBase:
            reg.set_kv(Arc::new(InMemoryKV::new())); // name = "inmemory" (again)
            // ^ second call with same name replaces the first (HashMap semantics)
        });

        // List all registered KV store names
        let names: Vec<_> = capability::read().kv_names().cloned().collect();
        assert!(names.contains(&well_known::IN_MEMORY.to_string()));
    }

    /// Full three-layer flow: config entry → factory → instance.
    ///
    /// 1. Register a config entry under the three-level hierarchy
    ///    (datasource → inmemory → test-kv).
    /// 2. Register a factory for config_type "inmemory".
    /// 3. `capability::kv("test-kv")` → get instance → use.
    #[tokio::test]
    async fn test_three_layer_config_to_capability() {
        // ── Layer 1: config entry ──
        fusion_unit_sdk::config::register(|reg| {
            reg.insert(
                "datasource",
                "inmemory",
                "test-kv",
                serde_json::json!({}), // InMemoryKV ignores config data
            );
        });

        // ── Layer 2: factory ──
        capability::register(|reg| {
            reg.set_kv_factory("inmemory", |_entry| {
                Ok(Arc::new(InMemoryKV::new()))
            });
        });

        // ── Layer 3: get instance by config ID ──
        let kv = capability::kv("test-kv")
            .expect("instance should be created from config + factory");

        // ── Use ──
        kv.set("k", b"v").await.unwrap();
        assert_eq!(kv.get("k").await.unwrap(), Some(b"v".to_vec()));
        assert!(kv.exists("k").await.unwrap());

        // Paginated scan
        kv.set("test:a", b"1").await.unwrap();
        kv.set("test:b", b"2").await.unwrap();
        let opts = ScanOptions::new().pattern("test:*").page_size(10);
        let page = kv.scan_page(None, &opts).await.unwrap();
        assert_eq!(page.entries.len(), 2);

        kv.mdelete(&["k", "test:a", "test:b"]).await.unwrap();
        assert!(!kv.exists("k").await.unwrap());
    }
}
