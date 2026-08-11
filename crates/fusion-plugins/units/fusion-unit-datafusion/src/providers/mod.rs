//! Table provider framework for `fusion-unit-sql`.
//!
//! Providers convert config entries into DataFusion `TableProvider`
//! implementations. New providers register themselves via
//! [`register_provider`].

use datafusion::datasource::TableProvider;
use fusion_unit_sdk::config::ConfigEntry;
use fusion_unit_sdk::runtime::UnitResult;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

pub mod csv;

// ============================================================
// TableProviderFactory trait
// ============================================================

/// Creates a DataFusion [`TableProvider`] from a config entry and
/// optional SQL subquery.
///
/// Each provider has a unique name (e.g. `"csv"`, `"tsv"`) that
/// is referenced in the unit's YAML config under `tables[*].provider`.
///
/// # Parameters
/// - `entry`: datasource config (connection info only)
/// - `sql`: optional subquery from the unit's table config
#[async_trait::async_trait]
pub trait TableProviderFactory: Send + Sync {
    /// Unique provider name, referenced in YAML as `provider: <name>`.
    fn name(&self) -> &str;

    /// Create a table provider from config + optional SQL.
    async fn create(
        &self,
        entry: &ConfigEntry,
        sql: Option<&str>,
    ) -> UnitResult<Arc<dyn TableProvider>>;
}

// ============================================================
// Global provider registry
// ============================================================

static REGISTRY: LazyLock<RwLock<HashMap<String, Arc<dyn TableProviderFactory>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Register a table provider factory. Called once at plugin init.
pub fn register_provider(factory: Arc<dyn TableProviderFactory>) {
    let mut reg = REGISTRY.write().unwrap();
    reg.insert(factory.name().to_string(), factory);
}

/// Look up a provider by name.
pub fn get_provider(name: &str) -> Option<Arc<dyn TableProviderFactory>> {
    let reg = REGISTRY.read().unwrap();
    reg.get(name).cloned()
}

// ============================================================
// ProviderPlugin trait — for dynamically loaded provider crates
// ============================================================

/// Trait implemented by external provider plugin crates.
///
/// Each provider plugin is a `cdylib` that exports an
/// `init_provider_plugin` symbol. This is the provider analogue
/// of [`GraphUnitPlugin`] and [`CapabilityPlugin`].
pub trait ProviderPlugin: Send + Sync {
    /// Register this provider's factories into the global registry.
    fn register(&self);

    /// Plugin version string.
    fn version(&self) -> &str {
        "1.0.0"
    }
}
