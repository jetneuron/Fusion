//! Table data providers — pluggable data sources for SQL engines.
//!
//! A provider knows how to turn a datasource config entry into rows
//! ([`Frame`](crate::proto::transfer::Frame)). Providers deliberately do
//! **not** speak engine types (no DataFusion `TableProvider`): their rows
//! cross the dylib boundary as data, and the engine (capability) materializes
//! them. This keeps provider dylibs tiny and engine-free.
//!
//! ```text
//! SqlUnitTask ──(provider: "sqlite")──▶ TableDataProvider.load_frames(sql)
//!      │                                        │
//!      └──── frames ──▶ engine.register_frame_table(name, frames)
//! ```

use std::sync::Arc;

use crate::runtime::UnitResult;

/// Loads the full row set of a datasource.
#[async_trait::async_trait]
pub trait TableDataProvider: Send + Sync {
    /// Execute the provider's own query (the table's optional `sql`
    /// fragment, or a full scan) and return all rows as frames.
    async fn load_frames(&self, sql: Option<&str>) -> UnitResult<Vec<crate::proto::transfer::Frame>>;
}

/// Plugin contract for provider dylibs.
///
/// Unlike the old `TableProviderFactory` design, `register_providers`
/// **returns** providers instead of registering them into a static — the
/// host collects them and injects them into unit dylibs, because statics
/// are per-binary-image and a provider dylib's registry would be invisible
/// to the unit that needs it.
pub trait ProviderPlugin: Send + Sync {
    /// Return `(name, provider)` pairs, e.g. `("sqlite", …)`.
    fn register_providers(&self) -> Vec<(String, Arc<dyn TableDataProvider>)>;

    /// Plugin version string.
    fn version(&self) -> &str {
        "1.0.0"
    }
}
