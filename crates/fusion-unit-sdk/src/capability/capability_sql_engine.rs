use super::Capability;
use crate::runtime::UnitResult;

/// SQL query engine capability.
///
/// Implementations provide SQL execution over registered data sources.
///
/// # Well-known names
///
/// ```ignore
/// use fusion_unit_sdk::capability::capability_sql_engine::well_known;
/// let df = capability::read().sql(well_known::DATAFUSION);
/// ```
///
/// # Table lifecycle
///
/// Data crosses the plugin boundary as [`Frame`](crate::proto::transfer::Frame)
/// byte-streams, never as engine-specific types (e.g. DataFusion
/// `TableProvider`), so callers never need to know the engine internals:
///
/// ```text
/// static tables:  register_csv_table(name, path)
/// stream tables:  register_frame_table(name, frames)  ×N  →  finalize_frame_table(name)
/// cleanup:        deregister_table(name)
/// ```
#[async_trait::async_trait]
pub trait CapabilitySqlEngine: Capability {
    /// Execute a SQL query and return results as Rows.
    async fn query(&self, sql: &str) -> UnitResult<Vec<crate::proto::transfer::Frame>>;

    /// Append rows to a stream table. The first call creates the table's
    /// accumulation buffer; subsequent calls append. Callers should batch
    /// appends (not one per row) and must call [`Self::finalize_frame_table`]
    /// at EOF before the table becomes queryable.
    async fn register_frame_table(
        &self,
        name: &str,
        frames: Vec<crate::proto::transfer::Frame>,
    ) -> UnitResult<()>;

    /// Freeze a stream table (registered via [`Self::register_frame_table`])
    /// so it becomes queryable.
    async fn finalize_frame_table(&self, name: &str) -> UnitResult<()>;

    /// Register a CSV file as a queryable table.
    async fn register_csv_table(&self, name: &str, path: &str) -> UnitResult<()>;

    /// Deregister a table and release its resources.
    async fn deregister_table(&self, name: &str) -> UnitResult<()>;

    /// Enable downcasting to concrete implementations (e.g. DataFusion)
    /// for access to engine-specific APIs like `SessionContext`.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Well-known `CapabilitySqlEngine` capability names.
pub mod well_known {
    /// Apache DataFusion — `"datafusion"`
    pub const DATAFUSION: &str = "datafusion";
    /// DuckDB — `"duckdb"`
    pub const DUCKDB: &str = "duckdb";
    /// SQLite (embedded) — `"sqlite"`
    pub const SQLITE: &str = "sqlite";
    /// Default / unspecified implementation — `"default"`
    pub const DEFAULT: &str = "default";
}
