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
#[async_trait::async_trait]
pub trait CapabilitySqlEngine: Capability {
    /// Execute a SQL query and return results as Rows.
    async fn query(&self, sql: &str) -> UnitResult<Vec<crate::proto::transfer::Row>>;
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
