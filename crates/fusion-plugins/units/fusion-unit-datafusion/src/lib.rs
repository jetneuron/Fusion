//! # Fusion Unit SQL
//!
//! Generic SQL execution unit with pluggable table providers.
//!
//! ## Architecture
//!
//! ```text
//! YAML config
//!   ├── datasource: datafusion     → capability::sql(id)
//!   ├── sql: "SELECT ..."
//!   └── tables:                    → provider registry → TableProvider
//!       ├── name: my_csv
//!       ├── provider: csv
//!       └── config_id: csv-data
//! ```
//!
//! ## YAML (Source)
//!
//! ```yaml
//! units:
//!   - id: sql-reader
//!     type: SqlUnitTask
//!     config:
//!       datasource: datafusion
//!       sql: "SELECT * FROM my_csv"
//!       tables:
//!         - name: my_csv
//!           provider: csv
//!           config_id: csv-file-1
//! ```

pub mod providers;

use fusion_capability_datafusion::DataFusionCapability;
use fusion_derive::LogicalTask;
use fusion_unit_sdk::capability::CapabilitySqlEngine;
use fusion_unit_sdk::config;
use fusion_unit_sdk::graph::types::{
    ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext, UnitMeta,
};
use fusion_unit_sdk::proto::transfer::Row;
use fusion_unit_sdk::runtime::logical::LogicalTaskMeta;
use fusion_unit_sdk::runtime::UnitResult;
use fusion_unit_sdk::units::config_util::UnitConfigExt;
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};
use std::future::Future;
use std::sync::Arc;

// ============================================================
// Plugin
// ============================================================

#[unsafe(no_mangle)]
pub extern "C" fn init_plugin() -> Box<dyn GraphUnitPlugin> {
    Box::new(SqlUnitPlugin {})
}

pub struct SqlUnitPlugin {}

impl GraphUnitPlugin for SqlUnitPlugin {
    fn register_units(&self) -> UnitManifest {
        providers::csv::register_csv_providers();
        let mut m = UnitManifest::default();
        SqlUnitTask::register_unit(&mut m, &self.plugin_version());
        m
    }
    fn plugin_version(&self) -> &str {
        "1.0.0"
    }
}

// ============================================================
// TableConfig — parsed from YAML
// ============================================================

#[derive(Debug, Clone)]
struct TableConfig {
    name: String,
    provider: String,
    config_id: String,
    /// Optional subquery — the table provider uses this SQL instead
    /// of a full table scan. Belongs to the unit (computation logic),
    /// not the datasource.
    sql: Option<String>,
}

// ============================================================
// SqlUnitTask
// ============================================================

#[derive(Default, LogicalTask)]
pub struct SqlUnitTask {
    meta: UnitMeta,
    /// Config entry ID for the SQL engine (e.g. "datafusion").
    datasource: String,
    /// SQL query string.
    sql: String,
    /// Table definitions referencing providers and config entries.
    tables: Vec<TableConfig>,
    /// Resolved capability.
    engine: Option<Arc<dyn CapabilitySqlEngine>>,
}

impl InitUnit for SqlUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let conf = unit
            .get_config()
            .ok_or_else(|| fusion_unit_sdk::runtime::UnitError::config_required("config"))?;

        self.datasource = conf.require_string("datasource")?;
        self.sql = conf.require_string("sql")?;

        // Parse table definitions.
        if let Some(tables_val) = conf.get("tables") {
            if let Some(arr) = tables_val.as_array() {
                for item in arr {
                    let name = item["name"].as_str().unwrap_or("unknown").to_string();
                    let provider = item["provider"].as_str().unwrap_or("csv").to_string();
                    let config_id = item["config_id"]
                        .as_str()
                        .ok_or_else(|| {
                            fusion_unit_sdk::runtime::UnitError::config_required(
                                "tables[*].config_id",
                            )
                        })?
                        .to_string();
                    let sql = item["sql"].as_str().map(|s| s.to_string());
                    self.tables.push(TableConfig {
                        name,
                        provider,
                        config_id,
                        sql,
                    });
                }
            }
        }

        // Resolve SQL engine capability.
        self.engine = Some(
            fusion_unit_sdk::capability::sql(&self.datasource).ok_or_else(|| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "SQL engine not found for datasource `{}`",
                    self.datasource
                ))
            })?,
        );

        Ok(())
    }
}

impl SqlUnitTask {
    /// Register all table providers with the DataFusion session.
    async fn register_tables(
        df: &DataFusionCapability,
        tables: &[TableConfig],
    ) -> UnitResult<()> {
        // Collect table providers FIRST (I/O), then register
        // (lock held briefly). Avoids holding the ctx lock across
        // async provider creation.
        struct Pending {
            name: String,
            provider: Arc<dyn datafusion::datasource::TableProvider>,
        }
        let mut pending = Vec::new();
        for table in tables {
            let factory = providers::get_provider(&table.provider).ok_or_else(|| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "table provider `{}` not found",
                    table.provider
                ))
            })?;
            let guard = config::read();
            let entry = guard.entry(&table.config_id).cloned().ok_or_else(|| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "config entry `{}` not found",
                    table.config_id
                ))
            })?;
            drop(guard);
            let tp = factory.create(&entry, table.sql.as_deref()).await?;
            pending.push(Pending {
                name: table.name.clone(),
                provider: tp,
            });
        }

        let ctx = df.ctx.lock().await;
        for p in &pending {
            ctx.register_table(&p.name, Arc::clone(&p.provider))
                .map_err(|e| {
                    fusion_unit_sdk::runtime::UnitError::unknown(format!(
                        "register table `{}`: {e}",
                        p.name
                    ))
                })?;
        }
        Ok(())
    }

    async fn execute_query(&self, ctx: &TaskContext) -> UnitResult<()> {
        let engine = self.engine.as_ref().ok_or_else(|| {
            fusion_unit_sdk::runtime::UnitError::unknown("engine not initialized")
        })?;

        // Downcast to DataFusion for table registration.
        let df = engine
            .as_any()
            .downcast_ref::<DataFusionCapability>()
            .ok_or_else(|| {
                fusion_unit_sdk::runtime::UnitError::unknown(
                    "SQL engine is not DataFusionCapability",
                )
            })?;

        // Register tables (idempotent — skips already-registered).
        Self::register_tables(df, &self.tables).await?;

        // Execute SQL and emit rows.
        let rows = engine.query(&self.sql).await?;
        for row in rows {
            ctx.send(row).await;
        }
        Ok(())
    }
}

// ============================================================
// Source
// ============================================================

impl SourceUnit for SqlUnitTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        Ok(async move { self.execute_query(&ctx).await })
    }
}

// ============================================================
// Map
// ============================================================

impl MapUnit for SqlUnitTask {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        _row: Row,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Ok(async move { self.execute_query(ctx).await })
    }
}
