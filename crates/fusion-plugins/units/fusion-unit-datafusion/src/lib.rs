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
pub mod stream_table;

use std::collections::HashMap;
use fusion_capability_datafusion::DataFusionCapability;
use fusion_derive::LogicalTask;
use fusion_unit_sdk::capability::CapabilitySqlEngine;
use fusion_unit_sdk::config;
use fusion_unit_sdk::graph::types::{
    ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext, UnitMeta,
};
use fusion_unit_sdk::proto::transfer::Frame;
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

/// An upstream stream table — rows from another node buffered and
/// registered as a DataFusion table at EOF.
#[derive(Debug, Clone)]
struct StreamTableConfig {
    name: String,   // table name in SQL
    source: String, // upstream node ID (frame.source)
}

const DEFAULT_ROW_THRESHOLD: usize = 80_000;

#[derive(Default, LogicalTask)]
pub struct SqlUnitTask {
    meta: UnitMeta,
    datasource: String,
    sql: String,
    tables: Vec<TableConfig>,
    stream_tables: Vec<StreamTableConfig>,
    /// Stream table providers — one per stream table. Rows are appended
    /// in compute(), frozen at EOF, then registered with DataFusion.
    stream_providers: Vec<Arc<stream_table::StreamTableProvider>>,
    /// Maps source node ID → index into stream_providers.
    source_index: HashMap<String, usize>,
    /// Work directory for this node's temp data.
    data_dir: String,
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

        // Parse stream_tables (upstream nodes whose data is buffered
        // and registered as DataFusion tables at EOF).
        if let Some(arr) = conf.get("stream_tables").and_then(|v| v.as_array()) {
            for item in arr {
                let name = item["name"].as_str().unwrap_or("stream").to_string();
                let source = item["source"]
                    .as_str()
                    .ok_or_else(|| {
                        fusion_unit_sdk::runtime::UnitError::config_required(
                            "stream_tables[*].source",
                        )
                    })?
                    .to_string();
                self.stream_tables.push(StreamTableConfig { name, source });
            }
        }

        // Map/sink roles receive upstream frames — they must declare
        // stream_tables so incoming data is buffered and queried at EOF.
        // Without them, compute() would re-run the SQL per frame.
        if (unit.is_mapper() || unit.is_sink()) && self.stream_tables.is_empty() {
            return Err(fusion_unit_sdk::runtime::UnitError::config_required(
                "stream_tables (map/sink role requires stream tables)",
            ));
        }

        // Spill threshold — rows buffered in memory before spilling to
        // Parquet. `usize::MAX` = pure in-memory stream table.
        let row_threshold = conf
            .get("row_threshold")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(DEFAULT_ROW_THRESHOLD);

        // Node data directory for spilled files.
        let graph_id = unit
            .get_runtime_states()
            .as_ref()
            .map(|s| s.graph_id().to_string())
            .unwrap_or_else(|| unit.get_id().clone());
        self.data_dir = fusion_unit_sdk::graph::utils::node_data_dir(
            &graph_id,
            unit.get_id(),
        )
        .to_string_lossy()
        .to_string();

        // One StreamTableProvider per source.
        self.source_index = self
            .stream_tables
            .iter()
            .enumerate()
            .map(|(i, st)| (st.source.clone(), i))
            .collect();
        self.stream_providers = self
            .stream_tables
            .iter()
            .map(|st| {
                let dir = format!("{}/{}", self.data_dir, st.source);
                Arc::new(stream_table::StreamTableProvider::new(
                    &st.name,
                    row_threshold,
                    &dir,
                ))
            })
            .collect();

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
        let frames = engine.query(&self.sql).await?;
        for frame in frames {
            ctx.send(frame).await;
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
        frame: Frame,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let source_idx = self.source_index.get(&frame.source).copied();
        let providers = self.stream_providers.clone();

        Ok(async move {
            let idx = match source_idx {
                Some(i) => i,
                None => {
                    // Config referenced an upstream that never sent data —
                    // surface it instead of silently dropping frames.
                    log::warn!(
                        "SqlUnitTask [{}] received frame from unknown source `{}` (declared: {:?})",
                        self.meta.get_id(),
                        frame.source,
                        self.source_index.keys().collect::<Vec<_>>()
                    );
                    return Ok(());
                }
            };

            // Append to the per-source provider. Thread-safe — parallel
            // workers share one provider per source.
            providers[idx].append(frame);
            Ok(())
        })
    }

    /// When stream_tables are configured, spill remaining buffers to
    /// Parquet, register spill directories as DataFusion tables, execute
    /// SQL, emit results, and clean up temp files.
    fn on_eof<'life0, 'async_trait>(
        &'life0 self,
        frame: Frame,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let has_streams = !self.stream_tables.is_empty();
        let has_static = !self.tables.is_empty();
        let engine = self.engine.clone();
        let sql = self.sql.clone();
        let tables = self.tables.clone();
        let stream_tables = self.stream_tables.clone();
        let stream_providers = self.stream_providers.clone();
        let data_dir = self.data_dir.clone();

        Ok(Box::pin(async move {
            if !has_streams && !has_static {
                return Ok(());
            }
            let engine = engine.ok_or_else(|| {
                fusion_unit_sdk::runtime::UnitError::unknown("engine not initialized")
            })?;
            let df = engine
                .as_any()
                .downcast_ref::<DataFusionCapability>()
                .ok_or_else(|| {
                    fusion_unit_sdk::runtime::UnitError::unknown(
                        "SQL engine is not DataFusionCapability",
                    )
                })?;

            // 1. Register static tables from config.
            if has_static {
                SqlUnitTask::register_tables(df, &tables).await?;
            }

            // 2. Stream tables: freeze each provider and register it.
            if has_streams {
                for provider in stream_providers.iter() {
                    provider.finish();
                }
                let session = df.ctx.lock().await;
                for (i, st) in stream_tables.iter().enumerate() {
                    session
                        .register_table(&st.name, stream_providers[i].clone())
                        .map_err(|e| {
                            fusion_unit_sdk::runtime::UnitError::unknown(format!(
                                "register stream table `{}`: {e}",
                                st.name
                            ))
                        })?;
                }
            }

            // 3. Execute SQL and emit rows.
            let rows = engine.query(&sql).await?;
            for r in rows {
                ctx.send(r).await;
            }

            // 4. Deregister this node's tables so they don't leak into
            // other graphs sharing the process-global session.
            {
                let session = df.ctx.lock().await;
                for name in tables.iter().map(|t| t.name.as_str()) {
                    let _ = session.deregister_table(name);
                }
                for name in stream_tables.iter().map(|st| st.name.as_str()) {
                    let _ = session.deregister_table(name);
                }
            }

            // 5. Cleanup node temp directory.
            if has_streams {
                let _ = std::fs::remove_dir_all(&data_dir);
            }
            Ok(())
        }))
    }
}
