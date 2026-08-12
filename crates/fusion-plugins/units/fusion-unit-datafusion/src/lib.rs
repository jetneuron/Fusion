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

use datafusion::arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;
use fusion_capability_datafusion::DataFusionCapability;
use std::sync::Mutex as StdMutex;
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

/// An upstream stream table — rows from another node buffered and
/// registered as a DataFusion table at EOF.
#[derive(Debug, Clone)]
struct StreamTableConfig {
    name: String,   // table name in SQL
    source: String, // upstream node ID (row.source)
}

const DEFAULT_ROW_THRESHOLD: usize = 80_000;

#[derive(Default, LogicalTask)]
pub struct SqlUnitTask {
    meta: UnitMeta,
    datasource: String,
    sql: String,
    tables: Vec<TableConfig>,
    stream_tables: Vec<StreamTableConfig>,
    /// Row buffers — one per stream table, each behind its own lock
    /// so forwarding tasks from different sources never contend.
    row_buffers: Vec<Arc<StdMutex<Vec<Row>>>>,
    /// Spilled Parquet file paths — one per stream table.
    spill_files: Vec<Arc<StdMutex<Vec<String>>>>,
    /// Maps source node ID → index into row_buffers / spill_files.
    source_index: HashMap<String, usize>,
    /// Max buffered rows before spilling to Parquet.
    row_threshold: usize,
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

        // Pre-allocate per-source buffers so forwarding tasks from
        // different sources never contend on the same lock.
        let n = self.stream_tables.len();
        self.row_buffers = (0..n).map(|_| Arc::new(StdMutex::new(Vec::new()))).collect();
        self.spill_files = (0..n).map(|_| Arc::new(StdMutex::new(Vec::new()))).collect();
        self.source_index = self
            .stream_tables
            .iter()
            .enumerate()
            .map(|(i, st)| (st.source.clone(), i))
            .collect();

        // Spill threshold — rows buffered before writing to Parquet.
        self.row_threshold = conf
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
// Helpers
// ============================================================

/// Convert buffered Fusion [`Row`]s into a DataFusion `RecordBatch`.
/// Single-pass: builds Arrow arrays directly without intermediate Vecs.
fn build_record_batch(rows: &[Row]) -> Result<RecordBatch, String> {
    if rows.is_empty() {
        return Err("no rows to build table".into());
    }
    let first = &rows[0];
    let col_count = first.columns.len();
    let n = rows.len();

    let mut fields = Vec::with_capacity(col_count);
    let mut col_types: Vec<DataType> = Vec::with_capacity(col_count);
    for col in &first.columns {
        let dt = fusion_dt_to_arrow(
            col.dt
                .enum_value()
                .unwrap_or(fusion_unit_sdk::proto::transfer::DataType::unknown),
        );
        col_types.push(dt.clone());
        fields.push(Field::new(&col.field, dt, true));
    }
    let schema = std::sync::Arc::new(Schema::new(fields));

    // Build Arrow arrays in a single pass over all rows.
    let mut builders: Vec<Box<dyn ArrayBuilder>> = col_types
        .iter()
        .map(|dt| match dt {
            DataType::Int64 => {
                Box::new(Int64Builder::with_capacity(n)) as Box<dyn ArrayBuilder>
            }
            DataType::Float64 => {
                Box::new(Float64Builder::with_capacity(n)) as Box<dyn ArrayBuilder>
            }
            DataType::Boolean => {
                Box::new(BooleanBuilder::with_capacity(n)) as Box<dyn ArrayBuilder>
            }
            _ => Box::new(StringBuilder::with_capacity(n, 0)) as Box<dyn ArrayBuilder>,
        })
        .collect();

    for row in rows {
        for (i, col) in row.columns.iter().enumerate() {
            match col_types[i] {
                DataType::Int64 => {
                    let v = match col.dt.enum_value() {
                        Ok(fusion_unit_sdk::proto::transfer::DataType::i32) => {
                            col.i32_val as i64
                        }
                        _ => col.i64_val,
                    };
                    builders[i]
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .unwrap()
                        .append_value(v);
                }
                DataType::Float64 => {
                    let v = match col.dt.enum_value() {
                        Ok(fusion_unit_sdk::proto::transfer::DataType::f32) => {
                            col.f32_val as f64
                        }
                        _ => col.f64_val,
                    };
                    builders[i]
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .unwrap()
                        .append_value(v);
                }
                DataType::Boolean => {
                    builders[i]
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .unwrap()
                        .append_value(col.bool_val);
                }
                _ => {
                    builders[i]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap()
                        .append_value(&col.str_val);
                }
            }
        }
    }

    let arrow_cols: Vec<ArrayRef> = builders
        .into_iter()
        .map(|mut b| b.finish())
        .collect();

    RecordBatch::try_new(schema, arrow_cols).map_err(|e| format!("build batch: {e}"))
}

/// Helper trait to erase the concrete Arrow builder type so we can
/// store heterogeneous builders in a `Vec<Box<dyn ArrayBuilder>>`.
trait ArrayBuilder: Send {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    fn finish(&mut self) -> ArrayRef;
}

impl ArrayBuilder for Int64Builder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}
impl ArrayBuilder for Float64Builder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}
impl ArrayBuilder for BooleanBuilder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}
impl ArrayBuilder for StringBuilder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

/// Write buffered rows to a Parquet file. Returns the number of rows written.
fn spill_to_parquet(path: &str, rows: &[Row]) -> Result<usize, String> {
    let batch = build_record_batch(rows)?;
    let row_count = batch.num_rows();
    let file = std::fs::File::create(path).map_err(|e| format!("create: {e}"))?;
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_compression(datafusion::parquet::basic::Compression::SNAPPY)
        .build();
    let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(
        file,
        batch.schema(),
        Some(props),
    )
    .map_err(|e| format!("writer: {e}"))?;
    writer.write(&batch).map_err(|e| format!("write: {e}"))?;
    writer.close().map_err(|e| format!("close: {e}"))?;
    Ok(row_count)
}

fn fusion_dt_to_arrow(dt: fusion_unit_sdk::proto::transfer::DataType) -> DataType {
    match dt {
        fusion_unit_sdk::proto::transfer::DataType::i32 => DataType::Int64,
        fusion_unit_sdk::proto::transfer::DataType::i64 => DataType::Int64,
        fusion_unit_sdk::proto::transfer::DataType::f32 => DataType::Float64,
        fusion_unit_sdk::proto::transfer::DataType::f64 => DataType::Float64,
        fusion_unit_sdk::proto::transfer::DataType::bool => DataType::Boolean,
        _ => DataType::Utf8,
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
        row: Row,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let has_streams = !self.stream_tables.is_empty();
        let source_idx = self.source_index.get(&row.source).copied();
        let buffers = self.row_buffers.clone();
        let spill_files = self.spill_files.clone();
        let threshold = self.row_threshold;
        let data_dir = self.data_dir.clone();
        let source = row.source.clone();

        Ok(async move {
            if !has_streams {
                // Legacy path: no stream tables, execute immediately.
                self.execute_query(ctx).await?;
                return Ok(());
            }

            let idx = match source_idx {
                Some(i) => i,
                None => return Ok(()), // unknown source, ignore
            };

            // Per-source lock — no contention with other sources.
            // Lock is held briefly (just Vec::push); std::sync::Mutex
            // is fine here and avoids an extra dependency.
            let should_spill = {
                let mut guard = buffers[idx].lock().unwrap();
                guard.push(row);
                guard.len() >= threshold
            };

            if should_spill {
                let rows = {
                    let mut guard = buffers[idx].lock().unwrap();
                    std::mem::take(&mut *guard)
                };

                let source_dir = format!("{data_dir}/{source}");
                std::fs::create_dir_all(&source_dir).ok();
                let mut spills = spill_files[idx].lock().unwrap();
                let seq = spills.len();
                let file_path = format!("{source_dir}/part_{seq}.parquet");
                if let Err(e) = spill_to_parquet(&file_path, &rows) {
                    log::error!("[spill] {source} seq={seq}: {e}");
                } else {
                    spills.push(file_path);
                }
            }
            Ok(())
        })
    }

    /// When stream_tables are configured, spill remaining buffers to
    /// Parquet, register spill directories as DataFusion tables, execute
    /// SQL, emit results, and clean up temp files.
    fn on_eof<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
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
        let row_buffers = self.row_buffers.clone();
        let spill_files = self.spill_files.clone();
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

            // 2. Stream tables: spill remaining buffers (parallel per
            //    source — each buffer has its own lock, no contention)
            //    then register each source directory.
            if has_streams {
                // Drain buffers and spill remaining rows in parallel
                // on blocking threads (Parquet I/O).
                let mut spill_tasks = Vec::new();
                for (i, st) in stream_tables.iter().enumerate() {
                    let source_dir = format!("{data_dir}/{}", st.source);
                    std::fs::create_dir_all(&source_dir).ok();

                    let remaining = {
                        let mut guard = row_buffers[i].lock().unwrap();
                        std::mem::take(&mut *guard)
                    };

                    if !remaining.is_empty() {
                        let seq = {
                            let guard = spill_files[i].lock().unwrap();
                            guard.len()
                        };
                        let path = format!("{source_dir}/part_{seq}.parquet");
                        let source = st.source.clone();
                        let spills = spill_files[i].clone();
                        // Do Parquet I/O on a blocking thread.
                        spill_tasks.push(tokio::task::spawn_blocking(move || {
                            match spill_to_parquet(&path, &remaining) {
                                Ok(_) => {
                                    spills.lock().unwrap().push(path);
                                }
                                Err(e) => {
                                    log::error!(
                                        "[spill] final {source} seq={seq}: {e}"
                                    );
                                }
                            }
                        }));
                    }
                }

                // Await all spill tasks.
                for task in spill_tasks {
                    if let Err(e) = task.await {
                        log::error!("[spill] join error: {e}");
                    }
                }

                // Register each source's data directory.
                {
                    let session = df.ctx.lock().await;
                    for st in &stream_tables {
                        let source_dir = format!("{data_dir}/{}", st.source);
                        if !std::path::Path::new(&source_dir).is_dir() {
                            continue;
                        }
                        session
                            .register_parquet(
                                &st.name,
                                &source_dir,
                                datafusion::prelude::ParquetReadOptions::default(),
                            )
                            .await
                            .map_err(|e| {
                                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                                    "register parquet `{}`: {e}", st.name
                                ))
                            })?;
                    }
                }
            }

            // 3. Execute SQL and emit rows.
            let rows = engine.query(&sql).await?;
            for r in rows {
                ctx.send(r).await;
            }

            // 4. Cleanup node temp directory.
            if has_streams {
                let _ = std::fs::remove_dir_all(&data_dir);
            }
            Ok(())
        }))
    }
}
