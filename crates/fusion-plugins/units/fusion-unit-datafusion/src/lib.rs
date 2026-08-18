//! # Fusion Unit SQL
//!
//! Generic SQL execution unit backed by a `CapabilitySqlEngine`.
//!
//! The DataFusion engine itself lives **only** in the capability dylib
//! (`fusion-capability-datafusion`); this unit is engine-free. It talks
//! to the engine through the SDK [`CapabilitySqlEngine`] trait, which is
//! either the capability dylib's C-ABI factory (dylib deployment, injected
//! via [`inject_sql_engine_factory`]) or the process-global capability
//! registry (static in-process tests).
//!
//! ```text
//! YAML config
//!   ├── datasource: datafusion     → engine (factory | capability::sql)
//!   ├── sql: "SELECT ..."
//!   └── tables:                    → static tables
//!       ├── name: my_csv
//!       ├── provider: csv          → engine.register_csv_table(path)
//!       └── config_id: csv-data
//!   └── stream_tables:             → upstream rows buffered here, pushed
//!       └── source: upstream_id       to the engine as frames at EOF
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

pub mod ffi_engine;

use std::collections::HashMap;
use std::ffi::CStr;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;

use fusion_derive::LogicalTask;
use fusion_unit_sdk::capability::CapabilitySqlEngine;
use fusion_unit_sdk::config;
use fusion_unit_sdk::ffi::config_ffi::HostConfigApi;
use fusion_unit_sdk::ffi::sql_engine_ffi::{HostProviders, SqlEngineFactory};
use fusion_unit_sdk::graph::types::{
    ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext, UnitMeta,
};
use fusion_unit_sdk::proto::transfer::Frame;
use fusion_unit_sdk::providers::TableDataProvider;
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use fusion_unit_sdk::units::config_util::UnitConfigExt;
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};

use ffi_engine::FfiSqlEngine;

// ============================================================
// Injection — host → dylib statics (dylib deployment)
//
// Rust statics are per-binary-image: the engine factory, provider
// objects and config registry populated in the host (or another dylib)
// are invisible to this unit dylib. The host injects them through the
// `set_*` C symbols below. Config arrives via the live `set_host_config`
// query API (entries registered after load stay visible); `set_config`
// is the legacy snapshot fallback for hosts that only know that symbol.
// ============================================================

static ENGINE_FACTORY: OnceLock<SqlEngineFactory> = OnceLock::new();
static HOST_PROVIDERS: OnceLock<HashMap<String, Arc<dyn TableDataProvider>>> =
    OnceLock::new();

/// Install the capability dylib's engine factory.
pub fn inject_sql_engine_factory(factory: SqlEngineFactory) {
    let _ = ENGINE_FACTORY.set(factory);
}

/// Install provider objects collected from provider dylibs by the host.
///
/// Keys are `"{provider}#{config_id}"` — a provider dylib emits one
/// entry per datasource config it owns.
pub fn inject_providers(providers: Vec<(String, Arc<dyn TableDataProvider>)>) {
    let map: HashMap<String, Arc<dyn TableDataProvider>> = providers.into_iter().collect();
    let _ = HOST_PROVIDERS.set(map);
}

#[cfg(feature = "cdylib")]
#[unsafe(no_mangle)]
pub extern "C" fn set_sql_engine_factory(factory: SqlEngineFactory) {
    inject_sql_engine_factory(factory);
}

#[cfg(feature = "cdylib")]
#[unsafe(no_mangle)]
pub extern "C" fn set_host_providers(providers: HostProviders) {
    // Safety: the host transferred ownership of the provider objects.
    let list = unsafe { providers.take() };
    inject_providers(list);
}

/// Install the host's live config query API — every `config::read()` in
/// this image then refreshes from the host registry, so entries
/// registered after dylib load stay visible.
#[cfg(feature = "cdylib")]
#[unsafe(no_mangle)]
pub extern "C" fn set_host_config(api: HostConfigApi) {
    fusion_unit_sdk::ffi::config_ffi::set_host_api(api);
}

/// Legacy snapshot fallback: the host serialized its registry at load
/// time. Only used by hosts that don't call [`set_host_config`].
#[cfg(feature = "cdylib")]
#[unsafe(no_mangle)]
pub extern "C" fn set_config(json: *const std::ffi::c_char) {
    if json.is_null() {
        return;
    }
    let s = unsafe { CStr::from_ptr(json) }.to_string_lossy().into_owned();
    match serde_json::from_str::<Vec<config::InjectedConfig>>(&s) {
        Ok(entries) => config::inject_entries(entries),
        Err(e) => log::warn!("[fusion-unit-datafusion] set_config: invalid JSON: {e}"),
    }
}

// ============================================================
// Plugin
// ============================================================

#[cfg(feature = "cdylib")]
#[unsafe(no_mangle)]
pub extern "C" fn init_plugin() -> Box<dyn GraphUnitPlugin + Send + Sync> {
    Box::new(SqlUnitPlugin {})
}

pub struct SqlUnitPlugin {}

impl GraphUnitPlugin for SqlUnitPlugin {
    fn register_units(&self) -> UnitManifest {
        let mut m = UnitManifest::default();
        SqlUnitTask::register_unit(&mut m, &self.plugin_version());
        m
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

/// An upstream stream table — rows from another node buffered here and
/// handed to the engine as frames at EOF.
#[derive(Debug, Clone)]
struct StreamTableConfig {
    name: String,   // table name in SQL
    source: String, // upstream node ID (frame.source)
}

/// Per-source row buffer shared with parallel compute workers.
struct StreamTableState {
    name: String,
    buf: tokio::sync::Mutex<Vec<Frame>>,
}

// ============================================================
// SqlUnitTask
// ============================================================

#[derive(Default, LogicalTask)]
pub struct SqlUnitTask {
    meta: UnitMeta,
    datasource: String,
    sql: String,
    tables: Vec<TableConfig>,
    /// Buffered upstream rows — drained and registered with the engine
    /// at EOF. Indexed via `source_index` (source node ID → index).
    stream_states: Vec<Arc<StreamTableState>>,
    /// Maps source node ID → index into stream_states.
    source_index: HashMap<String, usize>,
    engine: Option<Arc<dyn CapabilitySqlEngine>>,
}

impl InitUnit for SqlUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let conf = unit
            .get_config()
            .ok_or_else(|| UnitError::config_required("config"))?;

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
                        .ok_or_else(|| UnitError::config_required("tables[*].config_id"))?
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
        // and registered as engine tables at EOF).
        if let Some(arr) = conf.get("stream_tables").and_then(|v| v.as_array()) {
            for item in arr {
                let name = item["name"].as_str().unwrap_or("stream").to_string();
                let source = item["source"]
                    .as_str()
                    .ok_or_else(|| UnitError::config_required("stream_tables[*].source"))?
                    .to_string();
                self.source_index.insert(source, self.stream_states.len());
                self.stream_states.push(Arc::new(StreamTableState {
                    name,
                    buf: tokio::sync::Mutex::new(Vec::new()),
                }));
            }
        }

        // Map/sink roles receive upstream frames — they must declare
        // stream_tables so incoming data is buffered and queried at EOF.
        // Without them, compute() would re-run the SQL per frame.
        if (unit.is_mapper() || unit.is_sink()) && self.stream_states.is_empty() {
            return Err(UnitError::config_required(
                "stream_tables (map/sink role requires stream tables)",
            ));
        }

        // Resolve SQL engine capability: injected factory first (dylib
        // deployment), registry fallback (static in-process tests).
        self.engine = Some(if let Some(factory) = ENGINE_FACTORY.get() {
            Arc::new(FfiSqlEngine::new(*factory, None)?) as Arc<dyn CapabilitySqlEngine>
        } else {
            fusion_unit_sdk::capability::sql(&self.datasource).ok_or_else(|| {
                UnitError::unknown(format!(
                    "SQL engine not found for datasource `{}`",
                    self.datasource
                ))
            })?
        });

        Ok(())
    }
}

impl SqlUnitTask {
    /// Register static tables with the engine.
    ///
    /// - `csv` providers: resolve the file path from the config registry
    ///   and let the engine read the file directly.
    /// - Other providers: load frames through the injected
    ///   `TableDataProvider` and push them into the engine as a frame
    ///   table (data over FFI — engine types never cross the boundary).
    async fn register_tables(
        engine: &dyn CapabilitySqlEngine,
        tables: &[TableConfig],
    ) -> UnitResult<()> {
        for table in tables {
            match table.provider.as_str() {
                "csv" => {
                    let guard = config::read();
                    let entry = guard.entry(&table.config_id).cloned().ok_or_else(|| {
                        UnitError::unknown(format!(
                            "config entry `{}` not found",
                            table.config_id
                        ))
                    })?;
                    drop(guard);
                    let path = entry.data.get("path").and_then(|v| v.as_str()).ok_or_else(
                        || UnitError::config_required("csv provider: path"),
                    )?;
                    engine.register_csv_table(&table.name, path).await?;
                }
                _ => {
                    let key = format!("{}#{}", table.provider, table.config_id);
                    let map = HOST_PROVIDERS.get();
                    let provider = map.and_then(|m| m.get(&key)).ok_or_else(|| {
                        match map {
                            // set_host_providers was never called (or the
                            // host injected an empty list): the provider
                            // dylib chain never ran.
                            None => UnitError::unknown(format!(
                                "table provider `{key}` not found: no providers were \
                                 injected into this unit dylib (was a provider dylib \
                                 loaded before this unit? does the config registry \
                                 have datasource entries of type `{}`?)",
                                table.provider
                            )),
                            Some(m) => UnitError::unknown(format!(
                                "table provider `{key}` not found — available: [{}] \
                                 (add a `datasource: {}` config entry with that id?)",
                                m.keys().cloned().collect::<Vec<_>>().join(", "),
                                table.provider
                            )),
                        }
                    })?;
                    let frames = provider.load_frames(table.sql.as_deref()).await?;
                    engine.register_frame_table(&table.name, frames).await?;
                    engine.finalize_frame_table(&table.name).await?;
                }
            }
        }
        Ok(())
    }

    async fn execute_query(&self, ctx: &TaskContext) -> UnitResult<()> {
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| UnitError::unknown("engine not initialized"))?;

        // Register static tables from config.
        Self::register_tables(engine.as_ref(), &self.tables).await?;

        // Stream tables: drain each buffer and push the frames to the
        // engine, then freeze them so they become queryable.
        for st in self.stream_states.iter() {
            let frames = std::mem::take(&mut *st.buf.lock().await);
            engine.register_frame_table(&st.name, frames).await?;
            engine.finalize_frame_table(&st.name).await?;
        }

        // Execute SQL and emit rows.
        let frames = engine.query(&self.sql).await?;
        for frame in frames {
            ctx.send(frame).await;
        }

        // Deregister this node's tables so they don't leak into other
        // graphs sharing the process-global engine session.
        for table in &self.tables {
            let _ = engine.deregister_table(&table.name).await;
        }
        for st in &self.stream_states {
            let _ = engine.deregister_table(&st.name).await;
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
        _ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let source_idx = self.source_index.get(&frame.source).copied();
        let states = self.stream_states.clone();

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

            // Append to the per-source buffer. Thread-safe — parallel
            // workers share one buffer per source.
            states[idx].buf.lock().await.push(frame);
            Ok(())
        })
    }

    /// Drain the per-source buffers, register them as engine tables,
    /// execute SQL, emit results, and clean up.
    fn on_eof<'life0, 'async_trait>(
        &'life0 self,
        frame: Frame,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let _ = frame;
        let has_streams = !self.stream_states.is_empty();
        let has_static = !self.tables.is_empty();

        Ok(Box::pin(async move {
            if !has_streams && !has_static {
                return Ok(());
            }
            self.execute_query(ctx).await
        }))
    }
}
