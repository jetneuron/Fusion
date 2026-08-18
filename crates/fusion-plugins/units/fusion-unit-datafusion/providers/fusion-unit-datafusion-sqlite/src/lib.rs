//! # Fusion DataFusion Provider — SQLite
//!
//! External table provider for SQLite databases. Reads a configured
//! SQLite file and turns its rows into SDK [`Frame`]s — data over FFI:
//! no DataFusion types live in this dylib, so it stays small and the
//! engine boundary stays clean.
//!
//! The host injects the datasource config registry via the live
//! `set_host_config` query API (a C symbol, same protocol as unit
//! dylibs; `set_config` is the legacy snapshot fallback), then calls
//! `init_provider_plugin` and collects [`register_providers`](ProviderPlugin::register_providers)
//! — one provider per `sqlite` datasource config entry, keyed
//! `"sqlite#{config_id}"`.

use std::ffi::{CStr, c_char};
use std::sync::Arc;

use fusion_unit_sdk::config;
use fusion_unit_sdk::ffi::config_ffi::HostConfigApi;
use fusion_unit_sdk::proto::transfer::{Column, DataType, Frame};
use fusion_unit_sdk::providers::{ProviderPlugin, TableDataProvider};
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use protobuf::EnumOrUnknown;
use rusqlite::types::Value;

// ============================================================
// SqliteTableDataProvider
// ============================================================

pub struct SqliteTableDataProvider {
    path: String,
    table_name: String,
}

#[async_trait::async_trait]
impl TableDataProvider for SqliteTableDataProvider {
    async fn load_frames(&self, sql: Option<&str>) -> UnitResult<Vec<Frame>> {
        if self.path.is_empty() {
            return Err(UnitError::config_required("sqlite: path"));
        }

        // SQL query — from unit table config, or full scan.
        let query = sql
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("SELECT * FROM \"{}\"", self.table_name));

        let conn = rusqlite::Connection::open(&self.path).map_err(|e| {
            UnitError::unknown(format!("sqlite open `{}`: {e}", self.path))
        })?;

        // Column names from a zero-row probe — the engine infers the
        // schema from the first frame's columns.
        let probe = conn
            .prepare(&format!("SELECT * FROM ({query}) LIMIT 1"))
            .map_err(|e| UnitError::unknown(format!("sqlite prepare: {e}")))?;
        let col_count = probe.column_count();
        let col_names: Vec<String> = (0..col_count)
            .map(|i| probe.column_name(i).unwrap_or("?").to_string())
            .collect();
        drop(probe);

        // Read all rows as frames (one frame per row).
        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| UnitError::unknown(format!("sqlite prepare: {e}")))?;
        let rows = stmt
            .query_map([], |frame| {
                let mut values = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    values.push(
                        frame
                            .get::<_, Value>(i)
                            .unwrap_or(Value::Null),
                    );
                }
                Ok(values)
            })
            .map_err(|e| UnitError::unknown(format!("sqlite query_map: {e}")))?;

        let mut frames = Vec::new();
        for row in rows {
            let values = row
                .map_err(|e| UnitError::unknown(format!("sqlite row: {e}")))?;
            let mut frame = Frame::new();
            for (i, val) in values.iter().enumerate() {
                let mut c = Column::new();
                c.field = col_names[i].clone();
                match val {
                    Value::Integer(n) => {
                        c.dt = EnumOrUnknown::from(DataType::i64);
                        c.i64_val = *n;
                    }
                    Value::Real(f) => {
                        c.dt = EnumOrUnknown::from(DataType::f64);
                        c.f64_val = *f;
                    }
                    Value::Text(t) => {
                        c.dt = EnumOrUnknown::from(DataType::str);
                        c.str_val = t.clone();
                    }
                    Value::Blob(b) => {
                        // No bytes column type in this pipeline — encode
                        // lossily as text.
                        c.dt = EnumOrUnknown::from(DataType::str);
                        c.str_val = String::from_utf8_lossy(b).into_owned();
                    }
                    Value::Null => {
                        c.dt = EnumOrUnknown::from(DataType::str);
                        c.str_val = String::new();
                    }
                }
                frame.columns.push(c);
            }
            frames.push(frame);
        }
        Ok(frames)
    }
}

// ============================================================
// ProviderPlugin
// ============================================================

pub struct SqliteProviderPlugin;

impl ProviderPlugin for SqliteProviderPlugin {
    /// One provider per `sqlite` datasource config entry, keyed
    /// `"sqlite#{config_id}"` — matches the unit's
    /// `tables[*].provider` + `config_id` lookup.
    fn register_providers(&self) -> Vec<(String, Arc<dyn TableDataProvider>)> {
        let reg = config::read();
        let mut out: Vec<(String, Arc<dyn TableDataProvider>)> = Vec::new();
        for id in reg.ids_by("datasource", "sqlite") {
            let Some(entry) = reg.entry(id).cloned() else {
                continue;
            };
            let path = entry
                .data
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let table_name = entry
                .data
                .get("table")
                .and_then(|v| v.as_str())
                .unwrap_or("main")
                .to_string();
            out.push((
                format!("sqlite#{id}"),
                Arc::new(SqliteTableDataProvider { path, table_name }),
            ));
        }
        out
    }

    fn version(&self) -> &str {
        "0.1.0"
    }
}

// ============================================================
// FFI exports
// ============================================================

/// Install the host's live config query API — every `config::read()` in
/// this image then refreshes from the host registry, so entries
/// registered after dylib load stay visible.
#[unsafe(no_mangle)]
pub extern "C" fn set_host_config(api: HostConfigApi) {
    fusion_unit_sdk::ffi::config_ffi::set_host_api(api);
}

/// Legacy snapshot fallback: the host serialized its registry at load
/// time. Only used by hosts that don't call [`set_host_config`].
#[unsafe(no_mangle)]
pub extern "C" fn set_config(json: *const c_char) {
    if json.is_null() {
        return;
    }
    let s = unsafe { CStr::from_ptr(json) }.to_string_lossy().into_owned();
    match serde_json::from_str::<Vec<config::InjectedConfig>>(&s) {
        Ok(entries) => config::inject_entries(entries),
        Err(e) => log::warn!("[fusion-unit-datafusion-sqlite] set_config: invalid JSON: {e}"),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn init_provider_plugin() -> Box<dyn ProviderPlugin + Send + Sync> {
    Box::new(SqliteProviderPlugin)
}
