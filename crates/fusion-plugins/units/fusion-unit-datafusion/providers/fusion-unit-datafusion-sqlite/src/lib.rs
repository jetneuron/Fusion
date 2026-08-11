//! # Fusion DataFusion Provider — SQLite
//!
//! External table provider for SQLite databases. Supports custom
//! SQL queries and reads data in batches to limit peak memory.

use datafusion::arrow::array::{
    BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use fusion_unit_datafusion::providers::{self, ProviderPlugin, TableProviderFactory};
use fusion_unit_sdk::config::ConfigEntry;
use fusion_unit_sdk::runtime::UnitResult;
use std::sync::Arc;

/// Map a SQLite declared column type to an Arrow DataType.
fn sqlite_type_to_arrow(decl_type: Option<&str>) -> DataType {
    match decl_type.map(|s| s.to_uppercase()).as_deref() {
        Some("INTEGER") | Some("INT") => DataType::Int64,
        Some("REAL") | Some("FLOAT") | Some("DOUBLE") => DataType::Float64,
        Some("TEXT") | Some("VARCHAR") | Some("CHAR") => DataType::Utf8,
        Some("BLOB") => DataType::Binary,
        Some("BOOLEAN") | Some("BOOL") => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

// ============================================================
// SqliteFactory
// ============================================================

struct SqliteFactory;

#[async_trait::async_trait]
impl TableProviderFactory for SqliteFactory {
    fn name(&self) -> &str {
        "sqlite"
    }

    async fn create(
        &self,
        entry: &ConfigEntry,
        sql: Option<&str>,
    ) -> UnitResult<Arc<dyn TableProvider>> {
        let path = entry.data.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            fusion_unit_sdk::runtime::UnitError::config_required("sqlite: path")
        })?;

        let table_name = entry.data.get("table").and_then(|v| v.as_str()).unwrap_or("main");

        let batch_size = entry
            .data
            .get("batch_size")
            .and_then(|v| v.as_u64())
            .unwrap_or(1000) as usize;

        // SQL query — from unit table config, or full scan.
        let query = sql
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("SELECT * FROM \"{}\"", table_name));

        // Infer schema: use PRAGMA for base tables, or sample the query
        // result for subqueries (where expression types differ from base).
        let has_custom_sql = sql.is_some();

        let (col_names, col_types): (Vec<String>, Vec<DataType>) = if has_custom_sql {
            let (names, types) = {
                let conn = rusqlite::Connection::open(path).map_err(|e| {
                    fusion_unit_sdk::runtime::UnitError::unknown(format!("sqlite open: {e}"))
                })?;
                let mut stmt = conn
                    .prepare(&format!("SELECT * FROM ({query}) LIMIT 1"))
                    .map_err(|e| {
                        fusion_unit_sdk::runtime::UnitError::unknown(format!("prepare: {e}"))
                    })?;
                let col_count = stmt.column_count();
                let names: Vec<String> = (0..col_count)
                    .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
                    .collect();

                let types: Vec<DataType> = stmt
                    .query_row([], |row| {
                        let mut types = Vec::with_capacity(col_count);
                        for i in 0..col_count {
                            let val = row
                                .get::<_, rusqlite::types::Value>(i)
                                .unwrap_or(rusqlite::types::Value::Null);
                            let dt = match val {
                                rusqlite::types::Value::Integer(_) => DataType::Int64,
                                rusqlite::types::Value::Real(_) => DataType::Float64,
                                rusqlite::types::Value::Text(_) => DataType::Utf8,
                                rusqlite::types::Value::Blob(_) => DataType::Binary,
                                rusqlite::types::Value::Null => DataType::Utf8,
                            };
                            types.push(dt);
                        }
                        Ok(types)
                    })
                    .unwrap_or_else(|_| vec![DataType::Utf8; col_count]);
                (names, types)
            };
            (names, types)
        } else {
            let conn = rusqlite::Connection::open(path).map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!("sqlite open: {e}"))
            })?;
            let info: Vec<(String, DataType)> = conn
                .prepare(&format!("PRAGMA table_info(\"{}\")", table_name))
                .and_then(|mut s| {
                    s.query_map([], |row| {
                        let name: String = row.get(1)?;
                        let decl: Option<String> = row.get(2)?;
                        Ok((name, sqlite_type_to_arrow(decl.as_deref())))
                    })
                    .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
                })
                .map_err(|e| {
                    fusion_unit_sdk::runtime::UnitError::unknown(format!("pragma: {e}"))
                })?;
            drop(conn);
            let names: Vec<String> = info.iter().map(|(n, _)| n.clone()).collect();
            let types: Vec<DataType> = info.into_iter().map(|(_, t)| t).collect();
            (names, types)
        };

        let mut fields = Vec::with_capacity(col_names.len());
        for (name, dt) in col_names.iter().zip(col_types.iter()) {
            fields.push(Field::new(name.clone(), dt.clone(), true));
        }
        let schema = Arc::new(Schema::new(fields));

        // ── Read data in batches ──
        let mut batches: Vec<RecordBatch> = Vec::new();

        let conn = rusqlite::Connection::open(path).map_err(|e| {
            fusion_unit_sdk::runtime::UnitError::unknown(format!("sqlite open: {e}"))
        })?;

        let total_count: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM ({})",
                    query
                ),
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        let mut offset = 0usize;
        while offset < total_count as usize {
            let paginated = format!("{query} LIMIT {batch_size} OFFSET {offset}");
            let mut stmt = conn.prepare(&paginated).map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!("sqlite prepare: {e}"))
            })?;

            let rows = stmt
                .query_map([], |row| {
                    let mut values: Vec<rusqlite::types::Value> =
                        Vec::with_capacity(col_types.len());
                    for i in 0..col_types.len() {
                        let val = row
                            .get::<_, rusqlite::types::Value>(i)
                            .unwrap_or(rusqlite::types::Value::Null);
                        values.push(val);
                    }
                    Ok(values)
                })
                .map_err(|e| {
                    fusion_unit_sdk::runtime::UnitError::unknown(format!("sqlite query_map: {e}"))
                })?;

            let mut builders: Vec<ColBuilder> = col_types
                .iter()
                .map(|dt| ColBuilder::new(dt, batch_size))
                .collect();

            let mut row_count = 0usize;
            for row in rows {
                let vals = row.map_err(|e| {
                    fusion_unit_sdk::runtime::UnitError::unknown(format!("sqlite row: {e}"))
                })?;
                for (i, val) in vals.iter().enumerate() {
                    builders[i].append(val);
                }
                row_count += 1;
            }

            if row_count > 0 {
                let arrow_cols: Vec<Arc<dyn datafusion::arrow::array::Array>> =
                    builders.into_iter().map(|mut b| b.finish()).collect();
                let batch =
                    RecordBatch::try_new(schema.clone(), arrow_cols).map_err(|e| {
                        fusion_unit_sdk::runtime::UnitError::unknown(format!("batch: {e}"))
                    })?;
                batches.push(batch);
            }

            offset += batch_size;
        }

        let mem_table = MemTable::try_new(schema, vec![batches]).map_err(|e| {
            fusion_unit_sdk::runtime::UnitError::unknown(format!("mem table: {e}"))
        })?;

        Ok(Arc::new(mem_table))
    }
}

// ============================================================
// ColBuilder
// ============================================================

enum ColBuilder {
    Int(Int64Builder),
    Float(Float64Builder),
    Str(StringBuilder),
    Bool(BooleanBuilder),
}

impl ColBuilder {
    fn new(dt: &DataType, capacity: usize) -> Self {
        match dt {
            DataType::Int64 => Self::Int(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            _ => Self::Str(StringBuilder::with_capacity(capacity, 0)),
        }
    }

    fn append(&mut self, val: &rusqlite::types::Value) {
        match (self, val) {
            (Self::Int(b), rusqlite::types::Value::Integer(n)) => b.append_value(*n),
            (Self::Int(b), rusqlite::types::Value::Real(f)) => b.append_value(*f as i64),
            (Self::Int(b), rusqlite::types::Value::Text(t)) => {
                b.append_value(t.parse().unwrap_or(0))
            }
            (Self::Int(b), _) => b.append_null(),
            (Self::Float(b), rusqlite::types::Value::Real(f)) => b.append_value(*f),
            (Self::Float(b), rusqlite::types::Value::Integer(n)) => b.append_value(*n as f64),
            (Self::Float(b), rusqlite::types::Value::Text(t)) => {
                b.append_value(t.parse().unwrap_or(0.0))
            }
            (Self::Float(b), _) => b.append_null(),
            (Self::Str(b), rusqlite::types::Value::Text(t)) => b.append_value(t.as_str()),
            (Self::Str(b), rusqlite::types::Value::Integer(n)) => b.append_value(n.to_string()),
            (Self::Str(b), rusqlite::types::Value::Real(f)) => b.append_value(f.to_string()),
            (Self::Str(b), _) => b.append_null(),
            (Self::Bool(b), rusqlite::types::Value::Integer(n)) => b.append_value(*n != 0),
            (Self::Bool(b), rusqlite::types::Value::Text(t)) => {
                b.append_value(t.eq_ignore_ascii_case("true") || t == "1")
            }
            (Self::Bool(b), _) => b.append_null(),
        }
    }

    fn finish(self) -> Arc<dyn datafusion::arrow::array::Array> {
        match self {
            Self::Int(mut b) => Arc::new(b.finish()),
            Self::Float(mut b) => Arc::new(b.finish()),
            Self::Str(mut b) => Arc::new(b.finish()),
            Self::Bool(mut b) => Arc::new(b.finish()),
        }
    }
}

// ============================================================
// ProviderPlugin
// ============================================================

pub struct SqliteProviderPlugin;

impl ProviderPlugin for SqliteProviderPlugin {
    fn register(&self) {
        providers::register_provider(Arc::new(SqliteFactory));
    }

    fn version(&self) -> &str {
        "0.1.0"
    }
}

// ============================================================
// FFI export
// ============================================================

#[unsafe(no_mangle)]
pub extern "C" fn init_provider_plugin() -> Box<dyn ProviderPlugin + Send + Sync> {
    Box::new(SqliteProviderPlugin)
}
