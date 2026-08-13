//! # Fusion Capability DataFusion
//!
//! Apache DataFusion-backed implementation of [`CapabilitySqlEngine`].
//!
//! Registered as a **factory** for config type `"datafusion"`. Each config
//! entry with `config_type: datafusion` creates its own `SessionContext`.
//!
//! ## Usage (from a unit plugin)
//!
//! ```ignore
//! let ctx = capability::sql("my-datafusion").unwrap();
//! let rows = ctx.query("SELECT 1").await?;
//! ```

use datafusion::prelude::*;
use fusion_unit_sdk::capability::capability_sql_engine::well_known;
use fusion_unit_sdk::capability::{self, Capability, CapabilityPlugin, CapabilitySqlEngine};
use fusion_unit_sdk::proto::transfer::{Column, DataType, Frame};
use fusion_unit_sdk::runtime::UnitResult;
use protobuf::EnumOrUnknown;
use log;
use std::sync::Arc;
use tokio::sync::Mutex;

// ============================================================
// DataFusionCapability
// ============================================================

/// DataFusion-backed SQL engine capability.
///
/// Wraps a [`SessionContext`] from Apache DataFusion. Multiple
/// instances can coexist with different configurations (e.g.
/// different table registrations).
pub struct DataFusionCapability {
    /// The DataFusion session — `Mutex` because `SessionContext`
    /// methods take `&self` but may not be thread-safe internally.
    pub ctx: Mutex<SessionContext>,
}

impl DataFusionCapability {
    pub fn new() -> Self {
        Self {
            ctx: Mutex::new(SessionContext::new()),
        }
    }
}

// ============================================================
// Capability trait
// ============================================================

#[async_trait::async_trait]
impl Capability for DataFusionCapability {
    fn name(&self) -> &str {
        well_known::DATAFUSION
    }

    async fn init(&self) -> UnitResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> UnitResult<()> {
        Ok(())
    }
}

// ============================================================
// CapabilitySqlEngine
// ============================================================

#[async_trait::async_trait]
impl CapabilitySqlEngine for DataFusionCapability {
    async fn query(&self, sql: &str) -> UnitResult<Vec<Frame>> {
        let ctx = self.ctx.lock().await;
        let df = match ctx.sql(sql).await {
            Ok(df) => df,
            Err(e) => {
                log::error!("[DataFusion] SQL planning error for `{sql}`: {e}");
                return Err(fusion_unit_sdk::runtime::UnitError::unknown(e.to_string()));
            }
        };
        let batches = match df.collect().await {
            Ok(b) => b,
            Err(e) => {
                log::error!("[DataFusion] SQL execution error for `{sql}`: {e}");
                return Err(fusion_unit_sdk::runtime::UnitError::unknown(e.to_string()));
            }
        };

        let mut rows = Vec::new();
        for batch in &batches {
            let schema = batch.schema();
            for row_idx in 0..batch.num_rows() {
                let mut frame = Frame::new();
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let column = batch.column(col_idx);
                    let mut c = Column::new();
                    c.field = field.name().clone();
                    match field.data_type() {
                        datafusion::arrow::datatypes::DataType::Int32 => {
                            let arr = column
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                                .unwrap();
                            c.i32_val = arr.value(row_idx);
                            c.dt = EnumOrUnknown::from(DataType::i32);
                        }
                        datafusion::arrow::datatypes::DataType::Int64 => {
                            let arr = column
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                                .unwrap();
                            c.i64_val = arr.value(row_idx);
                            c.dt = EnumOrUnknown::from(DataType::i64);
                        }
                        datafusion::arrow::datatypes::DataType::Float32 => {
                            let arr = column
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Float32Array>()
                                .unwrap();
                            c.f32_val = arr.value(row_idx);
                            c.dt = EnumOrUnknown::from(DataType::f32);
                        }
                        datafusion::arrow::datatypes::DataType::Float64 => {
                            let arr = column
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Float64Array>()
                                .unwrap();
                            c.f64_val = arr.value(row_idx);
                            c.dt = EnumOrUnknown::from(DataType::f64);
                        }
                        datafusion::arrow::datatypes::DataType::Utf8 => {
                            let arr = column
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::StringArray>()
                                .unwrap();
                            c.str_val = arr.value(row_idx).to_string();
                            c.dt = EnumOrUnknown::from(DataType::str);
                        }
                        datafusion::arrow::datatypes::DataType::Boolean => {
                            let arr = column
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                                .unwrap();
                            c.bool_val = arr.value(row_idx);
                            c.dt = EnumOrUnknown::from(DataType::bool);
                        }
                        _ => {
                            c.str_val = format!("{:?}", column);
                            c.dt = EnumOrUnknown::from(DataType::str);
                        }
                    }
                    frame.columns.push(c);
                }
                rows.push(frame);
            }
        }
        Ok(rows)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ============================================================
// CapabilityPlugin
// ============================================================

pub struct DataFusionCapabilityPlugin;

impl CapabilityPlugin for DataFusionCapabilityPlugin {
    fn register(&self) {
        capability::register(|reg| {
            reg.set_sql_factory("datafusion", |_entry| {
                Ok(Arc::new(DataFusionCapability::new()))
            });
        });
    }

    fn version(&self) -> &str {
        "0.1.0"
    }
}

// ============================================================
// FFI export
// ============================================================

#[unsafe(no_mangle)]
pub extern "C" fn init_capability_plugin() -> Box<dyn CapabilityPlugin + Send + Sync> {
    Box::new(DataFusionCapabilityPlugin)
}
