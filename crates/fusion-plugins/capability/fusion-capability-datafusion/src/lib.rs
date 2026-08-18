//! # Fusion Capability DataFusion
//!
//! Apache DataFusion-backed implementation of [`CapabilitySqlEngine`].
//!
//! Registered as a **factory** for config type `"datafusion"`. Each config
//! entry with `config_type: datafusion` creates its own `SessionContext`.
//!
//! ## Data over FFI, types stay here
//!
//! This dylib is the **only** binary image that carries DataFusion. Tables
//! arrive as [`Frame`] streams ([`CapabilitySqlEngine::register_frame_table`])
//! and are materialized here; unit and provider dylibs never link the engine.
//! The engine is also exported through a plain C-ABI [`SqlEngineFactory`]
//! table ([`sql_engine_factory`]) so the host can inject it into unit dylibs.
//!
//! ## Usage (from a unit plugin)
//!
//! ```ignore
//! let ctx = capability::sql("my-datafusion").unwrap();
//! let rows = ctx.query("SELECT 1").await?;
//! ```

use datafusion::arrow::array::{
    BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, RecordBatch,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use fusion_unit_sdk::capability::capability_sql_engine::well_known;
use fusion_unit_sdk::capability::{self, Capability, CapabilityPlugin, CapabilitySqlEngine};
use fusion_unit_sdk::ffi::config_ffi::HostConfigApi;
use fusion_unit_sdk::ffi::sql_engine_ffi::{SqlEngineFactory, leak_frames, take_frames};
use fusion_unit_sdk::proto::transfer::{Column, DataType, Frame};
use fusion_unit_sdk::runtime::UnitResult;
use log;
use protobuf::EnumOrUnknown;
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_void};
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

// ============================================================
// DataFusionCapability
// ============================================================

/// Accumulator for a stream table registered via `register_frame_table`.
/// Frames are buffered until `finalize_frame_table` converts them into a
/// `MemTable` and registers it with the session.
#[derive(Default)]
struct StreamTableAccum {
    frames: Vec<Frame>,
    finalized: bool,
}

/// DataFusion-backed SQL engine capability.
///
/// Wraps a [`SessionContext`] from Apache DataFusion. Multiple
/// instances can coexist with different configurations (e.g.
/// different table registrations).
pub struct DataFusionCapability {
    /// The DataFusion session — `Mutex` because `SessionContext`
    /// methods take `&self` but may not be thread-safe internally.
    /// `Arc` so query can hand the lock across the dylib's own runtime.
    pub ctx: Arc<Mutex<SessionContext>>,
    /// Pending stream tables (registered but not yet finalized).
    stream_tables: Mutex<HashMap<String, StreamTableAccum>>,
}

impl DataFusionCapability {
    pub fn new() -> Self {
        Self {
            ctx: Arc::new(Mutex::new(SessionContext::new())),
            stream_tables: Mutex::new(HashMap::new()),
        }
    }
}

/// DataFusion execution plans spawn onto tokio internally (e.g.
/// `RepartitionExec`). A dylib links its own tokio, whose thread-local
/// runtime context is empty on the host runtime's threads — TLS is per
/// binary image — so `collect()` must run on this crate's own runtime
/// to guarantee a reactor exists.
static DF_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn df_runtime() -> &'static Runtime {
    DF_RUNTIME.get_or_init(|| {
        Runtime::new().expect("failed to create DataFusion tokio runtime")
    })
}

// ============================================================
// Frame ↔ RecordBatch conversion
// ============================================================

/// Convert query-result batches to frames (used by `query`).
fn batch_to_frames(batches: &[RecordBatch]) -> Vec<Frame> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut frame = Frame::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_idx);
                let mut c = Column::new();
                c.field = field.name().clone();
                match field.data_type() {
                    ArrowDataType::Int32 => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::Int32Array>()
                            .unwrap();
                        c.i32_val = arr.value(row_idx);
                        c.dt = EnumOrUnknown::from(DataType::i32);
                    }
                    ArrowDataType::Int64 => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::Int64Array>()
                            .unwrap();
                        c.i64_val = arr.value(row_idx);
                        c.dt = EnumOrUnknown::from(DataType::i64);
                    }
                    ArrowDataType::Float32 => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::Float32Array>()
                            .unwrap();
                        c.f32_val = arr.value(row_idx);
                        c.dt = EnumOrUnknown::from(DataType::f32);
                    }
                    ArrowDataType::Float64 => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::Float64Array>()
                            .unwrap();
                        c.f64_val = arr.value(row_idx);
                        c.dt = EnumOrUnknown::from(DataType::f64);
                    }
                    ArrowDataType::Utf8 => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                            .unwrap();
                        c.str_val = arr.value(row_idx).to_string();
                        c.dt = EnumOrUnknown::from(DataType::str);
                    }
                    ArrowDataType::Boolean => {
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
    rows
}

/// Convert a frame stream to a single RecordBatch. The schema is inferred
/// from the first frame's columns; every row must agree with it.
fn frames_to_batch(frames: &[Frame]) -> UnitResult<RecordBatch> {
    let first = frames
        .first()
        .ok_or_else(|| fusion_unit_sdk::runtime::UnitError::unknown("empty frame stream"))?;

    let mut fields = Vec::with_capacity(first.columns.len());
    let mut builders: Vec<ArrowBuilder> = Vec::with_capacity(first.columns.len());
    for col in &first.columns {
        let arrow_type = match col.dt.enum_value().unwrap_or(DataType::str) {
            DataType::i32 => ArrowDataType::Int32,
            DataType::i64 => ArrowDataType::Int64,
            DataType::f32 => ArrowDataType::Float32,
            DataType::f64 => ArrowDataType::Float64,
            DataType::bool => ArrowDataType::Boolean,
            _ => ArrowDataType::Utf8,
        };
        fields.push(Field::new(col.field.clone(), arrow_type.clone(), true));
        builders.push(match arrow_type {
            ArrowDataType::Int32 => ArrowBuilder::I32(Int32Builder::new()),
            ArrowDataType::Int64 => ArrowBuilder::I64(Int64Builder::new()),
            ArrowDataType::Float32 => ArrowBuilder::F32(Float32Builder::new()),
            ArrowDataType::Float64 => ArrowBuilder::F64(Float64Builder::new()),
            ArrowDataType::Boolean => ArrowBuilder::Bool(BooleanBuilder::new()),
            _ => ArrowBuilder::Str(StringBuilder::new()),
        });
    }

    for frame in frames {
        for (i, col) in frame.columns.iter().enumerate() {
            if let Some(b) = builders.get_mut(i) {
                match (b, col.dt.enum_value().unwrap_or(DataType::str)) {
                    (ArrowBuilder::I32(b), DataType::i32) => b.append_value(col.i32_val),
                    (ArrowBuilder::I64(b), DataType::i64) => b.append_value(col.i64_val),
                    (ArrowBuilder::F32(b), DataType::f32) => b.append_value(col.f32_val),
                    (ArrowBuilder::F64(b), DataType::f64) => b.append_value(col.f64_val),
                    (ArrowBuilder::Bool(b), DataType::bool) => b.append_value(col.bool_val),
                    (ArrowBuilder::Str(b), _) => b.append_value(col.str_val.clone()),
                    // Type mismatch between frames — treat as null.
                    (b, _) => b.append_null(),
                }
            }
        }
    }

    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let columns = builders
        .into_iter()
        .map(|b| b.finish())
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, columns)
        .map_err(|e| fusion_unit_sdk::runtime::UnitError::unknown(format!("record batch: {e}")))
}

enum ArrowBuilder {
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Bool(BooleanBuilder),
    Str(StringBuilder),
}

impl ArrowBuilder {
    fn finish(self) -> datafusion::arrow::array::ArrayRef {
        match self {
            Self::I32(mut b) => Arc::new(b.finish()),
            Self::I64(mut b) => Arc::new(b.finish()),
            Self::F32(mut b) => Arc::new(b.finish()),
            Self::F64(mut b) => Arc::new(b.finish()),
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::Str(mut b) => Arc::new(b.finish()),
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::I32(b) => b.append_null(),
            Self::I64(b) => b.append_null(),
            Self::F32(b) => b.append_null(),
            Self::F64(b) => b.append_null(),
            Self::Bool(b) => b.append_null(),
            Self::Str(b) => b.append_null(),
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
        let sql = sql.to_string();
        let ctx = self.ctx.clone();
        // `collect()` spawns DataFusion's internal tasks (repartition,
        // hash join) onto tokio. Run it on this dylib's own runtime —
        // on the host runtime's threads this crate's tokio has no
        // reactor ("there is no reactor running").
        df_runtime()
            .spawn(async move {
                let ctx = ctx.lock().await;
                let df = match ctx.sql(&sql).await {
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
                let frames = batch_to_frames(&batches);
                Ok(frames)
            })
            .await
            .map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "datafusion query task failed: {e}"
                ))
            })?
    }

    async fn register_frame_table(
        &self,
        name: &str,
        frames: Vec<Frame>,
    ) -> UnitResult<()> {
        let mut st = self.stream_tables.lock().await;
        let accum = st.entry(name.to_string()).or_default();
        if accum.finalized {
            // Already finalized by an earlier EOF — overwrite instead of
            // erroring, mirroring DataFusion's `register_table` semantics.
            // Parallel graphs sharing the process-global engine can reuse
            // table names (e.g. concurrent tests both using `stream_a`).
            *accum = StreamTableAccum {
                frames,
                finalized: false,
            };
            return Ok(());
        }
        accum.frames.extend(frames);
        Ok(())
    }

    async fn finalize_frame_table(&self, name: &str) -> UnitResult<()> {
        // Take the frames under the stream_tables lock, then release it
        // before touching ctx (lock ordering: never hold both).
        let frames = {
            let mut st = self.stream_tables.lock().await;
            let accum = st
                .get_mut(name)
                .ok_or_else(|| {
                    fusion_unit_sdk::runtime::UnitError::unknown(format!(
                        "stream table `{name}` not found"
                    ))
                })?;
            if accum.finalized {
                return Ok(());
            }
            accum.finalized = true;
            std::mem::take(&mut accum.frames)
        };
        let batch = frames_to_batch(&frames)?;
        let mem = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "mem table `{name}`: {e}"
                ))
            })?;
        let ctx = self.ctx.lock().await;
        // Tolerate re-registration: parallel graphs sharing the
        // process-global session may reuse table names. Under the ctx
        // lock this is deterministic (last writer wins).
        let _ = ctx.deregister_table(name);
        ctx.register_table(name, Arc::new(mem))
            .map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "register stream table `{name}`: {e}"
                ))
            })?;
        Ok(())
    }

    async fn register_csv_table(&self, name: &str, path: &str) -> UnitResult<()> {
        let ctx = self.ctx.lock().await;
        ctx.register_csv(name, path, CsvReadOptions::new())
            .await
            .map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "register csv table `{name}` from `{path}`: {e}"
                ))
            })
    }

    async fn deregister_table(&self, name: &str) -> UnitResult<()> {
        self.stream_tables.lock().await.remove(name);
        let ctx = self.ctx.lock().await;
        ctx.deregister_table(name)
            .map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "deregister table `{name}`: {e}"
                ))
            })?;
        Ok(())
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
// C-ABI engine factory (host → unit injection)
// ============================================================

/// Build the engine factory table. Statically-linked consumers (tests)
/// can call this directly; dylib consumers get it via `init_sql_engine_factory`.
pub fn sql_engine_factory() -> SqlEngineFactory {
    SqlEngineFactory {
        create_engine: create_engine_ffi,
        query: query_ffi,
        register_frame_table: register_frame_table_ffi,
        finalize_frame_table: finalize_frame_table_ffi,
        register_csv_table: register_csv_table_ffi,
        deregister_table: deregister_table_ffi,
        drop_engine: drop_engine_ffi,
    }
}

#[cfg(feature = "cdylib")]
#[unsafe(no_mangle)]
pub extern "C" fn init_sql_engine_factory() -> SqlEngineFactory {
    sql_engine_factory()
}

/// Install the host's live config query API — config-driven capability
/// factories read config through this image's own registry.
#[cfg(feature = "cdylib")]
#[unsafe(no_mangle)]
pub extern "C" fn set_host_config(api: HostConfigApi) {
    fusion_unit_sdk::ffi::config_ffi::set_host_api(api);
}

unsafe fn cap_from_handle<'a>(handle: *mut c_void) -> &'a DataFusionCapability {
    unsafe { &*(handle as *const DataFusionCapability) }
}

fn cstr(ptr: *const c_char) -> String {
    if ptr.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned()
    }
}

unsafe extern "C" fn create_engine_ffi(_config: *const c_char) -> *mut c_void {
    Box::into_raw(Box::new(DataFusionCapability::new())) as *mut c_void
}

unsafe extern "C" fn drop_engine_ffi(handle: *mut c_void) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle as *mut DataFusionCapability) });
    }
}

unsafe extern "C" fn query_ffi(
    handle: *mut c_void,
    sql: *const c_char,
    frames_out: *mut *mut Vec<Frame>,
) -> i32 {
    let cap = unsafe { cap_from_handle(handle) };
    let sql = cstr(sql);
    // Synchronous bridge: run the async engine on this dylib's own
    // runtime. Called from host/unit spawn_blocking threads, never from
    // a tokio worker that this runtime drives.
    let res = df_runtime().block_on(async { cap.query(&sql).await });
    match res {
        Ok(rows) => {
            unsafe { *frames_out = leak_frames(rows) };
            0
        }
        Err(e) => {
            log::error!("[DataFusion] query_ffi failed: {e}");
            1
        }
    }
}

unsafe extern "C" fn register_frame_table_ffi(
    handle: *mut c_void,
    name: *const c_char,
    frames: *mut Vec<Frame>,
) -> i32 {
    let cap = unsafe { cap_from_handle(handle) };
    let name = cstr(name);
    let frames = unsafe { take_frames(frames) };
    let res = df_runtime().block_on(async { cap.register_frame_table(&name, frames).await });
    match res {
        Ok(()) => 0,
        Err(e) => {
            log::error!("[DataFusion] register_frame_table_ffi failed: {e}");
            1
        }
    }
}

unsafe extern "C" fn finalize_frame_table_ffi(handle: *mut c_void, name: *const c_char) -> i32 {
    let cap = unsafe { cap_from_handle(handle) };
    let name = cstr(name);
    let res = df_runtime().block_on(async { cap.finalize_frame_table(&name).await });
    match res {
        Ok(()) => 0,
        Err(e) => {
            log::error!("[DataFusion] finalize_frame_table_ffi failed: {e}");
            1
        }
    }
}

unsafe extern "C" fn register_csv_table_ffi(
    handle: *mut c_void,
    name: *const c_char,
    path: *const c_char,
) -> i32 {
    let cap = unsafe { cap_from_handle(handle) };
    let name = cstr(name);
    let path = cstr(path);
    let res = df_runtime().block_on(async { cap.register_csv_table(&name, &path).await });
    match res {
        Ok(()) => 0,
        Err(e) => {
            log::error!("[DataFusion] register_csv_table_ffi failed: {e}");
            1
        }
    }
}

unsafe extern "C" fn deregister_table_ffi(handle: *mut c_void, name: *const c_char) -> i32 {
    let cap = unsafe { cap_from_handle(handle) };
    let name = cstr(name);
    let res = df_runtime().block_on(async { cap.deregister_table(&name).await });
    match res {
        Ok(()) => 0,
        Err(e) => {
            log::error!("[DataFusion] deregister_table_ffi failed: {e}");
            1
        }
    }
}

// ============================================================
// FFI export
// ============================================================

#[cfg(feature = "cdylib")]
#[unsafe(no_mangle)]
pub extern "C" fn init_capability_plugin() -> Box<dyn CapabilityPlugin + Send + Sync> {
    Box::new(DataFusionCapabilityPlugin)
}
