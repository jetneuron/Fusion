//! # StreamTableProvider
//!
//! A dynamic [`TableProvider`] that receives rows during `compute`
//! and becomes queryable at EOF.
//!
//! Two modes (configurable via `row_threshold`):
//! - `row_threshold: usize::MAX` — pure in-memory table (no Parquet I/O).
//! - finite `row_threshold` — rows spill to Parquet when the in-memory
//!   buffer exceeds the threshold; the directory is read back at scan.
//!
//! The unit holds [`StreamTableProvider`] refs and calls [`append`] per
//! row (thread-safe via internal Mutex — parallel workers share one
//! provider per source). At EOF it calls [`finish`], then registers the
//! provider with the DataFusion session and runs SQL.

use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::datatypes::FieldRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, ExecutionPlan, PlanProperties,
};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, Partitioning};
use fusion_unit_sdk::proto::transfer::Row;
use std::any::Any;
use std::fmt;
use std::fs::File;
use std::sync::{Arc, Mutex, OnceLock};

// ============================================================
// ArrayBuilder — erased Arrow column builder
// ============================================================

trait ArrayBuilder: Send {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn finish(&mut self) -> ArrayRef;
}

impl ArrayBuilder for Int64Builder {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}
impl ArrayBuilder for Float64Builder {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}
impl ArrayBuilder for BooleanBuilder {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}
impl ArrayBuilder for StringBuilder {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

// ============================================================
// Type mapping
// ============================================================

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

fn new_builder(dt: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match dt {
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        _ => Box::new(StringBuilder::with_capacity(capacity, 0)),
    }
}

fn append_value(builder: &mut Box<dyn ArrayBuilder>, dt: &DataType, col: &fusion_unit_sdk::proto::transfer::Column) {
    match dt {
        DataType::Int64 => {
            let v = match col.dt.enum_value() {
                Ok(fusion_unit_sdk::proto::transfer::DataType::i32) => col.i32_val as i64,
                _ => col.i64_val,
            };
            builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_value(v);
        }
        DataType::Float64 => {
            let v = match col.dt.enum_value() {
                Ok(fusion_unit_sdk::proto::transfer::DataType::f32) => col.f32_val as f64,
                _ => col.f64_val,
            };
            builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap()
                .append_value(v);
        }
        DataType::Boolean => {
            builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap()
                .append_value(col.bool_val);
        }
        _ => {
            builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_value(&col.str_val);
        }
    }
}

fn finish_batch(builders: &mut [Box<dyn ArrayBuilder>], schema: SchemaRef) -> RecordBatch {
    let cols: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
    RecordBatch::try_new(schema, cols).expect("build record batch")
}

fn spill_batch(path: &str, batch: &RecordBatch) -> Result<(), String> {
    let file = File::create(path).map_err(|e| format!("create: {e}"))?;
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_compression(datafusion::parquet::basic::Compression::SNAPPY)
        .build();
    let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(
        file,
        batch.schema(),
        Some(props),
    )
    .map_err(|e| format!("writer: {e}"))?;
    writer.write(batch).map_err(|e| format!("write: {e}"))?;
    writer.close().map_err(|e| format!("close: {e}"))?;
    Ok(())
}

fn read_parquet_batches(path: &str) -> Result<Vec<RecordBatch>, String> {
    let file = File::open(path).map_err(|e| format!("open {path}: {e}"))?;
    let reader =
        datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("parquet reader: {e}"))?
            .build()
            .map_err(|e| format!("build reader: {e}"))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| format!("read batch: {e}"))?);
    }
    Ok(batches)
}

// ============================================================
// StreamTableProvider
// ============================================================

pub struct StreamTableProvider {
    inner: Arc<Mutex<StreamTableInner>>,
    /// In-memory row threshold before spilling. `usize::MAX` = pure memory.
    threshold: usize,
    /// Spill directory (finite threshold only).
    data_dir: String,
    /// Snapshot schema (set on first row).
    schema: Mutex<Option<SchemaRef>>,
}

impl fmt::Debug for StreamTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamTableProvider")
            .field("threshold", &self.threshold)
            .field("data_dir", &self.data_dir)
            .field(
                "rows",
                &self.inner.lock().unwrap().memory_rows,
            )
            .field("spill_files", &self.inner.lock().unwrap().spill_files.len())
            .finish()
    }
}

struct StreamTableInner {
    builders: Vec<Box<dyn ArrayBuilder>>,
    memory_rows: usize,
    spill_files: Vec<String>,
    /// Frozen data (pure-memory mode): builders finished at finish().
    snapshot: Vec<RecordBatch>,
    finished: bool,
}

impl StreamTableProvider {
    pub fn new(name: &str, threshold: usize, data_dir: &str) -> Self {
        if threshold != usize::MAX {
            std::fs::create_dir_all(data_dir).ok();
        }
        let _ = name;
        Self {
            inner: Arc::new(Mutex::new(StreamTableInner {
                builders: Vec::new(),
                memory_rows: 0,
                spill_files: Vec::new(),
                snapshot: Vec::new(),
                finished: false,
            })),
            threshold,
            data_dir: data_dir.to_string(),
            schema: Mutex::new(None),
        }
    }

    /// Append a row (thread-safe — parallel workers share one provider).
    pub fn append(&self, row: Row) {
        // Infer schema from the first row.
        {
            let mut schema_guard = self.schema.lock().unwrap();
            if schema_guard.is_none() {
                let fields: Vec<FieldRef> = row
                    .columns
                    .iter()
                    .map(|c| {
                        Arc::new(Field::new(
                            &c.field,
                            fusion_dt_to_arrow(c.dt.enum_value().unwrap_or(
                                fusion_unit_sdk::proto::transfer::DataType::unknown,
                            )),
                            true,
                        ))
                    })
                    .collect();
                *schema_guard = Some(Arc::new(Schema::new(fields)));
            }
        }

        let mut inner = self.inner.lock().unwrap();

        // First row: allocate builders.
        if inner.builders.is_empty() && !row.columns.is_empty() {
            let schema = self.schema.lock().unwrap();
            let schema = schema.as_ref().unwrap();
            inner.builders = schema
                .fields()
                .iter()
                .map(|f| new_builder(f.data_type(), 1024))
                .collect();
        }

        for (i, col) in row.columns.iter().enumerate() {
            if let Some(builder) = inner.builders.get_mut(i) {
                let dt = {
                    let schema = self.schema.lock().unwrap();
                    schema.as_ref().unwrap().field(i).data_type().clone()
                };
                append_value(builder, &dt, col);
            }
        }
        inner.memory_rows += 1;

        // Spill when the in-memory buffer exceeds the threshold.
        if inner.memory_rows >= self.threshold {
            let schema = self.schema.lock().unwrap();
            let schema = schema.as_ref().unwrap().clone();
            let batch = finish_batch(&mut inner.builders, schema);
            inner.memory_rows = 0;
            let seq = inner.spill_files.len();
            drop(inner);

            let path = format!("{}/part_{seq}.parquet", self.data_dir);
            if let Err(e) = spill_batch(&path, &batch) {
                log::error!("[stream_table] spill {path}: {e}");
            } else {
                self.inner.lock().unwrap().spill_files.push(path);
            }
        }
    }

    /// Freeze the table at EOF. Remaining in-memory rows either spill
    /// to Parquet (finite threshold) or become the snapshot (memory mode).
    pub fn finish(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.finished {
            return;
        }
        inner.finished = true;

        let has_builders = !inner.builders.is_empty() && inner.memory_rows > 0;
        if !has_builders {
            return;
        }

        let schema = self.schema.lock().unwrap();
        let schema = schema.as_ref().unwrap().clone();
        let batch = finish_batch(&mut inner.builders, schema);
        inner.memory_rows = 0;

        if self.threshold == usize::MAX {
            // Pure memory: keep the batch as snapshot.
            inner.snapshot.push(batch);
        } else {
            let seq = inner.spill_files.len();
            drop(inner);
            let path = format!("{}/part_{seq}.parquet", self.data_dir);
            if let Err(e) = spill_batch(&path, &batch) {
                log::error!("[stream_table] final spill {path}: {e}");
            } else {
                self.inner.lock().unwrap().spill_files.push(path);
            }
        }
    }

    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }
}

// ============================================================
// TableProvider impl
// ============================================================

#[async_trait::async_trait]
impl TableProvider for StreamTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema
            .lock()
            .unwrap()
            .clone()
            .expect("schema not initialized — no rows appended")
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let projected_schema = match projection {
            Some(cols) => Arc::new(schema.project(cols)?),
            None => schema,
        };
        Ok(Arc::new(StreamTableExec {
            provider: self.inner.clone(),
            schema: projected_schema,
            projection: projection.cloned(),
            properties: OnceLock::new(),
        }))
    }
}

// ============================================================
// ExecutionPlan — reads snapshot + spill files into MemoryStream
// ============================================================

struct StreamTableExec {
    provider: Arc<Mutex<StreamTableInner>>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: OnceLock<Arc<PlanProperties>>,
}

impl fmt::Debug for StreamTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamTableExec")
    }
}

impl DisplayAs for StreamTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamTableExec")
    }
}

impl ExecutionPlan for StreamTableExec {
    fn name(&self) -> &str {
        "StreamTableExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.properties.get_or_init(|| {
            let eq_properties = EquivalenceProperties::new(self.schema.clone());
            Arc::new(PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ))
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        debug_assert!(children.is_empty());
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        // Snapshot (pure-memory mode) + spill files (spill mode).
        let (mut batches, spill_paths) = {
            let inner = self.provider.lock().unwrap();
            (inner.snapshot.clone(), inner.spill_files.clone())
        };

        for path in spill_paths {
            match read_parquet_batches(&path) {
                Ok(mut bs) => batches.append(&mut bs),
                Err(e) => log::error!("[stream_table] read {path}: {e}"),
            }
        }

        Ok(Box::pin(MemoryStream::try_new(
            batches,
            self.schema.clone(),
            self.projection.clone(),
        )?))
    }
}
