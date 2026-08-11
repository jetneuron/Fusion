use crate::types::FileFormat::{Auto, Csv, Excel, Json, Parquet, Tsv};
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use fusion_unit_sdk::proto::transfer::{DataType, Row};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

#[derive(Default, Clone)]
pub(crate) struct LocalTable {
    pub(crate) name: String,
    pub(crate) paths: Vec<String>,
    pub(crate) format: FileFormat,
}

#[derive(Default, Clone, Eq, PartialOrd, PartialEq)]
pub(crate) enum FileFormat {
    #[default]
    Auto,
    Csv,
    Tsv,
    Parquet,
    Excel,
    Json,
}

impl FromStr for FileFormat {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(Csv),
            "tsv" => Ok(Tsv),
            "parquet" => Ok(Parquet),
            "excel" => Ok(Excel),
            "json" => Ok(Json),
            &_ => Ok(Auto),
        }
    }
}

/// upstream table info
#[derive(Default)]
pub(crate) struct ExternalTable {
    pub(crate) table_name: String,
    pub(crate) source_id: String,
}

/// A custom datasource, used to represent a datastore with rows.
#[derive(Default, Clone)]
pub(crate) struct MemoryTable {
    inner: Arc<Mutex<MemoryTableInner>>,
}

impl MemoryTable {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        let mut inner = MemoryTableInner::default();
        inner.schema = Some(schema);
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub(crate) async fn add_row(&self, row: Row) {
        let mut inner = self.inner.lock().unwrap();
        row.columns
            .into_iter()
            .enumerate()
            .for_each(|(idx, column)| {
                if idx >= inner.columns.len() {
                    match column.dt.clone().unwrap() {
                        DataType::unknown => unreachable!(),
                        DataType::i32 => inner.columns.push(Box::new(Int32Builder::new())),
                        DataType::i64 => inner.columns.push(Box::new(Int64Builder::new())),
                        DataType::f32 => inner.columns.push(Box::new(Float32Builder::new())),
                        DataType::f64 => inner.columns.push(Box::new(Float64Builder::new())),
                        DataType::str => inner.columns.push(Box::new(StringBuilder::new())),
                        DataType::bool => inner.columns.push(Box::new(BooleanBuilder::new())),
                        DataType::bytes => unimplemented!(),
                        DataType::json => inner.columns.push(Box::new(StringBuilder::new())),
                    }
                }
                let any_column: &mut dyn Any = inner.columns[idx].as_any_mut();
                match column.dt.unwrap() {
                    DataType::unknown => {}
                    DataType::i32 => {
                        let mut s = any_column.downcast_mut::<Int32Builder>().unwrap();
                        s.append_value(column.i32_val);
                    }
                    DataType::i64 => {
                        let mut s = any_column.downcast_mut::<Int64Builder>().unwrap();
                        s.append_value(column.i64_val);
                    }
                    DataType::f32 => {
                        let mut s = any_column.downcast_mut::<Float32Builder>().unwrap();
                        s.append_value(column.f32_val);
                    }
                    DataType::f64 => {
                        let mut s = any_column.downcast_mut::<Float64Builder>().unwrap();
                        s.append_value(column.f64_val);
                    }
                    DataType::str => {
                        let mut s = any_column.downcast_mut::<StringBuilder>().unwrap();
                        s.append_value(column.str_val);
                    }
                    DataType::bool => {
                        let mut s = any_column.downcast_mut::<BooleanBuilder>().unwrap();
                        s.append_value(column.bool_val);
                    }
                    DataType::bytes => {
                        unimplemented!()
                    }
                    DataType::json => {
                        let mut s = any_column.downcast_mut::<StringBuilder>().unwrap();
                        s.append_value(column.str_val);
                    }
                }
            })
    }
}

impl Debug for MemoryTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("MemoryTable")
    }
}

#[derive(Default)]
pub(crate) struct MemoryTableInner {
    pub(crate) columns: Vec<Box<dyn ArrayBuilder>>,
    pub(crate) schema: Option<SchemaRef>,
}

impl MemoryTableInner {
    pub(crate) fn set_schema(&mut self, schema: SchemaRef) {
        self.schema = Some(schema)
    }
}

pub(crate) struct MemoryTableExec {
    db: MemoryTable,
    projected_schema: SchemaRef,
    cache: PlanProperties,
    projections: Vec<usize>,
}

impl Debug for MemoryTableExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemoryTableExec")
    }
}

impl DisplayAs for MemoryTableExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MemoryTableExec")
    }
}

impl ExecutionPlan for MemoryTableExec {
    fn name(&self) -> &str {
        "MemoryTableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let mut table = self.db.inner.lock().unwrap();
        let columns = table
            .columns
            .iter_mut()
            .enumerate()
            .filter(|(idx, c)| self.projections.contains(&idx))
            .map(|(idx, mut c)| c.finish())
            .collect::<Vec<_>>();
        let data = vec![RecordBatch::try_new(
            self.projected_schema.clone(),
            columns,
        )?];
        Ok(Box::pin(MemoryStream::try_new(data, self.schema(), None)?))
    }
}

impl MemoryTableExec {
    fn new(projections: Option<&Vec<usize>>, schema: SchemaRef, db: MemoryTable) -> Self {
        let mut projection_idx = vec![];
        let schema = match projections {
            Some(columns) => {
                projection_idx.extend_from_slice(columns);
                Arc::new(schema.project(columns).unwrap())
            }
            None => Arc::clone(&schema),
        };
        let cache = Self::compute_properties(schema.clone());
        Self {
            db,
            projected_schema: schema,
            cache,
            projections: projection_idx,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}
impl MemoryTable {
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>, datafusion::error::DataFusionError> {
        Ok(Arc::new(MemoryTableExec::new(
            projections,
            schema,
            self.clone(),
        )))
    }
}

#[async_trait]
impl TableProvider for MemoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let inner = self.inner.lock().unwrap();
        inner.schema.clone().unwrap()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        return self.create_physical_plan(projection, schema).await;
    }
}
