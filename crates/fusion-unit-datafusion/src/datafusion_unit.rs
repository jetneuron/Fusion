use crate::types::{FileFormat, MemoryTable, ThisTable, UpstreamTable};
use crate::utils::as_row;
use datafusion::arrow::array::{Datum, RecordBatch};
use datafusion::arrow::datatypes::DataType::{Boolean, Float32, Float64, Int32, Int64, Utf8};
use datafusion::arrow::datatypes::{Field, Fields, Schema, SchemaRef};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::options::ReadOptions;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::prelude::*;
use fusion_derive::LogicalTask;
use fusion_unit_sdk::graph::types::{ComputingUnit, Context, InitUnit, MapUnit, SourceUnit, UnitConfig};
use fusion_unit_sdk::proto::transfer::{DataType, Row};
use fusion_unit_sdk::runtime::UnitResult;
use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, Index};
use std::sync::Arc;
use tokio::sync::Mutex;

const ORIGIN_META_KEY: &str = "origin_name";

/// Unit's implementation for apache datafusion
/// https://datafusion.apache.org/
///
/// This unit could be one of `Source`, `Map` or `Sink`, depend on the computing unit configuration.
#[derive(Default, LogicalTask)]
pub struct DataFusionUnit {
    /// sql scripts
    sql: String,
    /// this datasource descriptor. table name always is `this`
    these: Vec<ThisTable>,
    /// upstream tables
    upstream: Vec<UpstreamTable>,

    runtime_tables: Arc<Mutex<HashMap<String, Arc<Mutex<MemoryTable>>>>>,
}

impl InitUnit for DataFusionUnit {
    fn init(&mut self, unit: ComputingUnit) {
        let conf = unit.get_config();
        conf.map(|c| {
            self.sql = c["sql"].as_str().expect("sql is not string").to_string();
            self.initialize_these_tables(&c);
            self.initialize_upstream_tables(&c);
            if self.upstream.is_empty() && self.these.is_empty() {
                panic!("There is no any tables.");
            }
        });
    }
}

impl DataFusionUnit {
    /// parse the tables which read from current node config
    fn initialize_these_tables(&mut self, c: &UnitConfig) {
        let these = &c["these"].as_array();
        if these.is_none() {
            return;
        }

        // check file descriptor config
        let these_tables = these.expect("file descriptor is not an object or empty.").clone();

        self.these = these_tables.iter().map(|conf| {
            let mut table = ThisTable::default();
            // set table name
            table.name = conf["name"].as_str().filter(|s| !s.is_empty())
                .expect("table name is required").to_string();

            // specify the format of th file.
            table.format = conf.get("format").map_or_else(|| FileFormat::Auto, |f| {
                let fmt = f.as_str().expect("file format is not a string");
                str::parse(fmt).expect("failed to parse Datasource")
            });

            // specify the paths will be read from.
            let paths = conf.get("paths").expect("paths is required.");
            let path_array = paths.as_array().expect("paths must be array.");
            table.paths = path_array.into_iter()
                .map(|p| p.as_str().expect("path must be a string").to_string())
                .collect();
            table
        }).collect::<Vec<_>>();
    }

    /// parse the upstream tables
    fn initialize_upstream_tables(&mut self, c: &UnitConfig) {
        let upstream = c["upstream"].as_array();
        if upstream.is_none() {
            return;
        }
        self.upstream = upstream.unwrap().into_iter().map(|ups| {
            let mut ups_table = UpstreamTable::default();
            ups_table.table_name = ups["name"].as_str().expect("table name must be str").to_string();
            ups_table.source_id = ups["source"].as_str().expect("source must be str").to_string();
            ups_table
        }).collect();
    }

    /// register data table into session context
    async fn register_data_table(table: &ThisTable, session: &SessionContext) {
        let table_name = &table.name;

        match table.format {
            FileFormat::Auto => {
                unimplemented!()
            }
            FileFormat::Csv => {
                let mut csv_read_options = CsvReadOptions::new();
                let mut schema = Schema::empty();
                csv_read_options = Self::infer_csv_schema(table, &session, &mut schema, csv_read_options).await;
                session.register_csv(table_name, table.paths.index(0), csv_read_options).await
                    .expect("registration of table failed");
            }
            FileFormat::Tsv => {
                unimplemented!()
            }
            FileFormat::Parquet => {
                session.register_parquet(table_name, table.paths.index(0), ParquetReadOptions::default()).await
                    .expect("registration of table failed");
            }
            FileFormat::Excel => {
                unimplemented!()
            }
        }
    }

    /// infer csv file schema, retain the origin column name in metadata
    async fn infer_csv_schema<'a>(table: &'a ThisTable, session: &'a SessionContext, schema: &'a mut Schema, mut options: CsvReadOptions<'a>) -> CsvReadOptions<'a> {
        let listing_options = options.to_listing_options(&session.copied_config(), session.copied_table_options());
        let table_path = ListingTableUrl::parse(table.paths[0].clone()).unwrap();
        if let Ok(schema_ref) = listing_options.infer_schema(&session.state(), &table_path).await {
            let new_fields = schema_ref.fields.iter().map(|t| {
                let mut meta = HashMap::new();
                meta.insert(ORIGIN_META_KEY.to_string(), t.name().clone());
                let name = t.name().to_lowercase();
                let data_type = t.data_type();
                let nullable = t.is_nullable();
                Field::new(name, data_type.clone(), nullable).with_metadata(meta)
            }).collect::<Vec<Field>>();
            let fields = Fields::from(new_fields);
            schema.fields = fields;
            options.schema = Some(schema);
        }
        options
    }

    /// initialize the table schema from upstream by provided row data.
    fn initialize_table_schema_with_row_data(row: &Row) -> MemoryTable {
        let fields = row.columns.iter().map(|c| {
            let mut meta = HashMap::new();
            meta.insert(ORIGIN_META_KEY.to_string(), c.field.clone());
            let field_name = c.field.to_lowercase();
            match c.dt.clone().unwrap() {
                DataType::unknown => unreachable!(),
                DataType::i32 => Field::new(field_name, Int32, true),
                DataType::i64 => Field::new(field_name, Int64, true),
                DataType::f32 => Field::new(field_name, Float32, true),
                DataType::f64 => Field::new(field_name, Float64, true),
                DataType::str => Field::new(field_name, Utf8, true),
                DataType::bool => Field::new(field_name, Boolean, true),
                DataType::bytes => unimplemented!(),
                DataType::json => Field::new(field_name, Utf8, true),
            }.with_metadata(meta)
        }).collect::<Vec<Field>>();
        let memory_table = MemoryTable::new(SchemaRef::new(Schema::new(fields)));
        memory_table
    }

    /// send the sql query result row by row to next node
    async fn send_query_result_row_by_row(ctx: &Context, batches: Vec<RecordBatch>, header: Vec<String>) {
        for batch in batches {
            let row_number = batch.num_rows();
            for row in 0..row_number {
                ctx.send(as_row(&batch, &header, row)).await;
            }
        }
    }

    fn fetch_schema_header(schema: &SchemaRef) -> Vec<String> {
        let mut header = Vec::new();
        for field in schema.fields() {
            let name = field.metadata().get(ORIGIN_META_KEY).unwrap_or_else(|| field.name());
            header.push(name.clone());
        }
        header
    }
}

impl SourceUnit for DataFusionUnit {
    fn launch(&self, ctx: Arc<Context>) -> impl Future<Output=UnitResult<()>> + Send {
        let upstream_is_empty = self.upstream.is_empty();
        async move {
            if !upstream_is_empty {
                return Ok(());
            }
            let this_cloned = (&self.these).to_vec();
            let sql = self.sql.clone();

            // read from specified datasource, such as csv, parquet, txt.
            let session_config = SessionConfig::new();
            let session = SessionContext::new_with_config(session_config);
            for this_tbl in this_cloned {
                Self::register_data_table(&this_tbl, &session).await;
            }

            let df = session.sql(sql.as_str()).await.expect("query failed");
            let results = df.collect().await.expect("query failed");
            let schema = results[0].schema();
            let header = Self::fetch_schema_header(&schema);
            Self::send_query_result_row_by_row(&ctx, results, header).await;
            Ok(())
        }
    }
}

impl MapUnit for DataFusionUnit {
    fn compute<'life0, 'async_trait>(&'life0 self, row: Row, _ctx: &'life0 Context) -> impl Future<Output=()> + Send
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let upstream_is_empty = self.upstream.is_empty();
        let cloned_arc_table = self.runtime_tables.clone();
        async move {
            if !upstream_is_empty {
                // get upstream memory table by source id
                let arc_table = {
                    let mut stream_tables = cloned_arc_table.lock().await;
                    let source_id = &row.source;
                    let option = stream_tables.get(source_id);
                    if option.is_none() {
                        // register the table schema
                        let memory_table = Self::initialize_table_schema_with_row_data(&row);
                        let arc = Arc::new(Mutex::new(memory_table));
                        stream_tables.insert(source_id.clone(), arc.clone());
                        arc
                    } else {
                        option.unwrap().clone()
                    }
                };

                {
                    let table = arc_table.lock().await;
                    // append row data into arrow columns
                    table.add_row(row).await;
                }
            }
        }
    }

    fn on_eof<'life0, 'async_trait>(&'life0 self, row: Row, ctx: &'life0 Context) -> impl Future<Output=()> + Send
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let upstream_is_empty = self.upstream.is_empty();
        let upstream_tables = self.upstream.iter().map(|t| t.table_name.clone()).collect::<Vec<String>>();
        let cloned_arc_table = self.runtime_tables.clone();
        let local_tables = self.these.clone();
        async move {
            if !upstream_is_empty {
                let session_config = SessionConfig::new();
                let session_ctx = SessionContext::new_with_config(session_config);
                for upstream_table in upstream_tables {
                    let mut stream_tables = cloned_arc_table.lock().await;
                    let source_id = &row.source;
                    let arc_memory_tbl = stream_tables.get(source_id).expect("source not found").clone();
                    let memory_tbl = arc_memory_tbl.lock().await.deref().clone();
                    session_ctx.register_table(upstream_table, Arc::new(memory_tbl)).expect("register table failed");
                }

                for local_tbl in local_tables {
                    Self::register_data_table(&local_tbl, &session_ctx).await;
                }

                if let Ok(dataframe) = session_ctx.sql(&self.sql).await {
                    if let Ok(batches) = dataframe.collect().await {
                        let schema = batches[0].schema();
                        let header = Self::fetch_schema_header(&schema);
                        Self::send_query_result_row_by_row(ctx, batches, header).await;
                    }
                }
            }
        }
    }
}
