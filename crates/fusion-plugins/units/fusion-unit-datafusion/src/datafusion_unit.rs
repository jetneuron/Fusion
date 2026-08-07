use crate::types::{ExternalTable, FileFormat, LocalTable, MemoryTable};
use crate::utils::as_row;
use datafusion::arrow::array::{Datum, RecordBatch};
use datafusion::arrow::datatypes::DataType::{Boolean, Float32, Float64, Int32, Int64, Utf8};
use datafusion::arrow::datatypes::{Field, Fields, Schema, SchemaRef};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::options::ReadOptions;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::prelude::*;
use fusion_derive::LogicalTask;
use fusion_unit_sdk::graph::types::{
    ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext, UnitConfig, UnitMeta,
};
use fusion_unit_sdk::proto::transfer::{DataType, Row};
use fusion_unit_sdk::runtime::logical::LogicalTaskMeta;
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, Index};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

const ORIGIN_META_KEY: &str = "origin_name";

/// Unit's implementation for apache datafusion
/// https://datafusion.apache.org/
///
/// This unit could be one of `Source`, `Map` or `Sink`, depend on the computing unit configuration.
#[derive(Default, LogicalTask)]
pub struct DataFusionUnit {
    meta: UnitMeta,
    /// sql scripts
    sql: String,
    /// local datasource descriptor.
    local: Vec<LocalTable>,
    /// external (upstream) tables
    external: Vec<ExternalTable>,
    runtime_tables: Arc<Mutex<HashMap<String, Arc<Mutex<MemoryTable>>>>>,
}

impl InitUnit for DataFusionUnit {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let conf = unit.get_config();
        conf.map(|c| {
            self.sql = c["sql"].as_str().expect("sql is not string").to_string();
            self.initialize_local_tables(&c);
            self.initialize_external_tables(&c);
            if self.external.is_empty() && self.local.is_empty() {
                panic!("There is no any tables.");
            }
        });
        Ok(())
    }
}

impl DataFusionUnit {
    /// parse the tables which read from current node config
    fn initialize_local_tables(&mut self, c: &UnitConfig) {
        let local = &c["local"].as_array();
        if local.is_none() {
            return;
        }

        // check file descriptor config
        let local_tables = local
            .expect("file descriptor is not an object or empty.")
            .clone();

        self.local = local_tables
            .iter()
            .map(|conf| {
                let mut table = LocalTable::default();
                // set table name
                table.name = conf["name"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .expect("table name is required")
                    .to_string();

                // specify the format of th file.
                table.format = conf.get("format").map_or_else(
                    || FileFormat::Auto,
                    |f| {
                        let fmt = f.as_str().expect("file format is not a string");
                        str::parse(fmt).expect("failed to parse Datasource")
                    },
                );

                // specify the paths will be read from.
                let paths = conf.get("paths").expect("paths is required.");
                let path_array = paths.as_array().expect("paths must be array.");
                table.paths = path_array
                    .into_iter()
                    .map(|p| p.as_str().expect("path must be a string").to_string())
                    .collect();
                table
            })
            .collect::<Vec<_>>();
    }

    /// parse the external tables
    fn initialize_external_tables(&mut self, c: &UnitConfig) {
        let external = c["external"].as_array();
        if external.is_none() {
            return;
        }
        self.external = external
            .unwrap()
            .into_iter()
            .map(|ups| {
                let mut ups_table = ExternalTable::default();
                ups_table.table_name = ups["name"]
                    .as_str()
                    .expect("table name must be str")
                    .to_string();
                ups_table.source_id = ups["source"]
                    .as_str()
                    .expect("source must be str")
                    .to_string();
                ups_table
            })
            .collect();
    }

    /// register data table into session context
    async fn register_data_table(table: &LocalTable, session: &SessionContext) -> UnitResult<()> {
        let table_name = &table.name;
        let final_format = if FileFormat::Auto.eq(&table.format) {
            let path = &table.paths[0];
            let path_seg = path.split(".").collect::<Vec<&str>>();
            let extension_seg = path_seg[path_seg.len() - 1].to_lowercase();
            let extension = extension_seg.as_str();
            let infer_format = FileFormat::from_str(extension).map_err(|err| {
                UnitError::config_invalidate(format!(
                    "Could not infer file format for extension: {}",
                    extension
                ))
            })?;
            if FileFormat::Auto.eq(&infer_format) {
                return Err(UnitError::config_invalidate(String::from(
                    "Could not infer table format.",
                )));
            }
            infer_format
        } else {
            table.format.clone()
        };

        match final_format {
            FileFormat::Auto => {
                unimplemented!()
            }
            FileFormat::Csv => {
                let mut csv_read_options = CsvReadOptions::new();
                let mut schema = Schema::empty();
                csv_read_options =
                    Self::infer_csv_schema(table, &session, &mut schema, csv_read_options).await;
                session
                    .register_csv(table_name, &table.paths[0], csv_read_options)
                    .await
                    .map_err(|err| {
                        UnitError::unknown(format!(
                            "register csv table failed: {}",
                            err.to_string()
                        ))
                    })
            }
            FileFormat::Tsv => {
                let mut csv_read_options = CsvReadOptions::new();
                let mut schema = Schema::empty();
                csv_read_options.delimiter = '\t' as u8;
                csv_read_options.file_extension = ".tsv";
                csv_read_options =
                    Self::infer_csv_schema(table, &session, &mut schema, csv_read_options).await;
                session
                    .register_csv(table_name, table.paths.index(0), csv_read_options)
                    .await
                    .map_err(|err| {
                        UnitError::unknown(format!(
                            "register tsv table failed: {}",
                            err.to_string()
                        ))
                    })
            }
            FileFormat::Parquet => session
                .register_parquet(
                    table_name,
                    table.paths.index(0),
                    ParquetReadOptions::default(),
                )
                .await
                .map_err(|err| {
                    UnitError::unknown(format!(
                        "register parquet table failed: {}",
                        err.to_string()
                    ))
                }),
            FileFormat::Excel => {
                unimplemented!()
            }
            FileFormat::Json => {
                let json_opt = NdJsonReadOptions::default();
                session
                    .register_json(table_name, &table.paths[0], json_opt)
                    .await
                    .map_err(|err| {
                        UnitError::unknown(format!(
                            "register json table failed: {}",
                            err.to_string()
                        ))
                    })
            }
        }
    }

    /// infer csv file schema, retain the origin column name in metadata
    async fn infer_csv_schema<'a>(
        table: &'a LocalTable,
        session: &'a SessionContext,
        schema: &'a mut Schema,
        mut options: CsvReadOptions<'a>,
    ) -> CsvReadOptions<'a> {
        let listing_options =
            options.to_listing_options(&session.copied_config(), session.copied_table_options());
        let table_path = ListingTableUrl::parse(table.paths[0].clone()).unwrap();
        if let Ok(schema_ref) = listing_options
            .infer_schema(&session.state(), &table_path)
            .await
        {
            let new_fields = schema_ref
                .fields
                .iter()
                .map(|t| {
                    let mut meta = HashMap::new();
                    meta.insert(ORIGIN_META_KEY.to_string(), t.name().clone());
                    let name = t.name().to_lowercase();
                    let data_type = t.data_type();
                    let nullable = t.is_nullable();
                    Field::new(name, data_type.clone(), nullable).with_metadata(meta)
                })
                .collect::<Vec<Field>>();
            let fields = Fields::from(new_fields);
            schema.fields = fields;
            options.schema = Some(schema);
        }
        options
    }

    /// initialize the table schema from external by provided row data.
    fn initialize_table_schema_with_row_data(row: &Row) -> MemoryTable {
        let fields = row
            .columns
            .iter()
            .map(|c| {
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
                }
                .with_metadata(meta)
            })
            .collect::<Vec<Field>>();
        let memory_table = MemoryTable::new(SchemaRef::new(Schema::new(fields)));
        memory_table
    }

    /// send the sql query result row by row to next node
    async fn send_query_result_row_by_row(
        ctx: &TaskContext,
        batches: Vec<RecordBatch>,
        header: Vec<String>,
    ) {
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
            let name = field
                .metadata()
                .get(ORIGIN_META_KEY)
                .unwrap_or_else(|| field.name());
            header.push(name.clone());
        }
        header
    }
}

impl SourceUnit for DataFusionUnit {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        let external_is_empty = self.external.is_empty();
        Ok(async move {
            if !external_is_empty {
                return Ok(());
            }
            let this_cloned = (&self.local).to_vec();
            let sql = self.sql.clone();

            // read from specified datasource, such as csv, parquet, txt.
            let session_config = SessionConfig::new();
            let session = SessionContext::new_with_config(session_config);
            for this_tbl in this_cloned {
                Self::register_data_table(&this_tbl, &session).await?;
            }

            let df = session.sql(sql.as_str()).await.expect("query failed");
            let results = df.collect().await.expect("query failed");
            let schema = results[0].schema();
            let header = Self::fetch_schema_header(&schema);
            Self::send_query_result_row_by_row(&ctx, results, header).await;
            Ok(())
        })
    }
}

impl MapUnit for DataFusionUnit {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        _ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let external_is_empty = self.external.is_empty();
        let cloned_arc_table = self.runtime_tables.clone();
        Ok(async move {
            if !external_is_empty {
                // get external memory table by source id
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
            Ok(())
        })
    }

    fn on_eof<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let external_is_empty = self.external.is_empty();
        let external_tables = self
            .external
            .iter()
            .map(|t| t.table_name.clone())
            .collect::<Vec<String>>();
        let cloned_arc_table = self.runtime_tables.clone();
        let local_tables = self.local.clone();
        Ok(async move {
            if !external_is_empty {
                let session_config = SessionConfig::new();
                let session_ctx = SessionContext::new_with_config(session_config);
                for external_table in external_tables {
                    let mut stream_tables = cloned_arc_table.lock().await;
                    let source_id = &row.source;
                    let arc_memory_tbl = stream_tables
                        .get(source_id)
                        .expect("source not found")
                        .clone();
                    let memory_tbl = arc_memory_tbl.lock().await.deref().clone();
                    session_ctx
                        .register_table(external_table, Arc::new(memory_tbl))
                        .expect("register table failed");
                }

                for local_tbl in local_tables {
                    Self::register_data_table(&local_tbl, &session_ctx).await?;
                }

                if let Ok(dataframe) = session_ctx.sql(&self.sql).await {
                    if let Ok(batches) = dataframe.collect().await {
                        let schema = batches[0].schema();
                        let header = Self::fetch_schema_header(&schema);
                        Self::send_query_result_row_by_row(ctx, batches, header).await;
                    }
                }
            }
            Ok(())
        })
    }
}
