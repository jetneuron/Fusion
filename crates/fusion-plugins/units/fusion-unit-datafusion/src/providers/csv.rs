use super::TableProviderFactory;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;
use datafusion::prelude::SessionContext;
use fusion_unit_sdk::config::ConfigEntry;
use fusion_unit_sdk::runtime::UnitResult;
use std::sync::Arc;

// ============================================================
// CsvProvider
// ============================================================

pub struct CsvProvider {
    delimiter: u8,
}

impl CsvProvider {
    pub fn new(delimiter: u8) -> Self {
        Self { delimiter }
    }
}

#[async_trait::async_trait]
impl TableProviderFactory for CsvProvider {
    fn name(&self) -> &str {
        if self.delimiter == b'\t' {
            "tsv"
        } else {
            "csv"
        }
    }

    async fn create(
        &self,
        entry: &ConfigEntry,
        _sql: Option<&str>,
    ) -> UnitResult<Arc<dyn TableProvider>> {
        let path_str = entry
            .data
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                fusion_unit_sdk::runtime::UnitError::config_required("csv provider: path")
            })?;

        let has_header = entry
            .data
            .get("has_header")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let table_path =
            ListingTableUrl::parse(path_str).map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!("parse path: {e}"))
            })?;

        let file_format = CsvFormat::default()
            .with_has_header(has_header)
            .with_delimiter(self.delimiter);

        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(if self.delimiter == b'\t' { ".tsv" } else { ".csv" });

        let resolved_schema = listing_options
            .infer_schema(&SessionContext::new().state(), &table_path)
            .await
            .map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!("infer schema: {e}"))
            })?;

        let config =
            ListingTableConfig::new(table_path).with_listing_options(listing_options).with_schema(resolved_schema);

        let table = ListingTable::try_new(config).map_err(|e| {
            fusion_unit_sdk::runtime::UnitError::unknown(format!("create table: {e}"))
        })?;

        Ok(Arc::new(table))
    }
}

// ============================================================
// Registration — called once at plugin init
// ============================================================

pub fn register_csv_providers() {
    super::register_provider(Arc::new(CsvProvider::new(b',')));
    super::register_provider(Arc::new(CsvProvider::new(b'\t')));
}
