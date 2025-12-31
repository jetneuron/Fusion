mod utils;

use crate::utils::with_field_names;
use anyhow::{Context, Result};
use calamine::{
    open_workbook, Data, DeError, RangeDeserializerBuilder, Reader, Xlsx,
    XlsxError as CalamineXlsxError,
};
use fusion_derive::LogicalTask;
use fusion_unit_sdk::graph::types::{
    ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext, UnitConfig,
};
use fusion_unit_sdk::proto::transfer::{Column, DataType, Row};
use fusion_unit_sdk::row::types::ColumnDescriptor;
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use fusion_unit_sdk::units::config_util::UnitConfigExt;
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};
use protobuf::{Enum, EnumOrUnknown};
use rust_xlsxwriter::{
    Color, DocProperties, Format, FormatAlign, FormatBorder, Workbook, Worksheet, XlsxError,
};
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

// default row number to infer
const DEFAULT_INFER_ROWS: usize = 20;

#[unsafe(no_mangle)]
pub extern "C" fn init_plugin() -> Box<dyn GraphUnitPlugin> {
    Box::new(ExcelUnitPlugin {})
}

pub struct ExcelUnitPlugin {}

impl GraphUnitPlugin for ExcelUnitPlugin {
    fn register_units(&self) -> UnitManifest {
        let mut unit_manifest = UnitManifest::default();
        SpreadSheetUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());
        // ... Register other units ...
        unit_manifest
    }

    fn plugin_version(&self) -> &str {
        "1.0.0"
    }
}

/// This task could read data from `Excel` and emit to suffix stream.
///
/// #### Reference:
/// [rustxlsxwriter](https://rustxlsxwriter.github.io/index.html)
#[derive(Default, LogicalTask)]
pub struct SpreadSheetUnitTask {
    /// file path to read
    path: String,
    /// skip rows
    skip_rows: Option<u64>,
    /// alias field names for table header
    field_names: Option<Vec<ColumnDescriptor>>,
    /// alias field row
    field_name_row_index: Option<u64>,
    /// field types when specify `field_name_row_index`, default is `DataType::str`
    field_types: Option<Vec<DataType>>,
    /// sheet name
    sheet_name: String,
    /// auto recognize column type.
    auto_types: bool,
    /// write target workbook
    workbook: Arc<Mutex<Workbook>>,
}

impl SpreadSheetUnitTask {
    /// parse config item: `field_names`
    fn parse_config_field_names(&mut self, c: &UnitConfig) {
        // obtain rows
        c["field_names"].as_array().map(|s| {
            let mut tmp_fields = vec![];
            for item in s {
                let len = tmp_fields.len();
                let mut descriptor = ColumnDescriptor::new();
                descriptor.name = item["name"]
                    .as_str()
                    .map_or_else(|| format!("c{}", len), |s| String::from(s));

                let data_type_str = item["data_type"]
                    .as_str()
                    .map_or_else(|| format!("{:?}", DataType::str), |s| String::from(s));
                descriptor.data_type =
                    DataType::from_str(data_type_str.as_str()).unwrap_or(DataType::str);
                tmp_fields.push(descriptor);
            }
            self.field_names = Some(tmp_fields);
        });
    }
    /// parse field names from excel by specified field name row index.
    fn parse_field_names_from_excel(&mut self, c: UnitConfig) -> Result<(), UnitError> {
        let path = &self.path;
        self.field_name_row_index = c["field_name_row_index"]
            .as_u64()
            .map(|idx| idx)
            .or(self.field_name_row_index);
        match self.field_name_row_index.clone() {
            None => Ok(()),
            Some(header_index) => {
                self.skip_rows = match self.skip_rows {
                    None => Some(header_index + 1),
                    Some(old) => Some(core::cmp::max(old, header_index + 1)),
                };

                let mut workbook: Xlsx<_> =
                    open_workbook(path).map_err(|e: calamine::XlsxError| {
                        UnitError::IOError(format!(
                            "{}: Fail to open workbook from path: `{path}`",
                            e.to_string()
                        ))
                    })?;
                let range = workbook
                    .worksheet_range(self.sheet_name.as_str())
                    .map_err(|e| {
                        UnitError::IOError(format!(
                            "Fail to open sheet by name: `{}`",
                            self.sheet_name
                        ))
                    })?;
                let header_opt = range.rows().nth(header_index as usize);

                let total_rows = range.rows().len();
                let mut data_rows = range.rows().skip(self.skip_rows.unwrap_or(0) as usize);
                let scan_rows = core::cmp::min(total_rows, DEFAULT_INFER_ROWS);
                let mut sample_data = vec![vec![]];
                for row_idx in 0..scan_rows {
                    match data_rows.nth(row_idx) {
                        None => {}
                        Some(v) => {
                            v.iter().enumerate().for_each(|(i, v)| {
                                if sample_data.len() <= i {
                                    sample_data.push(Vec::new());
                                }
                                sample_data[i].push(v.to_string());
                            });
                        }
                    };
                }

                match header_opt {
                    None => Err(UnitError::IOError(format!(
                        "Header of index: {} not exists.",
                        header_index
                    ))),
                    Some(headers) => {
                        let mut tmp_fields = vec![];
                        for (column_idx, header) in headers.iter().enumerate() {
                            let index = tmp_fields.len();
                            let column_name = header.to_string();
                            let mut column_descriptor =
                                ColumnDescriptor::from(column_name, &sample_data[column_idx]);
                            match &self.field_types {
                                None => {}
                                Some(types) => {
                                    if index <= types.len() - 1 {
                                        column_descriptor.data_type = types[index];
                                    }
                                }
                            };
                            tmp_fields.push(column_descriptor);
                        }
                        self.field_names = Some(tmp_fields);
                        Ok(())
                    }
                }
            }
        }
    }

    /// build row data from excel data.
    fn build_row_data(
        auto_types: bool,
        field_names: &Vec<ColumnDescriptor>,
        row_data: &Vec<Data>,
    ) -> Row {
        let mut row: Row;
        if !auto_types && !field_names.is_empty() {
            row = with_field_names(row_data, &field_names);
        } else {
            row = Row::default();
            for row_datum in row_data {
                let column_idx = row.columns.len();
                let column_name = format!("c{}", column_idx);
                let mut column = Column::default();
                column.field = column_name;
                match row_datum {
                    Data::Int(v) => {
                        column.i64_val = *v;
                        column.dt = EnumOrUnknown::from(DataType::i64);
                    }
                    Data::Float(v) => {
                        column.f64_val = *v;
                        column.dt = EnumOrUnknown::from(DataType::f64);
                    }
                    Data::String(v) => {
                        column.str_val = v.to_string();
                        column.dt = EnumOrUnknown::from(DataType::str);
                    }
                    _ => {}
                }
                row.columns.push(column);
            }
        }
        row
    }
}

impl InitUnit for SpreadSheetUnitTask {
    /// prepare unit configurations.
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let sink_mode = unit.is_sink();
        // default value
        self.skip_rows = None;
        self.field_names = None;
        self.field_name_row_index = None;
        self.auto_types = true;

        if let Some(Err(err)) = unit.get_config().map::<UnitResult<()>, _>(|c| {
            self.path = c.require_string("path")?;
            self.skip_rows = c.extract_u64("skip_rows")?.or(None);
            self.sheet_name = c
                .extract_string("sheet_name")?
                .unwrap_or(String::from("Sheet"));
            self.auto_types = c.extract_bool("auto_types")?.unwrap_or(true);

            self.parse_config_field_names(&c);
            if self.field_names.is_none() {
                if !sink_mode {
                    self.parse_field_names_from_excel(c)?;
                }
            }
            Ok(())
        }) {
            return Err(err);
        }

        if self.field_names.is_some() {
            self.auto_types = false;
        }
        if self.path.is_empty() {
            panic!("Must specify `xlsx` path for read.")
        }

        if sink_mode {
            let mut workbook = Workbook::new();
            let mut worksheet = Worksheet::new();
            worksheet.set_name(&self.sheet_name).unwrap();
            workbook.push_worksheet(worksheet);
            self.workbook = Arc::new(Mutex::new(workbook));
        }
        Ok(())
    }
}

impl SourceUnit for SpreadSheetUnitTask {
    /// start to read excel file data. and emit row data one by one.
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        let path = self.path.clone();
        let sheet_name = self.sheet_name.clone();

        let auto_types = self.auto_types;
        let field_names = self.field_names.clone().unwrap_or(vec![]);
        let skip_rows = self.skip_rows.map(|x| x as i64).unwrap_or(-1);
        Ok(async move {
            // open workbook file by provided path.
            let mut workbook: Xlsx<_> = open_workbook(&path).map_err(CalamineXlsxErrorWrapper)?;
            let range = workbook
                .worksheet_range(sheet_name.as_str())
                .map_err(CalamineXlsxErrorWrapper)?;
            let mut iter = RangeDeserializerBuilder::new()
                .from_range(&range)
                .map_err(DeErrorWrapper)?;
            let mut index = 0;
            let mut skipped = false;
            loop {
                if let Some(result) = iter.next() {
                    if !skipped && skip_rows > 0 && index + 1 < skip_rows {
                        index = index + 1;
                        continue;
                    }
                    skipped = true;
                    index = index + 1;

                    let row_data: Vec<Data> = result.unwrap();
                    let row = Self::build_row_data(auto_types, &field_names, &row_data);
                    ctx.send(row).await;
                } else {
                    break;
                }
            }
            Ok(())
        })
    }
}

impl MapUnit for SpreadSheetUnitTask {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Ok(async move {
            let mut workbook = self.workbook.lock().expect("fail to obtain workbook");
            let sheet = workbook
                .worksheet_from_name(&self.sheet_name)
                .map_err(XlsxErrorWrapper)?;
            if row.offset == 1 {
                // Add a general heading format.
                let header_format = Format::new()
                    .set_bold()
                    .set_align(FormatAlign::Center)
                    .set_align(FormatAlign::VerticalCenter)
                    .set_foreground_color(Color::RGB(0xD7E4BC))
                    .set_border(FormatBorder::Thin);
                for (idx, column) in row.columns.iter().enumerate() {
                    sheet
                        .write_string_with_format(
                            0u32,
                            idx as u16,
                            column.field.clone(),
                            &header_format,
                        )
                        .map_err(XlsxErrorWrapper)?;
                }
            }

            let offset = row.offset as u32;
            for (idx, column) in row.columns.iter().enumerate() {
                let column_idx = idx as u16;
                match column.dt.unwrap() {
                    DataType::unknown => sheet.write(offset, column_idx, None::<String>),
                    DataType::i32 => sheet.write(offset, column_idx, column.i32_val),
                    DataType::i64 => sheet.write(offset, column_idx, column.i64_val),
                    DataType::f32 => sheet.write(offset, column_idx, column.f32_val),
                    DataType::f64 => sheet.write(offset, column_idx, column.f64_val),
                    DataType::str => sheet.write(offset, column_idx, column.str_val.clone()),
                    DataType::bool => sheet.write(offset, column_idx, column.bool_val),
                    DataType::bytes => unimplemented!(),
                    DataType::json => sheet.write(offset, column_idx, column.str_val.clone()),
                }
                .map_err(XlsxErrorWrapper)?;
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
        Ok(async move {
            let mut workbook = self.workbook.lock().expect("fail to obtain workbook");
            let worksheet = workbook
                .worksheet_from_name(&self.sheet_name)
                .expect("Could not obtain worksheet");
            worksheet
                .set_freeze_panes(1, 0)
                .expect("fail to set freeze panes");
            worksheet.autofit_to_max_width(300);

            let properties = DocProperties::new()
                .set_title("This is an example spreadsheet")
                .set_subject("That demonstrates document properties")
                .set_author("A. Rust User")
                .set_manager("J. Alfred Prufrock")
                .set_company("Rust Solutions Inc")
                .set_category("Sample spreadsheets")
                .set_keywords("Sample, Example, Properties")
                .set_comment("Created with FusionPro");

            workbook.set_properties(&properties);

            let path_buf = PathBuf::from_str(self.path.as_str());
            if let Some(parent) = path_buf.map_err(|err| anyhow::anyhow!(err))?.parent() {
                if !fs::metadata(parent).is_ok() {
                    let _ = fs::create_dir_all(self.path.as_str()).is_ok();
                }
            };

            workbook.save(self.path.clone()).map_err(XlsxErrorWrapper)?;
            Ok(())
        })
    }
}

#[derive(Debug)]
pub struct XlsxErrorWrapper(pub XlsxError);

impl From<XlsxErrorWrapper> for UnitError {
    fn from(value: XlsxErrorWrapper) -> Self {
        UnitError::Unknown(value.0.to_string())
    }
}

#[derive(Debug)]
pub struct DeErrorWrapper(pub DeError);

impl From<DeErrorWrapper> for UnitError {
    fn from(value: DeErrorWrapper) -> Self {
        UnitError::Unknown(value.0.to_string())
    }
}

#[derive(Debug)]
struct CalamineXlsxErrorWrapper(pub CalamineXlsxError);

impl From<CalamineXlsxErrorWrapper> for UnitError {
    fn from(value: CalamineXlsxErrorWrapper) -> Self {
        UnitError::Unknown(value.0.to_string())
    }
}
