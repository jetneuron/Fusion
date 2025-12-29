use crate::proto::transfer::{Column, DataType, Row};
use crate::row::types::{ColumnDescriptor, IntoColumn};
use crate::row::utils::RawFormatter;
use protobuf::EnumOrUnknown;

#[derive(Clone)]
pub struct StrRawFormatter {
    separator: Option<String>,
    columns: Option<Vec<ColumnDescriptor>>,
}

impl StrRawFormatter {
    pub fn new() -> Self {
        StrRawFormatter {
            separator: None,
            columns: None,
        }
    }

    pub fn with_separator(mut self, separator: &str) -> Self {
        self.separator = Some(separator.to_string());
        self
    }

    pub fn with_columns(mut self, columns: Vec<ColumnDescriptor>) -> StrRawFormatter {
        self.columns = Some(columns);
        self
    }
}

impl<T: Into<String>> RawFormatter<T> for StrRawFormatter {
    fn into_row(&self, raw: T) -> anyhow::Result<Row> {
        let raw_str = raw.into();
        if self.separator.is_none() {
            let mut row = Row::new();
            let mut column = Column::new();
            column.dt = EnumOrUnknown::from(DataType::str);
            column.str_val = raw_str;
            column.field = String::from("c1");
            row.columns.push(column);
            Ok(row)
        } else {
            let sep = self.separator.as_ref().unwrap();
            let mut dyn_column_descriptors;
            let (split_raws, field_descriptor) = match self.columns.as_ref() {
                None => &{
                    let split_raw = raw_str.split(sep).collect::<Vec<&str>>();
                    let cnt = split_raw.len();
                    dyn_column_descriptors = (0..cnt)
                        .map(|idx| {
                            let mut column_desc = ColumnDescriptor::new();
                            column_desc.data_type = DataType::str;
                            column_desc.name = format!("c{}", idx);
                            column_desc
                        })
                        .collect::<Vec<ColumnDescriptor>>();
                    (split_raw, &dyn_column_descriptors)
                },
                Some(descriptors) => {
                    let split_raw = raw_str.split(sep).collect::<Vec<&str>>();
                    &(split_raw, descriptors)
                }
            };

            let columns = split_raws
                .iter()
                .enumerate()
                .map::<anyhow::Result<Column>, _>(|(idx, val)| {
                    let field = &field_descriptor[idx];
                    let column = val.into_column(&field)?;
                    Ok(column)
                })
                .map(|r| r.unwrap())
                .collect::<Vec<Column>>();
            let mut row = Row::new();
            row.columns = columns;
            Ok(row)
        }
    }
}
