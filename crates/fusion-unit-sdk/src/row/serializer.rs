use crate::proto::transfer::{Column, DataType, Row};
use crate::row::types::{ColumnDescriptor, IntoColumn};
use protobuf::EnumOrUnknown;

#[derive(Debug, thiserror::Error)]
pub enum SerializeError {
    #[error("unknown error: {0}")]
    Unknown(String),
}

pub trait Serializer<T> {
    #[allow(clippy::wrong_self_convention)]
    fn from_row(&self, row: Row) -> anyhow::Result<T, SerializeError>;
}

pub trait Deserializer<T> {
    #[allow(clippy::wrong_self_convention)]
    fn into_row(
        &self,
        value: T,
        column_descriptors: &Option<Vec<ColumnDescriptor>>,
    ) -> anyhow::Result<Row, SerializeError>;
}

pub trait Formatter<T>: Serializer<T> + Deserializer<T> {}

pub struct SeparatorFormatter {
    separator: String,
}

impl SeparatorFormatter {
    pub fn new<T: Into<String>>(separator: T) -> Self {
        let separator = separator.into();
        SeparatorFormatter { separator }
    }
}

impl Formatter<String> for SeparatorFormatter {}
impl Serializer<String> for SeparatorFormatter {
    fn from_row(&self, row: Row) -> anyhow::Result<String, SerializeError> {
        let result = row
            .columns
            .iter()
            .map(|s| {
                let dt = s.dt.enum_value_or(DataType::unknown);
                match dt {
                    DataType::str => s.str_val.clone(),
                    DataType::bool => s.bool_val.to_string(),
                    DataType::f64 => s.f64_val.to_string(),
                    DataType::f32 => s.f32_val.to_string(),
                    DataType::i32 => s.i32_val.to_string(),
                    DataType::i64 => s.i64_val.to_string(),
                    DataType::bytes => String::from_utf8(s.bytes_val.to_vec()).expect("bytes"),
                    DataType::json => s.str_val.clone(),
                    DataType::unknown => unimplemented!(),
                }
            })
            .collect::<Vec<String>>()
            .join(&self.separator);
        Ok(result)
    }
}

impl<T: Into<String>> Deserializer<T> for SeparatorFormatter {
    fn into_row(
        &self,
        value: T,
        column_descriptors: &Option<Vec<ColumnDescriptor>>,
    ) -> anyhow::Result<Row, SerializeError> {
        let value = value.into();
        if self.separator.is_empty() {
            let mut row = Row::new();
            let mut column = Column::new();
            column.dt = EnumOrUnknown::from(DataType::str);
            column.str_val = value;
            column.field = String::from("c1");
            row.columns.push(column);
            Ok(row)
        } else {
            let sep = &self.separator;
            let dyn_column_descriptors;
            let (split_raws, field_descriptor) = match column_descriptors {
                None => {
                    let split_raw = value.split(sep).collect::<Vec<&str>>();
                    let cnt = split_raw.len();
                    dyn_column_descriptors = (0..cnt)
                        .map(|idx| {
                            let mut column_desc = ColumnDescriptor::default();
                            column_desc.data_type = DataType::str;
                            column_desc.name = format!("c{idx}");
                            column_desc
                        })
                        .collect::<Vec<ColumnDescriptor>>();
                    (split_raw, &dyn_column_descriptors)
                }
                Some(descriptors) => {
                    let split_raw = value.split(sep).collect::<Vec<&str>>();
                    (split_raw, descriptors)
                }
            };
            let columns = split_raws
                .iter()
                .enumerate()
                .map::<anyhow::Result<Column>, _>(|(idx, val)| {
                    let field = if idx < field_descriptor.len() {
                        &field_descriptor[idx]
                    } else {
                        &ColumnDescriptor::new(format!("c{idx}"), DataType::str)
                    };

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