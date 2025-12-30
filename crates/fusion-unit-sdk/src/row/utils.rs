use crate::proto::transfer::{Column, DataType, Row};
use crate::row::types::{ColumnDescriptor, IntoColumn};
use anyhow::{bail, Context};
use protobuf::EnumOrUnknown;

impl<T: Into<String>> IntoColumn for T {
    fn into_column(self, descriptor: &ColumnDescriptor) -> anyhow::Result<Column> {
        let str = self.into();
        let mut column = Column::new();
        column.field = descriptor.name.clone();
        match descriptor.data_type {
            DataType::str => {
                column.str_val = str.to_string();
                column.dt = EnumOrUnknown::new(DataType::str)
            }
            DataType::bool => {
                column.bool_val = str
                    .parse::<bool>()
                    .with_context(|| "failed to parse bool")?;
                column.dt = EnumOrUnknown::new(DataType::bool);
            }
            DataType::f64 => {
                column.f64_val = str.parse::<f64>().with_context(|| "failed to parse f64")?;
                column.dt = EnumOrUnknown::new(DataType::f64);
            }
            DataType::f32 => {
                column.f32_val = str.parse::<f32>().with_context(|| "failed to parse f32")?;
                column.dt = EnumOrUnknown::new(DataType::f32);
            }
            DataType::i32 => {
                column.i32_val = str.parse::<i32>().with_context(|| "failed to parse i32")?;
                column.dt = EnumOrUnknown::new(DataType::i32);
            }
            DataType::i64 => {
                column.i64_val = str.parse::<i64>().with_context(|| "failed to parse i64")?;
                column.dt = EnumOrUnknown::new(DataType::i64);
            }
            DataType::bytes => {
                column.bytes_val = str.as_bytes().to_vec();
                column.dt = EnumOrUnknown::new(DataType::bytes);
            }
            DataType::json => {
                column.str_val = str.to_string();
                column.dt = EnumOrUnknown::new(DataType::json);
            }
            DataType::unknown => bail!("unknown data type: {:?}", descriptor.data_type),
        };
        Ok(column)
    }
}

#[allow(clippy::wrong_self_convention)]
pub trait RawFormatter<T> {
    fn into_row(&self, raw: T) -> anyhow::Result<Row>;
}
pub trait RowToString {
    fn row_to_string(&self, separator: &str) -> anyhow::Result<String>;
}

impl RowToString for Row {
    fn row_to_string(&self, separator: &str) -> anyhow::Result<String> {
        let str = self
            .columns
            .iter()
            .map(|c| match c.dt.unwrap() {
                DataType::str => c.str_val.clone(),
                DataType::bool => c.bool_val.to_string(),
                DataType::f64 => c.f64_val.to_string(),
                DataType::f32 => c.f32_val.to_string(),
                DataType::i32 => c.i32_val.to_string(),
                DataType::i64 => c.i64_val.to_string(),
                DataType::bytes => String::from_utf8(c.bytes_val.to_vec()).expect("bytes"),
                DataType::json => c.str_val.clone(),
                DataType::unknown => unimplemented!("unknown data type: {:?}", c.dt.unwrap()),
            })
            .collect::<Vec<String>>()
            .join(separator);
        Ok(str)
    }
}
