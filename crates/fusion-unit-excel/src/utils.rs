use std::fmt::Debug;

use calamine::Data;
use protobuf::EnumOrUnknown;
use fusion_unit_sdk::proto::transfer::{Column, DataType, Row};
use fusion_unit_sdk::row::types::ColumnDescriptor;

/// indicate the `end of file` for row data.
const ROW_MASK_EOF: u32 = 1 << 0;
/// indicate the barrier of the row data.
const ROW_MASK_BARRIER: u32 = 1 << 1;
/// indicate the task already signal.
const ROW_MASK_READY_SIGNAL: u32 = 1 << 2;
/// watermark
const WATERMARK: u32 = 1 << 3;

/// we transform [DataType] to [Row]
///
/// # Examples
///
/// ```
/// use calamine::{open_workbook, Reader, Xlsx};
/// use graph_unit_sdk::proto::transfer::Row;
/// let path = "<path_to_read>".to_string();
/// let mut excel: Xlsx<_> = open_workbook(&path)?;
///
/// if let Some(Ok(range)) = excel.worksheet_range("Sheet1") {
///     for columns in range.rows() {
///         println!("{:?}", Row::from(columns));
///     }
/// }
/// ```
fn from(columns: Vec<Data>) -> Row {
    let mut row = Row::default();
    for dt in columns {
        let mut column = Column::default();
        match dt {
            Data::Int(v) => {
                column.i64_val = v;
            }
            Data::Float(v) => {
                column.f64_val = v;
            }
            Data::String(v) => {
                column.str_val = v.to_string();
            }
            _ => {}
        }
        row.columns.push(column);
    }
    row
}

pub fn with_field_names(row_data: &Vec<Data>, field_names: &Vec<ColumnDescriptor>) -> Row {
    let mut row: Row = Row::default();
    for row_datum in row_data {
        let column_idx = row.columns.len();

        let mut column = Column::default();
        if column_idx < field_names.len() {
            let descriptor = &field_names[column_idx];
            let column_name = &descriptor.name;
            column.field = column_name.to_string();
            match descriptor.data_type {
                DataType::unknown => {
                    unimplemented!();
                }
                DataType::i32 => {
                    column.dt = EnumOrUnknown::from(DataType::i32);
                    match row_datum {
                        Data::Int(v) => column.i32_val = *v as i32,
                        Data::Float(v) => column.i32_val = *v as i32,
                        Data::String(v) => column.i32_val = v.parse().unwrap(),
                        _ => {
                            unimplemented!();
                        }
                    }
                }
                DataType::i64 => {
                    column.dt = EnumOrUnknown::from(DataType::i64);
                    match row_datum {
                        Data::Int(v) => column.i64_val = *v,
                        Data::Float(v) => column.i64_val = *v as i64,
                        Data::String(v) => column.i64_val = v.parse().unwrap(),
                        _ => {
                            unimplemented!();
                        }
                    }
                }
                DataType::f64 => {
                    column.dt = EnumOrUnknown::from(DataType::f64);
                    match row_datum {
                        Data::Int(v) => column.f64_val = *v as f64,
                        Data::Float(v) => column.f64_val = *v,
                        Data::String(v) => column.f64_val = v.parse().unwrap(),
                        _ => {
                            unimplemented!();
                        }
                    }
                }
                DataType::str => {
                    column.dt = EnumOrUnknown::from(DataType::str);
                    match row_datum {
                        Data::Int(v) => column.str_val = (*v).to_string(),
                        Data::Float(v) => column.str_val = (*v).to_string(),
                        Data::String(v) => column.str_val = v.to_string(),
                        Data::Empty => {
                            column.str_val = "".to_string();
                        }
                        _ => {
                            unimplemented!();
                        }
                    }
                }
                DataType::json => {}
                DataType::f32 => {
                    column.dt = EnumOrUnknown::from(DataType::f32);
                    match row_datum {
                        Data::Int(v) => column.f32_val = *v as f32,
                        Data::Float(v) => column.f32_val = *v as f32,
                        Data::String(v) => column.f32_val = v.parse().unwrap(),
                        _ => {
                            unimplemented!();
                        }
                    }
                }
                DataType::bool => {
                    unimplemented!();
                }
                DataType::bytes => {
                    unimplemented!();
                }
            };
        } else {
            column.field = format!("c{}", column_idx);
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
                Data::Empty => {
                    column.dt = EnumOrUnknown::from(DataType::str);
                }
                _ => {
                    unimplemented!("unsupported type: {:?}", row_datum);
                }
            }
        }
        row.columns.push(column);
    }
    row
}
