use datafusion::arrow::array::{Array, ArrayRef, BinaryViewArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use protobuf::EnumOrUnknown;
use fusion_unit_sdk::proto::transfer::{Column, Row};

pub(crate) fn as_row(batch: &RecordBatch, headers: &Vec<String>, row: usize) -> Row {
    let mut data_row = Row::default();
    data_row.offset = row as u64;

    let columns = batch.columns();
    let num_column = batch.num_columns();
    for idx in 0..num_column {
        let mut data_column = Column::default();
        data_column.field = headers[idx].clone();
        let column = &columns[idx];
        let data_type = column.data_type();

        match data_type {
            DataType::Null => {
                data_column.is_null = true;
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::unknown);
            }
            DataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                let bool_val = array.value(row);
                data_column.bool_val = bool_val;
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::bool);
            }
            DataType::Int8 => {
                let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
                let i8_val = array.value(row);
                data_column.i32_val = i8_val as i32;
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::i32);
            }
            DataType::Int16 => {
                let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                let i16_val = array.value(row);
                data_column.i32_val = i16_val as i32;
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::i32);
            }
            DataType::Int32 => {
                let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                let int32_val = array.value(row);
                data_column.i32_val = int32_val;
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::i32);
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
                let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                let int32_val = array.value(row);
                data_column.i32_val = int32_val;
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::i32);
            }
            DataType::Int64 | DataType::UInt64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                let int64_val = array.value(row);
                data_column.i64_val = int64_val;
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::i64);
            }
            DataType::Float16 | DataType::Float32 => {
                let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                let f32_val = array.value(row);
                data_column.f32_val = f32_val;
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::f32);
            }
            DataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                let f64_val = array.value(row);
                data_column.f64_val = f64_val;
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::f64);
            }
            DataType::Timestamp(time_unit, str) => {
                let timestamp_val = timestamp_value_in_millis(row, column, time_unit);
                data_column.i64_val = timestamp_val;
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::i64);
            }
            DataType::Date32 => { unimplemented!("date32 {:?}", data_column.field); }
            DataType::Date64 => { unimplemented!("date64 {:?}", data_column.field); }
            DataType::Time32(_) => { unimplemented!("time32 {:?}", data_column.field); }
            DataType::Time64(_) => { unimplemented!("time64 {:?}", data_column.field); }
            DataType::Duration(_) => { unimplemented!("duration {:?}", data_column.field); }
            DataType::Interval(_) => { unimplemented!("interval {:?}", data_column.field); }
            DataType::Binary => { unimplemented!("binary {:?}", data_column.field); }
            DataType::FixedSizeBinary(_) => { unimplemented!("fixed_size_binary {:?}", data_column.field); }
            DataType::LargeBinary => { unimplemented!("large_binary {:?}", data_column.field); }
            DataType::BinaryView => {
                let array = column.as_any().downcast_ref::<BinaryViewArray>().unwrap();
                let t = array.value(row);
                data_column.bytes_val = t.to_vec();
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::bytes);
            }
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                let str_value = array.value(row).to_string();
                data_column.str_val = str_value;
                data_column.dt = EnumOrUnknown::from(fusion_unit_sdk::proto::transfer::DataType::str);
            }
            DataType::LargeUtf8 => { unimplemented!("large_utf8 {:?}", data_column.field); }
            DataType::Utf8View => { unimplemented!("utf8_view {:?}", data_column.field); }
            DataType::List(_) => { unimplemented!("list {:?}", data_column.field); }
            DataType::ListView(_) => { unimplemented!("list_view {:?}", data_column.field); }
            DataType::FixedSizeList(_, _) => { unimplemented!("fixed_size_list {:?}", data_column.field); }
            DataType::LargeList(_) => { unimplemented!("large_list {:?}", data_column.field); }
            DataType::LargeListView(_) => { unimplemented!("large_list_view {:?}", data_column.field); }
            DataType::Struct(_) => { unimplemented!("struct {:?}", data_column.field); }
            DataType::Union(_, _) => { unimplemented!("union {:?}", data_column.field); }
            DataType::Dictionary(_, _) => { unimplemented!("dictionary {:?}", data_column.field); }
            DataType::Decimal32(_, _) => { unimplemented!("decimal32 {:?}", data_column.field); }
            DataType::Decimal64(_, _) => { unimplemented!("decimal64 {:?}", data_column.field); }
            DataType::Decimal128(_, _) => { unimplemented!("decimal128 {:?}", data_column.field); }
            DataType::Decimal256(_, _) => { unimplemented!("decimal256 {:?}", data_column.field); }
            DataType::Map(_, _) => { unimplemented!("map {:?}", data_column.field); }
            DataType::RunEndEncoded(_, _) => { unimplemented!("run_end_encoded {:?}", data_column.field); }
        }
        data_row.columns.push(data_column);
    }
    data_row
}

fn timestamp_value_in_millis(row: usize, column: &ArrayRef, time_unit: &TimeUnit) -> i64 {
    let timestamp_val = match time_unit {
        TimeUnit::Second => {
            let array = column.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            let second = array.value(row);
            second * 1000
        }
        TimeUnit::Millisecond => {
            let array = column.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
            array.value(row)
        }
        TimeUnit::Microsecond => {
            let array = column.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            let microsecond = array.value(row);
            microsecond / 1_000
        }
        TimeUnit::Nanosecond => {
            let array = column.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            let nanosecond = array.value(row);
            nanosecond / 1_000_000
        }
    };
    timestamp_val
}