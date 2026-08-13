use crate::proto::transfer::DataType::unknown;
use crate::proto::transfer::{Column, DataType, Frame};
use protobuf::Enum;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

/// indicate the `end of file` for frame data.
const FRAME_MASK_EOF: u32 = 1 << 0;
/// indicate the barrier of the frame data.
const FRAME_MASK_BARRIER: u32 = 1 << 1;
/// indicate the task already signal.
const FRAME_MASK_READY_SIGNAL: u32 = 1 << 2;
/// watermark
const WATERMARK: u32 = 1 << 3;
/// start
const START: u32 = 1 << 4;
/// indicate the frame data is raw string data.
pub const RAW_STR: u32 = 1 << 5;
/// indicate the frame data is raw bytes data.
pub const RAW_BYTES: u32 = 1 << 6;

/// static function for create `EOF`, `BARRIER` and so on.
impl Frame {
    /// create `EOF` mask frame.
    pub fn eof(source: String) -> Frame {
        let mut frame = Frame::new();
        frame.mask = FRAME_MASK_EOF;
        frame.source = source;
        frame
    }

    /// create `BARRIER` mask frame with source and barrier reference offset.
    pub fn barrier(source: String, offset: u64) -> Frame {
        let mut frame = Frame::new();
        frame.mask = FRAME_MASK_BARRIER;
        frame.source = source;
        frame.offset = offset;
        frame
    }

    /** watermark */
    pub fn watermark(source: String, offset: u64) -> Frame {
        let mut frame = Frame::new();
        frame.offset = offset;
        frame.mask = WATERMARK;
        frame.source = source;
        frame
    }

    pub fn start() -> Frame {
        let mut frame = Frame::new();
        frame.mask = START;
        frame
    }

    pub fn is_eof(&self) -> bool {
        self.mask & FRAME_MASK_EOF == FRAME_MASK_EOF
    }

    pub fn is_barrier(&self) -> bool {
        self.mask & FRAME_MASK_BARRIER == FRAME_MASK_BARRIER
    }

    pub fn is_watermark(&self) -> bool {
        self.mask & WATERMARK == WATERMARK
    }
}

impl Frame {
    pub fn display_column_names(&self) {
        let columns = &self.columns;
        println!(
            "{}",
            columns
                .iter()
                .map(|c| c.field.clone())
                .collect::<Vec<String>>()
                .join("\t")
        );
    }
}

impl Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let columns = &self.columns;
        let row_string = columns
            .iter()
            .map(|c| {
                let data_type = c.dt.unwrap();
                match data_type {
                    DataType::i32 => c.i32_val.to_string(),
                    DataType::i64 => c.i64_val.to_string(),
                    DataType::f32 => c.f32_val.to_string(),
                    DataType::f64 => c.f64_val.to_string(),
                    DataType::str => c.str_val.to_string(),
                    DataType::bool => c.bool_val.to_string(),
                    DataType::bytes => String::from_utf8(c.bytes_val.to_vec()).expect("bytes"),
                    DataType::unknown => "null".to_string(),
                    _ => unimplemented!("data type format unimplemented: {:?}", data_type),
                }
            })
            .collect::<Vec<String>>()
            .join("\t");
        f.write_str(row_string.as_str())?;
        Ok(())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let data_type = self.dt.unwrap();
        let field = self.field.clone();

        let val = match data_type {
            DataType::i32 => self.i32_val.to_string(),
            DataType::i64 => self.i64_val.to_string(),
            DataType::f32 => self.f32_val.to_string(),
            DataType::f64 => self.f64_val.to_string(),
            DataType::str => self.str_val.to_string(),
            DataType::bool => self.bool_val.to_string(),
            DataType::bytes => String::from_utf8(self.bytes_val.to_vec()).expect("bytes"),
            DataType::unknown => "null".to_string(),
            _ => unimplemented!("data type format unimplemented: {:?}", data_type),
        };
        f.write_str(format!("{}({:?}) = {}", field, &data_type, val).as_str())?;
        Ok(())
    }
}

pub trait IntoColumn {
    fn into_column(self, descriptor: &ColumnDescriptor) -> anyhow::Result<Column>;
}

#[derive(Default, Clone, Debug)]
pub struct ColumnDescriptor {
    pub name: String,
    pub data_type: DataType,
}

impl ColumnDescriptor {
    pub fn new<T: Into<String>>(name: T, data_type: DataType) -> Self {
        let name = name.into();
        ColumnDescriptor { name, data_type }
    }

    //noinspection DuplicatedCode
    pub fn from<T>(name: String, infers: &Vec<T>) -> Self
    where
        T: Into<String> + Clone,
    {
        let mut descriptor = Self::default();
        descriptor.name = name;
        descriptor.data_type = unknown;
        infers.into_iter().fold(descriptor, |mut acc, val| {
            let value = val.clone().into();
            if let Ok(v) = i32::from_str(&value) {
                if v.to_string().eq(&value.to_string())
                    && DataType::i32.value() <= acc.data_type.value()
                {
                    acc.data_type = DataType::i32;
                    return acc;
                }
            }
            if let Ok(_) = i64::from_str(&value) {
                if DataType::i64.value() <= acc.data_type.value() {
                    acc.data_type = DataType::i64;
                    return acc;
                }
            }
            if let Ok(v) = f32::from_str(&value) {
                if v.to_string().eq(&value.to_string())
                    && DataType::f32.value() <= acc.data_type.value()
                {
                    acc.data_type = DataType::f32;
                    return acc;
                }
            }
            if let Ok(_) = f64::from_str(&value) {
                if DataType::f64.value() <= acc.data_type.value() {
                    acc.data_type = DataType::f64;
                    return acc;
                }
            }
            if let Ok(_) = bool::from_str(&value.to_lowercase()) {
                if DataType::bool.value() <= acc.data_type.value() {
                    acc.data_type = DataType::bool;
                    return acc;
                }
            }
            acc.data_type = DataType::str;
            acc
        })
    }
}

#[test]
fn test_column_descriptor_inference() {
    let desc = ColumnDescriptor::from(
        "name".to_string(),
        &vec!["355", "4.52322222222222223222212", "231"],
    );
    println!("{:?}", desc);

    let desc = ColumnDescriptor::from("name".to_string(), &vec!["355", "231", "4.5232"]);
    println!("{:?}", desc);

    let desc = ColumnDescriptor::from("name".to_string(), &vec!["355", "231", "x", "4.5232"]);
    println!("{:?}", desc);

    let desc = ColumnDescriptor::from("name".to_string(), &vec!["355", "231", "true", "4.5232"]);
    println!("{:?}", desc);

    let desc = ColumnDescriptor::from("name".to_string(), &vec!["TRUE", "false", "true", "False"]);
    println!("{:?}", desc);
}
