use crate::proto::transfer::{Column, DataType, Row};
use std::fmt::{Debug, Display, Formatter};

/// indicate the `end of file` for row data.
const ROW_MASK_EOF: u32 = 1 << 0;
/// indicate the barrier of the row data.
const ROW_MASK_BARRIER: u32 = 1 << 1;
/// indicate the task already signal.
const ROW_MASK_READY_SIGNAL: u32 = 1 << 2;
/// watermark
const WATERMARK: u32 = 1 << 3;
/// start
const START: u32 = 1 << 4;
/// indicate the row data is raw string data.
pub const RAW_STR: u32 = 1 << 5;
/// indicate the row data is raw bytes data.
pub const RAW_BYTES: u32 = 1 << 6;

/// static function for create `EOF`, `BARRIER` and so on.
impl Row {
    /// create `EOF` mask row.
    pub fn eof(source: String) -> Row {
        let mut row = Row::new();
        row.mask = ROW_MASK_EOF;
        row.source = source;
        row
    }

    /// create `BARRIER` mask row.
    pub fn barrier() -> Row {
        let mut row = Row::new();
        row.mask = ROW_MASK_BARRIER;
        row
    }

    /** watermark */
    pub fn watermark(source: String, offset: u64) -> Row {
        let mut row = Row::new();
        row.offset = offset;
        row.mask = WATERMARK;
        row.source = source;
        row
    }

    pub fn start() -> Row {
        let mut row = Row::new();
        row.mask = START;
        row
    }

    pub fn is_eof(&self) -> bool {
        self.mask & ROW_MASK_EOF == ROW_MASK_EOF
    }

    pub fn is_barrier(&self) -> bool {
        self.mask & ROW_MASK_BARRIER == ROW_MASK_BARRIER
    }

    pub fn is_watermark(&self) -> bool {
        self.mask & WATERMARK == WATERMARK
    }
}

impl Display for Row {
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

impl Row {
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
