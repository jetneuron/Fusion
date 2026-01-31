use crate::core::{IntoUniversalIO, RowReader, RowWriter, UniversalIO, UniversalIOConfig};
use crate::error::IOError;
use anyhow::anyhow;
use fusion_unit_sdk::proto::transfer::Row;
use fusion_unit_sdk::row::serializer::{Formatter, SeparatorFormatter};
use fusion_unit_sdk::row::utils::RowToString;
use fusion_unit_sdk::units::config_util::UnitConfigExt;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub(crate) struct LocalFileSystem {
    path: PathBuf,
    config: UniversalIOConfig,
    column_separator: String,
    writer: Option<Arc<Mutex<BufWriter<File>>>>,
}

impl LocalFileSystem {
    pub fn new(path: PathBuf, config: UniversalIOConfig) -> Self {
        let column_separator = config
            .extract_string("separator")
            .unwrap_or_default()
            .unwrap_or_default();
        let write_mode = config
            .extract_bool("write_mode")
            .unwrap_or_default()
            .unwrap_or_default();
        let writer = if write_mode {
            let file = File::create(path.clone()).unwrap();
            Some(Arc::new(Mutex::new(BufWriter::new(file))))
        } else {
            None
        };
        LocalFileSystem {
            path,
            config,
            column_separator,
            writer,
        }
    }
}

impl RowReader for LocalFileSystem {
    type Reader = Box<dyn Iterator<Item = Result<Row, IOError>>>;

    fn rows(&self) -> anyhow::Result<Self::Reader, IOError> {
        let file = File::open(&self.path).map_err(|e| IOError::OpenError(e.to_string()))?;
        let reader = BufReader::new(file);
        let formatter = Box::new(SeparatorFormatter::new(self.column_separator.clone()));
        Ok(Box::new(IterableRow { reader, formatter }))
    }
}

impl RowWriter for LocalFileSystem {
    fn write(&self, row: Row) -> Result<(), IOError> {
        let line = row
            .row_to_string(&self.column_separator)
            .map_err(|err| IOError::FieldFormatError(err.to_string()))?;
        if let Some(writer) = self.writer.as_ref() {
            let mut mutex_writer = writer
                .lock()
                .map_err(|err| IOError::WriteFailed(err.to_string()))?;
            mutex_writer
                .write(line.as_bytes())
                .map_err(|e| IOError::WriteFailed(e.to_string()))?;
            mutex_writer
                .write(b"\n")
                .map_err(|e| IOError::WriteFailed(e.to_string()))?;
        };
        Ok(())
    }
}

impl UniversalIO for LocalFileSystem {
    type Reader = Box<dyn Iterator<Item = Result<Row, IOError>>>;

    fn get_universal_config(&self) -> &UniversalIOConfig {
        todo!()
    }

    fn set_universal_config(&mut self, config: UniversalIOConfig) {
        self.config = config;
    }

    fn iter_rows(&self) -> anyhow::Result<Self::Reader, IOError> {
        self.rows()
    }

    fn write_row(&self, row: Row) -> anyhow::Result<()> {
        self.write(row).map_err(|err| anyhow!(err))
    }

    fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
pub trait IntoIterableRow {
    type Source;

    fn into_row(self) -> anyhow::Result<Self::Source>;
}
pub struct IterableRow {
    reader: BufReader<File>,
    formatter: Box<dyn Formatter<String>>,
}

impl Iterator for IterableRow {
    type Item = anyhow::Result<Row, IOError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::default();
        match self.reader.read_line(&mut line) {
            Ok(0) => None,
            Ok(_) => {
                let line_trimmed = line
                    .trim_end_matches(|c| c == '\n' || c == '\r')
                    .to_string();
                line.clear();
                Some(
                    self.formatter
                        .into_row(line_trimmed, &None)
                        .map_err(|err| IOError::field_fmt_error(err.to_string())),
                )
            }
            Err(e) => Some(Err(IOError::OpenError(e.to_string()))),
        }
    }
}
