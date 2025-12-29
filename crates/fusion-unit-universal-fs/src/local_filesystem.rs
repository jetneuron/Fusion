use crate::core::{IntoUniversalIO, RowReader, UniversalIO, UniversalIOConfig};
use crate::error::IOError;
use fusion_unit_sdk::proto::transfer::Row;
use fusion_unit_sdk::row::formatter::StrRawFormatter;
use fusion_unit_sdk::row::utils::RawFormatter;
use fusion_unit_sdk::units::config_util::UnitConfigExt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

pub struct LocalFileSystem {
    path: PathBuf,
    config: UniversalIOConfig,
}

impl LocalFileSystem {
    pub fn new(path: PathBuf, config: UniversalIOConfig) -> Self {
        LocalFileSystem { path, config }
    }
}

impl RowReader for LocalFileSystem {
    type Reader = Box<dyn Iterator<Item = Result<Row, IOError>>>;

    fn rows(&self) -> anyhow::Result<Self::Reader, IOError> {
        let file = File::open(&self.path).map_err(|e| IOError::OpenError(e.to_string()))?;
        let reader = BufReader::new(file);

        let mut formatter = StrRawFormatter::new();
        let separator = self
            .config
            .extract_string("separator")
            .map_err(|e| {
                IOError::config_error(format!(
                    "Could not extract separator from {}: {}",
                    self.path.display(),
                    e
                ))
            })?
            .filter(|s| !s.is_empty());
        if let Some(sep) = separator {
            formatter = formatter.with_separator(&sep);
        }

        let formatter = Box::new(formatter);
        Ok(Box::new(IterableRow { reader, formatter }))
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
}
pub trait IntoIterableRow {
    type Source;

    fn into_row(self) -> anyhow::Result<Self::Source>;
}
pub struct IterableRow {
    reader: BufReader<File>,
    formatter: Box<dyn RawFormatter<String>>,
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
                        .into_row(line_trimmed)
                        .map_err(|err| IOError::field_fmt_error(err.to_string())),
                )
            }
            Err(e) => Some(Err(IOError::OpenError(e.to_string()))),
        }
    }
}
