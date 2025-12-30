use crate::error::IOError;
use aladdin_common::uri_utils::UriParser;
use anyhow::bail;
use fusion_unit_sdk::proto::transfer::Row;
use serde_json::Value;

pub type UniversalIOConfig = Value;

pub trait UniversalIO: Send + Sync {
    type Reader: Iterator<Item = Result<Row, IOError>>;

    fn get_universal_config(&self) -> &UniversalIOConfig;

    fn set_universal_config(&mut self, config: UniversalIOConfig);

    fn iter_rows(&self) -> anyhow::Result<Self::Reader, IOError>;

    fn write_row(&self, row: Row) -> anyhow::Result<()>;
    fn close(&self) -> anyhow::Result<()>;
}

pub trait RowReader {
    type Reader: Iterator<Item = Result<Row, IOError>>;
    fn rows(&self) -> anyhow::Result<Self::Reader, IOError>;
}

pub trait RowWriter {
    fn write(&self, row: Row) -> Result<(), IOError>;
}

pub trait IntoUniversalIO {
    type Reader: Iterator<Item = Result<Row, IOError>>;
    fn into_universal_io(self) -> anyhow::Result<Box<dyn UniversalIO<Reader = Self::Reader>>>;
}

impl IntoUniversalIO for &str {
    type Reader = Box<dyn Iterator<Item = Result<Row, IOError>>>;

    fn into_universal_io(self) -> anyhow::Result<Box<dyn UniversalIO<Reader = Self::Reader>>> {
        self.to_string().into_universal_io()
    }
}

impl IntoUniversalIO for String {
    type Reader = Box<dyn Iterator<Item = Result<Row, IOError>>>;

    fn into_universal_io(self) -> anyhow::Result<Box<dyn UniversalIO<Reader = Self::Reader>>> {
        match UriParser::parse_uri(&self) {
            Ok(uri) => {
                let lowercase_url = uri.scheme.to_lowercase();
                let scheme = lowercase_url.as_str();
                let path = uri.path;
                match scheme {
                    "file" | "fs" | "" => {
                        #[cfg(feature = "local-fs")]
                        {
                            let path_buf = std::path::PathBuf::from(path);
                            let config = serde_json::json!(uri.query_params);
                            Ok(Box::new(crate::local_filesystem::LocalFileSystem::new(
                                path_buf, config,
                            )))
                        }

                        #[cfg(not(feature = "local-fs"))]
                        bail!("Local filesystem unsupported");
                    }
                    _ => unimplemented!("Unsupported URL scheme: {}", scheme),
                }
            }
            Err(err) => {
                bail!("Could not parse URI: {}, {}", self, err);
            }
        }
    }
}

impl IntoUniversalIO for (&str, UniversalIOConfig) {
    type Reader = Box<dyn Iterator<Item = Result<Row, IOError>>>;

    fn into_universal_io(self) -> anyhow::Result<Box<dyn UniversalIO<Reader = Self::Reader>>> {
        (self.0.to_string(), self.1).into_universal_io()
    }
}

impl IntoUniversalIO for (String, UniversalIOConfig) {
    type Reader = Box<dyn Iterator<Item = Result<Row, IOError>>>;

    fn into_universal_io(self) -> anyhow::Result<Box<dyn UniversalIO<Reader = Self::Reader>>> {
        let path = self.0;
        let universal_config = self.1;
        match UriParser::parse_uri(&path) {
            Ok(uri) => {
                let lowercase_url = uri.scheme.to_lowercase();
                let scheme = lowercase_url.as_str();
                let path = uri.path;
                match scheme {
                    "file" | "fs" | "" => {
                        #[cfg(feature = "local-fs")]
                        {
                            let path_buf = std::path::PathBuf::from(path);
                            Ok(Box::new(crate::local_filesystem::LocalFileSystem::new(
                                path_buf,
                                universal_config,
                            )))
                        }

                        #[cfg(not(feature = "local-fs"))]
                        bail!("Local filesystem unsupported");
                    }
                    _ => unimplemented!("Unsupported URL scheme: {}", scheme),
                }
            }
            Err(err) => {
                bail!("Could not parse URI: {}, {}", path, err);
            }
        }
    }
}
