use crate::error::IOError;
use aladdin_common::uri_utils::UriParser;
use anyhow::bail;
use fusion_unit_sdk::proto::transfer::Row;
use serde_json::Value;

pub type UniversalIOConfig = Value;

pub trait UniversalIO {
    type Reader: Iterator<Item = Result<Row, IOError>>;

    fn get_universal_config(&self) -> &UniversalIOConfig;

    fn set_universal_config(&mut self, config: UniversalIOConfig);

    fn iter_rows(&self) -> anyhow::Result<Self::Reader, IOError>;
}

pub trait RowReader {
    type Reader: Iterator<Item = Result<Row, IOError>>;
    fn rows(&self) -> anyhow::Result<Self::Reader, IOError>;
}

pub trait IntoUniversalIO {
    type Reader: Iterator<Item = Result<Row, IOError>>;
    fn into_universal_io(self) -> anyhow::Result<Box<dyn UniversalIO<Reader = Self::Reader>>>;
}

impl<S: Into<String>> IntoUniversalIO for S {
    type Reader = Box<dyn Iterator<Item = Result<Row, IOError>>>;

    fn into_universal_io(self) -> anyhow::Result<Box<dyn UniversalIO<Reader = Self::Reader>>> {
        let path = self.into();
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
                bail!("Could not parse URI: {}, {}", path, err);
            }
        }
    }
}
