use crate::error::IOError::{ConfigError, FieldFormatError};

#[derive(Debug, thiserror::Error)]
pub enum IOError {
    #[error("IO open error: {0}")]
    OpenError(String),
    #[error("IO error, fail to format field: {0}")]
    FieldFormatError(String),
    #[error("Configuration invalidate: {0}")]
    ConfigError(String),
}

impl IOError {
    pub fn field_fmt_error<T: Into<String>>(msg: T) -> Self {
        FieldFormatError(msg.into())
    }

    pub fn config_error<T: Into<String>>(msg: T) -> Self {
        ConfigError(msg.into())
    }
}
