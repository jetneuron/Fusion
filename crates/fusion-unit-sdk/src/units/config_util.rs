use crate::graph::types::UnitConfig;
use crate::runtime::{UnitError, UnitResult};

pub trait UnitConfigExt {
    fn extract_string(&self, field: &str) -> UnitResult<Option<String>>;
    fn require_string(&self, field: &str) -> UnitResult<String>;

    fn extract_bool(&self, field: &str) -> UnitResult<Option<bool>>;
    fn require_bool(&self, field: &str) -> UnitResult<bool>;

    fn extract_u32(&self, field: &str) -> UnitResult<Option<u32>>;
    fn require_u32(&self, field: &str) -> UnitResult<u32>;

    fn extract_u64(&self, field: &str) -> UnitResult<Option<u64>>;
    fn require_u64(&self, field: &str) -> UnitResult<u64>;

    fn extract_i32(&self, field: &str) -> UnitResult<Option<i32>>;
    fn require_i32(&self, field: &str) -> UnitResult<i32>;

    fn extract_i64(&self, field: &str) -> UnitResult<Option<i64>>;
    fn require_i64(&self, field: &str) -> UnitResult<i64>;

    fn extract_f32(&self, field: &str) -> UnitResult<Option<f32>>;
    fn require_f32(&self, field: &str) -> UnitResult<f32>;

    fn extract_f64(&self, field: &str) -> UnitResult<Option<f64>>;
    fn require_f64(&self, field: &str) -> UnitResult<f64>;
}

impl UnitConfigExt for UnitConfig {
    fn extract_string(&self, field: &str) -> UnitResult<Option<String>> {
        match self.get(field) {
            None => Ok(None::<String>),
            Some(val) => match val.as_str() {
                None => Err(UnitError::config_parse_error(format!(
                    "Could not parse field {field}"
                ))),
                Some(val) => Ok(Some(val.to_string())),
            },
        }
    }

    fn require_string(&self, field: &str) -> UnitResult<String> {
        self.extract_string(field)?
            .ok_or_else(|| UnitError::config_required(field))
    }

    fn extract_bool(&self, field: &str) -> UnitResult<Option<bool>> {
        match self.get(field) {
            None => Ok(None::<bool>),
            Some(val) => match val.as_bool() {
                None => Err(UnitError::config_parse_error(format!(
                    "Could not parse field {field}"
                ))),
                Some(val) => Ok(Some(val)),
            },
        }
    }

    fn require_bool(&self, field: &str) -> UnitResult<bool> {
        self.extract_bool(field)?
            .ok_or_else(|| UnitError::config_required(field))
    }

    fn extract_u32(&self, field: &str) -> UnitResult<Option<u32>> {
        match self.get(field) {
            None => Ok(None::<u32>),
            Some(val) => match val.as_u64() {
                None => Err(UnitError::config_parse_error(format!(
                    "Could not parse field {field}"
                ))),
                Some(val) => Ok(Some(val as u32)),
            },
        }
    }

    fn require_u32(&self, field: &str) -> UnitResult<u32> {
        self.extract_u32(field)?
            .ok_or_else(|| UnitError::config_required(field))
    }

    fn extract_u64(&self, field: &str) -> UnitResult<Option<u64>> {
        match self.get(field) {
            None => Ok(None::<u64>),
            Some(val) => match val.as_u64() {
                None => Err(UnitError::config_parse_error(format!(
                    "Could not parse field {field}"
                ))),
                Some(val) => Ok(Some(val)),
            },
        }
    }

    fn require_u64(&self, field: &str) -> UnitResult<u64> {
        self.extract_u64(field)?
            .ok_or_else(|| UnitError::config_required(field))
    }

    fn extract_i32(&self, field: &str) -> UnitResult<Option<i32>> {
        match self.get(field) {
            None => Ok(None::<i32>),
            Some(val) => match val.as_i64() {
                None => Err(UnitError::config_parse_error(format!(
                    "Could not parse field {field}"
                ))),
                Some(val) => Ok(Some(val as i32)),
            },
        }
    }

    fn require_i32(&self, field: &str) -> UnitResult<i32> {
        self.extract_i32(field)?
            .ok_or_else(|| UnitError::config_required(field))
    }

    fn extract_i64(&self, field: &str) -> UnitResult<Option<i64>> {
        match self.get(field) {
            None => Ok(None::<i64>),
            Some(val) => match val.as_i64() {
                None => Err(UnitError::config_parse_error(format!(
                    "Could not parse field {field}"
                ))),
                Some(val) => Ok(Some(val)),
            },
        }
    }

    fn require_i64(&self, field: &str) -> UnitResult<i64> {
        self.extract_i64(field)?
            .ok_or_else(|| UnitError::config_required(field))
    }

    fn extract_f32(&self, field: &str) -> UnitResult<Option<f32>> {
        match self.get(field) {
            None => Ok(None::<f32>),
            Some(val) => match val.as_f64() {
                None => Err(UnitError::config_parse_error(format!(
                    "Could not parse field {field}"
                ))),
                Some(val) => Ok(Some(val as f32)),
            },
        }
    }

    fn require_f32(&self, field: &str) -> UnitResult<f32> {
        self.extract_f32(field)?
            .ok_or_else(|| UnitError::config_required(field))
    }

    fn extract_f64(&self, field: &str) -> UnitResult<Option<f64>> {
        match self.get(field) {
            None => Ok(None::<f64>),
            Some(val) => match val.as_f64() {
                None => Err(UnitError::config_parse_error(format!(
                    "Could not parse field {field}"
                ))),
                Some(val) => Ok(Some(val)),
            },
        }
    }

    fn require_f64(&self, field: &str) -> UnitResult<f64> {
        self.extract_f64(field)?
            .ok_or_else(|| UnitError::config_required(field))
    }
}
