use crate::graph::types::ComputingUnit;
use crate::runtime::logical::LogicalTask;
use anyhow::Error;
use serde::{Serialize, Serializer};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Mutex;

pub mod logical;

pub trait TaskFactoryType: Any + Send + Sync {
    fn create() -> Box<dyn TaskFactoryType>
    where
        Self: Sized;
    fn as_any(&self) -> &dyn Any;
}

pub struct TypeRegistry {
    pub registry:
        Mutex<HashMap<String, fn(unit: ComputingUnit) -> UnitResult<Box<dyn LogicalTask + Send>>>>,
}

lazy_static::lazy_static! {
    pub static ref GLOBAL_REGISTRY: TypeRegistry = TypeRegistry::new();
}

impl TypeRegistry {
    fn new() -> Self {
        Self {
            registry: Mutex::new(HashMap::new()),
        }
    }

    pub fn register<T: LogicalTask + 'static>(&self, name: &str) {
        let mut registry = self.registry.lock().unwrap();
        registry.insert(name.to_string(), |unit| T::create(unit));
    }

    pub fn create(&self, unit: ComputingUnit) -> UnitResult<Box<dyn LogicalTask + Send>> {
        let unit_type_name = unit.get_type();
        let registry = self.registry.lock().unwrap();
        let factory = registry.get(unit_type_name);
        if factory.is_none() {
            Err(UnitError::Unknown(format!(
                "Could not find the unit factory for type: {}",
                unit_type_name
            )))
        } else {
            let func = factory.unwrap();
            func(unit)
        }
    }

    pub fn clean_and_shrink(&self) {
        let mut map = GLOBAL_REGISTRY.registry.lock().unwrap();
        map.clear();
        map.shrink_to_fit();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UnitError {
    #[error("unknown error: {0}")]
    Unknown(String),
    #[error("could not parse config value of [{0}]")]
    ConfigParseError(&'static str),
    #[error("business configuration value is invalidate: {0}")]
    ConfigInvalidate(&'static str),
}

impl Serialize for UnitError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_ref())
    }
}

pub type UnitResult<T> = Result<T, UnitError>;

impl From<anyhow::Error> for UnitError {
    fn from(value: Error) -> Self {
        UnitError::Unknown(value.to_string())
    }
}
