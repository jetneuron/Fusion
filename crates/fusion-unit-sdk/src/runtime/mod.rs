use crate::graph::types::ComputingUnit;
use crate::runtime::logical::LogicalTask;
use crate::runtime::script::Scripter;
use anyhow::Error;
use serde::{Serialize, Serializer};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Mutex;
use crate::runtime::state::GraphStates;

pub mod logical;
pub mod script;
pub mod script_engine_factory;
pub mod state;

pub trait TaskFactoryType: Any + Send + Sync {
    fn create() -> Box<dyn TaskFactoryType>
    where
        Self: Sized;
    fn as_any(&self) -> &dyn Any;
}

lazy_static::lazy_static! {
    pub static ref GLOBAL_REGISTRY: TypeRegistry = TypeRegistry::new();
}

pub struct TypeRegistry {
    pub registry:
        Mutex<HashMap<String, fn(unit: ComputingUnit) -> UnitResult<Box<dyn LogicalTask + Send>>>>,
}

pub struct ScripterRegistry {
    pub registry: Mutex<HashMap<String, fn(script_type: String, states: GraphStates) -> Box<dyn Scripter + Send>>>,
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
//
// impl ScripterRegistry {
//     pub fn new() -> Self {
//         Self {
//             registry: Mutex::new(HashMap::new()),
//         }
//     }
//
//     pub fn register<T: Scripter + 'static>(&self, name: &str) {
//         let mut registry = self.registry.lock().unwrap();
//         registry.insert(name.to_string(), |script, states| T::create(script, states));
//     }
//
//     pub fn create(
//         &self,
//         script_type_name: &String,
//         origin_script: String,
//         states: GraphStates,
//     ) -> UnitResult<Box<dyn Scripter + Send>> {
//         let registry = self.registry.lock().unwrap();
//         if let Some(script_factory) = registry.get(script_type_name) {
//             let script = script_factory(origin_script, states);
//             Ok(script)
//         } else {
//             Err(UnitError::ScriptInitErr(script_type_name.to_string()))
//         }
//     }
// }

#[derive(Debug, thiserror::Error)]
pub enum UnitError {
    #[error("unknown error: {0}")]
    Unknown(String),
    #[error("could not parse config value of [{0}]")]
    ConfigParseError(String),
    #[error("business configuration value is invalidate: {0}")]
    ConfigInvalidate(String),
    #[error("config `{0}` is required")]
    ConfigFieldRequired(String),
    #[error("Unit IO error, cause by: {0}")]
    IOError(String),
    #[error("Row format error: {0}")]
    InvalidateRowFormat(String),
    #[error("Panic occur in physical task: {0}")]
    PhysicalTaskErr(String),
    #[error("Fail to initialize script engine: {0}")]
    ScriptInitErr(String),
}

impl UnitError {
    pub fn unknown<T: Into<String>>(msg: T) -> Self {
        Self::Unknown(msg.into())
    }

    pub fn physical_error<T: Into<String>>(msg: T) -> Self {
        Self::PhysicalTaskErr(msg.into())
    }

    pub fn config_invalidate<T: Into<String>>(msg: T) -> Self {
        Self::ConfigInvalidate(msg.into())
    }

    pub fn config_parse_error<T: Into<String>>(msg: T) -> Self {
        Self::ConfigParseError(msg.into())
    }

    pub fn config_required<T: Into<String>>(msg: T) -> Self {
        Self::ConfigFieldRequired(msg.into())
    }
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
