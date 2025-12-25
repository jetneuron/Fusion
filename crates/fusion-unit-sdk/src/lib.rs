use crate::graph::types::ComputingUnit;
use crate::runtime::logical::LogicalTask;
use crate::runtime::{UnitResult, GLOBAL_REGISTRY};
use std::collections::HashSet;
use std::hash::Hash;

pub mod graph;
pub mod proto;
pub mod row;
pub mod runtime;
pub mod units;
pub mod error;

#[derive(Default, Clone)]
pub struct UnitManifest {
    unit_provider: HashSet<String>,
}

impl UnitManifest {
    pub fn add(&mut self, key: String) {
        self.unit_provider.insert(key);
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.unit_provider.iter()
    }
}

pub trait GraphUnitPlugin {
    fn register_units(&self) -> UnitManifest;

    fn create(&self, unit: ComputingUnit) -> UnitResult<Box<dyn LogicalTask + Send>> {
        GLOBAL_REGISTRY.create(unit)
    }

    fn plugin_version(&self) -> &str {
        "unstable"
    }
}
