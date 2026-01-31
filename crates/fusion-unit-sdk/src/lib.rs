use crate::graph::types::ComputingUnit;
use crate::runtime::logical::LogicalTask;
use crate::runtime::{UnitResult, GLOBAL_REGISTRY};
use std::collections::HashSet;
use std::hash::Hash;

pub mod error;
pub mod graph;
pub mod proto;
pub mod row;
pub mod runtime;
pub mod units;

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
        let unit_unique_id = unit.get_id().clone();
        let mut logical = GLOBAL_REGISTRY.create(unit)?;
        logical.set_id(unit_unique_id);
        Ok(logical)
    }

    fn plugin_version(&self) -> &str {
        "unstable"
    }
}
