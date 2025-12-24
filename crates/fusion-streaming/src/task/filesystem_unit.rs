use crate::task::types::TaskCore;
use crate::task::UnitTask;
use fusion_derive::SrcLogicTask;
use fusion_unit_sdk::graph::types::{ComputingUnit, InitUnit};

#[derive(Default)]
pub struct CsvReaderUnitTask {
    core: TaskCore,
}

impl InitUnit for CsvReaderUnitTask {
    fn init(&mut self, unit: ComputingUnit) {
        todo!()
    }
}
