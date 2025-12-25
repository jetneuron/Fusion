use crate::task::types::TaskCore;
use fusion_unit_sdk::graph::types::{ComputingUnit, InitUnit};
use fusion_unit_sdk::runtime::UnitResult;

#[derive(Default)]
pub struct CsvReaderUnitTask {
    core: TaskCore,
}

impl InitUnit for CsvReaderUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        todo!()
    }
}
