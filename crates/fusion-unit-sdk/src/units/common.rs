use crate::units::compute_unit::{
    BaseMapUnit, BaseSinkUnit, BaseSourceUnit, ComputeUnit, UnitCreator,
};

pub struct InputUnit {}

impl UnitCreator for InputUnit {
    fn new() -> Self {
        InputUnit {}
    }
}

impl ComputeUnit for InputUnit {}

impl BaseSourceUnit for InputUnit {}

pub struct MapUnit {}

impl UnitCreator for MapUnit {
    fn new() -> Self {
        MapUnit {}
    }
}

impl ComputeUnit for MapUnit {}

impl BaseMapUnit for MapUnit {}

pub struct PrintUnit {}

impl UnitCreator for PrintUnit {
    fn new() -> Self {
        PrintUnit {}
    }
}
impl ComputeUnit for PrintUnit {}

impl BaseSinkUnit for PrintUnit {}
