// 计算单元的配置
struct UnitConfig {}

pub trait ComputeUnit {}
pub trait BaseSourceUnit: ComputeUnit {}
pub trait BaseMapUnit: ComputeUnit {}
pub trait BaseSinkUnit: ComputeUnit {}

pub trait UnitCreator {
    fn new() -> Self;
}

pub trait UnitLifeCycle {
    fn start(&self);
}

pub trait UnitGraphical {
    fn connect(&self, target: &dyn ComputeUnit);
}

impl<T: ComputeUnit> UnitLifeCycle for T {
    fn start(&self) {
        todo!()
    }
}

impl<T: ComputeUnit> UnitGraphical for T {
    fn connect(&self, target: &dyn ComputeUnit) {
        println!("connect")
    }
}
