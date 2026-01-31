use crate::runtime::script::Scripter;
use crate::runtime::state::GraphStates;
use std::any::Any;

// 产品 trait
pub trait Product: Any + Send + Sync {
    fn get_name(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: Any + Send + Sync + Default + 'static> Product for T {
    fn get_name(&self) -> &str {
        std::any::type_name::<T>()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub trait ScriptEngineFactory: Scripter + Send + 'static {
    fn name() -> &'static str;
    fn create_scripter(
        origin_script: String,
        states: GraphStates,
    ) -> anyhow::Result<Box<dyn Scripter + Send>>;
}