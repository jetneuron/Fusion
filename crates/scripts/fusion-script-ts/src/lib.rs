use fusion_derive::ScriptEngine;
use fusion_unit_sdk::graph::types::{TaskContext, UnitIdx};
use fusion_unit_sdk::proto::transfer::Frame;
use fusion_unit_sdk::runtime::script::script_registry::{FactoryRegistrar, FACTORY_REGISTRATIONS};
use fusion_unit_sdk::runtime::script::{ScriptContext, Scripter};
use fusion_unit_sdk::runtime::state::GraphStates;
use fusion_unit_sdk::runtime::UnitResult;
use linkme::distributed_slice;
use std::pin::Pin;

#[derive(Default, ScriptEngine)]
#[script_type = "ts"]
pub struct TypeScript {
    states: GraphStates,
    inner: String,
}

impl TypeScript {
    pub fn new(script: String, states: GraphStates) -> Self {
        Self {
            inner: script,
            states,
        }
    }
}

#[distributed_slice(FACTORY_REGISTRATIONS)]
static TS_FACTORY: fn() -> FactoryRegistrar = || FactoryRegistrar::new::<TypeScript>();

impl Scripter for TypeScript {
    fn create(
        origin_script: String,
        states: GraphStates,
    ) -> anyhow::Result<Box<dyn Scripter + Send>>
    where
        Self: Sized,
    {
        Ok(Box::new(TypeScript::new(origin_script, states)))
    }

    fn eval<'life0, 'async_trait>(
        &self,
        context: ScriptContext,
    ) -> Pin<Box<dyn Future<Output = UnitResult<String>> + Send>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { Ok(String::default()) })
    }

    fn frame_eval<'life0, 'async_trait>(
        &self,
        task_id: &UnitIdx,
        states: GraphStates,
        ctx: &TaskContext,
        frame: Frame,
    ) -> Pin<Box<dyn Future<Output = UnitResult<()>> + Send>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }
}
