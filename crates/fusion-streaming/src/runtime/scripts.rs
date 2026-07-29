use crate::runtime::core::{LuaContext, LuaRow};
use deno_core::JsRuntime;
use fusion_derive::ScriptEngine;
use fusion_unit_sdk::graph::types::{TaskContext, UnitIdx};
use fusion_unit_sdk::proto::transfer::Row;
use fusion_unit_sdk::runtime::script::script_registry::{FACTORY_REGISTRATIONS, FactoryRegistrar};
use fusion_unit_sdk::runtime::script::{ScriptContext, Scripter};
use fusion_unit_sdk::runtime::script_engine_factory::ScriptEngineFactory;
use fusion_unit_sdk::runtime::state::{GraphStates, State};
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use linkme::distributed_slice;
use log::{debug, warn};
use mlua::{Function, Lua};
use std::pin::Pin;
use std::sync::Arc;
use tera::Tera;
use tokio::sync::Mutex;

#[derive(Default, ScriptEngine)]
#[script_type = "lua"]
pub struct LuaScript {
    states: GraphStates,
    inner: String,
}

impl LuaScript {
    pub fn new(script: String, states: GraphStates) -> Self {
        Self {
            inner: script,
            states,
        }
    }
}

#[distributed_slice(FACTORY_REGISTRATIONS)]
static LUA_FACTORY: fn() -> FactoryRegistrar = || FactoryRegistrar::new::<LuaScript>();

impl Scripter for LuaScript {
    fn create(
        origin_script: String,
        states: GraphStates,
    ) -> anyhow::Result<Box<dyn Scripter + Send>>
    where
        Self: Sized,
    {
        Ok(Box::new(LuaScript::new(origin_script, states)))
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

    fn row_eval<'life0, 'async_trait>(
        &self,
        task_id: &UnitIdx,
        states: GraphStates,
        ctx: &TaskContext,
        row: Row,
    ) -> Pin<Box<dyn Future<Output = UnitResult<()>> + Send>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let inner_states = self.states.clone();
        let func_name = format!("lua_script_{}", task_id);
        let scripts = self.inner.lines().collect::<Vec<&str>>().join("\n  ");
        let ctx = LuaContext::wrap(ctx.clone());
        let arc_row = Arc::new(Mutex::new(row));
        Box::pin(async move {
            let lua_ref = inner_states.state::<GraphLua>()?;
            let lua_mutex = lua_ref.0.lock().await;
            let globals = lua_mutex.globals();
            let func = match globals.get::<Function>(func_name.clone()) {
                Ok(func) => func,
                Err(err) => {
                    debug!(
                        "Lua func: {func_name} not exist, create new chunk. reason: {}",
                        err.to_string()
                    );
                    let chunk = format!(
                        r#"
function {func_name}(ctx, data)
  {scripts}
  return true
end"#
                    );

                    log::debug!("lua code: {}", &chunk);
                    lua_mutex.load(chunk).exec().map_err(|err| {
                        UnitError::unknown(format!("Load lua script failed: {}", err.to_string()))
                    })?;
                    globals.get::<Function>(func_name).expect("")
                }
            };

            let lua_row = LuaRow::wrap(arc_row).await;
            match func.call_async::<bool>((ctx, lua_row)).await {
                Ok(_) => {}
                Err(err) => {
                    println!("{}", err);
                }
            };
            Ok(())
        })
    }
}

pub(crate) struct GraphLua(pub(crate) Arc<Mutex<Lua>>);

impl State for GraphLua {}

pub(crate) struct GraphTera(pub(crate) Arc<Mutex<Tera>>);

impl State for GraphTera {}

//pub(crate) struct GraphJavascript(pub(crate) Arc<Mutex<JsRuntime>>);

//impl State for GraphJavascript {}
