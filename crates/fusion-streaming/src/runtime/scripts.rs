use crate::runtime::core::{LuaContext, LuaFrame};
use fusion_derive::ScriptEngine;
use fusion_unit_sdk::graph::types::{TaskContext, UnitIdx};
use fusion_unit_sdk::proto::transfer::Frame;
use fusion_unit_sdk::runtime::script::script_registry::{FACTORY_REGISTRATIONS, FactoryRegistrar};
use fusion_unit_sdk::runtime::script::{ScriptContext, Scripter};
use fusion_unit_sdk::runtime::script_engine_factory::ScriptEngineFactory;
use fusion_unit_sdk::runtime::state::{GraphStates, State};
use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use linkme::distributed_slice;
use log::debug;
use mlua::{Function, Lua, Table};
use std::pin::Pin;
use std::sync::Arc;
use tera::Tera;
use tokio::sync::Mutex;

/// Key under which the compiled Lua function is stored in the node's
/// scope table.
const SCOPE_FUNC_KEY: &str = "func";

/// Key under which an optional `this` userdata is stored in the scope
/// table. Set by units (e.g. RedisUnitTask) during init.
const SCOPE_THIS_KEY: &str = "this";

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
        let inner_states = self.states.clone();
        let scripts = self.inner.lines().collect::<Vec<&str>>().join("\n  ");
        let lua_ctx = LuaContext::wrap(ctx.clone());
        let arc_row = Arc::new(Mutex::new(frame));
        // Scope name matches what init_script_env() / shutdown() use.
        let scope_name = task_id.to_string();
        // Per-worker Lua VM (parallelism > 1) or global GraphLua.
        let worker_lua = ctx.worker_lua.clone();
        Box::pin(async move {
            let lua_arc: Arc<tokio::sync::Mutex<Lua>> = match worker_lua {
                Some(lua) => lua,
                None => inner_states.state::<GraphLua>()?.0.clone(),
            };
            let lua = lua_arc.lock().await;
            let globals = lua.globals();

            let scope_table: Table = globals
                .get(scope_name.clone())
                .map_err(|_| UnitError::unknown(format!("scope table `{scope_name}` not found")))?;

            // Always use the same function signature — (ctx, data, this).
            // `this` may be nil when no userdata has been injected.
            let func = match scope_table.get::<Function>(SCOPE_FUNC_KEY) {
                Ok(f) => f,
                Err(_) => {
                    let chunk =
                        format!("local ctx, data, this = ...\n{scripts}\nreturn true");
                    let func: Function = lua
                        .load(&chunk)
                        .into_function()
                        .map_err(|e| UnitError::unknown(format!("Lua compile: {e}")))?;
                    scope_table.set(SCOPE_FUNC_KEY, func.clone()).map_err(|e| {
                        UnitError::unknown(format!("set scope func: {e}"))
                    })?;
                    func
                }
            };

            let lua_row = LuaFrame::wrap(arc_row).await;

            // `this` is nil when no userdata injected (e.g. plain MapUnitTask).
            let this_val = scope_table
                .get::<mlua::Value>(SCOPE_THIS_KEY)
                .unwrap_or(mlua::Value::Nil);
            let exec_result = func.call_async::<bool>((lua_ctx, lua_row, this_val)).await;
            match exec_result {
                Ok(_) => {}
                Err(err) => println!("{err}"),
            };
            Ok(())
        })
    }
}

pub struct GraphLua(pub Arc<Mutex<Lua>>);

impl State for GraphLua {}

pub struct GraphTera(pub Arc<Mutex<Tera>>);

impl State for GraphTera {}
