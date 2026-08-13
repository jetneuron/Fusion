//! # Fusion Unit Redis
//!
//! Graph-unit that executes Lua scripts with a KV capability exposed
//! as the `this` parameter.
//!
//! ## YAML (Source — script-driven KV operations)
//!
//! ```yaml
//! units:
//!   - id: kv-reader
//!     type: RedisUnitTask
//!     config:
//!       datasource: redis-cache
//!       $script_type: lua
//!       $script: |
//!         local keys = this:scan("user:*")
//!         for _, entry in ipairs(keys) do
//!           local frame = ctx:newFrame()
//!           frame['key'] = entry['key']
//!           frame['value'] = entry['value']
//!           ctx:send(frame)
//!         end
//! ```
//!
//! ## YAML (Map — enrich frame with KV lookup)
//!
//! ```yaml
//! units:
//!   - id: enrich
//!     type: RedisUnitTask
//!     config:
//!       datasource: redis-cache
//!       $script_type: lua
//!       $script: |
//!         local val = this:get(data['user_id'])
//!         data['cached'] = val
//!         ctx:send(data)
//! ```

use fusion_derive::LogicalTask;
use fusion_streaming::runtime::scripts::{GraphLua, GraphTera};
use fusion_unit_sdk::capability::CapabilityKeyValueStore;
use fusion_unit_sdk::capability::capability_key_value_store::ScanOptions;
use fusion_unit_sdk::graph::types::{
    ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext, UnitMeta,
};
use fusion_unit_sdk::proto::transfer::Frame;
use fusion_unit_sdk::runtime::logical::LogicalTaskMeta;
use fusion_unit_sdk::runtime::script::Scripter;
use fusion_unit_sdk::runtime::script::script_registry;
use fusion_unit_sdk::runtime::state::GraphStates;
use fusion_unit_sdk::runtime::UnitResult;
use fusion_unit_sdk::units::config_util::UnitConfigExt;
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};
use mlua::{UserData, UserDataMethods};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

// ============================================================
// Plugin
// ============================================================

#[unsafe(no_mangle)]
pub extern "C" fn init_plugin() -> Box<dyn GraphUnitPlugin> {
    Box::new(RedisUnitPlugin {})
}

pub struct RedisUnitPlugin {}

impl GraphUnitPlugin for RedisUnitPlugin {
    fn register_units(&self) -> UnitManifest {
        let mut m = UnitManifest::default();
        RedisUnitTask::register_unit(&mut m, &self.plugin_version());
        m
    }
    fn plugin_version(&self) -> &str {
        "1.0.0"
    }
}

// ============================================================
// LuaKvStore — UserData exposed as `this`
// ============================================================

struct LuaKvStore {
    store: Arc<dyn CapabilityKeyValueStore>,
}

fn to_lua_err(e: fusion_unit_sdk::runtime::UnitError) -> mlua::Error {
    mlua::Error::external(e.to_string())
}

impl UserData for LuaKvStore {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method("get", |_, this, key: String| async move {
            let val = this.store.get(&key).await.map_err(to_lua_err)?;
            Ok(val.map(|b| String::from_utf8_lossy(&b).into_owned()))
        });

        methods.add_async_method(
            "set",
            |_, this, (key, val): (String, mlua::String)| async move {
                let bytes = val.as_bytes().to_vec();
                this.store.set(&key, &bytes).await.map_err(to_lua_err)
            },
        );

        methods.add_async_method("del", |_, this, key: String| async move {
            this.store.delete(&key).await.map_err(to_lua_err)
        });

        methods.add_async_method("exists", |_, this, key: String| async move {
            this.store.exists(&key).await.map_err(to_lua_err)
        });

        methods.add_async_method(
            "setex",
            |_, this, (key, val, ttl): (String, mlua::String, u64)| async move {
                let bytes = val.as_bytes().to_vec();
                this.store
                    .set_ex(&key, &bytes, ttl)
                    .await
                    .map_err(to_lua_err)
            },
        );

        methods.add_async_method(
            "incrby",
            |_, this, (key, delta): (String, i64)| async move {
                this.store.incr_by(&key, delta).await.map_err(to_lua_err)
            },
        );

        methods.add_async_method(
            "expire",
            |_, this, (key, ttl_ms): (String, u64)| async move {
                this.store.expire(&key, ttl_ms).await.map_err(to_lua_err)
            },
        );

        methods.add_async_method("scan", |lua, this, pattern: String| async move {
            let opts = ScanOptions::new().pattern(&pattern);
            let results = this
                .store
                .scan_all(&opts)
                .await
                .map_err(to_lua_err)?;
            let table = lua.create_table()?;
            for (i, (k, v)) in results.into_iter().enumerate() {
                let entry = lua.create_table()?;
                entry.set("key", k)?;
                entry.set("value", String::from_utf8_lossy(&v).into_owned())?;
                table.set(i + 1, entry)?;
            }
            Ok(table)
        });
    }
}

// ============================================================
// Script execution mode
// ============================================================

/// How the Lua script is executed.
enum ScriptMode {
    /// Static script — compiled once via scripter, cached across rows.
    Static(Arc<Mutex<Box<dyn Scripter + Send>>>),
    /// Dynamic script with `{{ ... }}` Tera placeholders baked in.
    /// Rendered per-frame (context varies), executed as a raw Lua chunk.
    Dynamic { template: String },
}

// ============================================================
// RedisUnitTask
// ============================================================

#[derive(Default, LogicalTask)]
pub struct RedisUnitTask {
    meta: UnitMeta,
    /// Config entry ID (e.g. "redis-cache").
    datasource: String,
    /// Resolved capability instance.
    store: Option<Arc<dyn CapabilityKeyValueStore>>,
    /// Script execution strategy — chosen in init() based on whether
    /// the script contains Tera expressions and the node role.
    script_mode: Option<ScriptMode>,
}

impl InitUnit for RedisUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let conf = unit
            .get_config()
            .ok_or_else(|| fusion_unit_sdk::runtime::UnitError::config_required("config"))?;

        // Resolve capability by config instance ID.
        self.datasource = conf.require_string("datasource")?;
        self.store = Some(
            fusion_unit_sdk::capability::kv(&self.datasource).ok_or_else(|| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "capability not found for datasource `{}`",
                    self.datasource
                ))
            })?,
        );

        let raw_script = conf.require_string("$script")?;
        let states = unit
            .get_runtime_states()
            .ok_or_else(|| {
                fusion_unit_sdk::runtime::UnitError::ScriptInitErr(
                    "runtime states unready".into(),
                )
            })?;
        let script_type = conf
            .extract_string("$script_type")?
            .unwrap_or_else(|| "lua".into())
            .to_lowercase();

        let has_tera = raw_script.contains("{{");

        self.script_mode = if has_tera && unit.is_source() {
            // Source + Tera: render once, compile once via scripter.
            let tera_state = states.state::<GraphTera>().map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::ScriptInitErr(format!(
                    "GraphTera not found: {e}"
                ))
            })?;
            let mut tera = tera_state.0.try_lock().map_err(|_| {
                fusion_unit_sdk::runtime::UnitError::ScriptInitErr(
                    "Tera engine is busy".into(),
                )
            })?;
            let rendered = tera
                .render_str(&raw_script, &tera::Context::new())
                .map_err(|e| {
                    fusion_unit_sdk::runtime::UnitError::ScriptInitErr(format!(
                        "Tera render failed: {e}"
                    ))
                })?;
            drop(tera);
            let scripter = script_registry::create_scripter(&script_type, rendered, states);
            Some(ScriptMode::Static(scripter))
        } else if has_tera && unit.is_mapper() {
            // Map + Tera: render per-frame, execute as raw Lua chunk.
            Some(ScriptMode::Dynamic {
                template: raw_script,
            })
        } else {
            // No Tera: static script via scripter (source or map).
            let scripter = script_registry::create_scripter(&script_type, raw_script, states);
            Some(ScriptMode::Static(scripter))
        };

        Ok(())
    }
}

impl RedisUnitTask {
    /// Inject `this` userdata into scope table. Sync — caller holds Lua lock.
    fn inject_this_into_scope(
        lua: &mlua::Lua,
        scope_table: &mlua::Table,
        store: Arc<dyn CapabilityKeyValueStore>,
    ) -> UnitResult<()> {
        let ud = lua
            .create_userdata(LuaKvStore { store })
            .map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!("create LuaKvStore: {e}"))
            })?;
        scope_table.set("this", ud).map_err(|e| {
            fusion_unit_sdk::runtime::UnitError::unknown(format!("set this: {e}"))
        })
    }

    /// Execute via the cached scripter (static script, no per-frame Tera).
    async fn run_static(
        &self,
        frame: Frame,
        ctx: &TaskContext,
        states: &GraphStates,
        scripter: &Arc<Mutex<Box<dyn Scripter + Send>>>,
    ) -> UnitResult<()> {
        let task_id = self.meta.get_id();

        // Inject `this` userdata if not already present.
        {
            // Per-worker Lua VM (parallelism > 1) or global GraphLua.
            let lua_arc: Arc<tokio::sync::Mutex<mlua::Lua>> = match ctx.worker_lua.clone() {
                Some(lua) => lua,
                None => states
                    .state::<GraphLua>()
                    .map_err(|e| {
                        fusion_unit_sdk::runtime::UnitError::unknown(format!(
                            "GraphLua not found: {e}"
                        ))
                    })?
                    .0
                    .clone(),
            };
            let lua = lua_arc.lock().await;
            let globals = lua.globals();
            let scope_table: mlua::Table = globals.get(task_id.as_str()).map_err(|_| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "scope table `{task_id}` not ready"
                ))
            })?;
            Self::inject_this_into_scope(
                &lua,
                &scope_table,
                self.store
                    .clone()
                    .ok_or_else(|| fusion_unit_sdk::runtime::UnitError::unknown("store not ready"))?,
            )?;
        }

        // Take the eval future under the lock, then drop the guard
        // before awaiting — parallel workers don't serialize here.
        let eval_fut = {
            let scripter = scripter.lock().await;
            scripter.frame_eval(&task_id, states.clone(), ctx, frame)
        };
        eval_fut.await
    }

    /// Execute with per-frame Tera rendering (map mode only).
    async fn run_dynamic(
        &self,
        frame: Frame,
        ctx: &TaskContext,
        states: &GraphStates,
        template: &str,
    ) -> UnitResult<()> {
        use fusion_streaming::runtime::core::LuaContext;
        let task_id = self.meta.get_id();

        // Per-worker Lua VM (parallelism > 1) or global GraphLua.
        let lua_arc: Arc<tokio::sync::Mutex<mlua::Lua>> = match ctx.worker_lua.clone() {
            Some(lua) => lua,
            None => states
                .state::<GraphLua>()
                .map_err(|e| {
                    fusion_unit_sdk::runtime::UnitError::unknown(format!(
                        "GraphLua not found: {e}"
                    ))
                })?
                .0
                .clone(),
        };

        // Render Tera with frame column values.
        let mut tera_ctx = tera::Context::new();
        for col in &frame.columns {
            let val = match col.dt.unwrap() {
                fusion_unit_sdk::proto::transfer::DataType::str => col.str_val.clone(),
                fusion_unit_sdk::proto::transfer::DataType::i32 => col.i32_val.to_string(),
                fusion_unit_sdk::proto::transfer::DataType::i64 => col.i64_val.to_string(),
                fusion_unit_sdk::proto::transfer::DataType::f32 => col.f32_val.to_string(),
                fusion_unit_sdk::proto::transfer::DataType::f64 => col.f64_val.to_string(),
                fusion_unit_sdk::proto::transfer::DataType::bool => col.bool_val.to_string(),
                _ => String::new(),
            };
            tera_ctx.insert(&col.field, &val);
        }

        let tera_state = states.state::<GraphTera>().map_err(|e| {
            fusion_unit_sdk::runtime::UnitError::unknown(format!("GraphTera not found: {e}"))
        })?;
        let mut tera = tera_state.0.lock().await;
        let script = tera
            .render_str(template, &tera_ctx)
            .map_err(|e| fusion_unit_sdk::runtime::UnitError::unknown(format!("Tera: {e}")))?;
        drop(tera);

        // Execute as raw Lua chunk.
        let lua = lua_arc.lock().await;

        // Build data table from frame columns.
        let data = lua.create_table().map_err(|e| {
            fusion_unit_sdk::runtime::UnitError::unknown(format!("create table: {e}"))
        })?;
        for col in &frame.columns {
            let r: mlua::Result<()> = match col.dt.unwrap() {
                fusion_unit_sdk::proto::transfer::DataType::str => {
                    data.set(col.field.clone(), col.str_val.clone())
                }
                fusion_unit_sdk::proto::transfer::DataType::i32 => {
                    data.set(col.field.clone(), col.i32_val)
                }
                fusion_unit_sdk::proto::transfer::DataType::i64 => {
                    data.set(col.field.clone(), col.i64_val)
                }
                fusion_unit_sdk::proto::transfer::DataType::f32 => {
                    data.set(col.field.clone(), col.f32_val)
                }
                fusion_unit_sdk::proto::transfer::DataType::f64 => {
                    data.set(col.field.clone(), col.f64_val)
                }
                fusion_unit_sdk::proto::transfer::DataType::bool => {
                    data.set(col.field.clone(), col.bool_val)
                }
                _ => Ok(()),
            };
            r.map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!("set column: {e}"))
            })?;
        }

        let globals = lua.globals();
        let scope_table: mlua::Table = globals.get(task_id.as_str()).map_err(|_| {
            fusion_unit_sdk::runtime::UnitError::unknown(format!(
                "scope table `{task_id}` not found"
            ))
        })?;
        let this_val: mlua::Value = scope_table.get("this").map_err(|_| {
            fusion_unit_sdk::runtime::UnitError::unknown("`this` not found in scope table")
        })?;

        let lua_ctx = LuaContext::wrap(ctx.clone());
        let chunk = format!(
            "local ctx, data, this = ...\n{}\nreturn true",
            script
        );
        let func: mlua::Function = lua
            .load(&chunk)
            .into_function()
            .map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!("Lua compile: {e}"))
            })?;
        func.call_async::<bool>((lua_ctx, data, this_val))
            .await
            .map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!("Lua exec: {e}"))
            })?;
        Ok(())
    }

    async fn run_script(
        &self,
        frame: Frame,
        ctx: &TaskContext,
        states: &GraphStates,
    ) -> UnitResult<()> {
        match self.script_mode.as_ref() {
            Some(ScriptMode::Static(scripter)) => {
                self.run_static(frame, ctx, states, scripter).await
            }
            Some(ScriptMode::Dynamic { template }) => {
                self.run_dynamic(frame, ctx, states, template).await
            }
            None => Err(fusion_unit_sdk::runtime::UnitError::unknown(
                "script mode not initialized",
            )),
        }
    }
}

// ============================================================
// Source — execute Lua once with empty seed frame
// ============================================================

impl SourceUnit for RedisUnitTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        let states = ctx.states.clone();
        Ok(async move {
            self.run_script(Frame::new(), &ctx, &states).await
        })
    }
}

// ============================================================
// Map — execute Lua for each incoming frame
// ============================================================

impl MapUnit for RedisUnitTask {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        frame: Frame,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let states = ctx.states.clone();
        Ok(async move { self.run_script(frame, ctx, &states).await })
    }
}
