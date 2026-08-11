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
//!           local row = ctx:newRow()
//!           row['key'] = entry['key']
//!           row['value'] = entry['value']
//!           ctx:send(row)
//!         end
//! ```
//!
//! ## YAML (Map — enrich row with KV lookup)
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
use fusion_streaming::runtime::scripts::GraphLua;
use fusion_unit_sdk::capability::CapabilityKeyValueStore;
use fusion_unit_sdk::capability::capability_key_value_store::ScanOptions;
use fusion_unit_sdk::graph::types::{
    ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext, UnitMeta,
};
use fusion_unit_sdk::proto::transfer::Row;
use fusion_unit_sdk::runtime::logical::LogicalTaskMeta;
use fusion_unit_sdk::runtime::script::Scripter;
use fusion_unit_sdk::runtime::script::script_registry;
use fusion_unit_sdk::runtime::UnitResult;
use fusion_unit_sdk::units::config_util::UnitConfigExt;
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};
use mlua::{RegistryKey, UserData, UserDataMethods};
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

/// Convert `UnitError` to `mlua::Error`.
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
// RedisUnitTask
// ============================================================

#[derive(Default, LogicalTask)]
pub struct RedisUnitTask {
    meta: UnitMeta,
    /// Config entry ID (e.g. "redis-cache").
    datasource: String,
    /// Resolved capability.
    store: Option<Arc<dyn CapabilityKeyValueStore>>,
    /// Lua scripter.
    scripter: Option<Arc<Mutex<Box<dyn Scripter + Send>>>>,
}

impl InitUnit for RedisUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        let conf = unit
            .get_config()
            .ok_or_else(|| fusion_unit_sdk::runtime::UnitError::config_required("config"))?;

        self.datasource = conf.require_string("datasource")?;
        self.store = Some(
            fusion_unit_sdk::capability::kv(&self.datasource).ok_or_else(|| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "capability not found for datasource `{}`",
                    self.datasource
                ))
            })?,
        );

        let script = conf.require_string("$script")?;
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

        self.scripter = Some(script_registry::create_scripter(
            &script_type,
            script,
            states,
        ));

        Ok(())
    }
}

impl RedisUnitTask {
    /// Lazily create the Lua registry entry for `this`.
    async fn make_this_key(
        &self,
        states: &fusion_unit_sdk::runtime::state::GraphStates,
    ) -> UnitResult<RegistryKey> {
        let lua_state = states.state::<GraphLua>().map_err(|e| {
            fusion_unit_sdk::runtime::UnitError::unknown(format!("GraphLua not found: {e}"))
        })?;
        let lua = lua_state.0.lock().await;
        let store = self
            .store
            .clone()
            .ok_or_else(|| fusion_unit_sdk::runtime::UnitError::unknown("store not initialized"))?;
        let ud = lua
            .create_userdata(LuaKvStore { store })
            .map_err(|e| {
                fusion_unit_sdk::runtime::UnitError::unknown(format!(
                    "create LuaKvStore userdata: {e}"
                ))
            })?;
        lua.create_registry_value(ud).map_err(|e| {
            fusion_unit_sdk::runtime::UnitError::unknown(format!("create registry value: {e}"))
        })
    }

    async fn run_script(
        &self,
        row: Row,
        ctx: &TaskContext,
        states: &fusion_unit_sdk::runtime::state::GraphStates,
    ) -> UnitResult<()> {
        let key = self.make_this_key(states).await?;
        let scripter = self
            .scripter
            .as_ref()
            .ok_or_else(|| fusion_unit_sdk::runtime::UnitError::unknown("scripter not ready"))?;
        let scripter = scripter.lock().await;
        let id = self.meta.get_id();
        scripter
            .row_eval(&id, states.clone(), ctx, row, Some(key))
            .await?;
        Ok(())
    }
}

// ============================================================
// Source
// ============================================================

impl SourceUnit for RedisUnitTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        let states = ctx.states.clone();
        Ok(async move {
            // Source mode: empty row as seed for the script.
            self.run_script(Row::new(), &ctx, &states).await
        })
    }
}

// ============================================================
// Map
// ============================================================

impl MapUnit for RedisUnitTask {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let states = ctx.states.clone();
        Ok(async move { self.run_script(row, ctx, &states).await })
    }
}
