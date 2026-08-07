use deadpool_redis::{ConnectionAddr, ConnectionInfo, Pool, RedisConnectionInfo, Runtime};
use fusion_derive::LogicalTask;
use fusion_unit_sdk::graph::types::{ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext, UnitMeta};
use fusion_unit_sdk::proto::transfer::Row;
use fusion_unit_sdk::runtime::script::{Script, ScriptContext};
use fusion_unit_sdk::runtime::UnitResult;
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};
use std::sync::Arc;

#[unsafe(no_mangle)]
pub extern "C" fn init_plugin() -> Box<dyn GraphUnitPlugin> {
    Box::new(RedisUnitPlugin {})
}

pub struct RedisUnitPlugin {}

impl GraphUnitPlugin for RedisUnitPlugin {
    fn register_units(&self) -> UnitManifest {
        let mut unit_manifest = UnitManifest::default();
        RedisUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());
        unit_manifest
    }
    fn plugin_version(&self) -> &str {
        "1.0.0"
    }
}

#[derive(Default, LogicalTask)]
pub struct RedisUnitTask {
    meta: UnitMeta,
    host: String,
    port: u16,
    db: i64,
    username: Option<String>,
    password: Option<String>,
    script: Script,
}

impl RedisUnitTask {
    pub async fn initialize_pool(&self) -> anyhow::Result<Pool> {
        let mut connection_info = ConnectionInfo::default();
        connection_info.addr = ConnectionAddr::Tcp(self.host.clone(), self.port);

        let mut redis_connection_info = RedisConnectionInfo::default();
        redis_connection_info.username = self.username.clone();
        redis_connection_info.password = self.password.clone();
        redis_connection_info.db = self.db;
        connection_info.redis = redis_connection_info;
        let cfg = deadpool_redis::Config::from_connection_info(connection_info);
        Ok(cfg.create_pool(Some(Runtime::Tokio1))?)
    }
}

impl InitUnit for RedisUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        Ok(())
    }
}

impl SourceUnit for RedisUnitTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        Ok(async move {
            let pool = self.initialize_pool().await?;
            let script_context = ScriptContext::default();
            self.script
                .runtime(script_context, async |eval_script| {
                    let connection = pool.get().await?;
                    println!("{}", eval_script);
                    Ok(())
                })
                .await?;
            Ok(())
        })
    }
}

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
        Ok(async move { Ok(()) })
    }
}
