use fusion_derive::LogicalTask;
use fusion_unit_sdk::graph::types::{ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext};
use fusion_unit_sdk::proto::transfer::Row;
use fusion_unit_sdk::runtime::UnitResult;
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};
use std::future::Future;
use std::sync::Arc;

#[unsafe(no_mangle)]
pub extern "C" fn init_plugin() -> Box<dyn GraphUnitPlugin> {
    Box::new(UniversalFsUnitPlugin {})
}

pub struct UniversalFsUnitPlugin {}

impl GraphUnitPlugin for UniversalFsUnitPlugin {
    fn register_units(&self) -> UnitManifest {
        let mut unit_manifest = UnitManifest::default();
        UniversalFsUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());

        unit_manifest
    }
    fn plugin_version(&self) -> &str {
        "1.0.0"
    }
}

#[derive(Default, LogicalTask)]
pub struct UniversalFsUnitTask {
    uri: String,
    formatter: Option<String>,
    separator: Option<String>,
}

impl InitUnit for UniversalFsUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        unit.get_config().map(|c| {
            self.uri = c["uri"]
                .as_str()
                .expect("uri must be specified")
                .to_string();
        });
        Ok(())
    }
}

impl SourceUnit for UniversalFsUnitTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        tokio::task::spawn_blocking(move || {
            todo!();
        });
        Ok(async move { Ok(()) })
    }
}

impl MapUnit for UniversalFsUnitTask {
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
