use fusion_derive::{LogicalTask, MapLogicTask, SrcLogicTask};
use fusion_unit_sdk::{
    GraphUnitPlugin, UnitManifest,
    graph::types::{InitUnit, SourceUnit, TaskContext, UnitMeta},
    runtime::UnitResult,
    units::config_util::UnitConfigExt,
};
use std::{sync::Arc, u32};

#[unsafe(no_mangle)]
pub extern "C" fn init_plugin() -> Box<dyn GraphUnitPlugin> {
    Box::new(NetUnitPlugin {})
}

pub struct NetUnitPlugin {}

impl GraphUnitPlugin for NetUnitPlugin {
    fn register_units(&self) -> fusion_unit_sdk::UnitManifest {
        let mut unit_manifest = UnitManifest::default();
        HttpEndpointUnitTask::register_unit(&mut unit_manifest, &self.plugin_version());
        unit_manifest
    }

    fn plugin_version(&self) -> &str {
        "1.0.0"
    }
}

#[derive(Default, SrcLogicTask)]
pub struct HttpEndpointUnitTask {
    meta: UnitMeta,
    max_connections: Option<u32>,
    max_content_length: Option<u32>,
    port: Option<u16>,
    uri: Option<String>,
    api_key: Option<String>,
    remote_shutdown_enabled: Option<bool>,
}

impl InitUnit for HttpEndpointUnitTask {
    fn init(
        &mut self,
        unit: fusion_unit_sdk::graph::types::ComputingUnit,
    ) -> fusion_unit_sdk::runtime::UnitResult<()> {
        self.max_connections = Some(3);
        self.max_content_length = Some(u32::MAX);
        self.port = None;
        self.uri = Some(format!("/source/{}", &self.meta.get_id()));
        self.api_key = None;
        self.remote_shutdown_enabled = Some(false);

        if let Some(Err(err)) = unit.get_config().map::<UnitResult<()>, _>(|c| {
            self.max_connections = c.extract_u32("max_connections")?.or(self.max_connections);
            self.max_content_length = c
                .extract_u32("max_content_length")?
                .or(self.max_content_length);

            self.port = c.extract_u32("port")?.map(|v| v as u16).or(self.port);
            self.uri = c.extract_string("uri")?.or(self.uri.clone());
            self.api_key = c.extract_string("api_key")?.or(self.api_key.clone());
            self.remote_shutdown_enabled = c
                .extract_bool("remote_shutdown_enabled")?
                .or(self.remote_shutdown_enabled);
            Ok(())
        }) {
            return Err(err);
        }
        Ok(())
    }
}

impl SourceUnit for HttpEndpointUnitTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        Ok(async move {
            // TODO:
            Ok(())
        })
    }
}
