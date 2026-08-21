mod excel;
mod unit_test;

use fusion_streaming::runtime::GraphRuntime;
use fusion_streaming::runtime::plugin::PluginManager;
use fusion_streaming::runtime::sandbox::SandboxRuntime;
use fusion_streaming::task::builtin::{DebugInputUnitTask, DebugMapUnitTask, DebugOutputUnitTask};
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};

use fusion_streaming::runtime::core::LaunchEnv;
use fusion_unit_datafusion::SqlUnitTask;
use fusion_unit_excel::SpreadSheetUnitTask;
use fusion_unit_net::HttpEndpointUnitTask;
use fusion_unit_redis::RedisUnitTask;
use fusion_unit_ssh::SSHUnitTask;
use fusion_unit_universal_fs::UniversalFsUnitTask;
use serde_json::{Value, json};

#[derive(Default)]
pub(crate) struct TestPlugin {}

impl GraphUnitPlugin for TestPlugin {
    fn register_units(&self) -> UnitManifest {
        let mut manifest = UnitManifest::default();
        // Debug
        DebugInputUnitTask::register_unit(&mut manifest, self.plugin_version());
        DebugMapUnitTask::register_unit(&mut manifest, self.plugin_version());
        DebugOutputUnitTask::register_unit(&mut manifest, self.plugin_version());

        // Excel
        SpreadSheetUnitTask::register_unit(&mut manifest, self.plugin_version());

        // SSH
        SSHUnitTask::register_unit(&mut manifest, self.plugin_version());

        // Universal FS
        UniversalFsUnitTask::register_unit(&mut manifest, self.plugin_version());

        // Net (HTTP ingest endpoint)
        HttpEndpointUnitTask::register_unit(&mut manifest, self.plugin_version());

        // Redis (script-driven KV)
        RedisUnitTask::register_unit(&mut manifest, self.plugin_version());

        // DataFusion (SQL engine)
        SqlUnitTask::register_unit(&mut manifest, self.plugin_version());
        manifest
    }
}

pub(crate) async fn register_plugin_execute(
    plugin_paths: Vec<String>,
    graph: &str,
) -> anyhow::Result<()> {
    let plugin_manager = PluginManager::new().await;
    plugin_manager
        .add_plugin("test", Box::new(TestPlugin::default()))
        .await;
    for path in plugin_paths.iter() {
        plugin_manager.register_plugin(path).await;
    }
    let mut runtime = SandboxRuntime::new(plugin_manager);
    let file = graph;
    let graph_path = format!(
        "file://{}/tests/graphs/{}",
        env!("CARGO_MANIFEST_DIR"),
        file
    );
    let graph = runtime.create(graph_path);

    // execute the physical graph.
    graph.execute(None).await?;
    Ok(())
}

pub(crate) async fn execute_with_env(graph: &str, params: Option<Value>) -> anyhow::Result<()> {
    let mut launch_env = LaunchEnv::default();
    launch_env.update_params(params);
    launch_env.update_env(Some(json!({
        "SANDBOX": true,
        "LAUNCH_TIME": "{{ time() }}",
        "CARGO_MANIFEST_DIR": env!("CARGO_MANIFEST_DIR"),
    })));

    // try_init: tests run in parallel — the first init wins, later
    // ones no-op instead of panicking (init() would SetLoggerError).
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error"))
        .filter_module("fusion", log::LevelFilter::Trace)
        .try_init();
    let plugin_manager = PluginManager::new().await;
    plugin_manager
        .add_plugin("test", Box::new(TestPlugin::default()))
        .await;
    let mut runtime = SandboxRuntime::new(plugin_manager);
    let file = graph;
    let graph_path = format!(
        "file://{}/tests/graphs/{}",
        env!("CARGO_MANIFEST_DIR"),
        file
    );
    let graph = runtime.create(graph_path);

    // execute the physical graph.
    graph.execute(Some(launch_env)).await?;
    Ok(())
}

pub(crate) async fn execute(graph: &str) -> anyhow::Result<()> {
    execute_with_env(graph, None).await
}