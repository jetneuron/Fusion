use fusion_capability_redis::RedisCapabilityPlugin;
use fusion_streaming::runtime::core::LaunchEnv;
use fusion_streaming::runtime::plugin::PluginManager;
use fusion_streaming::runtime::sandbox::SandboxRuntime;
use fusion_streaming::runtime::GraphRuntime;
use fusion_unit_sdk::capability;
use fusion_unit_sdk::capability::CapabilityPlugin;
use fusion_unit_sdk::config;

use crate::TestPlugin;
use serde_json::json;

/// Execute a graph with Redis capability pre-registered.
async fn execute_with_redis(graph: &str) -> anyhow::Result<()> {
    // Register config entry for Redis.
    config::register(|reg| {
        reg.insert(
            "datasource",
            "redis",
            "test-redis",
            json!({"host": "127.0.0.1", "port": 6379}),
        );
    });

    // Register Redis capability factory.
    let redis_plugin = RedisCapabilityPlugin;
    redis_plugin.register();

    let mut launch_env = LaunchEnv::default();
    launch_env.update_env(Some(json!({
        "SANDBOX": true,
        "LAUNCH_TIME": "{{ time() }}",
        "CARGO_MANIFEST_DIR": env!("CARGO_MANIFEST_DIR"),
    })));

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error"))
        .filter_module("fusion", log::LevelFilter::Trace)
        .init();

    let plugin_manager = PluginManager::new().await;
    plugin_manager
        .add_plugin("test", Box::new(TestPlugin::default()))
        .await;
    let mut runtime = SandboxRuntime::new(plugin_manager);

    let graph_path = format!(
        "file://{}/tests/graphs/{}",
        env!("CARGO_MANIFEST_DIR"),
        graph
    );
    let graph = runtime.create(graph_path);
    graph.execute(Some(launch_env)).await?;
    Ok(())
}

/// Source → RedisUnitTask (Lua `this:set`) → DebugOutput.
/// Validates the full script-driven Redis flow.
#[tokio::test]
async fn test_redis_lua_set() -> anyhow::Result<()> {
    execute_with_redis("redis_lua_set.yaml").await?;
    Ok(())
}
