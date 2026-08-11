use fusion_capability_datafusion::DataFusionCapabilityPlugin;
use fusion_streaming::runtime::core::LaunchEnv;
use fusion_streaming::runtime::plugin::PluginManager;
use fusion_streaming::runtime::sandbox::SandboxRuntime;
use fusion_streaming::runtime::GraphRuntime;
use fusion_streaming::utils::tera_func::RegisterTeraBuiltinFunc;
use fusion_unit_sdk::capability::{self, CapabilityPlugin};
use fusion_unit_sdk::config;

use crate::TestPlugin;
use serde_json::json;

async fn execute_with_datafusion(graph: &str) -> anyhow::Result<()> {
    // Config entry for the DataFusion engine itself.
    config::register(|reg| {
        reg.insert(
            "datasource",
            "datafusion",
            "datafusion",
            json!({}),
        );
    });

    // Config entry for the CSV data file.
    config::register(|reg| {
        reg.insert(
            "datasource",
            "csv",
            "csv-test-data",
            json!({
                "path": format!(
                    "{}/tests/data/capitalized_example.csv",
                    env!("CARGO_MANIFEST_DIR")
                )
            }),
        );
    });

    // Register built-in CSV/TSV table providers.
    fusion_unit_datafusion::providers::csv::register_csv_providers();

    // Register DataFusion capability factory.
    let df_plugin = DataFusionCapabilityPlugin;
    df_plugin.register();

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
    runtime.register_builtin_tera_functions().await;

    let graph_path = format!(
        "file://{}/tests/graphs/{}",
        env!("CARGO_MANIFEST_DIR"),
        graph
    );
    let graph = runtime.create(graph_path);
    graph.execute(Some(launch_env)).await?;
    Ok(())
}

/// Source → SqlUnitTask (CSV via DataFusion) → DebugOutput.
#[tokio::test]
async fn test_datafusion_csv_source() -> anyhow::Result<()> {
    execute_with_datafusion("datafusion_csv_source.yaml").await?;
    Ok(())
}

/// Source → SqlUnitTask (SQLite via DataFusion) → DebugOutput.
#[tokio::test]
async fn test_datafusion_sqlite_source() -> anyhow::Result<()> {
    // Register SQLite table provider.
    use fusion_unit_datafusion::providers::ProviderPlugin;
    fusion_unit_datafusion_sqlite::SqliteProviderPlugin.register();

    // Config entry for the SQLite db file.
    config::register(|reg| {
        reg.insert(
            "datasource",
            "sqlite",
            "sqlite-test-db",
            json!({
                "path": format!(
                    "{}/tests/data/test_datafusion.db",
                    env!("CARGO_MANIFEST_DIR")
                ),
                "table": "users",
            }),
        );
    });

    execute_with_datafusion("datafusion_sqlite_source.yaml").await?;
    Ok(())
}

/// Source → SqlUnitTask (SQLite subquery with GROUP BY + aggregation) → DebugOutput.
/// Tests that a complex subquery can serve as a table provider with proper
/// multi-type column output.
#[tokio::test]
async fn test_datafusion_sqlite_aggregation() -> anyhow::Result<()> {
    use fusion_unit_datafusion::providers::ProviderPlugin;
    fusion_unit_datafusion_sqlite::SqliteProviderPlugin.register();

    config::register(|reg| {
        reg.insert(
            "datasource",
            "sqlite",
            "sqlite-test-db",
            json!({
                "path": format!(
                    "{}/tests/data/test_datafusion.db",
                    env!("CARGO_MANIFEST_DIR")
                ),
                "table": "employees",
            }),
        );
    });

    execute_with_datafusion("datafusion_sqlite_aggr.yaml").await
}

