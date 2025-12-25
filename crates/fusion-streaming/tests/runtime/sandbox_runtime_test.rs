use fusion_streaming::runtime::GraphRuntime;
use fusion_streaming::runtime::plugin::PluginManager;
use fusion_streaming::runtime::sandbox::SandboxRuntime;

#[tokio::test]
pub async fn test_base_runtime() {
    // step1. create plugin manager, which had provided extension plugin implementations.
    let plugin_mgr = PluginManager::new().await;
    let dylib_path = format!(
        "{}/../graph-units/graph-unit-example/target/release/libgraph_unit_example.dylib",
        env!("CARGO_MANIFEST_DIR")
    );

    // step2. register the plugin by provided lib's path
    plugin_mgr.register_plugin(&dylib_path).await;

    // step3. initialize the global runtime.
    let runtime = SandboxRuntime::new(plugin_mgr);

    // step4. execute the graph by provided descriptor. test graph config file at location tests/graphs/
    // we will transform the description of graph as `LogicalTask`, and then transform as `PhysicalTask`
    // automatic.
    let file = "example_unit.yaml";
    let graph_path = format!(
        "file://{}/tests/graphs/{}",
        env!("CARGO_MANIFEST_DIR"),
        file
    );
    let graph = runtime.create(graph_path);

    // execute the physical graph.
    graph.execute().await;
}
