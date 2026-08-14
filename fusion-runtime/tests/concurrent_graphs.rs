//! Multi-graph concurrency test — verifies per-graph script engine
//! isolation when two graphs with identical unit ids run concurrently.

use fusion_runtime::{FusionRuntimeBuilder, GraphExecutor};
use std::sync::Arc;

fn graph_yaml(id: &str, times: i64, marker: &str, interval: i64) -> String {
    format!(
        r#"
name: concurrent_{id}
units:
  - id: src
    name: src
    type: DebugInputUnitTask
    version: builtin
    config:
      times: {times}
      column_count: 1
      interval: {interval}
      gen_mode: ascending

  - id: map
    name: map
    type: MapUnitTask
    version: builtin
    config:
      $script: |
        local ctx, data, this = ...
        local out = ctx:newFrame()
        out['{marker}'] = data['c0']
        ctx:send(out)
      $script_type: Lua

  - id: output
    name: output
    type: DebugOutputUnitTask
    version: builtin
    config:
      show_report: true

edges:
  - id: e1
    source: src
    target: map
  - id: e2
    source: map
    target: output
"#
    )
}

#[tokio::test]
async fn concurrent_graphs_are_isolated() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error"))
        .filter_module("fusion", log::LevelFilter::Info)
        .try_init()
        .ok();

    let runtime = Arc::new(
        FusionRuntimeBuilder::new()
            .with_builtin_units()
            .build()
            .await?,
    );
    let executor = GraphExecutor::new(runtime.clone());

    // Same unit ids (`src`/`map`/`output`) in both graphs — a shared Lua
    // VM would have the second graph's scope table overwrite the first's.
    let graph_a = graph_yaml("graph_a", 50, "out_a", 30);
    let graph_b = graph_yaml("graph_b", 50, "out_b", 10);

    let id_a = executor.submit(graph_a, None).await;
    let id_b = executor.submit(graph_b, None).await;

    // Wait for both concurrently.
    let (ra, rb) = tokio::join!(executor.wait(&id_a), executor.wait(&id_b));
    assert!(ra.is_some(), "graph A handle missing");
    assert!(rb.is_some(), "graph B handle missing");
    assert!(
        ra.unwrap().is_ok(),
        "graph A failed — script scope likely crossed graphs"
    );
    assert!(
        rb.unwrap().is_ok(),
        "graph B failed — script scope likely crossed graphs"
    );

    // Executor bookkeeping: wait() consumed the handles, so status is
    // None (reaped) and nothing is running.
    assert!(executor.status(&id_a).await.is_none());
    assert!(executor.status(&id_b).await.is_none());
    assert_eq!(executor.running_count().await, 0);
    Ok(())
}
