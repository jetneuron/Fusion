//! Fusion Tauri backend.
//!
//! Owns the [`FusionRuntime`] and a [`GraphExecutor`] as app state, and
//! exposes them to the frontend via tauri commands:
//!
//! - [`run_graph`] — submit a YAML graph, returns its id
//! - [`graph_status`] / [`graph_cancel`] / [`graph_wait`] / [`running_count`]
//!
//! The runtime is initialized from the embedded app config
//! (`config/fusion-conf-app.yaml`). In dev mode the relative `assets/libs`
//! paths resolve from the `app/` working directory; in packaged builds the
//! plugin dylibs are re-pointed to the bundle resource directory.

use fusion_runtime::config::{FusionConfig, LibPathsConfig};
use fusion_runtime::{FusionRuntime, FusionRuntimeBuilder, GraphExecutor, GraphStatus};
use std::sync::Arc;
use tauri::{Manager, State};

/// Initialize the `log` facade so fusion crates' logs reach stderr.
/// Same configuration as the integration tests: everything under the
/// `fusion` module prefix at trace level, everything else at error.
fn init_logger() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error"))
        .filter_module("fusion", log::LevelFilter::Trace)
        .init();
}

/// App-managed state: the fusion runtime plus its background executor.
struct AppState {
    #[allow(dead_code)]
    runtime: Arc<FusionRuntime>,
    executor: GraphExecutor,
}

/// Submit a YAML graph for background execution. Returns the graph id.
#[tauri::command]
async fn run_graph(state: State<'_, AppState>, yaml: String) -> Result<String, String> {
    Ok(state.executor.submit(yaml, None).await)
}

/// Current status of a graph: `"running"`, `"done"`, or `"failed: <msg>"`.
/// `None` if the id is unknown (never submitted, or already reaped).
#[tauri::command]
async fn graph_status(state: State<'_, AppState>, id: String) -> Result<Option<String>, String> {
    Ok(state.executor.status(&id).await.map(|s| match s {
        GraphStatus::Running => "running".to_string(),
        GraphStatus::Done => "done".to_string(),
        GraphStatus::Failed(msg) => format!("failed: {msg}"),
    }))
}

/// Cancel a running graph. Returns `true` if it was still running.
#[tauri::command]
async fn graph_cancel(state: State<'_, AppState>, id: String) -> Result<bool, String> {
    Ok(state.executor.cancel(&id).await)
}

/// Block until a graph finishes; errors surface the failure message.
#[tauri::command]
async fn graph_wait(state: State<'_, AppState>, id: String) -> Result<String, String> {
    match state.executor.wait(&id).await {
        Some(Ok(())) => Ok("done".to_string()),
        Some(Err(e)) => Err(e.to_string()),
        None => Err("graph not found or already reaped".to_string()),
    }
}

/// Number of currently running graphs.
#[tauri::command]
async fn running_count(state: State<'_, AppState>) -> Result<usize, String> {
    Ok(state.executor.running_count().await)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            run_graph,
            graph_status,
            graph_cancel,
            graph_wait,
            running_count
        ])
        .setup(|app| {
            init_logger();
            tauri::async_runtime::block_on(async {
                // Dev: embedded config's `assets/libs` paths resolve from
                // the cwd (`src-tauri/assets` → symlink → `app/assets`).
                // Packaged: re-point to the bundle resource directory.
                // NOTE: resource_dir() is Ok(exe_dir) in dev (target/),
                // so it must only be used when not dev.
                let mut cfg = FusionConfig::load_or_embedded()?;
                if !cfg!(dev) {
                    if let Ok(resource_dir) = app.path().resource_dir() {
                        let libs = resource_dir.join("assets").join("libs");
                        cfg.libs = LibPathsConfig {
                            capability: vec![libs.join("capability").display().to_string()],
                            unit: vec![libs.join("unit").display().to_string()],
                        };
                    }
                }

                let runtime = Arc::new(
                    FusionRuntimeBuilder::new()
                        .with_builtin_units()
                        .with_config(cfg)
                        .build()
                        .await
                        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?,
                );
                let executor = GraphExecutor::new(runtime.clone());
                app.manage(AppState { runtime, executor });
                Ok(())
            })
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
