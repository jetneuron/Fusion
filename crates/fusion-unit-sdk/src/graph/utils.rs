//! Graph runtime utilities — directory layout and env-based config.

use std::path::PathBuf;

// ---- Environment variable names ----

const ENV_FUSION_ROOT: &str = "FUSION_ROOT";
const ENV_FUSION_DATA_ROOT: &str = "FUSION_DATA_ROOT";

// ---- Defaults ----

fn default_fusion_root() -> PathBuf {
    PathBuf::from("/tmp/fusion")
}

// ---- Resolution functions ----

/// Root directory for all Fusion project data.
/// Controlled by `FUSION_ROOT` env var; defaults to `/tmp/fusion`.
pub fn fusion_root() -> PathBuf {
    std::env::var(ENV_FUSION_ROOT)
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_fusion_root())
}

/// Data directory for Fusion.
/// Controlled by `FUSION_DATA_ROOT` env var; defaults to `$FUSION_ROOT/data`.
pub fn fusion_data_root() -> PathBuf {
    std::env::var(ENV_FUSION_DATA_ROOT)
        .map(PathBuf::from)
        .unwrap_or_else(|_| fusion_root().join("data"))
}

/// Data directory for a specific graph: `$FUSION_DATA_ROOT/$graph_id`.
pub fn graph_data_root(graph_id: &str) -> PathBuf {
    fusion_data_root().join(graph_id)
}

/// Data directory for a graph node: `$FUSION_DATA_ROOT/$graph_id/$task_id`.
/// The directory is created automatically.
pub fn node_data_dir(graph_id: &str, task_id: &str) -> PathBuf {
    let dir = graph_data_root(graph_id).join(task_id);
    std::fs::create_dir_all(&dir).ok();
    dir
}

/// Remove a node's data directory.
/// Called by unit plugins at EOF / shutdown.
pub fn cleanup_node_dir(graph_id: &str, task_id: &str) {
    let dir = graph_data_root(graph_id).join(task_id);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Try to remove the graph's data directory.
/// Only succeeds if the directory is empty — some nodes may retain
/// data intentionally. Returns `true` if removed.
pub fn cleanup_graph_dir_if_empty(graph_id: &str) -> bool {
    let dir = graph_data_root(graph_id);
    if dir.is_dir() && std::fs::read_dir(&dir).map_or(false, |mut d| d.next().is_none()) {
        let _ = std::fs::remove_dir(&dir);
        true
    } else {
        false
    }
}
