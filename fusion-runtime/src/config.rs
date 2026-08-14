//! Fusion configuration parsing.
//!
//! Reads `fusion-conf.yaml` (server) or `fusion-conf-app.yaml` (embedded)
//! and produces a [`FusionConfig`] used by the runtime builder.

use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

// ============================================================
// Config types
// ============================================================

/// Top-level Fusion configuration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct FusionConfig {
    /// Run mode.
    #[serde(default)]
    pub mode: RunMode,

    /// Dynamic library search paths.
    #[serde(default)]
    pub libs: LibPathsConfig,

    /// Config registry entries, three-level hierarchy
    /// `category → config_type → instance_id → data`.
    ///
    /// Populated into the process-global `config` registry at build time
    /// (and injected into unit/provider dylibs, whose registries are
    /// separate binary images).
    #[serde(default)]
    pub config: Option<serde_json::Value>,
}

/// Execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunMode {
    /// Standalone / embedded mode.
    /// Used when Fusion runs inside a desktop app (Tauri) or as a library.
    Singleton,
    /// Server / cluster mode.
    /// Used for distributed deployments (planned).
    Cluster,
}

impl Default for RunMode {
    fn default() -> Self {
        Self::Singleton
    }
}

/// Dynamic library search paths for plugins.
#[derive(Debug, Clone, Deserialize)]
pub struct LibPathsConfig {
    /// Directories to scan for capability plugin dylibs.
    #[serde(default)]
    pub capability: Vec<String>,

    /// Directories to scan for unit plugin dylibs.
    #[serde(default)]
    pub unit: Vec<String>,
}

impl Default for LibPathsConfig {
    fn default() -> Self {
        Self {
            capability: Vec::new(),
            unit: Vec::new(),
        }
    }
}

// ============================================================
// Config loading
// ============================================================

/// Well-known config file names, searched in order.
const CONFIG_SEARCH_ORDER: &[&str] = &[
    "config/fusion-conf.yaml",     // server / explicit
    "config/fusion-conf-app.yaml", // embedded / app fallback
];

/// Embedded app config (compiled into the binary for singleton mode).
const EMBEDDED_APP_CONFIG: &str = include_str!("../../config/fusion-conf-app.yaml");

impl FusionConfig {
    /// Load configuration from the filesystem.
    ///
    /// Searches [`CONFIG_SEARCH_ORDER`] and returns the first match.
    /// If no file is found, returns an error.
    pub fn load() -> anyhow::Result<Self> {
        for rel_path in CONFIG_SEARCH_ORDER {
            let path = Path::new(rel_path);
            if path.exists() {
                log::info!("Loading config from `{}`", path.display());
                let content = fs::read_to_string(path)?;
                return Ok(serde_yaml::from_str(&content)?);
            }
        }
        anyhow::bail!(
            "No config file found. Searched: {:?}. \
             Place `fusion-conf.yaml` in the `config/` directory.",
            CONFIG_SEARCH_ORDER
        )
    }

    /// Load the embedded app config (always available).
    ///
    /// Used in singleton mode when Fusion is embedded in a desktop
    /// application and no external config file is present.
    pub fn load_embedded() -> anyhow::Result<Self> {
        log::info!("Loading embedded app config");
        Ok(serde_yaml::from_str(EMBEDDED_APP_CONFIG)?)
    }

    /// Load config, falling back to embedded if no file exists.
    ///
    /// This is the recommended method for singleton mode.
    pub fn load_or_embedded() -> anyhow::Result<Self> {
        match Self::load() {
            Ok(cfg) => Ok(cfg),
            Err(_) => {
                log::warn!("No config file found, using embedded defaults");
                Self::load_embedded()
            }
        }
    }

    /// Canonicalize all lib paths into absolute [`PathBuf`]s.
    ///
    /// Paths that don't exist are logged as warnings but not filtered
    /// out — the caller decides whether to skip or error.
    pub fn capability_lib_paths(&self) -> Vec<PathBuf> {
        self.libs.capability.iter().map(PathBuf::from).collect()
    }

    pub fn unit_lib_paths(&self) -> Vec<PathBuf> {
        self.libs.unit.iter().map(PathBuf::from).collect()
    }

    /// Discover all dylib files in the capability search directories.
    ///
    /// Returns (library_name, full_path) pairs, deduplicated by
    /// library name (first found wins).
    pub fn discover_capability_libs(&self) -> Vec<(String, PathBuf)> {
        discover_libs(&self.libs.capability)
    }

    /// Discover all dylib files in the unit search directories.
    pub fn discover_unit_libs(&self) -> Vec<(String, PathBuf)> {
        discover_libs(&self.libs.unit)
    }
}

/// Scan directories for dynamic library files (.dylib / .so / .dll).
///
/// Library name is derived from the filename stem with the `lib` prefix
/// and platform extension stripped. The first occurrence of each name wins.
fn discover_libs(dirs: &[String]) -> Vec<(String, PathBuf)> {
    let mut found: Vec<(String, PathBuf)> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for dir in dirs {
        let path = Path::new(dir);
        if !path.is_dir() {
            log::debug!("Lib dir not found or not a directory: `{}`", dir);
            continue;
        }

        let entries = match fs::read_dir(path) {
            Ok(e) => e,
            Err(e) => {
                log::warn!("Cannot read lib dir `{}`: {}", dir, e);
                continue;
            }
        };

        for entry in entries.flatten() {
            let file_path = entry.path();
            let file_name = file_path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            // Platform-appropriate library extension
            let is_lib = file_name.ends_with(".dylib")   // macOS
                || file_name.ends_with(".so")            // Linux
                || file_name.ends_with(".dll");          // Windows

            if !is_lib {
                continue;
            }

            // Strip "lib" prefix and extension to get the library name.
            let name = file_name
                .strip_prefix("lib")
                .unwrap_or(file_name)
                .split('.')
                .next()
                .unwrap_or(file_name)
                .to_string();

            if seen.insert(name.clone()) {
                log::debug!("Discovered lib: `{}` → `{}`", name, file_path.display());
                found.push((name, file_path));
            }
        }
    }

    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_embedded_config() {
        let cfg = FusionConfig::load_embedded().expect("embedded config should parse");
        assert_eq!(cfg.mode, RunMode::Singleton);
        assert!(!cfg.libs.capability.is_empty());
        assert!(!cfg.libs.unit.is_empty());
        assert_eq!(cfg.libs.capability[0], "assets/libs/capability");
        assert_eq!(cfg.libs.unit[0], "assets/libs/unit");
    }

    #[test]
    fn test_parse_server_config() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../config/fusion-conf.yaml");
        let content =
            std::fs::read_to_string(&path).expect("server config should exist");
        let cfg: FusionConfig =
            serde_yaml::from_str(&content).expect("server config should parse");
        assert_eq!(cfg.mode, RunMode::Cluster);
        assert!(cfg.libs.capability.len() >= 2);
    }
}
