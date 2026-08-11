use super::ConfigRegistry;
use serde_json::Value;
use std::fs;
use std::path::Path;

// ============================================================
// ConfigProvider trait
// ============================================================

/// A source of configuration entries.
///
/// Providers are called in priority order at startup. When two
/// providers define the same instance ID, the higher-priority
/// (larger number) one wins.
pub trait ConfigProvider: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Load configs into the registry.
    fn load(&self, registry: &mut ConfigRegistry) -> anyhow::Result<()>;

    /// Priority (0–255, higher = later = wins on conflict).
    fn priority(&self) -> u8 {
        50
    }
}

// ============================================================
// FileConfigProvider
// ============================================================

/// Loads configuration from a YAML file.
///
/// # File format (three-level hierarchy)
///
/// ```yaml
/// config:
///   datasource:
///     redis:
///       redis-cache:
///         host: localhost
///         port: 6379
///       redis-session:
///         host: redis-cluster.internal
///         port: 6379
///     postgres:
///       pg-analytics:
///         host: localhost
///         port: 5432
///         database: analytics
///   setting:
///     pool:
///       default:
///         max_size: 16
///   metadata:
///     cluster:
///       production:
///         id: "fusion-prod-01"
/// ```
///
/// The top-level key must be `config`. Under it, the three levels are:
/// `category` → `config_type` → `instance_id` → data.
///
/// Each instance ID must be **globally unique** across all categories
/// and types — capabilities look up config by ID alone.
pub struct FileConfigProvider {
    path: String,
}

impl FileConfigProvider {
    /// Create a provider that reads from the given YAML file path.
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    /// Create a provider that reads from `fusion-conf.yaml` in the
    /// current working directory.
    pub fn default_config() -> Self {
        Self::new("config/fusion-conf.yaml")
    }
}

impl ConfigProvider for FileConfigProvider {
    fn name(&self) -> &str {
        "FileConfigProvider"
    }

    fn priority(&self) -> u8 {
        10
    }

    fn load(&self, registry: &mut ConfigRegistry) -> anyhow::Result<()> {
        let path = Path::new(&self.path);
        if !path.exists() {
            log::warn!(
                "{}: config file `{}` not found, skipping",
                self.name(),
                self.path
            );
            return Ok(());
        }

        let content = fs::read_to_string(path)?;
        let root: Value = serde_yaml::from_str(&content)?;

        // Navigate: root → "config" → category → type → instance_id → data
        let config_root = match &root {
            Value::Object(map) => match map.get("config") {
                Some(v) => v,
                None => {
                    log::warn!(
                        "{}: no `config` key found in `{}`",
                        self.name(),
                        self.path
                    );
                    return Ok(());
                }
            },
            _ => {
                log::warn!("{}: root is not an object", self.name());
                return Ok(());
            }
        };

        let config_map = match config_root {
            Value::Object(map) => map,
            _ => {
                log::warn!("{}: `config` is not an object", self.name());
                return Ok(());
            }
        };

        let mut count = 0;
        for (category, types_value) in config_map {
            let types_map = match types_value {
                Value::Object(map) => map,
                _ => continue,
            };

            for (config_type, instances_value) in types_map {
                let instances_map = match instances_value {
                    Value::Object(map) => map,
                    _ => continue,
                };

                for (instance_id, data) in instances_map {
                    registry.insert(
                        category.clone(),
                        config_type.clone(),
                        instance_id.clone(),
                        data.clone(),
                    );
                    count += 1;
                }
            }
        }

        log::info!(
            "{}: loaded {} config(s) from `{}`",
            self.name(),
            count,
            self.path
        );
        Ok(())
    }
}

// ============================================================
// ProgrammaticConfigProvider
// ============================================================

/// A config provider that accepts entries directly in code.
///
/// # Example
///
/// ```ignore
/// let provider = ProgrammaticConfigProvider::new()
///     .with_entry("datasource", "redis", "redis-cache",
///         serde_json::json!({"host": "localhost", "port": 6379}));
/// ```
pub struct ProgrammaticConfigProvider {
    entries: Vec<(String, String, String, serde_json::Value)>,
    //           category  type    id      data
}

impl ProgrammaticConfigProvider {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Add a config entry.
    pub fn with_entry(
        mut self,
        category: impl Into<String>,
        config_type: impl Into<String>,
        id: impl Into<String>,
        data: serde_json::Value,
    ) -> Self {
        self.entries
            .push((category.into(), config_type.into(), id.into(), data));
        self
    }
}

impl Default for ProgrammaticConfigProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigProvider for ProgrammaticConfigProvider {
    fn name(&self) -> &str {
        "ProgrammaticConfigProvider"
    }

    fn priority(&self) -> u8 {
        90
    }

    fn load(&self, registry: &mut ConfigRegistry) -> anyhow::Result<()> {
        for (category, config_type, id, data) in &self.entries {
            registry.insert(
                category.clone(),
                config_type.clone(),
                id.clone(),
                data.clone(),
            );
        }
        log::info!(
            "{}: registered {} config(s)",
            self.name(),
            self.entries.len()
        );
        Ok(())
    }
}
