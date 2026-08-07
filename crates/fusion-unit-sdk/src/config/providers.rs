use super::{ConfigRegistry, DataSourceConfig, GenericDataSourceConfig};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

// ============================================================
// ConfigProvider trait
// ============================================================

/// A source of datasource configurations.
///
/// Providers are called in priority order at startup. When two
/// providers define the same datasource id, the higher-priority
/// (larger number) one wins.
pub trait ConfigProvider: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Load configs into the registry.
    ///
    /// Should call [`super::register`] or directly mutate the registry.
    fn load(&self, registry: &mut ConfigRegistry) -> anyhow::Result<()>;

    /// Priority (0–255, higher = later = wins on conflict).
    fn priority(&self) -> u8 {
        50
    }
}

// ============================================================
// FileConfigProvider
// ============================================================

/// Loads datasource configs from a YAML file.
///
/// # File format
///
/// ```yaml
/// datasources:
///   redis-cache:
///     type: redis
///     host: localhost
///     port: 6379
///     db: 0
///   pg-analytics:
///     type: postgres
///     host: localhost
///     port: 5432
///     database: analytics
/// ```
///
/// The top-level key must be `datasources`. Each entry becomes a
/// [`GenericDataSourceConfig`] keyed by the YAML key name. The `type`
/// field is required on every entry — it becomes the
/// [`source_type()`](DataSourceConfig::source_type).
pub struct FileConfigProvider {
    path: String,
}

impl FileConfigProvider {
    /// Create a provider that reads from the given YAML file path.
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    /// Create a provider that reads from `fusion-config.yaml` in the
    /// current working directory.
    pub fn default_config() -> Self {
        Self::new("fusion-config.yaml")
    }
}

impl ConfigProvider for FileConfigProvider {
    fn name(&self) -> &str {
        "FileConfigProvider"
    }

    fn priority(&self) -> u8 {
        10 // Low priority — programmatic overrides win.
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

        let datasources = match &root {
            Value::Object(map) => match map.get("datasources") {
                Some(Value::Object(ds)) => ds,
                _ => {
                    log::warn!(
                        "{}: no `datasources` key found in `{}`",
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

        let mut count = 0;
        for (id, raw) in datasources {
            let source_type = raw
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            let config = Arc::new(GenericDataSourceConfig::new(
                id.clone(),
                source_type,
                raw.clone(),
            ));

            // Basic validation
            if let Err(errors) = config.validate() {
                log::error!(
                    "{}: datasource `{}` validation failed: {:?}",
                    self.name(),
                    id,
                    errors
                );
                continue;
            }

            registry.register(config);
            count += 1;
        }

        log::info!(
            "{}: loaded {} datasource(s) from `{}`",
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

/// A config provider that accepts datasource defs directly in code.
///
/// Useful for tests, embedding, or dynamic configuration from sources
/// that aren't YAML files (e.g. a database or remote API).
///
/// # Example
///
/// ```ignore
/// let provider = ProgrammaticConfigProvider::new()
///     .with_datasource("redis-cache", "redis", json!({
///         "host": "localhost", "port": 6379
///     }));
/// ```
pub struct ProgrammaticConfigProvider {
    datasources: Vec<(String, String, serde_json::Value)>, // (id, type, raw)
}

impl ProgrammaticConfigProvider {
    pub fn new() -> Self {
        Self {
            datasources: Vec::new(),
        }
    }

    /// Add a datasource.
    pub fn with_datasource(
        mut self,
        id: impl Into<String>,
        source_type: impl Into<String>,
        config: serde_json::Value,
    ) -> Self {
        self.datasources.push((id.into(), source_type.into(), config));
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
        90 // High priority — overrides file configs.
    }

    fn load(&self, registry: &mut ConfigRegistry) -> anyhow::Result<()> {
        for (id, source_type, raw) in &self.datasources {
            let config = Arc::new(GenericDataSourceConfig::new(
                id.clone(),
                source_type.clone(),
                raw.clone(),
            ));
            registry.register(config);
        }
        log::info!(
            "{}: registered {} datasource(s)",
            self.name(),
            self.datasources.len()
        );
        Ok(())
    }
}
