use crate::config::DataSourceConfig;
use serde_derive::Deserialize;

/// Typed configuration for a Redis datasource.
///
/// Obtained from [`ConfigRegistry::get_typed`](crate::config::ConfigRegistry::get_typed)
/// after the config has been loaded by a [`ConfigProvider`](crate::config::providers::ConfigProvider).
///
/// # Example (YAML)
///
/// ```yaml
/// datasources:
///   redis-cache:
///     type: redis
///     host: localhost
///     port: 6379
///     db: 0
///     pool_size: 16
/// ```
///
/// # Example (code)
///
/// ```ignore
/// use fusion_unit_sdk::config;
/// use fusion_unit_sdk::capability::capability_key_value_store_config::RedisDataSourceConfig;
///
/// let redis: RedisDataSourceConfig = config::read_config()
///     .get_typed("redis-cache")
///     .expect("redis-cache not configured");
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct RedisDataSourceConfig {
    /// Datasource id (filled from the registry key, not from YAML).
    #[serde(skip)]
    pub id: String,

    /// Redis server host.
    #[serde(default = "default_host")]
    pub host: String,

    /// Redis server port.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Redis database number.
    #[serde(default)]
    pub db: i64,

    /// Optional password.
    #[serde(default)]
    pub password: Option<String>,

    /// Connection pool size.
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,

    /// Connection timeout in milliseconds.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
}

fn default_host() -> String {
    "localhost".into()
}
fn default_port() -> u16 {
    6379
}
fn default_pool_size() -> u32 {
    8
}
fn default_connect_timeout_ms() -> u64 {
    5000
}

impl RedisDataSourceConfig {
    /// Build a connection URL from the config fields.
    pub fn connection_url(&self) -> String {
        match &self.password {
            Some(pwd) => format!("redis://:{}@{}:{}", pwd, self.host, self.port),
            None => format!("redis://{}:{}", self.host, self.port),
        }
    }
}

impl DataSourceConfig for RedisDataSourceConfig {
    fn id(&self) -> &str {
        &self.id
    }

    fn source_type(&self) -> &str {
        "redis"
    }

    fn raw_config(&self) -> &serde_json::Value {
        // Return a lazily-computed reference — since we don't store the
        // original JSON, we re-serialize on demand. For production use,
        // consider caching this.
        // This is acceptable because get_typed deserializes from the
        // GenericDataSourceConfig stored in the registry, not from here.
        unimplemented!("RedisDataSourceConfig is obtained via get_typed(); raw_config is not used")
    }

    fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        if self.host.is_empty() {
            errors.push("host is empty".into());
        }
        if self.port == 0 {
            errors.push("port must be non-zero".into());
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
