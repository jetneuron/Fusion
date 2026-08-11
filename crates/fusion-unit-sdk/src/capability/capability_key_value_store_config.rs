use serde_derive::Deserialize;

/// Typed configuration for a Redis datasource.
///
/// Retrieved from the unified [`ConfigRegistry`](crate::config::ConfigRegistry)
/// by instance ID:
///
/// ```ignore
/// let redis: RedisDataSourceConfig = fusion_unit_sdk::config::get("redis-cache")?;
/// println!("{}:{}", redis.host, redis.port);
/// ```
///
/// # YAML format
///
/// ```yaml
/// config:
///   datasource:
///     redis:
///       redis-cache:
///         host: localhost
///         port: 6379
///         db: 0
///         pool_size: 16
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct RedisDataSourceConfig {
    /// Redis server host (default: "localhost").
    #[serde(default = "default_host")]
    pub host: String,

    /// Redis server port (default: 6379).
    #[serde(default = "default_port")]
    pub port: u16,

    /// Redis database number (default: 0).
    #[serde(default)]
    pub db: i64,

    /// Optional password.
    #[serde(default)]
    pub password: Option<String>,

    /// Connection pool size (default: 8).
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,

    /// Connection timeout in milliseconds (default: 5000).
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
    /// Build a Redis connection URL from the config fields.
    pub fn connection_url(&self) -> String {
        match &self.password {
            Some(pwd) => format!("redis://:{}@{}:{}", pwd, self.host, self.port),
            None => format!("redis://{}:{}", self.host, self.port),
        }
    }
}
