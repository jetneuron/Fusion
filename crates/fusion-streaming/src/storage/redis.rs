use std::str::FromStr;

use redis::Commands;

use crate::storage::connection::ConnectionTrait;
use crate::storage::types::{ConnectError, IntoDataSource};
use crate::storage::types::ConnectionPropResult;
use crate::storage::types::DataSource;
use crate::storage::types::DataSourceProp;
use crate::storage::types::DataSourceResult;
use crate::storage::types::IntoConnectionProp;

#[derive(Debug)]
struct Redis {
    prop: RedisProp,
}

#[derive(Debug, Clone)]
struct RedisProp {
    host: String,
    port: u16,
}

impl DataSourceProp for RedisProp {}

impl DataSource for Redis {}

impl IntoDataSource<Redis> for RedisProp {
    fn into_datasource(self) -> DataSourceResult<Box<Redis>> {
        Ok(Box::new(Redis { prop: self }))
    }
}

impl FromStr for RedisProp {
    type Err = ConnectError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.to_string().into_connection_prop().unwrap())
    }
}

/// transfer str to redis prop
impl<'a> IntoConnectionProp<RedisProp> for &'a str {
    fn into_connection_prop(self) -> ConnectionPropResult<RedisProp> {
        let parts: Vec<_> = self.split(':').collect();
        let host = String::from(parts[0]);
        let port: u16 = parts[1].parse().unwrap();
        Ok(RedisProp { host, port })
    }
}

/// transfer tuple2 as redis prop
impl<T> IntoConnectionProp<RedisProp> for (T, u16)
    where T: Into<String> {
    fn into_connection_prop(self) -> ConnectionPropResult<RedisProp> {
        Ok(RedisProp { host: self.0.into(), port: self.1 })
    }
}

impl Redis {
    pub fn new<T: IntoConnectionProp<RedisProp>>(connection_prop: T) -> Self {
        let redis_prop = connection_prop.into_connection_prop().unwrap();
        println!("{:?}", redis_prop);
        Redis { prop: redis_prop }
    }

    pub fn exec(&self) {
        // connect to redis
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let mut con = client.get_connection().unwrap();
        let val: String = con.get("teststr").unwrap();
        let m: Vec<String> = con.zrange("zsetsss01:002:001", 0, -1).unwrap();
        println!("val = {}, val2 = {:?}", val, m);
    }
}

impl ConnectionTrait for Redis {
    fn info(&self) -> String {
        format!("redis://{}:{}", self.prop.host, self.prop.port)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::redis::RedisProp;
    use crate::storage::storage_manager::StorageManager;
    use crate::storage::types::IntoConnectionProp;

    #[test]
    fn test_add_storage() {
        let redis_prop: RedisProp = "127.0.0.1:6379".into_connection_prop().unwrap();
        let mut sm = StorageManager::new();
        sm.add_datasource("id001", redis_prop);
    }
}
