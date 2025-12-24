use crate::storage::types::{ConnectionPropResult, DataSource, DataSourceResult, IntoDataSource};
use crate::storage::types::DataSourceProp;
use crate::storage::types::IntoConnectionProp;

#[derive(Debug)]
struct Zookeeper {
    prop: ZookeeperProp,
}

#[derive(Debug, Clone)]
struct ZookeeperProp {
    host: String,
    port: u16,
}

impl DataSourceProp for ZookeeperProp {}

impl DataSource for Zookeeper {}

impl<T> IntoConnectionProp<ZookeeperProp> for (T, u16)
    where T: Into<String> {
    fn into_connection_prop(self) -> ConnectionPropResult<ZookeeperProp> {
        Ok(ZookeeperProp { host: self.0.into(), port: self.1 })
    }
}

impl<'a> IntoConnectionProp<ZookeeperProp> for &'a str {
    fn into_connection_prop(self) -> ConnectionPropResult<ZookeeperProp> {
        let parts: Vec<_> = self.split(":").collect();
        let host = String::from(parts[0]);
        let port: u16 = parts[1].parse().unwrap();
        Ok(ZookeeperProp { host, port })
    }
}

impl IntoDataSource<Zookeeper> for ZookeeperProp {
    fn into_datasource(self) -> DataSourceResult<Box<Zookeeper>> {
        Ok(Box::new(Zookeeper { prop: self }))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::storage_manager::StorageManager;
    use crate::storage::types::IntoConnectionProp;
    use crate::storage::zookeeper::{Zookeeper, ZookeeperProp};

    #[test]
    fn test_prop_parse() {
        let zk_prop: ZookeeperProp = "127.0.0.1:2181".into_connection_prop().unwrap();
        println!("zookeeper: {:?}", zk_prop);
        let mut sm = StorageManager::new();
        sm.add_datasource("id001", zk_prop);

        // match sm.use_datasource::<&str, Zookeeper, ZookeeperProp>("id001") {
        //     Some(ds) => {}
        //     None => {}
        // }
    }
}
