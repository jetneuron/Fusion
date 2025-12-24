use std::collections::HashMap;
use std::fmt::Debug;

use crate::storage::connection::ConnectionTrait;
use crate::storage::types::{DataSource, DataSourceId, DataSourceProp, IntoDataSourceId, IntoDataSource};

pub struct StorageManager {
    datasources: HashMap<DataSourceId, Box<dyn IntoDataSource<dyn DataSource>>>,
}

impl StorageManager {
    pub fn new() -> Self {
        StorageManager {
            datasources: HashMap::new()
        }
    }

    pub fn add_storage(&self, con: &mut dyn ConnectionTrait) {
        println!("add connection props: {}", con.info());
    }

    pub fn add_datasource<ID, DATASOURCE_PROP>(&mut self, id: ID, prop: DATASOURCE_PROP)
        where ID: IntoDataSourceId, DATASOURCE_PROP: DataSourceProp + Clone + 'static {
        let id = id.into_datasource_id().unwrap();
        // self.datasources.insert(id, Box::new(prop.clone()));
        //println!("{:?}", id);
        // let datasource_instance = prop.into_datasource().unwrap();
        //println!("{:?}", datasource_instance);
    }

    // pub fn use_datasource<ID: IntoDataSourceId, DS: DataSource>(self, id: ID) -> Option<DS> {
    //     let t: Option<&Box<dyn DataSourceProp>> = self.datasources.get(&id.into_datasource_id().unwrap());
    //
    //     match t {
    //         None => {}
    //         Some(ds) => {
    //             println!("--");
    //         }
    //     }
    //
    //     None
    // }
}
