use std::str::FromStr;

#[derive(Debug)]
pub struct ConnectError {}

pub type ConnectionPropResult<T> = Result<T, ConnectError>;

pub trait DataSource {}

pub trait DataSourceProp {}

pub trait IntoConnectionProp<T: DataSourceProp> {
    /// transfer to connection property result
    fn into_connection_prop(self) -> ConnectionPropResult<T>;
}


#[derive(Debug)]
pub struct DataSourceErr {}

pub type DataSourceResult<T> = Result<T, DataSourceErr>;

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct DataSourceId {
    value: String,
}

pub trait IntoDataSourceId {
    fn into_datasource_id(self) -> Result<DataSourceId, DataSourceErr>;
}

impl IntoDataSourceId for String {
    fn into_datasource_id(self) -> Result<DataSourceId, DataSourceErr> {
        Ok(DataSourceId { value: self })
    }
}

impl<'a> IntoDataSourceId for &'a str {
    fn into_datasource_id(self) -> Result<DataSourceId, DataSourceErr> {
        Ok(self.to_string().into_datasource_id().unwrap())
    }
}

impl IntoDataSourceId for DataSourceId {
    fn into_datasource_id(self) -> Result<DataSourceId, DataSourceErr> {
        Ok(self)
    }
}

impl FromStr for DataSourceId {
    type Err = DataSourceErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.to_string().into_datasource_id().unwrap())
    }
}

pub trait IntoDataSource<T> where T: DataSource + ?Sized {
    fn into_datasource(self) -> DataSourceResult<Box<T>>;
}
