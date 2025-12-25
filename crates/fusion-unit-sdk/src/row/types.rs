use crate::proto::transfer::DataType;
use crate::proto::transfer::DataType::unknown;
use protobuf::Enum;
use std::str::FromStr;

#[derive(Default, Clone, Debug)]
pub struct ColumnDescriptor {
    pub name: String,
    pub data_type: DataType,
}

impl ColumnDescriptor {
    pub fn new() -> Self {
        ColumnDescriptor::default()
    }

    //noinspection DuplicatedCode
    pub fn from<T>(name: String, infers: &Vec<T>) -> Self
    where
        T: Into<String> + Clone,
    {
        let mut descriptor = Self::new();
        descriptor.name = name;
        descriptor.data_type = unknown;
        infers.into_iter().fold(descriptor, |mut acc, val| {
            let value = val.clone().into();
            if let Ok(v) = i32::from_str(&value) {
                if v.to_string().eq(&value.to_string())
                    && DataType::i32.value() <= acc.data_type.value()
                {
                    acc.data_type = DataType::i32;
                    return acc;
                }
            }
            if let Ok(_) = i64::from_str(&value) {
                if DataType::i64.value() <= acc.data_type.value() {
                    acc.data_type = DataType::i64;
                    return acc;
                }
            }
            if let Ok(v) = f32::from_str(&value) {
                if v.to_string().eq(&value.to_string())
                    && DataType::f32.value() <= acc.data_type.value()
                {
                    acc.data_type = DataType::f32;
                    return acc;
                }
            }
            if let Ok(_) = f64::from_str(&value) {
                if DataType::f64.value() <= acc.data_type.value() {
                    acc.data_type = DataType::f64;
                    return acc;
                }
            }
            if let Ok(_) = bool::from_str(&value.to_lowercase()) {
                if DataType::bool.value() <= acc.data_type.value() {
                    acc.data_type = DataType::bool;
                    return acc;
                }
            }
            acc.data_type = DataType::str;
            acc
        })
    }
}

#[test]
fn test_column_descriptor_inference() {
    let desc = ColumnDescriptor::from(
        "name".to_string(),
        &vec!["355", "4.52322222222222223222212", "231"],
    );
    println!("{:?}", desc);

    let desc = ColumnDescriptor::from("name".to_string(), &vec!["355", "231", "4.5232"]);
    println!("{:?}", desc);

    let desc = ColumnDescriptor::from("name".to_string(), &vec!["355", "231", "x", "4.5232"]);
    println!("{:?}", desc);

    let desc = ColumnDescriptor::from("name".to_string(), &vec!["355", "231", "true", "4.5232"]);
    println!("{:?}", desc);

    let desc = ColumnDescriptor::from("name".to_string(), &vec!["TRUE", "false", "true", "False"]);
    println!("{:?}", desc);
}
