use fusion_unit_sdk::proto::transfer::{Column, DataType, Row};
use jsonpath_rust::JsonPath;
use petgraph::data::DataMap;
use petgraph::graph::DiGraph;
use protobuf::EnumOrUnknown;
use serde_json::{Error, Value};
use std::collections::HashMap;
use std::str::FromStr;

pub struct JsonRowMapper {
    name: String,
    json_path: Option<String>,
}

impl JsonRowMapper {
    pub fn new(name: String, json_path: Option<String>) -> JsonRowMapper {
        JsonRowMapper { name, json_path }
    }
}

pub trait JsonParse {
    fn as_row(&self, mappers: Vec<JsonRowMapper>) -> Result<Vec<Row>, Error>;
}

struct JsonColumnFamilyNode {
    column_family: Vec<Column>,
}

impl JsonParse for Value {
    fn as_row(&self, mappers: Vec<JsonRowMapper>) -> Result<Vec<Row>, Error> {
        let mut graph = DiGraph::<JsonColumnFamilyNode, String>::new();
        let mapper_index = transform_as_tree(self, mappers, graph);

        // --------------------------------

        // 存储按照 json_path 分组，数组索引 (idx, 列数据集合) 的形式
        let mut all_mapped: Vec<(String, Vec<(String, Vec<Column>)>)> = Vec::new();
        // = transform_json_data(&self, mappers, &mut all_mapped);

        // let mut map = HashMap::new();
        let cloned_for_map = all_mapped.clone();
        //
        // let mut rows: Vec<Row> = vec![];
        // let rows_column = cartesian_product(&merged_table);
        // for r in rows_column {
        //     let mut row = Row::new();
        //     for columns in r.into_iter() {
        //         for column in columns.into_iter() {
        //             row.columns.push(column);
        //         }
        //     }
        //     row.columns.sort_by(|c1, c2| {
        //         let idx1 = mapper_index.get(&c1.field).unwrap_or(&0u16);
        //         let idx2 = mapper_index.get(&c2.field).unwrap_or(&0u16);
        //         idx1.cmp(&idx2)
        //     });
        //     rows.push(row);
        // }
        Ok(vec![])
    }
}

fn transform_as_tree(
    json: &Value,
    mappers: Vec<JsonRowMapper>,
    mut graph: DiGraph<JsonColumnFamilyNode, String>,
) -> HashMap<String, u16> {
    let mut mapper_index = HashMap::new();
    let mut idx = 0;
    for mapper in mappers {
        let name = mapper.name;
        mapper_index.insert(name.clone(), idx);
        idx += 1;
        let json_path = match mapper.json_path {
            None => {
                let path = format!("$.{}", &name);
                (path.clone(), JsonPath::from_str(path.as_str()).expect(""))
            }
            Some(path) => (path.clone(), JsonPath::from_str(path.as_str()).expect("")),
        };

        let data = json_path.1.find_slice(json);
        let pure_parent = get_parent(Some(json_path.0));
        let mut column_rows: Vec<(String, Vec<Column>)> = Vec::new();

        println!("---------------");
        for t in data {
            let mut c = Column::new();
            c.field = name.clone();
            let path = t.clone().to_path().unwrap_or("$".to_string());

            let data = t.to_data();
            println!("path = {}, {:?}", path, data);
            // map_json_type(&mut c, data);
            let paths = path.split(".").collect::<Vec<&str>>();
            println!("paths = {:?}", paths);

            //graph.node_weight()

            // graph.add_node();
        }
        // graph.node_weight();
    }
    mapper_index
}

fn map_json_type(c: &mut Column, data: Value) {
    match data {
        Value::Null => {
            c.is_null = true;
            c.dt = EnumOrUnknown::from(DataType::unknown);
        }
        Value::Bool(b) => {
            c.bool_val = b;
            c.dt = EnumOrUnknown::from(DataType::bool);
        }
        Value::Number(number) => {
            if number.is_i64() {
                c.i64_val = number.as_i64().unwrap();
                c.dt = EnumOrUnknown::from(DataType::i64);
            } else if number.is_u64() {
                c.i64_val = number.as_u64().unwrap() as i64;
                c.dt = EnumOrUnknown::from(DataType::i64);
            } else if number.is_f64() {
                c.f64_val = number.as_f64().unwrap();
                c.dt = EnumOrUnknown::from(DataType::f64);
            } else {
                unreachable!()
            }
        }
        Value::String(string) => {
            c.dt = EnumOrUnknown::from(DataType::str);
            c.str_val = string.clone();
        }
        Value::Array(array) => {
            unimplemented!()
        }
        Value::Object(obj) => {
            unimplemented!()
        }
    }
}

fn get_parent(path: Option<String>) -> String {
    match path {
        None => "$".to_string(),
        Some(p) => {
            let cloned = p.clone();
            let parts: Vec<&str> = cloned.split(".").collect();
            if parts.len() <= 1 {
                return ".".to_string();
            }
            parts[..parts.len() - 1].join(".")
        }
    }
}

fn cartesian_product<T: Clone>(arrays: &[Vec<T>]) -> Vec<Vec<T>> {
    if arrays.is_empty() {
        return vec![vec![]];
    }

    let first_array = &arrays[0];
    let rest_arrays = &arrays[1..];

    let rest_product = cartesian_product(rest_arrays);
    let mut result = Vec::new();

    for item in first_array {
        for combination in &rest_product {
            let mut new_combination = vec![item.clone()];
            new_combination.extend(combination.clone());
            result.push(new_combination);
        }
    }

    result
}
