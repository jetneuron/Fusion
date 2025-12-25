use fusion_streaming::utils::json_util::{JsonParse, JsonRowMapper};
use jsonpath_rust::JsonPath;
use jsonpath_rust::path::JsonLike;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::str::FromStr;

#[test]
pub fn test_json_path() {
    let json = json!({
        "ip": "127.0.0.1",
        "store": {
           "book": [
             {
               "category": "reference",
               "author": "Nigel Rees",
               "title": "Sayings of the Century",
               "price": 8.95
             },
             {
               "category": "fiction",
               "author": "Evelyn Waugh",
               "title": "Sword of Honour",
               "price": 12.99
             },
             {
               "category": "fiction",
               "author": "Herman Melville",
               "title": "Moby Dick",
               "isbn": "0-553-21311-3",
               "price": 8.99
             },
             {
               "category": "fiction",
               "author": "J. R. R. Tolkien",
               "title": "The Lord of the Rings",
               "isbn": "0-395-19395-8",
               "price": 22.99
             }
           ],
           "bicycle": {
             "color": "red",
             "price": 19.95
           }
         },
         "expensive": 10
    });
    let path = JsonPath::from_str("$.store.book[*].author").unwrap();
    let slice_of_data = path.find_slice(&json);
    for t in slice_of_data {
        println!("{}", t.clone().to_data().to_string());
    }

    let path = JsonPath::from_str("$.ip").unwrap();
    let slice_of_data = path.find_slice(&json);
    for t in slice_of_data {
        println!("{}", t.clone().to_data().to_string());
    }
}

#[test]
pub fn test_as_row() {
    let json = json!({
        "foo": "foo_value",
        "bar": 1.2,
        "baz": [
            {"name": "t1", "age": 21, "sub_array": [{"s": "v1"}, {"s": "v2"}]},
            {"name": "t2", "age": 26, "sub_array": [{"s": "v3"}, {"s": "v4"}]},
            {"name": "t3", "age": 22},
        ],
    });

    let mappers = vec![
        JsonRowMapper::new("foo".to_string(), None),
        JsonRowMapper::new(
            "sub".to_string(),
            Some("$.baz[*].sub_array[*].s".to_string()),
        ),
        JsonRowMapper::new("bazAge".to_string(), Some("$.baz[*].age".to_string())),
        JsonRowMapper::new("bar".to_string(), None),
        JsonRowMapper::new("bare".to_string(), None),
        JsonRowMapper::new("bazName".to_string(), Some("$.baz[*].name".to_string())),
    ];
    let rows = json.as_row(mappers).expect("err");
    for row in rows.iter() {
        println!("{}", row);
    }
}

/// 从 JSON 提取数据，同时保留父子层级关系
fn extract_nested_values(json: &Value, path: &str) -> Vec<HashMap<String, String>> {
    let compiled_path = JsonPath::from_str(path).expect("Invalid JSONPath");
    let nodes = compiled_path.find_slice(json);

    let mut results = Vec::new();

    for node in nodes {
        let data = node.to_data();
        if let Some(obj) = data.as_object() {
            let mut row = HashMap::new();
            for (key, value) in obj.iter() {
                row.insert(key.clone(), value.as_str().unwrap_or("?").to_string());
            }
            results.push(row);
        } else {
            // 处理简单类型
            let mut row = HashMap::new();
            row.insert(path.to_string(), data.as_str().unwrap_or("?").to_string());
            results.push(row);
        }
    }

    results
}

/// 递归计算笛卡尔积，保持层级结构
fn cartesian_product(
    nested_data: &[Vec<HashMap<String, String>>],
    depth: usize,
    current: &mut Vec<HashMap<String, String>>,
    result: &mut Vec<HashMap<String, String>>,
) {
    if depth == nested_data.len() {
        let mut merged = HashMap::new();
        for row in current.iter() {
            merged.extend(row.clone());
        }
        result.push(merged);
        return;
    }

    for item in &nested_data[depth] {
        current.push(item.clone());
        cartesian_product(nested_data, depth + 1, current, result);
        current.pop();
    }
}

#[test]
pub fn test() {
    let data = json!({
        "users": [
            {
                "name": "Alice",
                "age": "25",
                "addresses": [
                    {"city": "NY", "zip": "10001"},
                    {"city": "LA", "zip": "90001"}
                ]
            },
            {
                "name": "Bob",
                "age": "30",
                "addresses": [
                    {"city": "SF", "zip": "94101"},
                    {"city": "Chicago", "zip": "60601"}
                ]
            }
        ],
        "orders": [
            {"id": 1, "amount": 100},
            {"id": 2, "amount": 200}
        ]
    });

    let paths = vec![
        "$.users[*].name",
        "$.users[*].age",
        "$.users[*].addresses[*].city",
        "$.users[*].addresses[*].zip",
        "$.orders[*].id",
    ];

    // 提取所有字段的值，同时保持嵌套关系
    let extracted_values: Vec<Vec<HashMap<String, String>>> = paths
        .iter()
        .map(|p| extract_nested_values(&data, p))
        .collect();

    // 计算笛卡尔积
    let mut final_rows = Vec::new();
    cartesian_product(&extracted_values, 0, &mut Vec::new(), &mut final_rows);

    // 输出表格
    let headers: Vec<String> = paths
        .iter()
        .map(|s| {
            // s.replace("$.users[*].", "")
            //     .replace("$.orders[*].", "")
            s.to_string()
        })
        .collect();
    println!("{}", headers.join("\t"));

    for row in final_rows {
        let row_values: Vec<String> = headers
            .iter()
            .map(|header| row.get(header).cloned().unwrap_or("".to_string()))
            .collect();
        println!("{}", row_values.join("\t"));
    }
}
