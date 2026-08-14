use chrono::{Local, TimeZone, Utc};
use serde_json::{Value, to_value};
use std::collections::HashMap;
use tera::Result;

pub fn yyyy_mm_dd(args: &HashMap<String, Value>) -> Result<Value> {
    let mut copied_args = args.clone();
    copied_args.insert("fmt".to_string(), Value::String("%Y-%m-%d".to_string()));
    format_time(&copied_args)
}

pub fn yyyymmdd(args: &HashMap<String, Value>) -> Result<Value> {
    let mut copied_args = args.clone();
    copied_args.insert("fmt".to_string(), Value::String("%Y%m%d".to_string()));
    format_time(&copied_args)
}

pub fn human_time(args: &HashMap<String, Value>) -> Result<Value> {
    let mut copied_args = args.clone();
    copied_args.insert(
        "fmt".to_string(),
        Value::String("%Y-%m-%d %H:%M:%S".to_string()),
    );
    format_time(&copied_args)
}

pub fn now(args: &HashMap<String, Value>) -> Result<Value> {
    Ok(to_value(now_ts())?)
}

pub fn format_time(args: &HashMap<String, Value>) -> Result<Value> {
    let millis = match args.get("ts") {
        Some(v) => v.as_i64().unwrap_or_else(|| now_ts()),
        None => now_ts(),
    };

    let dt = Local.timestamp_millis_opt(millis).unwrap();
    let fmt = match args.get("fmt") {
        None => "%Y%m%d",
        Some(fmt) => fmt.as_str().unwrap_or_else(|| "%Y%m%d"),
    };
    let str = dt.format(fmt).to_string();
    Ok(to_value(str)?)
}

fn now_ts() -> i64 {
    let utc_time = Utc::now();
    let local_time = utc_time.with_timezone(&Local);
    local_time.timestamp_millis()
}
