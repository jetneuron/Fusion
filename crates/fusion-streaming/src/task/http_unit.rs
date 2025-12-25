use crate::runtime::PRODUCT_INFO;
use crate::task::http_unit::UnitMode::Source;
use fusion_derive::{LogicalTask, SrcLogicTask};
use fusion_unit_sdk::graph::types::{ComputingUnit, InitUnit, MapUnit, SourceUnit, TaskContext};
use fusion_unit_sdk::proto::transfer::Row;
use fusion_unit_sdk::runtime::UnitResult;
use jsonpath_rust::JsonPath;
use mlua::Lua;
use reqwest::{Client, Method};
use serde_json::{from_str, Value};
use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

#[derive(Default, SrcLogicTask)]
pub struct HttpApiTask {
    uri: String,
    port: Option<u16>,
}

impl InitUnit for HttpApiTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        Ok(())
    }
}

impl SourceUnit for HttpApiTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        Ok(async move { Ok(()) })
    }
}

#[derive(Default, LogicalTask)]
pub struct HttpUnitTask {
    url: String,
    content_type: Option<String>,
    client: Client,
    method: Option<Method>,
    query: Option<HashMap<String, String>>,
    unit_mode: Option<UnitMode>,
    lua: Arc<Mutex<Lua>>,
}

#[derive(PartialEq, Eq)]
enum UnitMode {
    Source,
    Map,
}

impl FromStr for UnitMode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "source" => Ok(Source),
            "map" => Ok(UnitMode::Map),
            &_ => Ok(Source),
        }
    }
}

impl InitUnit for HttpUnitTask {
    fn init(&mut self, unit: ComputingUnit) -> UnitResult<()> {
        self.client = Client::new();
        self.method = Some(Method::GET);
        self.unit_mode = Some(Source);

        let conf = unit.get_config();
        conf.map(|c| {
            self.url = c["url"]
                .as_str()
                .expect("url must not be empty.")
                .to_string();

            let method = c["method"].as_str().unwrap_or_else(|| "GET").to_uppercase();
            self.method = Some(Method::from_str(&method).unwrap_or(Method::GET));

            let unit_mode = c["unit_mode"].as_str().unwrap_or_else(|| "Source");
            self.unit_mode = Some(UnitMode::from_str(unit_mode).unwrap_or(Source));
        });
        Ok(())
    }
}

impl SourceUnit for HttpUnitTask {
    fn launch(
        &self,
        ctx: Arc<TaskContext>,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send> {
        let client = self.client.clone();
        let url_str = self.url.clone();
        let method = (&self.method).clone().expect("could not determine method");
        let query = self.query.clone();

        Ok(async move {
            if let Some(m) = &self.unit_mode {
                if Source.eq(&m) {
                    let url = Url::parse(&url_str).expect("url must not be empty.");
                    let domain = url
                        .domain()
                        .or_else(|| url.host_str())
                        .expect("unknown host");
                    let mut request = client
                        .request(method, &url_str)
                        .header(reqwest::header::USER_AGENT, PRODUCT_INFO.as_str())
                        .header(reqwest::header::HOST, domain);

                    if let Some(query_params) = query {
                        let q = query_params
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect::<Vec<(String, String)>>();
                        request = request.query(&q);
                    }

                    let resp_result = request.send().await;
                    match resp_result {
                        Ok(resp) => {
                            let status = resp.status();
                            if status.is_success() {
                                let headers = resp.headers();
                                if let Some(content_type) =
                                    headers.get(reqwest::header::CONTENT_TYPE)
                                {
                                    let content_type_str = content_type.to_str();
                                    println!("content-type = {:?}", content_type_str);
                                }
                            }
                            let text = resp.text().await.expect("failed to read response text");
                            println!("response text = {}", text);
                            let path = JsonPath::from_str("$.ip").unwrap();
                            let json: Value = from_str(&text).expect("failed to deserialize json");
                            println!("json = {:?}", json);
                            let slice_of_data = path.find_slice(&json);
                            for t in slice_of_data {
                                let v = t.clone();
                                let data = v.to_data();
                                match data {
                                    Value::Null => {}
                                    Value::Bool(_) => {}
                                    Value::Number(_) => {}
                                    Value::String(str) => {
                                        println!("{}", str);
                                    }
                                    Value::Array(_) => {}
                                    Value::Object(_) => {}
                                }
                            }
                            // println!("{:#?}", text);
                        }
                        Err(err) => {}
                    }
                }
            }
            Ok(())
        })
    }
}

impl MapUnit for HttpUnitTask {
    fn compute<'life0, 'async_trait>(
        &'life0 self,
        row: Row,
        ctx: &'life0 TaskContext,
    ) -> anyhow::Result<impl Future<Output = UnitResult<()>> + Send>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Ok(async move { Ok(()) })
    }
}
