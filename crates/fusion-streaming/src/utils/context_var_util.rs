use fusion_unit_sdk::runtime::{UnitError, UnitResult};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use std::sync::Arc;
use tera::{Context, Tera};
use tokio::sync::Mutex;

pub(crate) fn calculate_runtime(
    tera: Arc<Mutex<Tera>>,
    context: Context,
    origin: Value,
) -> BoxFuture<'static, UnitResult<Value>> {
    async move {
        if origin.is_null() {
            return Ok(Value::Null);
        }
        let json = origin.to_string();
        let mut tera = tera.lock().await;
        let new_val = &tera
            .render_str(&json, &context)
            .map_err(|_| UnitError::config_parse_error("fail to parse var"))?;
        Ok(serde_json::from_str(&new_val)
            .map_err(|_| UnitError::config_parse_error("fail to parse var"))?)
    }
    .boxed()
}
