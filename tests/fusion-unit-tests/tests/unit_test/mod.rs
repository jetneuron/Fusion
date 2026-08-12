use crate::execute;
use crate::execute_with_env;
use serde_json::json;

mod datafusion_test;
mod filesystem_test;
mod redis_test;
mod ssh_test;

#[tokio::test]
async fn test_parallel_lua_map() -> anyhow::Result<()> {
    execute("parallel_lua_map.yaml").await?;
    Ok(())
}

#[tokio::test]
async fn test_excel_example_rw_unit() -> anyhow::Result<()> {
    execute_with_env("example_unit.yaml", Some(json!({"foo": "bar"}))).await?;
    Ok(())
}
