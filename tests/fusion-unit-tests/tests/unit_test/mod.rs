use crate::execute_with_env;
use serde_json::json;

mod filesystem_test;
mod ssh_test;

#[tokio::test]
async fn test_excel_example_rw_unit() -> anyhow::Result<()> {
    execute_with_env("example_unit.yaml", Some(json!({"foo": "bar"}))).await?;
    Ok(())
}
