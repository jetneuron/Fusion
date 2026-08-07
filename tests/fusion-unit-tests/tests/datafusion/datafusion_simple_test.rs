use crate::execute;

#[tokio::test]
async fn test_datafusion_simple_graph() -> anyhow::Result<()> {
    execute("datafusion_simple_graph.yaml").await?;
    Ok(())
}

#[tokio::test]
async fn test_datafusion_big_data_graph() -> anyhow::Result<()> {
    execute("datafusion_big_data.yaml").await?;
    Ok(())
}

#[tokio::test]
async fn test_datafusion_simple_json() -> anyhow::Result<()> {
    execute("datafusion_simple_json.yaml").await?;
    Ok(())
}
