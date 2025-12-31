use crate::execute;

#[tokio::test]
async fn test_simple_excel_graph() -> anyhow::Result<()> {
    execute("excel_example_read_unit.yaml").await?;
    Ok(())
}

#[tokio::test]
async fn test_excel_example_rw_unit() -> anyhow::Result<()> {
    execute("excel_example_rw_unit.yaml").await?;
    Ok(())
}

#[tokio::test]
async fn test_excel_mix_unit() -> anyhow::Result<()> {
    execute("excel_read_mix_unit.yaml").await?;
    Ok(())
}

#[tokio::test]
async fn test_simple_map() -> anyhow::Result<()> {
    execute("simple_map.yaml").await?;
    Ok(())
}

#[tokio::test]
async fn test_simple_http_source() -> anyhow::Result<()> {
    execute("simple_http_source.yaml").await?;
    Ok(())
}

#[tokio::test]
async fn test_simple_condition_edge() -> anyhow::Result<()> {
    execute("simple_condition_edge.yaml").await?;
    Ok(())
}

#[tokio::test]
async fn test_simple_ssh_sink() -> anyhow::Result<()> {
    execute("simple_ssh_sink.yaml").await?;
    Ok(())
}
