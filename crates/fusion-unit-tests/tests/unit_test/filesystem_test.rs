use crate::execute;

#[tokio::test]
async fn test_simple_filesystem() -> anyhow::Result<()> {
    execute("simple_filesystem_source.yaml").await?;
    Ok(())
}

#[tokio::test]
async fn test_simple_rw_filesystem() -> anyhow::Result<()> {
    execute("simple_filesystem_rw.yaml").await?;
    Ok(())
}
