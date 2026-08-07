use crate::execute;

#[tokio::test]
async fn test_simple_ssh_source() -> anyhow::Result<()> {
    execute("simple_ssh_source.yaml").await?;
    Ok(())
}
