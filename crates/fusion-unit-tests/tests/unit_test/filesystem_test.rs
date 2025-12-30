use crate::execute;

#[tokio::test]
async fn test_simple_filesystem() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();
    execute("simple_filesystem_source.yaml").await
}

#[tokio::test]
async fn test_simple_rw_filesystem() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();
    execute("simple_filesystem_rw.yaml").await
}
