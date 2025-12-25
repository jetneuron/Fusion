use crate::execute;

#[tokio::test]
async fn test_simple_ssh_source() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();
    execute("simple_ssh_source.yaml").await
}
