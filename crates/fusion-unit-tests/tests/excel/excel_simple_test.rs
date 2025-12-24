use crate::execute;

#[tokio::test]
async fn test_datafusion_simple_graph() {
    execute("datafusion_simple_graph.yaml").await;
}

#[tokio::test]
async fn test_simple_excel_graph() {
    execute("excel_example_read_unit.yaml").await
}

#[tokio::test]
async fn test_excel_example_rw_unit() {
    execute("excel_example_rw_unit.yaml").await
}

#[tokio::test]
async fn test_excel_mix_unit() {
    env_logger::Builder::from_env(env_logger::Env::default()
        .default_filter_or("trace"))
        .init();
    execute("excel_read_mix_unit.yaml").await
}

#[tokio::test]
async fn test_simple_map() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();
    execute("simple_map.yaml").await
}

#[tokio::test]
async fn test_simple_http_source() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();
    execute("simple_http_source.yaml").await
}

#[tokio::test]
async fn test_simple_condition_edge() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();
    execute("simple_condition_edge.yaml").await
}

#[tokio::test]
async fn test_simple_ssh_sink() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();
    execute("simple_ssh_sink.yaml").await
}