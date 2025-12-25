use crate::execute;

#[tokio::test]
async fn test_datafusion_simple_graph() {
    execute("datafusion_simple_graph.yaml").await;
}

#[tokio::test]
async fn test_datafusion_big_data_graph() {
    execute("datafusion_big_data.yaml").await;
}