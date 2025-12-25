use crate::execute;

#[tokio::test]
async fn test_datafusion_simple_graph() {
    execute("datafusion_simple_graph.yaml").await;
}
