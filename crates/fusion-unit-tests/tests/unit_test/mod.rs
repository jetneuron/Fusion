use crate::execute;

mod ssh_test;
mod filesystem_test;

#[tokio::test]
async fn test_excel_example_rw_unit() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();
    execute("example_unit.yaml").await
}
