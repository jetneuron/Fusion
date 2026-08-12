use fusion_unit_datafusion::init_plugin;
use fusion_unit_sdk::graph::types::ComputingUnit;
use fusion_unit_sdk::runtime::logical::LogicalTask;
use fusion_unit_sdk::units::dev::create_dev_context;
use serde_json::json;

#[tokio::test]
async fn apache_data_fusion_basic_test() {
    // let path = "tests/data/capitalized_example.csv";
    let path = "tests/data/alltypes_plain.parquet";
    let sql = r#"
    select * from this
    "#;
    let conf = json!({
        "sql": sql,
        "these": [{
            "name": "this",
            "paths": [
                path
            ],
            "format": "parquet"
        }]
    });
    let unit = ComputingUnit::new("test_id", "DataFusionUnit").with_config(conf);

    let logical_task = create_test_unit(unit.clone());
    let context = create_dev_context(unit);
    let context_ptr = Box::into_raw(Box::new(context.0));
    logical_task
        .internal_launch(context_ptr)
        .expect("fail to launch task")
        .await
        .expect("fail to launch");

    let mut m = context.1;
    let mut idx = 0;
    while let Some(row) = m.recv().await {
        if idx == 0 {
            row.display_column_names();
        }
        idx += 1;
        println!("{}", row);
    }
}

fn create_test_unit(unit: ComputingUnit) -> Box<dyn LogicalTask> {
    let cloned_unit = unit.clone();
    let type_name = cloned_unit.get_type();
    let plugin = init_plugin();
    plugin.register_units();
    plugin
        .create(unit)
        .expect(format!("Failed to create Graph unit plugin: {}", type_name).as_str())
}
