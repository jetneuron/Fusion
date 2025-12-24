mod sdk_test {
    use fusion_unit_sdk::graph::types::UnitConf;
    use serde_json::json;

    #[test]
    fn test_unit_config() {
        let conf = UnitConf::wrap(json!({"foo": "foo_value", "bar": "bar_value"}));
        let foo = &conf["foo2"];
        let bar = &conf["bar"];
        println!("{:?}", foo.as_i64());
        println!("{:?}", bar.as_str());
    }

    #[test]
    fn test_unit_express() {
        let conf = UnitConf::wrap(json!({"foo": "foo_value", "dt": "{{yyyyMMdd()}}"}));
        let ss = &conf["dt"];
        println!("{:?}", ss);
    }
}
