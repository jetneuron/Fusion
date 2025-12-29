#[cfg(test)]
mod sdk_test {
    use fusion_unit_sdk::graph::types::UnitConfig;
    use fusion_unit_sdk::units::config_util::UnitConfigExt;
    use serde_json::json;

    #[test]
    fn test_unit_config_parse() -> anyhow::Result<()> {
        let conf: UnitConfig = json!({});
        conf.require_string("x")?;
        Ok(())
    }
}
