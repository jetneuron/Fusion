use fusion_unit_sdk::row::formatter::StrRawFormatter;
use fusion_unit_sdk::row::utils::RawFormatter;

#[test]
fn test_raw_string_formatter() -> anyhow::Result<()> {
    let fmt = StrRawFormatter::new().with_separator("\t");
    let row = fmt.into_row("a\tb\tc")?;
    println!("{}", row);
    Ok(())
}
