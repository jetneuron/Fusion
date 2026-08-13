use fusion_unit_universal_fs::core::IntoUniversalIO;

#[test]
fn test_universal_filesystem() -> anyhow::Result<()> {
    let universal_io = "file:///Users/nigel/tmp/test.log?separator=%2C".into_universal_io()?;
    for (idx, frame) in universal_io.iter_rows()?.enumerate() {
        let unwrapped_row = frame?;
        if idx == 0 {
            unwrapped_row.display_column_names();
        }
        println!("{idx}: {}", unwrapped_row)
    }
    Ok(())
}
