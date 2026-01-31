use fusion_unit_sdk::proto::transfer::DataType;
use fusion_unit_sdk::row::serializer::{Deserializer, SeparatorFormatter, Serializer};
use fusion_unit_sdk::row::types::ColumnDescriptor;

#[test]
fn test_string_serializer() -> anyhow::Result<()> {
    let formatter = SeparatorFormatter::new(",");
    let row = formatter.into_row(
        "a,3.1415,c,d",
        &Some(vec![
            ColumnDescriptor::new("column0", DataType::str),
            ColumnDescriptor::new("column1", DataType::f64),
        ]),
    )?;
    println!("String to Row: ");
    row.display_column_names();
    println!("{}\n", row);

    println!("Row to String: ");
    let value = formatter.from_row(row)?;
    println!("{}", value);
    Ok(())
}
