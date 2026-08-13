use fusion_unit_sdk::proto::transfer::DataType;
use fusion_unit_sdk::frame::serializer::{Deserializer, SeparatorFormatter, Serializer};
use fusion_unit_sdk::frame::types::ColumnDescriptor;

#[test]
fn test_string_serializer() -> anyhow::Result<()> {
    let formatter = SeparatorFormatter::new(",");
    let frame = formatter.into_row(
        "a,3.1415,c,d",
        &Some(vec![
            ColumnDescriptor::new("column0", DataType::str),
            ColumnDescriptor::new("column1", DataType::f64),
        ]),
    )?;
    println!("String to Frame: ");
    frame.display_column_names();
    println!("{}\n", frame);

    println!("Frame to String: ");
    let value = formatter.from_row(frame)?;
    println!("{}", value);
    Ok(())
}
