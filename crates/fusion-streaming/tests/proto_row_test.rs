use fusion_unit_sdk::proto::transfer::{Column, Row};
use protobuf::Message;

#[test]
fn test_serialize_and_deserialize() -> anyhow::Result<()> {
    let mut row = Row::new();

    let mut c1 = Column::new();
    c1.index = 1;
    c1.field = "id".to_string();
    row.columns.push(c1);

    let mut c2 = Column::new();
    c2.index = 2;
    c2.field = "value".to_string();
    row.columns.push(c2);

    let bytes = row.write_to_bytes()?;
    println!("{:?}", bytes);
    Ok(())
}