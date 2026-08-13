use fusion_unit_sdk::proto::transfer::{Column, Frame};
use protobuf::Message;

#[test]
fn test_serialize_and_deserialize() -> anyhow::Result<()> {
    let mut frame = Frame::new();

    let mut c1 = Column::new();
    c1.index = 1;
    c1.field = "id".to_string();
    frame.columns.push(c1);

    let mut c2 = Column::new();
    c2.index = 2;
    c2.field = "value".to_string();
    frame.columns.push(c2);

    let bytes = frame.write_to_bytes()?;
    println!("{:?}", bytes);
    Ok(())
}
