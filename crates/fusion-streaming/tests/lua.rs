use fusion_unit_sdk::proto::transfer::{Column, DataType, Frame};
use mlua::{Function, Lua, Table};
use protobuf::EnumOrUnknown;

#[tokio::test]
async fn test_simple_lua() -> Result<(), Box<dyn std::error::Error>> {
    let lua = Lua::new();
    let func = lua
        .load(
            r#"
    function(val)
        return "s" .. val.field1 .. val.field2
    end
    "#,
        )
        .eval::<Function>()?;

    let mut frame = Frame::new();
    let mut c1 = Column::new();
    c1.field = "field1".to_string();
    c1.str_val = "foo01".to_string();
    c1.dt = EnumOrUnknown::new(DataType::str);

    let mut c2 = Column::new();
    c2.field = "field2".to_string();
    c2.f64_val = 5.321451;
    c2.dt = EnumOrUnknown::new(DataType::f64);
    frame.columns = vec![c1, c2];

    let mut t = lua.create_table()?;
    for column in frame.columns.clone() {
        if column.str_val.is_empty() {
            t.set(column.field, column.f64_val)?;
        } else {
            t.set(column.field, column.str_val)?;
        }
    }
    let c = frame.columns.get_mut(0).expect("");
    c.str_val = func.call::<String>(t)?;

    println!("{}", &frame);
    Ok(())
}

#[tokio::test]
async fn test_collect_data() -> Result<(), Box<dyn std::error::Error>> {
    let lua = Lua::new();

    let send_fn = lua.create_function(|lua, table: Table| {
        println!("rust code");
        for pair in table.pairs::<mlua::Value, mlua::Value>() {
            let (key, val) = pair.unwrap();
            println!("{:?} = {:?}", &key, &val);
        }
        println!("rust code");
        Ok(())
    })?;
    let globals = lua.globals();
    globals.set("send", send_fn)?;
    let script = r#"
    print(frame.foo)
    local data = {
        f1 = frame.foo,
        f2 = tonumber(string.format("%.2f", frame.bar + 1))
    }
    send(data)
    "#;
    lua.load(format!(
        r#"
    function _test_fn(frame)
      {}
    end
    "#,
        script
    ))
    .exec()?;

    let table = lua.create_table()?;
    table.set("foo", "foo01")?;
    table.set("bar", 3.14)?;

    let test_fn = globals.get::<Function>("_test_fn")?;
    let () = test_fn.call(table)?;

    Ok(())
}
