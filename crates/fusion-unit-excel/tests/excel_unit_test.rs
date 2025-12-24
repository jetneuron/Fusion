use calamine::{open_workbook, Error, RangeDeserializerBuilder, Reader, Xlsx};
use fusion_unit_sdk::graph::types::ComputingUnit;
use serde_json::json;

#[test]
fn test_file_path() {
    let path = format!(
        "{}/tests/test_res/example_data_01.xlsx",
        env!("CARGO_MANIFEST_DIR")
    );
    println!("path = {}", path);
}

#[test]
fn test_excel_file_read() -> Result<(), Error> {
    let path = format!(
        "{}/tests/test_res/example_data_01.xlsx",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut workbook: Xlsx<_> = open_workbook(path)?;

    let range = workbook.worksheet_range("Sheet1")?;
    let first = range.rows().nth(0);
    let d = first.unwrap();
    for ss in d {
        println!("{}", ss.to_string());
    }

    let mut iter = RangeDeserializerBuilder::new().from_range(&range)?;
    loop {
        if let Some(result) = iter.next() {
            let (label, value, v3): (u32, String, String) = result?;
            println!("{}, {}, {}", label, value, v3);
        } else {
            break;
        }
    }
    println!("读取完成，耗时：");
    Ok(())
}

#[tokio::test]
async fn test_excel_read_unit() {
    let path = format!(
        "{}/tests/test_res/example_data_01.xlsx",
        env!("CARGO_MANIFEST_DIR")
    );
    let excel_config = json!({
        "path": path,
        "sheet_name": "Sheet1",
        // "skip_rows": 3,
        // "field_name_row_index": 0,
        "field_names": [
            {
                "name": "foo1",
                "data_type": "i32"
            },
            {
                "name": "bar",
                "data_type": "str"
            }
        ]
    });

    let excel_input =
        ComputingUnit::new("excel_input", "SpreadSheetUnitTask").with_config(excel_config);
    // let input = Arc::new(Mutex::new(SpreadSheetUnitTask::new(excel_input)));

    let map = ComputingUnit::new("map", "DebugMapUnitTask");
    // let map = Arc::new(Mutex::new(DebugMapUnitTask::new(map)));

    let output_unit = ComputingUnit::new("id2", "DebugOutputUnitTask");
    // let output = Arc::new(Mutex::new(DebugOutputUnitTask::new(output_unit)));

    // map.lock().unwrap().link(output);
    // input.lock().unwrap().link(map);
    // input.lock().unwrap().launch().await.unwrap();
}
