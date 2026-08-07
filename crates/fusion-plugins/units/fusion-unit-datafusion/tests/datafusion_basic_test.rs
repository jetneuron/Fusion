use datafusion::prelude::*;
use std::time::Instant;

#[tokio::test]
async fn simple_read_csv_test() -> datafusion::error::Result<()> {
    // register the table
    let ctx = SessionContext::new();
    ctx.register_csv(
        "example",
        "tests/data/capitalized_example.csv",
        CsvReadOptions::new(),
    )
    .await?;

    // create a plan to run a SQL query
    let df = ctx
        .sql("SELECT \"A\", MIN(b) FROM example WHERE \"A\" <= c GROUP BY \"A\" LIMIT 100")
        .await?;

    // execute and print results
    df.show().await?;
    Ok(())
}

#[tokio::test]
async fn simple_read_parquet_test() -> datafusion::error::Result<()> {
    // register the table
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "example",
        "tests/data/alltypes_plain.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    let all = ctx.sql("SELECT * from example").await?;
    all.show().await?;

    let instant = Instant::now();
    // create a plan to run a SQL query
    let df = ctx
        .sql("SELECT id <= 5, sum(double_col) FROM example group by id <= 5")
        .await?;
    let elapsed = instant.elapsed().as_millis();
    println!("Elapsed: {} ms", elapsed);
    // execute and print results
    df.show().await?;

    Ok(())
}
