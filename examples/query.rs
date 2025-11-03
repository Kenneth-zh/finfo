use anyhow::Result;
use dotenv::dotenv;
use finfo::query::FlightSqlClient;
use std::env;

use df_interchange::Interchange;
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let url = env::var("INFLUX_URL")?;
    let token = env::var("INFLUXDB_AUTH_TOKEN")?;

    let mut client = FlightSqlClient::new(&url, &token, "test").await?;

    let query_sql = "SELECT * FROM kline WHERE symbol = 'AAPL.US'";

    // 执行查询
    let batches = client.execute_sql(query_sql).await?;

    let df = Interchange::from_arrow_56(batches)?.to_polars_0_51()?;

    println!("{:?}", df);

    Ok(())
}
