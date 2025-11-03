use anyhow::Result;
use dotenv::dotenv;
use finfo::query::FlightSqlClient;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let url = env::var("INFLUX_URL")?;
    let token = env::var("INFLUXDB_AUTH_TOKEN")?;

    let mut client = FlightSqlClient::new(&url, &token, "test").await?;

    let query_sql = "SELECT * FROM kline WHERE symbol = 'AAPL.US'";

    // 执行查询
    let df = client.execute_sql(query_sql).await?;

    println!("{:?}", df);

    Ok(())
}
