use anyhow::Result;
use arrow::util::pretty::print_batches;
use dotenv::dotenv;
use finfo::FlightSqlClient;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let url = dotenv::var("INFLUXURL")?;
    let token = dotenv::var("INFLUXDB_AUTH_TOKEN")?;

    let mut client = FlightSqlClient::new(&url, &token, "test").await?;

    let query_sql = "SELECT * FROM stock_price WHERE symbol = 'AAPL.US'";

    // 执行查询
    let batches = client.execute_sql(query_sql).await?;

    // 打印
    print_batches(&batches)?;

    Ok(())
}
