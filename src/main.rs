use anyhow::Result;
use dotenv::dotenv;
use std::time::Duration;
use tokio::time::interval;

mod fetch_price;
mod storage;

use fetch_price::QuoteFetcher;
use storage::InfluxDBStorage;
use tokio::fs;

async fn load_watchlist() -> Result<Vec<String>> {
    let data = fs::read_to_string("watchlist.json").await?;
    let watchlist: Vec<String> = serde_json::from_str(&data)?;
    Ok(watchlist)
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let url = std::env::var("INFLUX_URL")?;
    let db = std::env::var("DATABASE")?;
    let storage_url = format!("{}/api/v3/write_lp?db={}&precision=second", url, db);
    let token = std::env::var("INFLUXDB_AUTH_TOKEN")?;

    let fetcher = QuoteFetcher::new().await?;
    let storage = InfluxDBStorage::new(storage_url, token)?;
    let watchlist = load_watchlist().await?;

    let mut ticker = interval(Duration::from_secs(30));
    loop {
        ticker.tick().await;
        let prices = fetcher.fetch_prices(&watchlist).await?;
        storage.write_prices(&prices).await?;
    }
}
