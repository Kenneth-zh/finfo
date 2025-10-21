use anyhow::Result;
use dotenv::dotenv;
use std::time::Duration;
use tokio::time::interval;

mod fetch_price;
mod storage;

use fetch_price::QuoteFetcher;
use storage::InfluxDBStorage;

async fn load_watchlist() -> Vec<String> {
    if let Ok(list) = std::env::var("WATCHLIST") {
        return list.split(',').map(|s| s.trim().to_string()).collect();
    }
    vec!["700.HK".to_string(), "AAPL.US".to_string()]
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let url = std::env::var("INFLUX_URL")?;
    let token = std::env::var("INFLUXDB_AUTH_TOKEN")?;
    
    let fetcher = QuoteFetcher::new().await?;
    let storage = InfluxDBStorage::new(url, token)?;
    let watchlist = load_watchlist().await;

    let mut ticker = interval(Duration::from_secs(30));
    loop {
        ticker.tick().await;
        let prices = fetcher.fetch_prices(&watchlist).await?;
        storage.write_prices(&prices).await?;
    }
}
