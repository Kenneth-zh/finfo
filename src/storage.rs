use crate::fetch_price::StockPrice;
use anyhow::{Result, anyhow};
use reqwest::Client;

pub struct InfluxDBStorage {
    client: Client,
    url: String,
    token: String,
}

impl InfluxDBStorage {
    pub fn new(url: String, token: String) -> Result<Self> {
        Ok(Self {
            client: Client::new(),
            url,
            token,
        })
    }

    pub async fn write_prices(&self, prices: &[StockPrice]) -> Result<()> {
        if prices.is_empty() {
            return Ok(());
        }
        let line_protocol = prices
            .iter()
            .map(|p| format!("stock_price,symbol={} last_done={},prev_close={},open={},high={},low={},volume={},turnover={} {}", 
                p.symbol, p.last_done, p.prev_close, p.open, p.high, p.low, p.volume, p.turnover, p.timestamp))
            .collect::<Vec<_>>()
            .join("\n");

        let resp = self
            .client
            .post(&self.url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "text/plain")
            .body(line_protocol)
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(anyhow!(
                "InfluxDB write failed: {}",
                resp.text().await.unwrap_or_default()
            ))
        }
    }
}
