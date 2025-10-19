use crate::fetch_price::StockPrice;
use anyhow::{Result, anyhow};
use reqwest::Client;

pub struct InfluxDBStorage {
    client: Client,
    url: String,
    token: String,
}

impl InfluxDBStorage {
    pub fn new() -> Result<Self> {
        let url = std::env::var("INFLUX_URL")?;
        let token = std::env::var("INFLUXDB_AUTH_TOKEN")?;
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
            .map(|p| format!("stock_price,symbol={} price={}", p.symbol, p.price))
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

#[cfg(test)]
mod test {
    use super::*;
    #[tokio::test]
    async fn test_influxdb_write() {
        let storage = InfluxDBStorage::new().unwrap();
        let prices = vec![
            StockPrice {
                symbol: "AAPL.US".to_string(),
                last_done: 150.0,
            },
            StockPrice {
                symbol: "AMD.US".to_string(),
                last_done: 100.0,
            },
        ];
        let res = storage.write_prices(&prices).await;
        assert!(res.is_ok());
    }
}
