use anyhow::Result;
use dotenv::dotenv;
use longport::Config;
use longport::quote::QuoteContext;
use std::sync::Arc;

pub struct StockPrice {
    pub symbol: String,
    pub last_done: f64,
    pub prev_close: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub timestamp: i64,
    pub volume: i64,
    pub turnover: f64,
}

pub struct QuoteFetcher {
    quote_ctx: QuoteContext,
}

impl QuoteFetcher {
    pub async fn new() -> Result<Self> {
        dotenv().ok();
        let config = Arc::new(Config::from_env()?);

        let (quote_ctx, _) = QuoteContext::try_new(config).await?;
        Ok(Self { quote_ctx })
    }

    pub async fn fetch_prices(&self, symbols: &[String]) -> Result<Vec<StockPrice>> {
        let quotes = self.quote_ctx.quote(symbols).await?;
        let prices = quotes
            .into_iter()
            .filter_map(|q| {
                let price = q.last_done.to_string().parse::<f64>().ok()?; //possible precision loss
                let prev_close = q.prev_close.to_string().parse::<f64>().ok()?;
                let open = q.open.to_string().parse::<f64>().ok()?;
                let high = q.high.to_string().parse::<f64>().ok()?;
                let low = q.low.to_string().parse::<f64>().ok()?;
                let volume = q.volume as i64;
                let turnover = q.turnover.to_string().parse::<f64>().ok()?;
                let timestamp = q.timestamp.unix_timestamp();
                Some(StockPrice {
                    symbol: q.symbol.clone(),
                    last_done: price,
                    prev_close,
                    open,
                    high,
                    low,
                    timestamp,
                    volume,
                    turnover,
                })
            })
            .collect();
        Ok(prices)
    }
}
