use anyhow::Result;
use decimal::Decimal;
use dotenv::dotenv;
use longport::Config;
use longport::quote::QuoteContext;
use std::sync::Arc;

pub struct StockPrice {
    pub symbol: String,
    pub last_done: f64,
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
                Some(StockPrice {
                    symbol: q.symbol.clone(),
                    last_done: q.last_done.to_f64()?,
                })
            })
            .collect();
        Ok(prices)
    }
}
