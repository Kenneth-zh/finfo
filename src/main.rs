use anyhow::Result;
use dotenv::dotenv;
use longport::{Config, Decimal, quote::*};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use time::macros::date;
use tokio::fs;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::*;

const BATCH_MAX: usize = 500;
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct Kline {
    symbol: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
    turnover: f64,
    timestamp: i64,
}

async fn load_watchlist() -> Result<Vec<String>> {
    let data = fs::read_to_string("watchlist.json").await?;
    let watchlist: Vec<String> = serde_json::from_str(&data)?;
    Ok(watchlist)
}

fn decimal_to_f64(d: Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let url = std::env::var("INFLUX_URL")?;
    let db = std::env::var("DATABASE")?;
    let storage_url = format!("{}/api/v3/write_lp?db={}&precision=second", url, db);
    let token = std::env::var("INFLUXDB_AUTH_TOKEN")?;

    //let storage = InfluxDBStorage::new(storage_url, token)?;
    let watchlist = load_watchlist().await?;

    let (tx, rx) = mpsc::channel::<Vec<Kline>>(256);

    let writer_handle = tokio::spawn(async move {
        if let Err(e) = writer_task(rx, storage_url, token).await {
            eprintln!("writer error: {}", e);
        }
    });

    let config = Arc::new(Config::from_env()?);
    let (quote_ctx, _) = QuoteContext::try_new(config).await?;
    for sym in watchlist {
        let tx_clone = tx.clone();
        let qctx_clone = quote_ctx.clone();
        tokio::spawn(async move {
            if let Err(e) = getter_task(tx_clone, qctx_clone, sym).await {
                eprintln!("produce error: {}", e);
            }
        });
    }

    drop(tx);

    if let Err(e) = writer_handle.await {
        eprintln!("writer task join error: {:?}", e);
    }

    Ok(())
}

async fn getter_task(
    sender: Sender<Vec<Kline>>,
    quote_ctx: QuoteContext,
    symbol: String,
) -> Result<()> {
    let candlesticks = quote_ctx
        .history_candlesticks_by_date(
            symbol.clone(),
            Period::Day,
            AdjustType::NoAdjust,
            Some(date!(2025 - 01 - 01)),
            None,
            TradeSessions::Intraday,
        )
        .await?;

    let mut klines = Vec::with_capacity(candlesticks.len());
    for c in candlesticks.into_iter() {
        let k = Kline {
            symbol: symbol.clone(),
            open: decimal_to_f64(c.open),
            high: decimal_to_f64(c.high),
            low: decimal_to_f64(c.low),
            close: decimal_to_f64(c.close),
            volume: c.volume,
            turnover: decimal_to_f64(c.turnover),
            timestamp: c.timestamp.unix_timestamp(),
        };
        klines.push(k);
    }

    sender
        .send(klines)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(())
}

async fn writer_task(mut rx: Receiver<Vec<Kline>>, url: String, token: String) -> Result<()> {
    let client = Client::new();
    let mut buffer: Vec<String> = Vec::with_capacity(BATCH_MAX);
    let mut last_flush = Instant::now();

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(batch) => {
                        for k in batch {
                            buffer.push(format!(
                                "kline,symbol={} open={},high={},low={},close={},volume={},turnover={} {}",
                                k.symbol,
                                k.open,
                                k.high,
                                k.low,
                                k.close,
                                k.volume,
                                k.turnover,
                                k.timestamp
                            ));
                        }

                        if buffer.len() >= BATCH_MAX {
                            flush_lines(&client, &url, &token, &mut buffer).await?;
                            last_flush = Instant::now();
                        }
                    }
                    None => {
                        if !buffer.is_empty() {
                            flush_lines(&client, &url, &token, &mut buffer).await?;
                        }
                        break;
                    }
                }
            }
            _ = sleep_until(last_flush + FLUSH_INTERVAL) => {
                if !buffer.is_empty() {
                    flush_lines(&client, &url, &token, &mut buffer).await?;
                    last_flush = Instant::now();
                }
            }
        }
    }

    Ok(())
}

async fn flush_lines(
    client: &Client,
    url: &str,
    token: &str,
    buffer: &mut Vec<String>,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let body = buffer.join("\n");

    let mut err_opt = None;
    for attempt in 0..3 {
        let resp = client
            .post(url)
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "text/plain")
            .body(body.clone())
            .send()
            .await;

        match resp {
            Ok(r) if r.status().is_success() => {
                buffer.clear();
                return Ok(());
            }
            Ok(r) => {
                err_opt = Some(anyhow::anyhow!("write failed {}", r.status()));
            }
            Err(e) => {
                err_opt = Some(anyhow::anyhow!(e));
            }
        }
        // 指数退避
        sleep(Duration::from_millis(100u64 * 2u64.pow(attempt))).await;
    }

    Err(err_opt.unwrap_or_else(|| anyhow::anyhow!("unknown write error")))
}
