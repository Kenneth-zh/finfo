use dotenv;
use finfo::fetch_price::StockPrice;
use finfo::storage::InfluxDBStorage;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    let url = std::env::var("INFLUX_URL").unwrap();
    let token = std::env::var("INFLUXDB_AUTH_TOKEN").unwrap();
    let storage = InfluxDBStorage::new(url, token).unwrap();
    let prices = vec![StockPrice {
        symbol: "AAPL.US".to_string(),
        last_done: 150.0,
        prev_close: 148.0,
        open: 149.0,
        high: 151.0,
        low: 147.5,
        timestamp: 1700000000,
        volume: 1000000,
        turnover: 150000000.0,
    }];
    let res = storage.write_prices(&prices).await;
    assert!(res.is_ok());
}
