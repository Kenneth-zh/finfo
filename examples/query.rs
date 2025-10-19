use finfo::query::FlightQueryClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = "http://localhost:8082";
    let sql =
        "SELECT time, temp, hum FROM home WHERE room = 'Living Room' ORDER BY time DESC LIMIT 5";

    let mut client = FlightQueryClient::connect(endpoint).await?;
    let batches = client.query(sql).await?;

    for batch in batches {
        println!(
            "RecordBatch: {} rows, columns: {:?}",
            batch.num_rows(),
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );
        // 打印每行
        for row in 0..batch.num_rows() {
            let mut row_str = String::new();
            for col in 0..batch.num_columns() {
                row_str.push_str(&format!("{:?} ", batch.column(col).as_any()));
            }
            println!("Row {}: {}", row, row_str);
        }
    }
    Ok(())
}
