use anyhow::Result;
use arrow_flight::sql::client::FlightSqlServiceClient;
use df_interchange::Interchange;
use futures::stream::TryStreamExt;
use polars::prelude::*;
use tonic::transport::Channel;

pub struct FlightSqlClient {
    client: FlightSqlServiceClient<Channel>,
}

impl FlightSqlClient {
    pub async fn new(endpoint: &str, token: &str, database: &str) -> Result<Self> {
        let channel = Channel::from_shared(endpoint.to_string())?
            .connect()
            .await?;

        let mut client = FlightSqlServiceClient::new(channel);

        client.set_header("database", database);

        client.set_header("Authorization", format!("Bearer {}", token));

        Ok(Self { client })
    }

    /// 使用 SQL 语句创建 Flight 并获取数据
    pub async fn execute_sql(&mut self, sql: &str) -> Result<DataFrame> {
        let flight_info = self.client.execute(sql.to_string(), None).await?;

        let mut batches = Vec::new();

        for endpoint in flight_info.endpoint {
            if let Some(ticket) = endpoint.ticket {
                let mut stream = self.client.do_get(ticket).await?;

                while let Some(batch) = stream.try_next().await? {
                    batches.push(batch);
                }
            }
        }

        let df = Interchange::from_arrow_56(batches)?.to_polars_0_51()?;

        Ok(df)
    }
}
