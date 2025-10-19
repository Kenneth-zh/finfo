use anyhow::{Result, anyhow};
use arrow_array::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::CommandStatementQuery;
use arrow_flight::sql::client::FlightSqlClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_schema::Schema;
use std::sync::Arc;
use tonic::Request;
use tonic::transport::Channel;

/// FlightSQL 查询组件
pub struct FlightQueryClient {
    client: FlightSqlClient<FlightServiceClient<Channel>>,
}

impl FlightQueryClient {
    /// 创建新的 FlightQueryClient
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let channel = Channel::from_shared(endpoint.to_string())?
            .connect()
            .await?;
        let client = FlightSqlClient::new(FlightServiceClient::new(channel));
        Ok(Self { client })
    }

    /// 执行 SQL 查询，返回所有 RecordBatch
    pub async fn query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        // 1. 获取 FlightInfo
        let query_command = CommandStatementQuery {
            query: sql.to_string(),
        };
        let flight_info = self
            .client
            .get_flight_info(Request::new(query_command))
            .await?
            .into_inner();

        let endpoint = flight_info
            .endpoint
            .first()
            .ok_or_else(|| anyhow!("FlightInfo did not contain any endpoint"))?;
        let ticket = endpoint
            .ticket
            .clone()
            .ok_or_else(|| anyhow!("Endpoint did not contain a ticket"))?;

        // 2. DoGet 获取数据流
        let mut stream = self.client.do_get(Request::new(ticket)).await?.into_inner();

        // 3. 解析 Arrow RecordBatch
        let mut batches = Vec::new();
        let mut schema: Option<Arc<Schema>> = None;
        while let Some(flight_data) = stream.message().await? {
            if flight_data.data_header.is_empty() {
                continue;
            }
            if schema.is_none() {
                schema = Some(Arc::new(arrow_schema::Schema::try_from(&flight_data)?));
                continue;
            }
            let batch = flight_data_to_arrow_batch(
                &flight_data,
                schema.as_ref().unwrap(),
                &Default::default(),
            )?;
            batches.push(batch);
        }
        Ok(batches)
    }
}
