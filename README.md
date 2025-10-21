# Finfo

Finfo 是一个基于 Rust 的金融数据 getter，将长桥的特定股票行情定时存入influxdb, 并提供了基于Arrow_flight接口的SQL高性能查询接口（如果未来RUST的arrow生态不像现在这样割裂会加入基于Polars的数据分析）。

## 依赖
longport = "3.0.13"
tokio = { version = "1.28", features = ["full"] }
dotenv = "0.15.0"
reqwest = { version = "0.11", features = ["json"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
anyhow = "1.0"

arrow-array = "56" 
arrow-schema = "56"
arrow = { version = "56", features = ["prettyprint"] }
arrow-flight = { version = "56", features = ["flight-sql"] }

tonic = "0.13.1"
bytes = "1.10.1"
futures = "0.3.31"

## 配置

### 环境变量

创建 `.env` 文件：

```env
INFLUXDB_URL=http://localhost:8181
INFLUXDB_TOKEN=your-influxdb-token
DATABASE=your-database-name
```
或者在系统环境变量中进行配置

### 调用规范
详见examples文件夹

## 困境
使用rust + influxdb3 + Apache Arrow + Polars 进行数据存储和分析根本是天方夜谭，原因如下：
1.Influxdb3使用rust开发，但是没有提供一个官方的crate进行数据查询和存储，想要以Arrow格式在进程间传输，只能用apache Arrow开发者们构建的Arrow crate
2.Polars 底层的arrow库用的不是官方的arrow库，而是自己重写了一个polars_arrow，并且据我所知没有提供与官方Arrow 的对接方式，无论是batch的转换还是从网络流接收，与此同时，python的polars库对于这两种对接方式都有良好的支持
3.综上所述，rust的Arrow生态有严重的割裂问题，并且很多地方没有得到良好支持

## TODO
1.加入订阅拉取
2.更多金融数据
3.数据分析（如果Polars的开发人员终于意识到现在的解决方案到底有多愚蠢）