//! Sink implementations for oversync.

pub mod clickhouse_sink;
pub mod factory;
pub mod http_sink;
pub mod kafka;
pub mod mcp_sink;
pub mod mysql_sink;
pub mod postgres_sink;
pub mod stdout;
pub mod surrealdb_sink;

pub use clickhouse_sink::ClickHouseSink;
pub use factory::{
	ClickHouseTargetFactory, HttpTargetFactory, KafkaTargetFactory, McpTargetFactory,
	MysqlTargetFactory, PostgresTargetFactory, StdoutTargetFactory, SurrealDbTargetFactory,
};
pub use http_sink::HttpSink;
pub use kafka::{KafkaKeyFormat, KafkaSink, KafkaSinkFormat, KafkaValueFormat};
pub use mcp_sink::McpSink;
pub use mysql_sink::MysqlSink;
pub use oversync_core::traits::Sink;
pub use postgres_sink::PostgresSink;
pub use stdout::StdoutSink;
pub use surrealdb_sink::{SinkMode, SurrealDbSink, SurrealDbSinkConfig};
