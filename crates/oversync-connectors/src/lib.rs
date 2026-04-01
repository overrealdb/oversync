//! Source connector implementations for oversync.
//!
//! Each connector implements [`OriginConnector`] to fetch rows from an external source.

pub mod clickhouse;
pub mod factory;
pub mod flight_sql;
pub mod graphql;
pub mod http_common;
pub mod http_source;
pub mod kafka_source;
pub mod mcp;
pub mod mysql;
pub mod postgres;
pub mod surrealdb_source;
pub mod trino;

pub use clickhouse::ClickHouseConnector;
pub use factory::{
	ClickHouseOriginFactory, FlightSqlOriginFactory, GraphqlOriginFactory, HttpOriginFactory,
	KafkaOriginFactory, McpOriginFactory, MysqlOriginFactory, PostgresOriginFactory,
	SurrealDbOriginFactory, TrinoOriginFactory,
};
pub use flight_sql::FlightSqlConnector;
pub use graphql::GraphqlConnector;
pub use http_source::HttpSource;
pub use kafka_source::KafkaSourceConnector;
pub use mcp::McpOriginConnector;
pub use mysql::MysqlConnector;
pub use oversync_core::traits::OriginConnector;
pub use postgres::PostgresConnector;
pub use surrealdb_source::SurrealDbConnector;
pub use trino::TrinoConnector;
