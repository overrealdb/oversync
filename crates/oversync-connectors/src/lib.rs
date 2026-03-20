//! Source connector implementations for oversync.
//!
//! Each connector implements [`OriginConnector`] to fetch rows from an external source.

pub mod clickhouse;
pub mod factory;
pub mod graphql;
pub mod http_common;
pub mod http_source;
pub mod mcp;
pub mod mysql;
pub mod postgres;
pub mod flight_sql;
pub mod trino;

pub use clickhouse::ClickHouseConnector;
pub use factory::{
	ClickHouseOriginFactory, FlightSqlOriginFactory, GraphqlOriginFactory, HttpOriginFactory,
	McpOriginFactory, MysqlOriginFactory, PostgresOriginFactory, TrinoOriginFactory,
};
pub use mcp::McpOriginConnector;
pub use graphql::GraphqlConnector;
pub use http_source::HttpSource;
pub use mysql::MysqlConnector;
pub use oversync_core::traits::OriginConnector;
pub use postgres::PostgresConnector;
pub use trino::TrinoConnector;
pub use flight_sql::FlightSqlConnector;
