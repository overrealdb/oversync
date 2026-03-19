//! Source connector implementations for oversync.
//!
//! Each connector implements [`SourceConnector`] to fetch rows from an external source.

pub mod factory;
pub mod http_source;
pub mod mysql;
pub mod postgres;
pub mod flight_sql;

pub use factory::{FlightSqlSourceFactory, HttpSourceFactory, MysqlSourceFactory, PostgresSourceFactory};
pub use http_source::HttpSource;
pub use mysql::MysqlConnector;
pub use oversync_core::traits::SourceConnector;
pub use postgres::PostgresConnector;
pub use flight_sql::FlightSqlConnector;
