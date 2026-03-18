//! Source connector implementations for oversync.
//!
//! Each connector implements [`SourceConnector`] to fetch rows from an external source.

pub mod factory;
pub mod postgres;

pub use factory::PostgresSourceFactory;
pub use oversync_core::traits::SourceConnector;
pub use postgres::PostgresConnector;
