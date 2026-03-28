# oversync-connectors

Source connector implementations for oversync.

Part of [oversync](https://github.com/overrealdb/oversync).

## What this crate provides

- **`PostgresConnector`** -- PostgreSQL via sqlx connection pool
- **`MysqlConnector`** -- MySQL via sqlx connection pool
- **`ClickHouseConnector`** -- ClickHouse via HTTP API
- **`TrinoConnector`** -- Trino/Presto via HTTP API
- **`HttpSource`** -- generic REST/JSON API connector
- **`GraphqlConnector`** -- GraphQL endpoint connector
- **`FlightSqlConnector`** -- Apache Arrow Flight SQL connector
- **`McpOriginConnector`** -- MCP protocol connector

Each connector implements `OriginConnector` and has a corresponding `OriginFactory` for creation from JSON config.

## Usage

```rust
use oversync_connectors::{PostgresOriginFactory, HttpOriginFactory};
use oversync_core::traits::OriginFactory;

let factory = PostgresOriginFactory;
let connector = factory.create("pg-prod", &serde_json::json!({
    "dsn": "postgres://user:pass@localhost:5432/mydb"
})).await?;

let rows = connector.fetch_all("SELECT id, name FROM users", "id").await?;
```

## License

Apache-2.0
