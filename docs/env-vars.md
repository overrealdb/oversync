# Environment Variables

## Core

| Variable | Default | Description |
|----------|---------|-------------|
| `OVERSYNC_CONFIG` | `oversync.toml` | Path to TOML config file |
| `OVERSYNC_BIND` | `0.0.0.0:4200` | Control-plane server bind address for the embedded UI and API |
| `OVERSYNC_LOG_LEVEL` | `info` | Log level (trace/debug/info/warn/error) |

## SurrealDB

| Variable | Default | Description |
|----------|---------|-------------|
| `OVERSYNC_SURREALDB_URL` | `http://127.0.0.1:8000` | SurrealDB connection URL |
| `OVERSYNC_SURREALDB_USER` | `root` | SurrealDB username |
| `OVERSYNC_SURREALDB_PASS` | `root` | SurrealDB password |
| `OVERSYNC_SURREALDB_NS` | `oversync` | SurrealDB namespace |
| `OVERSYNC_SURREALDB_DB` | `sync` | SurrealDB database |

## Security

| Variable | Default | Description |
|----------|---------|-------------|
| `OVERSYNC_CREDENTIAL_KEY` | **REQUIRED** | AES-256-GCM encryption passphrase for stored credentials. Must be set when pipes use `credential` references. Use 32+ character random string. |
| `OVERSYNC_API_KEY` | *(none)* | API key for protected endpoints. When set, all mutation/operation endpoints require `Authorization: Bearer <key>` or `X-API-Key: <key>`. Health and metrics endpoints remain public. |

## Observability

| Variable | Default | Description |
|----------|---------|-------------|
| `OVERSYNC_OTEL_ENDPOINT` | *(none)* | OpenTelemetry collector endpoint (e.g. `http://otel-collector:4317`). Enables trace export when set. |

## Horizontal Scaling

| Variable | Default | Description |
|----------|---------|-------------|
| `OVERSYNC_INSTANCE_ID` | *(auto-generated per process)* | Optional instance identifier for distributed locking. If unset, oversync generates a unique process-scoped ID automatically. Set it only when you need a stable explicit identifier in logs or orchestration. |

## Config Interpolation

All values in `oversync.toml` support env var expansion:

```toml
[surrealdb]
url = "${SURREALDB_URL}"
password = "${DB_PASS:-default-pass}"

[[pipes]]
name = "prod-sync"

[pipes.origin]
dsn = "${PG_DSN}"
```

- `${VAR}` — expands to env var value, errors if unset
- `${VAR:-default}` — expands to env var value, falls back to default if unset
- `$$` — literal `$`
