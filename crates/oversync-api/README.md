# oversync-api

Type-safe HTTP API for managing oversync sources, sinks, and sync status.

Part of [oversync](https://github.com/overrealdb/oversync).

## What this crate provides

- **CRUD endpoints for sources, sinks, and pipes** -- create, read, update, delete via REST
- **Query management** -- per-source query configuration endpoints
- **Operational controls** -- trigger sync, pause/resume, view sync status and cycle history
- **OpenAPI spec** -- auto-generated via utoipa, served at `/openapi.json`
- **API key auth middleware** -- optional authentication on protected routes

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET/POST | `/sources` | List / create sources |
| GET/PUT/DELETE | `/sources/{name}` | Get / update / delete source |
| POST | `/sources/{name}/trigger` | Trigger immediate sync |
| GET/POST | `/sinks` | List / create sinks |
| GET/POST | `/pipes` | List / create pipes |
| POST | `/sync/pause` | Pause sync |
| POST | `/sync/resume` | Resume sync |
| GET | `/sync/status` | Current sync status |
| GET | `/history` | Cycle history |

## Usage

```rust
use oversync_api::{router, ApiDoc};
use std::sync::Arc;

let state = Arc::new(api_state);
let app = router(state);
let listener = tokio::net::TcpListener::bind("0.0.0.0:4300").await?;
axum::serve(listener, app).await?;
```

## License

Apache-2.0
