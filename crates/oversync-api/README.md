# oversync-api

Server-side Axum API crate for the shared oversync control-plane surface.

Part of [oversync](https://github.com/overrealdb/oversync).

## What this crate provides

- **CRUD endpoints for pipes, saved recipes, and sinks** -- create, read, update, delete via REST
- **Pipe-first control plane** -- runtime onboarding, resolve, dry-run, and import/export all flow through pipes
- **Operational controls** -- trigger sync, pause/resume, view sync status and cycle history
- **OpenAPI spec** -- auto-generated via utoipa, served at `/openapi.json`
- **SDK source-of-truth** -- the merged spec drives both the React TypeScript SDK and the Rust client crate
- **API key auth middleware** -- optional authentication on protected routes

## Engine vs crate routes

`oversync-api::router(...)` serves the shared control-plane routes and its base OpenAPI document.

`oversync-client` is the external Rust consumer surface. This crate is intentionally server-oriented.

`OversyncEngine::api_router()` in the root `oversync` crate merges this base spec with engine-owned routes such as:

- `/pipes/dry-run`
- `/pipes/{name}/resolve`
- `/credentials`
- `/config/versions`

That merged router is the recommended surface for standalone and embedded API servers. In the standalone control plane it is also mounted under `/api/*` so the embedded UI can use a same-origin API path without a second frontend service.

The `oversync` binary also exposes that same merged spec via:

```bash
oversync openapi
oversync openapi --file ui/openapi.json
```

The frontend then generates `ui/src/api/generated/*` from that file with `@hey-api/openapi-ts`.

External Rust consumers should depend on `oversync-client`, which ships:

- consumer-safe wire DTOs
- a typed `reqwest` client
- a generated Rust client module from the same merged OpenAPI snapshot
- update-request serialization semantics that preserve `omit` vs `null`

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET/POST | `/pipes` | List / create pipes |
| GET/PUT/DELETE | `/pipes/{name}` | Get / update / delete pipe |
| POST | `/pipes/{name}/run` | Trigger a single manual run for one pipe |
| GET/POST | `/pipe-presets` | List / create saved recipes |
| GET/PUT/DELETE | `/pipe-presets/{name}` | Get / update / delete saved recipe |
| GET/POST | `/sinks` | List / create sinks |
| GET/POST | `/config/export`, `/config/import` | Export / replace control-plane config |
| POST | `/sync/pause` | Pause sync |
| POST | `/sync/resume` | Resume sync |
| GET | `/sync/status` | Current sync status |
| GET | `/history` | Cycle history |

There are no legacy `/sources` routes in the control plane anymore. Pipe CRUD is the only runtime onboarding surface.

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
