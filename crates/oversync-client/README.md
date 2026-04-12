# oversync-client

Consumer-safe Rust SDK for the oversync control-plane API.

This crate provides:

- wire DTOs shared with the server OpenAPI contract
- a typed `reqwest` client for the pipe-first control-plane API
- a generated Rust client module built from the same OpenAPI snapshot used by the UI SDK
- a stable dependency for external services that should not depend on the server crate internals

`oversync-api` is the server crate. `oversync-client` is the external Rust consumer surface.

## API surfaces

- `OversyncClient` -- the stable handwritten facade for external Rust consumers
- `GeneratedClient` -- the raw client generated from the merged OpenAPI document
- `types` -- consumer-safe wire DTOs shared with the server schema annotations

The generated module is built from `openapi.json`. In the workspace that snapshot is synchronized from `ui/openapi.json`; in a published crate the packaged snapshot is used directly.
