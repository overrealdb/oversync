# oversync-client

Consumer-safe Rust SDK for the oversync control-plane API.

This crate provides:

- wire DTOs shared with the server OpenAPI contract
- a typed `reqwest` client for the pipe-first control-plane API
- a stable dependency for external services that should not depend on the server crate internals

`oversync-api` is the server crate. `oversync-client` is the external Rust consumer surface.
