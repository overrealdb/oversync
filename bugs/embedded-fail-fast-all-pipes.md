# Bug: One pipe failure stops all pipes in EmbeddedSync

**Reported:** 2026-04-03
**Severity:** High
**Component:** `oversync::embedded::run_all_pipes`

## Problem

When `EmbeddedSync` runs multiple pipes and one pipe fails to create a connector (e.g., PostgreSQL pool timeout), the entire embedded sync stops — including pipes that could have succeeded.

## Reproduction

Configure 3 pipes pointing to the same PostgreSQL (different queries):
- `store_pg` (entities, 30s interval)
- `lineage_pg` (edges, 60s interval)  
- `quality_pg` (checks/results, 300s interval)

All 3 start simultaneously. If PG `max_connections` is low or pool exhaustion occurs, one pipe fails:

```
ERROR oversync::embedded: failed to create connector pipe=lineage_pg 
  error=connector: postgres connect: pool timed out while waiting for an open connection
INFO  oversync::embedded: embedded sync stopped
```

All pipes stop, including those that might have connected fine.

## Expected behavior

- Each pipe should be independent — one failure should not stop others
- Failed pipes should retry according to `max_retries` / `retry_base_delay_secs`
- At minimum, log which pipes succeeded vs failed

## Secondary issue

When 3 pipes target the same PG DSN, they each create separate connection pools. This can exhaust `max_connections`. Consider sharing a pool per unique DSN.

## Workaround

Use standalone oversync (`oversync serve`) which handles this correctly — scheduler retries individual pipes independently.
