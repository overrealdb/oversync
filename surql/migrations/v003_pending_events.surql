-- v003_pending_events.surql -- Outbox for undelivered delta events
DEFINE TABLE IF NOT EXISTS pending_event SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS source_id   ON pending_event TYPE string;
DEFINE FIELD IF NOT EXISTS query_id    ON pending_event TYPE string;
DEFINE FIELD IF NOT EXISTS cycle_id    ON pending_event TYPE int;
DEFINE FIELD IF NOT EXISTS events_json ON pending_event TYPE string;
DEFINE FIELD IF NOT EXISTS created_at  ON pending_event TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS idx_pending_source ON pending_event FIELDS source_id, query_id;
