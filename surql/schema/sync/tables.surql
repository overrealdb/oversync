-- sync domain: snapshot, cycle_log, pending_event

-- snapshot: stores the latest known state of each row
DEFINE TABLE OVERWRITE snapshot SCHEMAFULL;
DEFINE FIELD OVERWRITE source_id  ON snapshot TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE query_id   ON snapshot TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE row_key    ON snapshot TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE row_data   ON snapshot TYPE object FLEXIBLE;
DEFINE FIELD OVERWRITE row_hash   ON snapshot TYPE string;
DEFINE FIELD OVERWRITE cycle_id   ON snapshot TYPE int;
DEFINE FIELD OVERWRITE updated_at ON snapshot TYPE datetime DEFAULT time::now();
DEFINE FIELD OVERWRITE prev_hash  ON snapshot TYPE option<string>;
DEFINE INDEX OVERWRITE idx_snapshot_key   ON snapshot FIELDS source_id, query_id, row_key UNIQUE;
DEFINE INDEX OVERWRITE idx_snapshot_cycle ON snapshot FIELDS source_id, query_id, cycle_id;

-- cycle_log: one record per sync cycle
DEFINE TABLE OVERWRITE cycle_log SCHEMAFULL;
DEFINE FIELD OVERWRITE source_id    ON cycle_log TYPE string;
DEFINE FIELD OVERWRITE query_id     ON cycle_log TYPE string;
DEFINE FIELD OVERWRITE cycle_id     ON cycle_log TYPE int;
DEFINE FIELD OVERWRITE started_at   ON cycle_log TYPE datetime;
DEFINE FIELD OVERWRITE finished_at  ON cycle_log TYPE option<datetime>;
DEFINE FIELD OVERWRITE status       ON cycle_log TYPE string DEFAULT 'running';
DEFINE FIELD OVERWRITE rows_fetched ON cycle_log TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE rows_created ON cycle_log TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE rows_updated ON cycle_log TYPE int DEFAULT 0;
DEFINE FIELD OVERWRITE rows_deleted ON cycle_log TYPE int DEFAULT 0;
DEFINE INDEX OVERWRITE idx_cycle_source ON cycle_log FIELDS source_id, query_id, cycle_id UNIQUE;

-- pending_event: outbox for undelivered delta events
DEFINE TABLE OVERWRITE pending_event SCHEMAFULL;
DEFINE FIELD OVERWRITE source_id   ON pending_event TYPE string;
DEFINE FIELD OVERWRITE query_id    ON pending_event TYPE string;
DEFINE FIELD OVERWRITE cycle_id    ON pending_event TYPE int;
DEFINE FIELD OVERWRITE events_json ON pending_event TYPE string;
DEFINE FIELD OVERWRITE created_at  ON pending_event TYPE datetime DEFAULT time::now();
DEFINE INDEX OVERWRITE idx_pending_source ON pending_event FIELDS source_id, query_id;
