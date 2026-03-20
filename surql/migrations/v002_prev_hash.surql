-- v002_prev_hash.surql -- Add prev_hash for DB-side delta computation
DEFINE FIELD IF NOT EXISTS prev_hash ON snapshot TYPE option<string>;
DEFINE INDEX IF NOT EXISTS idx_snapshot_cycle ON snapshot FIELDS origin_id, query_id, cycle_id;
