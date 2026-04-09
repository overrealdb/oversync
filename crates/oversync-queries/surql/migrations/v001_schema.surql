-- v001_schema.surql -- Core tables for the oversync delta engine

-- snapshot: stores the latest known state of each row
DEFINE TABLE IF NOT EXISTS snapshot SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS origin_id  ON snapshot TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS query_id   ON snapshot TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS row_key    ON snapshot TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS row_data   ON snapshot TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS row_hash   ON snapshot TYPE string;
DEFINE FIELD IF NOT EXISTS cycle_id   ON snapshot TYPE int;
DEFINE FIELD IF NOT EXISTS updated_at ON snapshot TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS idx_snapshot_key ON snapshot FIELDS origin_id, query_id, row_key UNIQUE;

-- cycle_log: one record per sync cycle
DEFINE TABLE IF NOT EXISTS cycle_log SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS origin_id    ON cycle_log TYPE string;
DEFINE FIELD IF NOT EXISTS query_id     ON cycle_log TYPE string;
DEFINE FIELD IF NOT EXISTS cycle_id     ON cycle_log TYPE int;
DEFINE FIELD IF NOT EXISTS started_at   ON cycle_log TYPE datetime;
DEFINE FIELD IF NOT EXISTS finished_at  ON cycle_log TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS status       ON cycle_log TYPE string DEFAULT 'running';
DEFINE FIELD IF NOT EXISTS rows_fetched ON cycle_log TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS rows_created ON cycle_log TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS rows_updated ON cycle_log TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS rows_deleted ON cycle_log TYPE int DEFAULT 0;
DEFINE INDEX IF NOT EXISTS idx_cycle_source ON cycle_log FIELDS origin_id, query_id, cycle_id UNIQUE;

-- source_config: registered source connectors
DEFINE TABLE IF NOT EXISTS source_config SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS name       ON source_config TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS connector  ON source_config TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS config     ON source_config TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS enabled    ON source_config TYPE bool DEFAULT true;
DEFINE FIELD IF NOT EXISTS created_at ON source_config TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS updated_at ON source_config TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS idx_source_name ON source_config FIELDS name UNIQUE;

-- query_config: queries within a source
DEFINE TABLE IF NOT EXISTS query_config SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS origin_id  ON query_config TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS name       ON query_config TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS query      ON query_config TYPE string;
DEFINE FIELD IF NOT EXISTS key_column ON query_config TYPE string;
DEFINE FIELD IF NOT EXISTS schedule   ON query_config TYPE option<string>;
DEFINE FIELD IF NOT EXISTS sinks      ON query_config TYPE option<array>;
DEFINE FIELD IF NOT EXISTS transform  ON query_config TYPE option<string>;
DEFINE FIELD IF NOT EXISTS enabled    ON query_config TYPE bool DEFAULT true;
DEFINE FIELD IF NOT EXISTS created_at ON query_config TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS idx_query_source ON query_config FIELDS origin_id, name UNIQUE;

-- pipe_config: complete pipeline definitions (origin → delta → transform → target)
DEFINE TABLE IF NOT EXISTS pipe_config SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS name               ON pipe_config TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS origin_connector   ON pipe_config TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS origin_dsn         ON pipe_config TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS origin_credential  ON pipe_config TYPE option<string>;
DEFINE FIELD IF NOT EXISTS trino_url          ON pipe_config TYPE option<string>;
DEFINE FIELD IF NOT EXISTS origin_config      ON pipe_config TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS targets            ON pipe_config TYPE array DEFAULT [];
DEFINE FIELD IF NOT EXISTS schedule           ON pipe_config TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS delta              ON pipe_config TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS retry              ON pipe_config TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS filters            ON pipe_config TYPE array DEFAULT [];
DEFINE FIELD IF NOT EXISTS filters.*          ON pipe_config TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS transforms         ON pipe_config TYPE array DEFAULT [];
DEFINE FIELD IF NOT EXISTS transforms.*       ON pipe_config TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS links              ON pipe_config TYPE array DEFAULT [];
DEFINE FIELD IF NOT EXISTS links.*            ON pipe_config TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS enabled            ON pipe_config TYPE bool DEFAULT true;
DEFINE FIELD IF NOT EXISTS created_at         ON pipe_config TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS updated_at         ON pipe_config TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS idx_pipe_name ON pipe_config FIELDS name UNIQUE;

-- sink_config: registered sinks
DEFINE TABLE IF NOT EXISTS sink_config SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS name       ON sink_config TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS sink_type  ON sink_config TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS config     ON sink_config TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS enabled    ON sink_config TYPE bool DEFAULT true;
DEFINE FIELD IF NOT EXISTS created_at ON sink_config TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS updated_at ON sink_config TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS idx_sink_name ON sink_config FIELDS name UNIQUE;

-- link_rule: cross-source entity linking rules
DEFINE TABLE IF NOT EXISTS link_rule SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS name       ON link_rule TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS rule_type  ON link_rule TYPE string;
DEFINE FIELD IF NOT EXISTS config     ON link_rule TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS enabled    ON link_rule TYPE bool DEFAULT true;
DEFINE FIELD IF NOT EXISTS created_at ON link_rule TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS idx_link_name ON link_rule FIELDS name UNIQUE;

-- resolved_link: discovered cross-source relationships
DEFINE TABLE IF NOT EXISTS resolved_link SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS source_key  ON resolved_link TYPE string;
DEFINE FIELD IF NOT EXISTS target_key  ON resolved_link TYPE string;
DEFINE FIELD IF NOT EXISTS rule_name   ON resolved_link TYPE string;
DEFINE FIELD IF NOT EXISTS confidence  ON resolved_link TYPE float DEFAULT 1.0;
DEFINE FIELD IF NOT EXISTS created_at  ON resolved_link TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS idx_resolved_pair ON resolved_link FIELDS source_key, target_key UNIQUE;

-- plugin: WASM plugin registry
DEFINE TABLE IF NOT EXISTS plugin SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS name       ON plugin TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS version    ON plugin TYPE string;
DEFINE FIELD IF NOT EXISTS wasm_hash  ON plugin TYPE option<string>;
DEFINE FIELD IF NOT EXISTS enabled    ON plugin TYPE bool DEFAULT true;
DEFINE FIELD IF NOT EXISTS created_at ON plugin TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS idx_plugin_name ON plugin FIELDS name UNIQUE;
