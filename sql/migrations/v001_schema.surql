-- v001_schema.surql -- Core tables for the oversync delta engine

-- snapshot: stores the latest known state of each row
DEFINE TABLE snapshot SCHEMAFULL;
DEFINE FIELD source_id  ON snapshot TYPE string ASSERT $value != NONE;
DEFINE FIELD query_id   ON snapshot TYPE string ASSERT $value != NONE;
DEFINE FIELD row_key    ON snapshot TYPE string ASSERT $value != NONE;
DEFINE FIELD row_data   ON snapshot TYPE object FLEXIBLE;
DEFINE FIELD row_hash   ON snapshot TYPE string;
DEFINE FIELD cycle_id   ON snapshot TYPE int;
DEFINE FIELD updated_at ON snapshot TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_snapshot_key ON snapshot FIELDS source_id, query_id, row_key UNIQUE;

-- cycle_log: one record per sync cycle
DEFINE TABLE cycle_log SCHEMAFULL;
DEFINE FIELD source_id    ON cycle_log TYPE string;
DEFINE FIELD query_id     ON cycle_log TYPE string;
DEFINE FIELD cycle_id     ON cycle_log TYPE int;
DEFINE FIELD started_at   ON cycle_log TYPE datetime;
DEFINE FIELD finished_at  ON cycle_log TYPE option<datetime>;
DEFINE FIELD status       ON cycle_log TYPE string DEFAULT 'running';
DEFINE FIELD rows_fetched ON cycle_log TYPE int DEFAULT 0;
DEFINE FIELD rows_created ON cycle_log TYPE int DEFAULT 0;
DEFINE FIELD rows_updated ON cycle_log TYPE int DEFAULT 0;
DEFINE FIELD rows_deleted ON cycle_log TYPE int DEFAULT 0;
DEFINE INDEX idx_cycle_source ON cycle_log FIELDS source_id, query_id, cycle_id UNIQUE;

-- source_config: registered source connectors
DEFINE TABLE source_config SCHEMAFULL;
DEFINE FIELD name       ON source_config TYPE string ASSERT $value != NONE;
DEFINE FIELD connector  ON source_config TYPE string ASSERT $value != NONE;
DEFINE FIELD config     ON source_config TYPE object FLEXIBLE;
DEFINE FIELD enabled    ON source_config TYPE bool DEFAULT true;
DEFINE FIELD created_at ON source_config TYPE datetime DEFAULT time::now();
DEFINE FIELD updated_at ON source_config TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_source_name ON source_config FIELDS name UNIQUE;

-- query_config: queries within a source
DEFINE TABLE query_config SCHEMAFULL;
DEFINE FIELD source_id  ON query_config TYPE string ASSERT $value != NONE;
DEFINE FIELD name       ON query_config TYPE string ASSERT $value != NONE;
DEFINE FIELD query      ON query_config TYPE string;
DEFINE FIELD key_column ON query_config TYPE string;
DEFINE FIELD schedule   ON query_config TYPE option<string>;
DEFINE FIELD sinks      ON query_config TYPE option<array>;
DEFINE FIELD enabled    ON query_config TYPE bool DEFAULT true;
DEFINE FIELD created_at ON query_config TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_query_source ON query_config FIELDS source_id, name UNIQUE;

-- sink_config: registered sinks
DEFINE TABLE sink_config SCHEMAFULL;
DEFINE FIELD name       ON sink_config TYPE string ASSERT $value != NONE;
DEFINE FIELD sink_type  ON sink_config TYPE string ASSERT $value != NONE;
DEFINE FIELD config     ON sink_config TYPE object FLEXIBLE;
DEFINE FIELD enabled    ON sink_config TYPE bool DEFAULT true;
DEFINE FIELD created_at ON sink_config TYPE datetime DEFAULT time::now();
DEFINE FIELD updated_at ON sink_config TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_sink_name ON sink_config FIELDS name UNIQUE;

-- link_rule: cross-source entity linking rules
DEFINE TABLE link_rule SCHEMAFULL;
DEFINE FIELD name       ON link_rule TYPE string ASSERT $value != NONE;
DEFINE FIELD rule_type  ON link_rule TYPE string;
DEFINE FIELD config     ON link_rule TYPE object FLEXIBLE;
DEFINE FIELD enabled    ON link_rule TYPE bool DEFAULT true;
DEFINE FIELD created_at ON link_rule TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_link_name ON link_rule FIELDS name UNIQUE;

-- resolved_link: discovered cross-source relationships
DEFINE TABLE resolved_link SCHEMAFULL;
DEFINE FIELD source_key  ON resolved_link TYPE string;
DEFINE FIELD target_key  ON resolved_link TYPE string;
DEFINE FIELD rule_name   ON resolved_link TYPE string;
DEFINE FIELD confidence  ON resolved_link TYPE float DEFAULT 1.0;
DEFINE FIELD created_at  ON resolved_link TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_resolved_pair ON resolved_link FIELDS source_key, target_key UNIQUE;

-- plugin: WASM plugin registry
DEFINE TABLE plugin SCHEMAFULL;
DEFINE FIELD name       ON plugin TYPE string ASSERT $value != NONE;
DEFINE FIELD version    ON plugin TYPE string;
DEFINE FIELD wasm_hash  ON plugin TYPE option<string>;
DEFINE FIELD enabled    ON plugin TYPE bool DEFAULT true;
DEFINE FIELD created_at ON plugin TYPE datetime DEFAULT time::now();
DEFINE INDEX idx_plugin_name ON plugin FIELDS name UNIQUE;
