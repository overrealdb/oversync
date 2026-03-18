-- config domain: source_config, query_config, sink_config

-- source_config: registered source connectors
DEFINE TABLE OVERWRITE source_config SCHEMAFULL;
DEFINE FIELD OVERWRITE name       ON source_config TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE connector  ON source_config TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE config     ON source_config TYPE object FLEXIBLE;
DEFINE FIELD OVERWRITE enabled    ON source_config TYPE bool DEFAULT true;
DEFINE FIELD OVERWRITE created_at ON source_config TYPE datetime DEFAULT time::now();
DEFINE FIELD OVERWRITE updated_at ON source_config TYPE datetime DEFAULT time::now();
DEFINE INDEX OVERWRITE idx_source_name ON source_config FIELDS name UNIQUE;

-- query_config: queries within a source
DEFINE TABLE OVERWRITE query_config SCHEMAFULL;
DEFINE FIELD OVERWRITE source_id  ON query_config TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE name       ON query_config TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE query      ON query_config TYPE string;
DEFINE FIELD OVERWRITE key_column ON query_config TYPE string;
DEFINE FIELD OVERWRITE schedule   ON query_config TYPE option<string>;
DEFINE FIELD OVERWRITE sinks      ON query_config TYPE option<array>;
DEFINE FIELD OVERWRITE enabled    ON query_config TYPE bool DEFAULT true;
DEFINE FIELD OVERWRITE created_at ON query_config TYPE datetime DEFAULT time::now();
DEFINE INDEX OVERWRITE idx_query_source ON query_config FIELDS source_id, name UNIQUE;

-- sink_config: registered sinks
DEFINE TABLE OVERWRITE sink_config SCHEMAFULL;
DEFINE FIELD OVERWRITE name       ON sink_config TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE sink_type  ON sink_config TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE config     ON sink_config TYPE object FLEXIBLE;
DEFINE FIELD OVERWRITE enabled    ON sink_config TYPE bool DEFAULT true;
DEFINE FIELD OVERWRITE created_at ON sink_config TYPE datetime DEFAULT time::now();
DEFINE FIELD OVERWRITE updated_at ON sink_config TYPE datetime DEFAULT time::now();
DEFINE INDEX OVERWRITE idx_sink_name ON sink_config FIELDS name UNIQUE;
