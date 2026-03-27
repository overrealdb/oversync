-- plugin domain: WASM plugin registry

DEFINE TABLE OVERWRITE plugin SCHEMAFULL;
DEFINE FIELD OVERWRITE name       ON plugin TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE version    ON plugin TYPE string;
DEFINE FIELD OVERWRITE wasm_hash  ON plugin TYPE option<string>;
DEFINE FIELD OVERWRITE enabled    ON plugin TYPE bool DEFAULT true;
DEFINE FIELD OVERWRITE created_at ON plugin TYPE datetime DEFAULT time::now();
DEFINE INDEX OVERWRITE idx_plugin_name ON plugin FIELDS name UNIQUE;
