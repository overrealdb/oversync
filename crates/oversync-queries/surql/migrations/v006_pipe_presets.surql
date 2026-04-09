DEFINE TABLE OVERWRITE pipe_preset_config SCHEMAFULL;
DEFINE FIELD OVERWRITE name               ON pipe_preset_config TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE description        ON pipe_preset_config TYPE option<string>;
DEFINE FIELD OVERWRITE spec               ON pipe_preset_config TYPE object FLEXIBLE;
DEFINE FIELD OVERWRITE created_at         ON pipe_preset_config TYPE datetime DEFAULT time::now();
DEFINE FIELD OVERWRITE updated_at         ON pipe_preset_config TYPE datetime DEFAULT time::now();
DEFINE INDEX OVERWRITE idx_pipe_preset_name ON pipe_preset_config FIELDS name UNIQUE;
