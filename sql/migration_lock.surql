DEFINE TABLE IF NOT EXISTS migration_lock SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS version     ON migration_lock TYPE int;
DEFINE FIELD IF NOT EXISTS applied_at  ON migration_lock TYPE datetime;
DEFINE FIELD IF NOT EXISTS checksum    ON migration_lock TYPE string;
DEFINE FIELD IF NOT EXISTS instance_id ON migration_lock TYPE string;
DEFINE INDEX IF NOT EXISTS idx_ml_version ON migration_lock FIELDS version UNIQUE;
