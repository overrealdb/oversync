-- v004: Link tables for cross-source entity resolution

DEFINE TABLE IF NOT EXISTS link_rule SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS name       ON link_rule TYPE string ASSERT $value != NONE;
DEFINE FIELD IF NOT EXISTS rule_type  ON link_rule TYPE string;
DEFINE FIELD IF NOT EXISTS config     ON link_rule TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS enabled    ON link_rule TYPE bool DEFAULT true;
DEFINE FIELD IF NOT EXISTS created_at ON link_rule TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS idx_link_name ON link_rule FIELDS name UNIQUE;

DEFINE TABLE IF NOT EXISTS resolved_link SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS source_key  ON resolved_link TYPE string;
DEFINE FIELD IF NOT EXISTS target_key  ON resolved_link TYPE string;
DEFINE FIELD IF NOT EXISTS rule_name   ON resolved_link TYPE string;
DEFINE FIELD IF NOT EXISTS confidence  ON resolved_link TYPE float DEFAULT 1.0;
DEFINE FIELD IF NOT EXISTS created_at  ON resolved_link TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS idx_resolved_pair ON resolved_link FIELDS source_key, target_key UNIQUE;
