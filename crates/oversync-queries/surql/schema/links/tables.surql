-- links domain: link_rule, resolved_link

-- link_rule: cross-source entity linking rules
DEFINE TABLE OVERWRITE link_rule SCHEMAFULL;
DEFINE FIELD OVERWRITE name       ON link_rule TYPE string ASSERT $value != NONE;
DEFINE FIELD OVERWRITE rule_type  ON link_rule TYPE string;
DEFINE FIELD OVERWRITE config     ON link_rule TYPE object FLEXIBLE;
DEFINE FIELD OVERWRITE enabled    ON link_rule TYPE bool DEFAULT true;
DEFINE FIELD OVERWRITE created_at ON link_rule TYPE datetime DEFAULT time::now();
DEFINE INDEX OVERWRITE idx_link_name ON link_rule FIELDS name UNIQUE;

-- resolved_link: discovered cross-source relationships
DEFINE TABLE OVERWRITE resolved_link SCHEMAFULL;
DEFINE FIELD OVERWRITE source_key  ON resolved_link TYPE string;
DEFINE FIELD OVERWRITE target_key  ON resolved_link TYPE string;
DEFINE FIELD OVERWRITE rule_name   ON resolved_link TYPE string;
DEFINE FIELD OVERWRITE confidence  ON resolved_link TYPE float DEFAULT 1.0;
DEFINE FIELD OVERWRITE created_at  ON resolved_link TYPE datetime DEFAULT time::now();
DEFINE INDEX OVERWRITE idx_resolved_pair ON resolved_link FIELDS source_key, target_key UNIQUE;
