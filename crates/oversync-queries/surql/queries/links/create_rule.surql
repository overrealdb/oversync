CREATE link_rule SET
	name = $name,
	rule_type = $rule_type,
	config = $config,
	enabled = $enabled,
	created_at = time::now();
