UPSERT type::record('resolved_link', [$source_key, $target_key]) SET
	source_key = $source_key,
	target_key = $target_key,
	rule_name = $rule_name,
	confidence = $confidence,
	created_at = time::now();
