SELECT row_key, row_hash FROM {snapshot}
WHERE source_id = $source_id
	AND query_id = $query_id
	AND cycle_id < $cycle_id