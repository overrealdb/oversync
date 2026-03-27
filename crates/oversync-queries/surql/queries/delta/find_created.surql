SELECT row_key, row_data, row_hash FROM {snapshot}
WHERE origin_id = $origin_id
	AND query_id = $query_id
	AND cycle_id = $cycle_id
	AND prev_hash IS NONE