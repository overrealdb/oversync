DELETE {snapshot}
WHERE origin_id = $origin_id
	AND query_id = $query_id
	AND cycle_id < $cycle_id
RETURN NONE