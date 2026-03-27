CREATE {cycle_log} SET
	origin_id = $origin_id,
	query_id = $query_id,
	cycle_id = $cycle_id,
	started_at = time::now(),
	status = 'running'