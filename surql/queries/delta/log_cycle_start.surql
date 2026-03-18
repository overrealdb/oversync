CREATE cycle_log SET
	source_id = $source_id,
	query_id = $query_id,
	cycle_id = $cycle_id,
	started_at = time::now(),
	status = 'running'