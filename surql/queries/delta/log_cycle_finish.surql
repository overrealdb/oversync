UPDATE cycle_log SET
	finished_at = time::now(),
	status = $status,
	rows_fetched = $rows_fetched,
	rows_created = $rows_created,
	rows_updated = $rows_updated,
	rows_deleted = $rows_deleted
WHERE source_id = $source_id
	AND query_id = $query_id
	AND cycle_id = $cycle_id