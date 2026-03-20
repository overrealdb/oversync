SELECT * FROM {pending_event}
WHERE source_id = $source_id AND query_id = $query_id
ORDER BY cycle_id ASC