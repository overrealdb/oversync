SELECT * FROM {pending_event}
WHERE origin_id = $origin_id AND query_id = $query_id
ORDER BY cycle_id ASC