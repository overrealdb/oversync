DELETE {pending_event}
WHERE origin_id = $origin_id AND query_id = $query_id AND cycle_id <= $cycle_id