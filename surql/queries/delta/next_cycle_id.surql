SELECT cycle_id FROM {cycle_log}
WHERE origin_id = $origin_id AND query_id = $query_id
ORDER BY cycle_id DESC LIMIT 1