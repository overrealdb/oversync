SELECT cycle_id FROM cycle_log
WHERE source_id = $source_id AND query_id = $query_id
ORDER BY cycle_id DESC LIMIT 1