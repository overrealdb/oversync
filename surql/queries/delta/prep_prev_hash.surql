UPDATE {snapshot} SET prev_hash = row_hash
WHERE origin_id = $origin_id AND query_id = $query_id
RETURN NONE