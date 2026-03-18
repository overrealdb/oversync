UPDATE snapshot SET prev_hash = row_hash
WHERE source_id = $source_id AND query_id = $query_id
RETURN NONE