SELECT row_key, row_data FROM snapshot
WHERE origin_id = $origin_id AND query_id = $query_id;
