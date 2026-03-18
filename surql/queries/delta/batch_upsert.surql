FOR $row IN $rows {
	UPSERT type::record('snapshot', [$row.source_id, $row.query_id, $row.row_key]) SET
		source_id = $row.source_id,
		query_id = $row.query_id,
		row_key = $row.row_key,
		row_data = $row.row_data,
		row_hash = $row.row_hash,
		cycle_id = $row.cycle_id,
		updated_at = time::now();
}