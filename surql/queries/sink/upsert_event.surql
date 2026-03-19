UPSERT type::record($table, $key) SET
	data = $data,
	_meta = {
		source_id: $source_id,
		query_id: $query_id,
		op: $op,
		hash: $hash,
		cycle_id: $cycle_id,
		synced_at: time::now()
	};
