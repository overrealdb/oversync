UPSERT type::record($table, $key) SET
	data = $data,
	_meta = {
		origin_id: $origin_id,
		query_id: $query_id,
		op: $op,
		hash: $hash,
		cycle_id: $cycle_id,
		synced_at: time::now()
	};
