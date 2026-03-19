FOR $e IN $events {
	UPSERT type::record($e.table, $e.key) SET
		data = $e.data,
		_meta = {
			source_id: $e.source_id,
			query_id: $e.query_id,
			op: $e.op,
			hash: $e.hash,
			cycle_id: $e.cycle_id,
			synced_at: time::now()
		};
};
