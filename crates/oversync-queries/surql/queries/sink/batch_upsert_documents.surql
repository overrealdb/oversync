FOR $e IN $events {
	UPSERT type::record($e.table, $e.key) MERGE $e.doc;
	UPDATE type::record($e.table, $e.key) SET
		created_at = created_at ?? time::now(),
		updated_at = time::now();
};
