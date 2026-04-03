UPSERT type::record($table, $key) MERGE $doc;
UPDATE type::record($table, $key) SET
	created_at = created_at ?? time::now(),
	updated_at = time::now();
