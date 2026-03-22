CREATE dlq SET pipe = $pipe, query = $query, cycle_id = $cycle_id, events_json = $events_json, error = $error, attempts = $attempts, created_at = time::now();
