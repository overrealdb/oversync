UPDATE query_config SET enabled = $enabled WHERE origin_id = $source AND name = $name;
