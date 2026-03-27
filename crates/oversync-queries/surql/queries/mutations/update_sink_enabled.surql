UPDATE sink_config SET enabled = $enabled, updated_at = time::now() WHERE name = $name;
