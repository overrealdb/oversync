UPDATE sink_config SET sink_type = $sink_type, updated_at = time::now() WHERE name = $name;
