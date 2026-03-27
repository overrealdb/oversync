CREATE sink_config SET name = $name, sink_type = $sink_type, config = $config, enabled = true, updated_at = time::now();
