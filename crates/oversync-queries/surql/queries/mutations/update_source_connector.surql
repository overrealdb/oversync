UPDATE source_config SET connector = $connector, updated_at = time::now() WHERE name = $name;
