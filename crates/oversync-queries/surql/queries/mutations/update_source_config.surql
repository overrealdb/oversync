UPDATE source_config SET config = $config, updated_at = time::now() WHERE name = $name;
