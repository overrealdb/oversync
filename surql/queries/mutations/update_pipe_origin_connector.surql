UPDATE pipe_config SET origin_connector = $v, updated_at = time::now() WHERE name = $name;
