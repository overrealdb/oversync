UPDATE pipe_config SET trino_url = $v, updated_at = time::now() WHERE name = $name;
