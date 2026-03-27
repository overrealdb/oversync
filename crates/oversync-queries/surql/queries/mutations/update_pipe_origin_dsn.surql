UPDATE pipe_config SET origin_dsn = $v, updated_at = time::now() WHERE name = $name;
