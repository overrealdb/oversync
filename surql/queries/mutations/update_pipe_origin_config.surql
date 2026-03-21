UPDATE pipe_config SET origin_config = $v, updated_at = time::now() WHERE name = $name;
