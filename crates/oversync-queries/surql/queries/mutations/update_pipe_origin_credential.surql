UPDATE pipe_config SET origin_credential = $v, updated_at = time::now() WHERE name = $name;
