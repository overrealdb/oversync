UPDATE pipe_config SET links = $v, updated_at = time::now() WHERE name = $name;
