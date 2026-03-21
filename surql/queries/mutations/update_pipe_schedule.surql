UPDATE pipe_config SET schedule = $v, updated_at = time::now() WHERE name = $name;
