UPDATE pipe_config SET recipe = $v, updated_at = time::now() WHERE name = $name;
