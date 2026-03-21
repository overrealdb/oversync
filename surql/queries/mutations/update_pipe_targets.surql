UPDATE pipe_config SET targets = $v, updated_at = time::now() WHERE name = $name;
