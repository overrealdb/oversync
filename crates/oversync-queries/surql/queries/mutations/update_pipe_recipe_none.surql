UPDATE pipe_config SET recipe = NONE, updated_at = time::now() WHERE name = $name;
