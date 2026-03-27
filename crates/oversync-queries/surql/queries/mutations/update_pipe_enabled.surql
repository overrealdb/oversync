UPDATE pipe_config SET enabled = $v, updated_at = time::now() WHERE name = $name;
