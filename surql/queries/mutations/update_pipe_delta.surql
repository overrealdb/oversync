UPDATE pipe_config SET delta = $v, updated_at = time::now() WHERE name = $name;
