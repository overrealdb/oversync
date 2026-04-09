UPDATE pipe_config SET filters = $v, updated_at = time::now() WHERE name = $name;
