UPDATE pipe_config SET retry = $v, updated_at = time::now() WHERE name = $name;
