UPDATE pipe_config SET transforms = $v, updated_at = time::now() WHERE name = $name;
