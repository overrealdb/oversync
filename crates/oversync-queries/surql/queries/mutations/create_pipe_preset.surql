CREATE pipe_preset_config SET name = $name, description = $description, spec = $spec, updated_at = time::now();
