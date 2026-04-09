UPDATE pipe_preset_config SET description = $description, spec = $spec, updated_at = time::now() WHERE name = $name;
