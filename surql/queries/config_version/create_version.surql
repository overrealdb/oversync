CREATE config_version SET version = $version, config_json = $config, description = $desc, created_at = time::now();
