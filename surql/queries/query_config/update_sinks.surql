UPDATE query_config SET sinks = $sinks WHERE origin_id = $source AND name = $name;
