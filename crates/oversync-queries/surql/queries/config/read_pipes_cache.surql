SELECT name, origin_connector, origin_dsn, targets, schedule.interval_secs AS interval_secs, enabled FROM pipe_config WHERE enabled = true;
