SELECT name, connector, config.interval_secs AS interval_secs FROM source_config WHERE enabled = true;
