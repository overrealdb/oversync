-- v005: Persist pipe recipe definitions in config DB

DEFINE FIELD IF NOT EXISTS recipe             ON pipe_config TYPE option<object> FLEXIBLE;
