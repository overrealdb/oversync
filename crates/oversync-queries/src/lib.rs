pub mod config {
	pub const LOAD_PIPES: &str = include_str!("../surql/queries/config/load_pipes.surql");
	pub const LOAD_PIPE_PRESETS: &str =
		include_str!("../surql/queries/config/load_pipe_presets.surql");
	pub const LOAD_QUERIES: &str = include_str!("../surql/queries/config/load_queries.surql");
	pub const LOAD_SINKS: &str = include_str!("../surql/queries/config/load_sinks.surql");
	pub const READ_PIPES_CACHE: &str =
		include_str!("../surql/queries/config/read_pipes_cache.surql");
	pub const READ_PIPE_PRESETS_CACHE: &str =
		include_str!("../surql/queries/config/read_pipe_presets_cache.surql");
	pub const READ_SINKS_CACHE: &str =
		include_str!("../surql/queries/config/read_sinks_cache.surql");
}

pub mod config_version {
	pub const CREATE_VERSION: &str =
		include_str!("../surql/queries/config_version/create_version.surql");
	pub const GET_VERSION: &str = include_str!("../surql/queries/config_version/get_version.surql");
	pub const LIST_VERSIONS: &str =
		include_str!("../surql/queries/config_version/list_versions.surql");
	pub const NEXT_VERSION: &str =
		include_str!("../surql/queries/config_version/next_version.surql");
}

pub mod credential {
	pub const CREATE_CREDENTIAL: &str =
		include_str!("../surql/queries/credential/create_credential.surql");
	pub const DELETE_CREDENTIAL: &str =
		include_str!("../surql/queries/credential/delete_credential.surql");
	pub const LIST_CREDENTIALS: &str =
		include_str!("../surql/queries/credential/list_credentials.surql");
	pub const LOOKUP_CREDENTIAL: &str =
		include_str!("../surql/queries/credential/lookup_credential.surql");
}

pub mod delta {
	pub const BATCH_UPSERT: &str = include_str!("../surql/queries/delta/batch_upsert.surql");
	pub const DELETE_PENDING: &str = include_str!("../surql/queries/delta/delete_pending.surql");
	pub const DELETE_STALE: &str = include_str!("../surql/queries/delta/delete_stale.surql");
	pub const FIND_CREATED: &str = include_str!("../surql/queries/delta/find_created.surql");
	pub const FIND_DELETED: &str = include_str!("../surql/queries/delta/find_deleted.surql");
	pub const FIND_UPDATED: &str = include_str!("../surql/queries/delta/find_updated.surql");
	pub const LIST_CYCLE_HISTORY: &str =
		include_str!("../surql/queries/delta/list_cycle_history.surql");
	pub const LOG_CYCLE_FINISH: &str =
		include_str!("../surql/queries/delta/log_cycle_finish.surql");
	pub const LOG_CYCLE_START: &str = include_str!("../surql/queries/delta/log_cycle_start.surql");
	pub const NEXT_CYCLE_ID: &str = include_str!("../surql/queries/delta/next_cycle_id.surql");
	pub const PREP_PREV_HASH: &str = include_str!("../surql/queries/delta/prep_prev_hash.surql");
	pub const READ_PENDING: &str = include_str!("../surql/queries/delta/read_pending.surql");
	pub const READ_SNAPSHOT_KEYS: &str =
		include_str!("../surql/queries/delta/read_snapshot_keys.surql");
	pub const READ_SNAPSHOT_KEYS_PAGED: &str =
		include_str!("../surql/queries/delta/read_snapshot_keys_paged.surql");
	pub const SAVE_PENDING: &str = include_str!("../surql/queries/delta/save_pending.surql");
}

pub mod mutations {
	pub const CREATE_DLQ_ENTRY: &str =
		include_str!("../surql/queries/mutations/create_dlq_entry.surql");
	pub const CREATE_PIPE: &str = include_str!("../surql/queries/mutations/create_pipe.surql");
	pub const CREATE_PIPE_PRESET: &str =
		include_str!("../surql/queries/mutations/create_pipe_preset.surql");
	pub const CREATE_QUERY: &str = include_str!("../surql/queries/mutations/create_query.surql");
	pub const CREATE_QUERY_WITH_SINKS: &str =
		include_str!("../surql/queries/mutations/create_query_with_sinks.surql");
	pub const CREATE_SINK: &str = include_str!("../surql/queries/mutations/create_sink.surql");
	pub const DELETE_DLQ_ENTRY: &str =
		include_str!("../surql/queries/mutations/delete_dlq_entry.surql");
	pub const DELETE_PIPE: &str = include_str!("../surql/queries/mutations/delete_pipe.surql");
	pub const DELETE_PIPE_PRESET: &str =
		include_str!("../surql/queries/mutations/delete_pipe_preset.surql");
	pub const DELETE_PIPE_QUERIES: &str =
		include_str!("../surql/queries/mutations/delete_pipe_queries.surql");
	pub const DELETE_SINK: &str = include_str!("../surql/queries/mutations/delete_sink.surql");
	pub const LIST_DLQ: &str = include_str!("../surql/queries/mutations/list_dlq.surql");
	pub const LOCK_RELEASE: &str = include_str!("../surql/queries/mutations/lock_release.surql");
	pub const LOCK_RENEW: &str = include_str!("../surql/queries/mutations/lock_renew.surql");
	pub const LOCK_TRY_ACQUIRE: &str =
		include_str!("../surql/queries/mutations/lock_try_acquire.surql");
	pub const UPDATE_PIPE_DELTA: &str =
		include_str!("../surql/queries/mutations/update_pipe_delta.surql");
	pub const UPDATE_PIPE_ENABLED: &str =
		include_str!("../surql/queries/mutations/update_pipe_enabled.surql");
	pub const UPDATE_PIPE_FILTERS: &str =
		include_str!("../surql/queries/mutations/update_pipe_filters.surql");
	pub const UPDATE_PIPE_LINKS: &str =
		include_str!("../surql/queries/mutations/update_pipe_links.surql");
	pub const UPDATE_PIPE_ORIGIN_CREDENTIAL: &str =
		include_str!("../surql/queries/mutations/update_pipe_origin_credential.surql");
	pub const UPDATE_PIPE_ORIGIN_CONFIG: &str =
		include_str!("../surql/queries/mutations/update_pipe_origin_config.surql");
	pub const UPDATE_PIPE_ORIGIN_CONNECTOR: &str =
		include_str!("../surql/queries/mutations/update_pipe_origin_connector.surql");
	pub const UPDATE_PIPE_ORIGIN_DSN: &str =
		include_str!("../surql/queries/mutations/update_pipe_origin_dsn.surql");
	pub const UPDATE_PIPE_TRINO_URL: &str =
		include_str!("../surql/queries/mutations/update_pipe_trino_url.surql");
	pub const UPDATE_PIPE_RETRY: &str =
		include_str!("../surql/queries/mutations/update_pipe_retry.surql");
	pub const UPDATE_PIPE_RECIPE: &str =
		include_str!("../surql/queries/mutations/update_pipe_recipe.surql");
	pub const UPDATE_PIPE_RECIPE_NONE: &str =
		include_str!("../surql/queries/mutations/update_pipe_recipe_none.surql");
	pub const UPDATE_PIPE_PRESET: &str =
		include_str!("../surql/queries/mutations/update_pipe_preset.surql");
	pub const UPDATE_PIPE_SCHEDULE: &str =
		include_str!("../surql/queries/mutations/update_pipe_schedule.surql");
	pub const UPDATE_PIPE_TARGETS: &str =
		include_str!("../surql/queries/mutations/update_pipe_targets.surql");
	pub const UPDATE_PIPE_TRANSFORMS: &str =
		include_str!("../surql/queries/mutations/update_pipe_transforms.surql");
	pub const UPDATE_SINK_CONFIG: &str =
		include_str!("../surql/queries/mutations/update_sink_config.surql");
	pub const UPDATE_SINK_ENABLED: &str =
		include_str!("../surql/queries/mutations/update_sink_enabled.surql");
	pub const UPDATE_SINK_TYPE: &str =
		include_str!("../surql/queries/mutations/update_sink_type.surql");
}

pub mod query_config {
	pub const DELETE_ONE: &str = include_str!("../surql/queries/query_config/delete_one.surql");
	pub const LIST_BY_SOURCE: &str =
		include_str!("../surql/queries/query_config/list_by_source.surql");
	pub const UPDATE_ENABLED: &str =
		include_str!("../surql/queries/query_config/update_enabled.surql");
	pub const UPDATE_KEY_COLUMN: &str =
		include_str!("../surql/queries/query_config/update_key_column.surql");
	pub const UPDATE_QUERY: &str = include_str!("../surql/queries/query_config/update_query.surql");
	pub const UPDATE_SINKS: &str = include_str!("../surql/queries/query_config/update_sinks.surql");
	pub const UPDATE_TRANSFORM: &str =
		include_str!("../surql/queries/query_config/update_transform.surql");
}

pub mod links {
	pub const CREATE_RULE: &str = include_str!("../surql/queries/links/create_rule.surql");
	pub const LIST_RULES: &str = include_str!("../surql/queries/links/list_rules.surql");
	pub const DELETE_RULE: &str = include_str!("../surql/queries/links/delete_rule.surql");
	pub const UPSERT_LINK: &str = include_str!("../surql/queries/links/upsert_link.surql");
	pub const READ_LINKS: &str = include_str!("../surql/queries/links/read_links.surql");
	pub const DELETE_LINK: &str = include_str!("../surql/queries/links/delete_link.surql");
	pub const READ_SNAPSHOT_ROWS: &str =
		include_str!("../surql/queries/links/read_snapshot_rows.surql");
	pub const BATCH_UPSERT_LINKS: &str =
		include_str!("../surql/queries/links/batch_upsert_links.surql");
}

pub mod sink {
	pub const BATCH_UPSERT_EVENTS: &str =
		include_str!("../surql/queries/sink/batch_upsert_events.surql");
	pub const UPSERT_EVENT: &str = include_str!("../surql/queries/sink/upsert_event.surql");
	pub const UPSERT_DOCUMENT: &str = include_str!("../surql/queries/sink/upsert_document.surql");
	pub const BATCH_UPSERT_DOCUMENTS: &str =
		include_str!("../surql/queries/sink/batch_upsert_documents.surql");
}
