//! Per-pipeline SurrealDB table names.
//!
//! Each sync pipeline gets its own set of tables for isolation,
//! named by a deterministic pattern: `sync_{source}_{type}`.

const MAX_SOURCE_IDENTIFIER_LEN: usize = 48;
const TRUNCATED_HASH_LEN: usize = 12;

/// Table names for a sync pipeline's internal state.
#[derive(Debug, Clone)]
pub struct TableNames {
	pub snapshot: String,
	pub cycle_log: String,
	pub pending_event: String,
}

impl TableNames {
	/// Build table names from a source name.
	///
	/// Sanitizes the source name to a valid SurrealDB identifier
	/// (lowercase alphanumeric + underscore).
	pub fn for_source(source_name: &str) -> Self {
		let safe = sanitize_name(source_name);
		Self {
			snapshot: format!("sync_{safe}_snapshot"),
			cycle_log: format!("sync_{safe}_cycle_log"),
			pending_event: format!("sync_{safe}_pending"),
		}
	}

	/// Default table names (backward compatible with existing single-pipeline deployments).
	pub fn default_shared() -> Self {
		Self {
			snapshot: "snapshot".into(),
			cycle_log: "cycle_log".into(),
			pending_event: "pending_event".into(),
		}
	}

	/// Generate SurrealQL DDL to create these tables (idempotent).
	pub fn create_ddl(&self) -> String {
		format!(
			"DEFINE TABLE IF NOT EXISTS {snap} SCHEMAFULL;\
			 DEFINE FIELD IF NOT EXISTS origin_id  ON {snap} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS query_id   ON {snap} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS row_key    ON {snap} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS row_data   ON {snap} TYPE object FLEXIBLE;\
			 DEFINE FIELD IF NOT EXISTS row_hash   ON {snap} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS cycle_id   ON {snap} TYPE int;\
			 DEFINE FIELD IF NOT EXISTS updated_at ON {snap} TYPE datetime DEFAULT time::now();\
			 DEFINE FIELD IF NOT EXISTS prev_hash  ON {snap} TYPE option<string>;\
			 DEFINE INDEX IF NOT EXISTS idx_{snap}_key   ON {snap} FIELDS origin_id, query_id, row_key UNIQUE;\
			 DEFINE INDEX IF NOT EXISTS idx_{snap}_cycle ON {snap} FIELDS origin_id, query_id, cycle_id;\
			 DEFINE TABLE IF NOT EXISTS {cl} SCHEMAFULL;\
			 DEFINE FIELD IF NOT EXISTS origin_id    ON {cl} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS query_id     ON {cl} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS cycle_id     ON {cl} TYPE int;\
			 DEFINE FIELD IF NOT EXISTS started_at   ON {cl} TYPE datetime;\
			 DEFINE FIELD IF NOT EXISTS finished_at  ON {cl} TYPE option<datetime>;\
			 DEFINE FIELD IF NOT EXISTS status       ON {cl} TYPE string DEFAULT 'running';\
			 DEFINE FIELD IF NOT EXISTS rows_fetched ON {cl} TYPE int DEFAULT 0;\
			 DEFINE FIELD IF NOT EXISTS rows_created ON {cl} TYPE int DEFAULT 0;\
			 DEFINE FIELD IF NOT EXISTS rows_updated ON {cl} TYPE int DEFAULT 0;\
			 DEFINE FIELD IF NOT EXISTS rows_deleted ON {cl} TYPE int DEFAULT 0;\
			 DEFINE INDEX IF NOT EXISTS idx_{cl}_source ON {cl} FIELDS origin_id, query_id, cycle_id UNIQUE;\
			 DEFINE TABLE IF NOT EXISTS {pe} SCHEMAFULL;\
			 DEFINE FIELD IF NOT EXISTS origin_id   ON {pe} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS query_id    ON {pe} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS cycle_id    ON {pe} TYPE int;\
			 DEFINE FIELD IF NOT EXISTS events_json ON {pe} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS created_at  ON {pe} TYPE datetime DEFAULT time::now();\
			 DEFINE INDEX IF NOT EXISTS idx_{pe}_source ON {pe} FIELDS origin_id, query_id;",
			snap = self.snapshot,
			cl = self.cycle_log,
			pe = self.pending_event,
		)
	}

	/// Replace `{snapshot}`, `{cycle_log}`, `{pending_event}` placeholders in SQL.
	pub fn resolve_sql(&self, template: &str) -> String {
		template
			.replace("{snapshot}", &self.snapshot)
			.replace("{cycle_log}", &self.cycle_log)
			.replace("{pending_event}", &self.pending_event)
	}
}

fn sanitize_name(name: &str) -> String {
	let mut sanitized = String::with_capacity(name.len());
	let mut prev_was_underscore = false;

	for c in name.chars() {
		let mapped = if c.is_ascii_alphanumeric() || c == '_' {
			c.to_ascii_lowercase()
		} else {
			'_'
		};

		if mapped == '_' {
			if !prev_was_underscore {
				sanitized.push('_');
			}
			prev_was_underscore = true;
		} else {
			sanitized.push(mapped);
			prev_was_underscore = false;
		}
	}

	let sanitized = sanitized.trim_matches('_');
	if sanitized.is_empty() {
		return "unnamed".to_string();
	}

	if sanitized.len() <= MAX_SOURCE_IDENTIFIER_LEN {
		return sanitized.to_string();
	}

	let hash = short_hash(sanitized);
	let prefix_len = MAX_SOURCE_IDENTIFIER_LEN - TRUNCATED_HASH_LEN - 1;
	let prefix = &sanitized[..prefix_len];
	format!("{prefix}_{hash}")
}

fn short_hash(name: &str) -> String {
	use sha2::{Digest, Sha256};

	let digest = Sha256::digest(name.as_bytes());
	const_hex::encode(&digest[..TRUNCATED_HASH_LEN / 2])
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn for_source_basic() {
		let t = TableNames::for_source("pg-prod");
		assert_eq!(t.snapshot, "sync_pg_prod_snapshot");
		assert_eq!(t.cycle_log, "sync_pg_prod_cycle_log");
		assert_eq!(t.pending_event, "sync_pg_prod_pending");
	}

	#[test]
	fn for_source_sanitizes_special_chars() {
		let t = TableNames::for_source("my.trino/analytics");
		assert_eq!(t.snapshot, "sync_my_trino_analytics_snapshot");
	}

	#[test]
	fn for_source_collapses_repeated_underscores() {
		let t = TableNames::for_source("my...trino///analytics");
		assert_eq!(t.snapshot, "sync_my_trino_analytics_snapshot");
	}

	#[test]
	fn for_source_lowercases() {
		let t = TableNames::for_source("PG_Prod");
		assert_eq!(t.snapshot, "sync_pg_prod_snapshot");
	}

	#[test]
	fn for_source_uses_fallback_for_empty_name() {
		let t = TableNames::for_source("!!!");
		assert_eq!(t.snapshot, "sync_unnamed_snapshot");
	}

	#[test]
	fn for_source_truncates_with_hash_suffix() {
		let long = "a".repeat(200);
		let t = TableNames::for_source(&long);
		assert!(
			t.snapshot
				.starts_with("sync_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_")
		);
		assert!(t.snapshot.ends_with("_snapshot"));
		assert!(t.snapshot.len() < 80);
	}

	#[test]
	fn for_source_long_names_remain_distinct() {
		let a = format!("{}-one", "x".repeat(120));
		let b = format!("{}-two", "x".repeat(120));
		let a_names = TableNames::for_source(&a);
		let b_names = TableNames::for_source(&b);
		assert_ne!(a_names.snapshot, b_names.snapshot);
	}

	#[test]
	fn default_shared_backward_compat() {
		let t = TableNames::default_shared();
		assert_eq!(t.snapshot, "snapshot");
		assert_eq!(t.cycle_log, "cycle_log");
		assert_eq!(t.pending_event, "pending_event");
	}

	#[test]
	fn resolve_sql_replaces_all() {
		let t = TableNames::for_source("pg");
		let sql = "SELECT * FROM {snapshot} WHERE origin_id = $s; DELETE {pending_event}; UPDATE {cycle_log}";
		let resolved = t.resolve_sql(sql);
		assert!(resolved.contains("sync_pg_snapshot"));
		assert!(resolved.contains("sync_pg_pending"));
		assert!(resolved.contains("sync_pg_cycle_log"));
		assert!(!resolved.contains("{snapshot}"));
	}
}
