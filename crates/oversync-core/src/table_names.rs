//! Per-pipeline SurrealDB table names.
//!
//! Each sync pipeline gets its own set of tables for isolation,
//! named by a deterministic pattern: `sync_{source}_{type}`.

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
			 DEFINE FIELD IF NOT EXISTS source_id  ON {snap} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS query_id   ON {snap} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS row_key    ON {snap} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS row_data   ON {snap} TYPE object FLEXIBLE;\
			 DEFINE FIELD IF NOT EXISTS row_hash   ON {snap} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS cycle_id   ON {snap} TYPE int;\
			 DEFINE FIELD IF NOT EXISTS updated_at ON {snap} TYPE datetime DEFAULT time::now();\
			 DEFINE FIELD IF NOT EXISTS prev_hash  ON {snap} TYPE option<string>;\
			 DEFINE INDEX IF NOT EXISTS idx_{snap}_key   ON {snap} FIELDS source_id, query_id, row_key UNIQUE;\
			 DEFINE INDEX IF NOT EXISTS idx_{snap}_cycle ON {snap} FIELDS source_id, query_id, cycle_id;\
			 DEFINE TABLE IF NOT EXISTS {cl} SCHEMAFULL;\
			 DEFINE FIELD IF NOT EXISTS source_id    ON {cl} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS query_id     ON {cl} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS cycle_id     ON {cl} TYPE int;\
			 DEFINE FIELD IF NOT EXISTS started_at   ON {cl} TYPE datetime;\
			 DEFINE FIELD IF NOT EXISTS finished_at  ON {cl} TYPE option<datetime>;\
			 DEFINE FIELD IF NOT EXISTS status       ON {cl} TYPE string DEFAULT 'running';\
			 DEFINE FIELD IF NOT EXISTS rows_fetched ON {cl} TYPE int DEFAULT 0;\
			 DEFINE FIELD IF NOT EXISTS rows_created ON {cl} TYPE int DEFAULT 0;\
			 DEFINE FIELD IF NOT EXISTS rows_updated ON {cl} TYPE int DEFAULT 0;\
			 DEFINE FIELD IF NOT EXISTS rows_deleted ON {cl} TYPE int DEFAULT 0;\
			 DEFINE INDEX IF NOT EXISTS idx_{cl}_source ON {cl} FIELDS source_id, query_id, cycle_id UNIQUE;\
			 DEFINE TABLE IF NOT EXISTS {pe} SCHEMAFULL;\
			 DEFINE FIELD IF NOT EXISTS source_id   ON {pe} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS query_id    ON {pe} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS cycle_id    ON {pe} TYPE int;\
			 DEFINE FIELD IF NOT EXISTS events_json ON {pe} TYPE string;\
			 DEFINE FIELD IF NOT EXISTS created_at  ON {pe} TYPE datetime DEFAULT time::now();\
			 DEFINE INDEX IF NOT EXISTS idx_{pe}_source ON {pe} FIELDS source_id, query_id;",
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
	name.chars()
		.map(|c| {
			if c.is_ascii_alphanumeric() || c == '_' {
				c.to_ascii_lowercase()
			} else {
				'_'
			}
		})
		.collect()
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
	fn for_source_lowercases() {
		let t = TableNames::for_source("PG_Prod");
		assert_eq!(t.snapshot, "sync_pg_prod_snapshot");
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
		let sql = "SELECT * FROM {snapshot} WHERE source_id = $s; DELETE {pending_event}; UPDATE {cycle_log}";
		let resolved = t.resolve_sql(sql);
		assert!(resolved.contains("sync_pg_snapshot"));
		assert!(resolved.contains("sync_pg_pending"));
		assert!(resolved.contains("sync_pg_cycle_log"));
		assert!(!resolved.contains("{snapshot}"));
	}
}
