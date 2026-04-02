use std::collections::HashMap;
use std::sync::Arc;

use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use tracing::{debug, info};

use oversync_core::error::OversyncError;
use oversync_core::model::{
	CycleStatus, DeltaEvent, DeltaResult, EventEnvelope, OpType, RawRow, hash_row_data,
};
use oversync_core::table_names::TableNames;

use oversync_queries::delta;

const READ_SNAPSHOT_KEYS_SQL: &str = delta::READ_SNAPSHOT_KEYS;
const READ_SNAPSHOT_KEYS_PAGED_SQL: &str = delta::READ_SNAPSHOT_KEYS_PAGED;
const BATCH_UPSERT_SQL: &str = delta::BATCH_UPSERT;
const DELETE_STALE_SQL: &str = delta::DELETE_STALE;
const PREP_PREV_HASH_SQL: &str = delta::PREP_PREV_HASH;
const FIND_CREATED_SQL: &str = delta::FIND_CREATED;
const FIND_UPDATED_SQL: &str = delta::FIND_UPDATED;
const FIND_DELETED_SQL: &str = delta::FIND_DELETED;
const SAVE_PENDING_SQL: &str = delta::SAVE_PENDING;
const READ_PENDING_SQL: &str = delta::READ_PENDING;
const DELETE_PENDING_SQL: &str = delta::DELETE_PENDING;

const BATCH_SIZE: usize = 500;
const NEXT_CYCLE_ID_SQL: &str = delta::NEXT_CYCLE_ID;
const LOG_CYCLE_START_SQL: &str = delta::LOG_CYCLE_START;
const LOG_CYCLE_FINISH_SQL: &str = delta::LOG_CYCLE_FINISH;

pub struct DeltaEngine {
	state_client: Arc<Surreal<Any>>,
	snapshot_client: Surreal<Any>,
	tables: TableNames,
}

impl DeltaEngine {
	pub fn new(state_client: Arc<Surreal<Any>>, snapshot_client: Surreal<Any>) -> Self {
		Self {
			state_client,
			snapshot_client,
			tables: TableNames::default_shared(),
		}
	}

	pub fn single(client: Surreal<Any>) -> Self {
		Self {
			state_client: Arc::new(client.clone()),
			snapshot_client: client,
			tables: TableNames::default_shared(),
		}
	}

	pub fn with_tables(mut self, tables: TableNames) -> Self {
		self.tables = tables;
		self
	}

	/// Create a new engine sharing the same DB connections but with different table names.
	pub fn for_source(&self, source_name: &str) -> Self {
		Self {
			state_client: Arc::clone(&self.state_client),
			snapshot_client: self.snapshot_client.clone(),
			tables: TableNames::for_source(source_name),
		}
	}

	pub fn tables(&self) -> &TableNames {
		&self.tables
	}

	pub fn state_client(&self) -> &Arc<Surreal<Any>> {
		&self.state_client
	}

	/// Ensure the per-pipeline tables exist in SurrealDB (idempotent).
	pub async fn ensure_tables(&self) -> Result<(), OversyncError> {
		let ddl = self.tables.create_ddl();
		self.state_client
			.query(&ddl)
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("ensure tables: {e}")))?;
		// Also ensure on snapshot client (may be a separate DB).
		self.snapshot_client
			.query(&ddl)
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("ensure snapshot tables: {e}")))?;
		debug!("ensured tables: {:?}", self.tables);
		Ok(())
	}

	fn sql(&self, template: &str) -> String {
		self.tables.resolve_sql(template)
	}

	pub async fn next_cycle_id(
		&self,
		origin_id: &str,
		query_id: &str,
	) -> Result<i64, OversyncError> {
		let mut response = self
			.state_client
			.query(self.sql(NEXT_CYCLE_ID_SQL))
			.bind(("origin_id", origin_id.to_string()))
			.bind(("query_id", query_id.to_string()))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("next_cycle_id: {e}")))?;

		let rows: Vec<serde_json::Value> = response
			.take(0)
			.map_err(|e| OversyncError::SurrealDb(format!("next_cycle_id take: {e}")))?;

		let max_id = rows
			.first()
			.and_then(|r| r.get("cycle_id"))
			.and_then(|v| v.as_i64())
			.unwrap_or(0);

		Ok(max_id + 1)
	}

	pub async fn read_snapshot_keys(
		&self,
		origin_id: &str,
		query_id: &str,
	) -> Result<HashMap<String, String>, OversyncError> {
		let mut response = self
			.snapshot_client
			.query(self.sql(READ_SNAPSHOT_KEYS_SQL))
			.bind(("origin_id", origin_id.to_string()))
			.bind(("query_id", query_id.to_string()))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("read_snapshot_keys: {e}")))?;

		let rows: Vec<serde_json::Value> = response
			.take(0)
			.map_err(|e| OversyncError::SurrealDb(format!("read_snapshot_keys take: {e}")))?;

		let mut map = HashMap::with_capacity(rows.len());
		for row in &rows {
			let key = row
				.get("row_key")
				.and_then(|v| v.as_str())
				.unwrap_or_default();
			let hash = row
				.get("row_hash")
				.and_then(|v| v.as_str())
				.unwrap_or_default();
			map.insert(key.to_string(), hash.to_string());
		}

		debug!(count = map.len(), "read snapshot keys");
		Ok(map)
	}

	/// Read all snapshot keys paginated (avoids flatbuffers overflow on large datasets).
	/// Returns HashMap<row_key, row_hash> for in-memory diff.
	pub async fn read_snapshot_keys_paged(
		&self,
		origin_id: &str,
		query_id: &str,
	) -> Result<HashMap<String, String>, OversyncError> {
		const PAGE: usize = 50000;
		let mut map = HashMap::new();
		let mut offset: usize = 0;

		loop {
			let base = self.sql(READ_SNAPSHOT_KEYS_PAGED_SQL);
			let sql = format!("{base}\nLIMIT {PAGE} START {offset}");
			let mut resp = self
				.snapshot_client
				.query(&sql)
				.bind(("origin_id", origin_id.to_string()))
				.bind(("query_id", query_id.to_string()))
				.await
				.map_err(|e| OversyncError::SurrealDb(format!("read_keys_paged: {e}")))?;

			let rows: Vec<serde_json::Value> = resp
				.take(0)
				.map_err(|e| OversyncError::SurrealDb(format!("read_keys_paged take: {e}")))?;

			let count = rows.len();
			for row in &rows {
				let key = row
					.get("row_key")
					.and_then(|v| v.as_str())
					.unwrap_or_default();
				let hash = row
					.get("row_hash")
					.and_then(|v| v.as_str())
					.unwrap_or_default();
				map.insert(key.to_string(), hash.to_string());
			}

			if count < PAGE {
				break;
			}
			offset += PAGE;
			info!(loaded = map.len(), "reading snapshot keys");
		}

		debug!(total = map.len(), "snapshot keys loaded");
		Ok(map)
	}

	/// Snapshot prev_hash before upserting. Sets prev_hash = row_hash
	/// on all existing rows so we can detect created vs updated after upsert.
	pub async fn prep_prev_hash(
		&self,
		origin_id: &str,
		query_id: &str,
	) -> Result<(), OversyncError> {
		self.snapshot_client
			.query(self.sql(PREP_PREV_HASH_SQL))
			.bind(("origin_id", origin_id.to_string()))
			.bind(("query_id", query_id.to_string()))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("prep_prev_hash: {e}")))?;
		debug!("prepped prev_hash");
		Ok(())
	}

	pub async fn upsert_batch(
		&self,
		origin_id: &str,
		query_id: &str,
		cycle_id: i64,
		rows: &[RawRow],
	) -> Result<usize, OversyncError> {
		self.prep_prev_hash(origin_id, query_id).await?;
		self.upsert_batch_raw(origin_id, query_id, cycle_id, rows)
			.await
	}

	/// Upsert without calling prep_prev_hash (caller is responsible).
	pub async fn upsert_batch_raw(
		&self,
		origin_id: &str,
		query_id: &str,
		cycle_id: i64,
		rows: &[RawRow],
	) -> Result<usize, OversyncError> {
		let mut total = 0;

		for chunk in rows.chunks(BATCH_SIZE) {
			let batch: Vec<serde_json::Value> = chunk
				.iter()
				.map(|row| {
					serde_json::json!({
						"origin_id": origin_id,
						"query_id": query_id,
						"row_key": row.row_key,
						"row_data": row.row_data,
						"row_hash": hash_row_data(&row.row_data),
						"cycle_id": cycle_id,
					})
				})
				.collect();

			let response = self
				.snapshot_client
				.query(self.sql(BATCH_UPSERT_SQL))
				.bind(("rows", batch))
				.await
				.map_err(|e| OversyncError::SurrealDb(format!("batch upsert: {e}")))?;

			if let Err(e) = response.check() {
				return Err(OversyncError::SurrealDb(format!("batch upsert check: {e}")));
			}

			total += chunk.len();
			if total % 5000 == 0 || total == rows.len() {
				info!(total, remaining = rows.len() - total, "upsert progress");
			}
		}
		Ok(total)
	}

	pub async fn delete_stale(
		&self,
		origin_id: &str,
		query_id: &str,
		cycle_id: i64,
	) -> Result<(), OversyncError> {
		self.snapshot_client
			.query(self.sql(DELETE_STALE_SQL))
			.bind(("origin_id", origin_id.to_string()))
			.bind(("query_id", query_id.to_string()))
			.bind(("cycle_id", cycle_id))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("delete_stale: {e}")))?;

		debug!("rotated stale snapshots for cycle < {cycle_id}");
		Ok(())
	}

	/// Atomic upsert + delete_stale in a single transaction.
	/// For datasets that fit in one batch, this is a single round trip.
	/// For larger datasets, upserts are chunked and the delete runs at the end.
	pub async fn commit_cycle(
		&self,
		origin_id: &str,
		query_id: &str,
		cycle_id: i64,
		rows: &[RawRow],
	) -> Result<usize, OversyncError> {
		if rows.len() <= BATCH_SIZE {
			let batch: Vec<serde_json::Value> = rows
				.iter()
				.map(|row| {
					serde_json::json!({
						"origin_id": origin_id,
						"query_id": query_id,
						"row_key": row.row_key,
						"row_data": row.row_data,
						"row_hash": hash_row_data(&row.row_data),
						"cycle_id": cycle_id,
					})
				})
				.collect();

			let txn_query = self.sql(&format!(
				"BEGIN TRANSACTION;\n{PREP_PREV_HASH_SQL};\n{BATCH_UPSERT_SQL};\n{DELETE_STALE_SQL};\nCOMMIT TRANSACTION;"
			));

			let response = self
				.snapshot_client
				.query(&txn_query)
				.bind(("rows", batch))
				.bind(("origin_id", origin_id.to_string()))
				.bind(("query_id", query_id.to_string()))
				.bind(("cycle_id", cycle_id))
				.await
				.map_err(|e| OversyncError::SurrealDb(format!("commit_cycle txn: {e}")))?;

			if let Err(e) = response.check() {
				return Err(OversyncError::SurrealDb(format!(
					"commit_cycle txn check: {e}"
				)));
			}

			info!(rows = rows.len(), "committed cycle in single transaction");
			Ok(rows.len())
		} else {
			let count = self
				.upsert_batch(origin_id, query_id, cycle_id, rows)
				.await?;
			self.delete_stale(origin_id, query_id, cycle_id).await?;
			info!(rows = count, "committed cycle in chunked batches");
			Ok(count)
		}
	}

	/// Compute delta from DB after upsert_batch. Uses prev_hash field
	/// to distinguish created/updated/deleted. Paginates to handle large datasets.
	pub async fn compute_delta_from_db(
		&self,
		origin_id: &str,
		query_id: &str,
		cycle_id: i64,
	) -> Result<DeltaResult, OversyncError> {
		let now = chrono::Utc::now();
		let src = origin_id.to_string();
		let qid = query_id.to_string();

		let to_event = |row: &serde_json::Value, op: OpType| DeltaEvent {
			op,
			origin_id: src.clone(),
			query_id: qid.clone(),
			row_key: row
				.get("row_key")
				.and_then(|v| v.as_str())
				.unwrap_or_default()
				.to_string(),
			row_data: row
				.get("row_data")
				.cloned()
				.unwrap_or(serde_json::Value::Null),
			row_hash: row
				.get("row_hash")
				.and_then(|v| v.as_str())
				.unwrap_or_default()
				.to_string(),
			cycle_id: cycle_id as u64,
			timestamp: now,
		};

		let created = self
			.paginated_query(&self.sql(FIND_CREATED_SQL), &src, &qid, cycle_id, |r| {
				to_event(r, OpType::Created)
			})
			.await?;
		let updated = self
			.paginated_query(&self.sql(FIND_UPDATED_SQL), &src, &qid, cycle_id, |r| {
				to_event(r, OpType::Updated)
			})
			.await?;
		let deleted = self
			.paginated_query(&self.sql(FIND_DELETED_SQL), &src, &qid, cycle_id, |r| {
				to_event(r, OpType::Deleted)
			})
			.await?;

		let result = DeltaResult {
			created,
			updated,
			deleted,
		};

		debug!(
			created = result.created.len(),
			updated = result.updated.len(),
			deleted = result.deleted.len(),
			"computed delta from db"
		);

		Ok(result)
	}

	async fn paginated_query(
		&self,
		base_sql: &str,
		origin_id: &str,
		query_id: &str,
		cycle_id: i64,
		map_fn: impl Fn(&serde_json::Value) -> DeltaEvent,
	) -> Result<Vec<DeltaEvent>, OversyncError> {
		const PAGE_SIZE: usize = 5000;
		let mut all = Vec::new();
		let mut offset: usize = 0;

		loop {
			let sql = format!("{base_sql}\nLIMIT {PAGE_SIZE} START {offset}");

			let mut resp = self
				.snapshot_client
				.query(&sql)
				.bind(("origin_id", origin_id.to_string()))
				.bind(("query_id", query_id.to_string()))
				.bind(("cycle_id", cycle_id))
				.await
				.map_err(|e| OversyncError::SurrealDb(format!("paginated query: {e}")))?;

			let rows: Vec<serde_json::Value> = resp
				.take(0)
				.map_err(|e| OversyncError::SurrealDb(format!("paginated take: {e}")))?;

			let count = rows.len();
			all.extend(rows.iter().map(&map_fn));

			if all.len() % 10000 == 0 || count < PAGE_SIZE {
				info!(
					fetched = all.len(),
					page_rows = count,
					"delta query progress"
				);
			}

			if count < PAGE_SIZE {
				break;
			}
			offset += PAGE_SIZE;
		}

		Ok(all)
	}

	pub async fn save_pending_events(
		&self,
		origin_id: &str,
		query_id: &str,
		cycle_id: i64,
		events: &[EventEnvelope],
	) -> Result<(), OversyncError> {
		let events_json = serde_json::to_string(events)
			.map_err(|e| OversyncError::Internal(format!("serialize events: {e}")))?;
		self.state_client
			.query(self.sql(SAVE_PENDING_SQL))
			.bind(("origin_id", origin_id.to_string()))
			.bind(("query_id", query_id.to_string()))
			.bind(("cycle_id", cycle_id))
			.bind(("events_json", events_json))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("save_pending: {e}")))?;
		debug!(count = events.len(), "saved pending events");
		Ok(())
	}

	pub async fn read_pending_events(
		&self,
		origin_id: &str,
		query_id: &str,
	) -> Result<Vec<(i64, Vec<EventEnvelope>)>, OversyncError> {
		let mut response = self
			.state_client
			.query(self.sql(READ_PENDING_SQL))
			.bind(("origin_id", origin_id.to_string()))
			.bind(("query_id", query_id.to_string()))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("read_pending: {e}")))?;

		let rows: Vec<serde_json::Value> = response
			.take(0)
			.map_err(|e| OversyncError::SurrealDb(format!("read_pending take: {e}")))?;

		let mut result = Vec::new();
		for row in &rows {
			let cycle_id = row.get("cycle_id").and_then(|v| v.as_i64()).unwrap_or(0);
			let json_str = row
				.get("events_json")
				.and_then(|v| v.as_str())
				.unwrap_or("[]");
			let events: Vec<EventEnvelope> = serde_json::from_str(json_str).unwrap_or_default();
			result.push((cycle_id, events));
		}

		debug!(pending_batches = result.len(), "read pending events");
		Ok(result)
	}

	pub async fn delete_pending_events(
		&self,
		origin_id: &str,
		query_id: &str,
		up_to_cycle_id: i64,
	) -> Result<(), OversyncError> {
		self.state_client
			.query(self.sql(DELETE_PENDING_SQL))
			.bind(("origin_id", origin_id.to_string()))
			.bind(("query_id", query_id.to_string()))
			.bind(("cycle_id", up_to_cycle_id))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("delete_pending: {e}")))?;
		Ok(())
	}

	pub async fn log_cycle_start(
		&self,
		origin_id: &str,
		query_id: &str,
		cycle_id: i64,
	) -> Result<(), OversyncError> {
		self.state_client
			.query(self.sql(LOG_CYCLE_START_SQL))
			.bind(("origin_id", origin_id.to_string()))
			.bind(("query_id", query_id.to_string()))
			.bind(("cycle_id", cycle_id))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("log_cycle_start: {e}")))?;
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	pub async fn log_cycle_finish(
		&self,
		origin_id: &str,
		query_id: &str,
		cycle_id: i64,
		status: CycleStatus,
		rows_fetched: i64,
		rows_created: i64,
		rows_updated: i64,
		rows_deleted: i64,
	) -> Result<(), OversyncError> {
		self.state_client
			.query(self.sql(LOG_CYCLE_FINISH_SQL))
			.bind(("origin_id", origin_id.to_string()))
			.bind(("query_id", query_id.to_string()))
			.bind(("cycle_id", cycle_id))
			.bind(("status", status.to_string()))
			.bind(("rows_fetched", rows_fetched))
			.bind(("rows_created", rows_created))
			.bind(("rows_updated", rows_updated))
			.bind(("rows_deleted", rows_deleted))
			.await
			.map_err(|e| OversyncError::SurrealDb(format!("log_cycle_finish: {e}")))?;
		Ok(())
	}

	/// Apply a SurrealQL transform function to event envelopes.
	/// The function receives an array of event objects and returns transformed events.
	/// Events where the function returns NONE are filtered out.
	pub async fn apply_transform(
		&self,
		fn_name: &str,
		envelopes: Vec<EventEnvelope>,
	) -> Result<Vec<EventEnvelope>, OversyncError> {
		// Validate function name to prevent injection
		if !fn_name
			.chars()
			.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
		{
			return Err(OversyncError::Config(format!(
				"invalid transform function name: '{fn_name}'"
			)));
		}

		let events_json: Vec<serde_json::Value> = envelopes
			.iter()
			.map(serde_json::to_value)
			.collect::<Result<_, _>>()?;

		let sql = format!("RETURN fn::{fn_name}($events)");
		let mut response = self
			.state_client
			.query(&sql)
			.bind(("events", events_json))
			.await
			.map_err(|e| OversyncError::Internal(format!("transform fn::{fn_name}: {e}")))?;

		let items: Vec<serde_json::Value> = response
			.take(0)
			.map_err(|e| OversyncError::Internal(format!("transform fn::{fn_name} result: {e}")))?;
		let mut transformed = Vec::with_capacity(items.len());
		for item in items {
			if item.is_null() {
				continue;
			}
			let envelope: EventEnvelope = serde_json::from_value(item).map_err(|e| {
				OversyncError::Internal(format!(
					"transform fn::{fn_name} returned invalid envelope: {e}"
				))
			})?;
			transformed.push(envelope);
		}

		debug!(
			fn_name,
			input = envelopes.len(),
			output = transformed.len(),
			"applied transform"
		);
		Ok(transformed)
	}

	/// Read all snapshot rows (key + data) for a given origin/query.
	/// Used by the link step to load target source rows for cross-matching.
	pub async fn read_snapshot_rows(
		&self,
		origin_id: &str,
		query_id: &str,
	) -> Result<Vec<RawRow>, OversyncError> {
		let mut resp = self
			.snapshot_client
			.query(oversync_queries::links::READ_SNAPSHOT_ROWS)
			.bind(("origin_id", origin_id.to_string()))
			.bind(("query_id", query_id.to_string()))
			.await
			.map_err(|e| OversyncError::Internal(format!("read_snapshot_rows: {e}")))?;

		let rows: Vec<serde_json::Value> = resp
			.take(0)
			.map_err(|e| OversyncError::Internal(format!("read_snapshot_rows take: {e}")))?;

		Ok(rows
			.into_iter()
			.filter_map(|v| {
				Some(RawRow {
					row_key: v.get("row_key")?.as_str()?.to_string(),
					row_data: v.get("row_data")?.clone(),
				})
			})
			.collect())
	}

	/// Store resolved links from the entity linking step.
	pub async fn upsert_resolved_links(
		&self,
		links: &[oversync_links::LinkMatch],
	) -> Result<(), OversyncError> {
		for link in links {
			self.state_client
				.query(oversync_queries::links::UPSERT_LINK)
				.bind(("source_key", link.left_key.clone()))
				.bind(("target_key", link.right_key.clone()))
				.bind(("rule_name", link.rule_name.clone()))
				.bind(("confidence", link.confidence))
				.await
				.map_err(|e| OversyncError::Internal(format!("upsert_resolved_link: {e}")))?;
		}
		Ok(())
	}
}

pub fn check_fail_safe(previous_count: usize, deleted_count: usize, threshold_pct: f64) -> bool {
	if previous_count == 0 {
		return true;
	}
	let deleted_pct = (deleted_count as f64 / previous_count as f64) * 100.0;
	deleted_pct <= threshold_pct
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn fail_safe_allows_below_threshold() {
		assert!(check_fail_safe(100, 10, 30.0));
	}

	#[test]
	fn fail_safe_allows_at_threshold() {
		assert!(check_fail_safe(100, 30, 30.0));
	}

	#[test]
	fn fail_safe_rejects_above_threshold() {
		assert!(!check_fail_safe(100, 31, 30.0));
	}

	#[test]
	fn fail_safe_allows_zero_previous() {
		assert!(check_fail_safe(0, 0, 30.0));
	}

	#[test]
	fn fail_safe_allows_zero_deleted() {
		assert!(check_fail_safe(50, 0, 30.0));
	}

	#[test]
	fn fail_safe_rejects_all_deleted() {
		assert!(!check_fail_safe(10, 10, 30.0));
	}

	#[tokio::test]
	async fn per_source_table_upsert_and_read() {
		let db = surrealdb::engine::any::connect("mem://").await.unwrap();
		db.use_ns("t").use_db("t").await.unwrap();

		let engine =
			DeltaEngine::new(db.clone().into(), db).with_tables(TableNames::for_source("my-src"));
		engine.ensure_tables().await.unwrap();

		let rows = vec![
			RawRow {
				row_key: "k1".into(),
				row_data: serde_json::json!({"v": 1}),
			},
			RawRow {
				row_key: "k2".into(),
				row_data: serde_json::json!({"v": 2}),
			},
		];
		engine.upsert_batch_raw("s", "q", 1, &rows).await.unwrap();

		let keys = engine.read_snapshot_keys("s", "q").await.unwrap();
		assert_eq!(keys.len(), 2, "expected 2 keys, got: {keys:?}");
	}

	#[tokio::test]
	async fn for_source_creates_isolated_engine() {
		let db = surrealdb::engine::any::connect("mem://").await.unwrap();
		db.use_ns("t").use_db("t").await.unwrap();

		let base = DeltaEngine::new(db.clone().into(), db);
		let eng_a = base.for_source("source-a");
		let eng_b = base.for_source("source-b");
		eng_a.ensure_tables().await.unwrap();
		eng_b.ensure_tables().await.unwrap();

		let rows = vec![RawRow {
			row_key: "k1".into(),
			row_data: serde_json::json!({"v": 1}),
		}];
		eng_a
			.upsert_batch_raw("source-a", "q", 1, &rows)
			.await
			.unwrap();

		let keys_a = eng_a.read_snapshot_keys("source-a", "q").await.unwrap();
		let keys_b = eng_b.read_snapshot_keys("source-b", "q").await.unwrap();
		assert_eq!(keys_a.len(), 1);
		assert_eq!(keys_b.len(), 0, "source-b should be isolated");
	}

	#[tokio::test]
	async fn full_cycle_with_per_source_tables() {
		let db = surrealdb::engine::any::connect("mem://").await.unwrap();
		db.use_ns("t").use_db("t").await.unwrap();

		let engine =
			DeltaEngine::new(db.clone().into(), db).with_tables(TableNames::for_source("test-src"));
		engine.ensure_tables().await.unwrap();

		// Cycle 1: create rows
		let cycle_id = engine.next_cycle_id("test-src", "q").await.unwrap();
		assert_eq!(cycle_id, 1);
		engine
			.log_cycle_start("test-src", "q", cycle_id)
			.await
			.unwrap();

		let rows = vec![
			RawRow {
				row_key: "a".into(),
				row_data: serde_json::json!({"v": 1}),
			},
			RawRow {
				row_key: "b".into(),
				row_data: serde_json::json!({"v": 2}),
			},
		];
		engine
			.upsert_batch_raw("test-src", "q", cycle_id, &rows)
			.await
			.unwrap();

		let keys = engine
			.read_snapshot_keys_paged("test-src", "q")
			.await
			.unwrap();
		assert_eq!(keys.len(), 2, "should have 2 keys after upsert");
	}
}

#[cfg(test)]
mod prop_tests {
	use super::*;
	use proptest::prelude::*;

	proptest! {
		#[test]
		fn zero_previous_always_safe(
			deleted in 0usize..1000,
			threshold in 0.0f64..100.0,
		) {
			prop_assert!(check_fail_safe(0, deleted, threshold));
		}

		#[test]
		fn zero_deleted_always_safe(
			previous in 1usize..1000,
			threshold in 0.0f64..100.0,
		) {
			prop_assert!(check_fail_safe(previous, 0, threshold));
		}

		#[test]
		fn monotonic_in_deleted(
			previous in 1usize..500,
			d1 in 0usize..500,
			d2 in 0usize..500,
			threshold in 0.1f64..100.0,
		) {
			// If fewer deletes pass, more deletes should also fail or pass
			let (lo, hi) = if d1 <= d2 { (d1, d2) } else { (d2, d1) };
			if !check_fail_safe(previous, lo, threshold) {
				prop_assert!(!check_fail_safe(previous, hi, threshold));
			}
		}

		#[test]
		fn monotonic_in_threshold(
			previous in 1usize..500,
			deleted in 0usize..500,
			t1 in 0.0f64..100.0,
			t2 in 0.0f64..100.0,
		) {
			// Higher threshold is more permissive
			let (lo, hi) = if t1 <= t2 { (t1, t2) } else { (t2, t1) };
			if check_fail_safe(previous, deleted, lo) {
				prop_assert!(check_fail_safe(previous, deleted, hi));
			}
		}

		#[test]
		fn hundred_percent_threshold_allows_all(
			previous in 1usize..1000,
			deleted in 0usize..1000,
		) {
			prop_assert!(check_fail_safe(previous, deleted.min(previous), 100.0));
		}

		#[test]
		fn boundary_at_threshold(
			previous in 1usize..1000,
			threshold_pct in 1.0f64..99.0,
		) {
			// Compute exact boundary count
			let boundary = (previous as f64 * threshold_pct / 100.0).floor() as usize;
			if boundary <= previous {
				prop_assert!(check_fail_safe(previous, boundary, threshold_pct));
			}
		}
	}
}
