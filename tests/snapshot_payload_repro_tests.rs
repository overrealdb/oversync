mod common;

use std::time::Instant;

use common::surreal::TestSurrealContainer;
use oversync_core::TableNames;
use oversync_core::model::RawRow;
use oversync_delta::DeltaEngine;
use serde_json::{Value, json};

fn env_usize(name: &str, default: usize) -> usize {
	std::env::var(name)
		.ok()
		.and_then(|value| value.parse::<usize>().ok())
		.unwrap_or(default)
}

fn repeated_text(seed: usize, len: usize) -> String {
	let base = format!(
		"synthetic description {seed} credit underwriting delinquency portfolio monitoring "
	);
	let mut text = String::with_capacity(len.max(base.len()));
	while text.len() < len {
		text.push_str(&base);
	}
	text.truncate(len);
	text
}

fn synthetic_columns(entity_idx: usize, aspect_idx: usize, cols_per_aspect: usize) -> Value {
	Value::Array(
		(0..cols_per_aspect)
			.map(|col_idx| {
				json!({
					"columnName": format!("col_{aspect_idx}_{col_idx}"),
					"ordinalPosition": col_idx,
					"type": if col_idx % 5 == 0 { "decimal(18,2)" } else { "string" },
					"description": repeated_text(entity_idx + aspect_idx + col_idx, 96),
				})
			})
			.collect(),
	)
}

fn synthetic_aspects(
	entity_idx: usize,
	aspects_per_entity: usize,
	cols_per_aspect: usize,
) -> Vec<Value> {
	(0..aspects_per_entity)
		.map(|aspect_idx| {
			let aspect_schema_id = format!("aspect_{:02}", aspect_idx % 12);
			let data = if aspect_idx % 3 == 0 {
				json!({
					"columns": synthetic_columns(entity_idx, aspect_idx, cols_per_aspect),
				})
			} else {
				json!({
					"owners": [
						{
							"name": format!("Credit Team {}", entity_idx % 12),
							"email": format!("credit-team-{}@corp.test", entity_idx % 12)
						}
					],
					"notes": repeated_text(entity_idx + aspect_idx, 192),
				})
			};

			json!({
				"aspect_schema_id": aspect_schema_id,
				"type": if aspect_idx % 3 == 0 { "CUSTOM" } else { "OWNER" },
				"data": data,
			})
		})
		.collect()
}

fn synthetic_entity_row(
	entity_idx: usize,
	aspects_per_entity: usize,
	cols_per_aspect: usize,
) -> RawRow {
	let entity_id = format!("dm.synthetic.credit_entity_{entity_idx:05}");
	RawRow {
		row_key: entity_id.clone(),
		row_data: json!({
			"id": entity_id,
			"type": "entity",
			"name": format!("credit_entity_{entity_idx:05}"),
			"entityTypeId": if entity_idx.is_multiple_of(2) { "dm_data_lake" } else { "odwh" },
			"description": repeated_text(entity_idx, 320),
			"search_text": repeated_text(entity_idx, 640),
			"aspects": synthetic_aspects(entity_idx, aspects_per_entity, cols_per_aspect),
			"version": 1,
			"created_at": "2026-01-01T00:00:00Z",
			"updated_at": "2026-01-02T00:00:00Z",
		}),
	}
}

fn synthetic_rows(
	row_count: usize,
	aspects_per_entity: usize,
	cols_per_aspect: usize,
) -> Vec<RawRow> {
	(0..row_count)
		.map(|entity_idx| synthetic_entity_row(entity_idx, aspects_per_entity, cols_per_aspect))
		.collect()
}

async fn count_snapshot_rows(
	client: &surrealdb::Surreal<surrealdb::engine::any::Any>,
	table: &str,
) -> usize {
	let mut response = client
		.query(format!("SELECT VALUE count() FROM {table}"))
		.await
		.expect("snapshot count query");
	response
		.take::<Option<usize>>(0)
		.expect("snapshot count take")
		.unwrap_or(0)
}

#[tokio::test]
async fn snapshot_heavy_payload_repro_logs_50_vs_500() {
	let state_db = TestSurrealContainer::new().await;
	let source = "store_pg";

	let engine = DeltaEngine::single(state_db.client.clone()).for_source(source);
	engine.ensure_tables().await.expect("ensure tables");

	let aspects_per_entity = env_usize("OVERSYNC_REPRO_ASPECTS_PER_ENTITY", 12);
	let cols_per_aspect = env_usize("OVERSYNC_REPRO_COLS_PER_ASPECT", 12);
	let small_rows = env_usize("OVERSYNC_REPRO_SMALL_ROWS", 50);
	let large_rows = env_usize("OVERSYNC_REPRO_LARGE_ROWS", 500);

	let small = synthetic_rows(small_rows, aspects_per_entity, cols_per_aspect);
	let large = synthetic_rows(large_rows, aspects_per_entity, cols_per_aspect);

	let small_payload_bytes =
		serde_json::to_vec(&small.iter().map(|row| &row.row_data).collect::<Vec<_>>())
			.expect("serialize small")
			.len();
	let large_payload_bytes =
		serde_json::to_vec(&large.iter().map(|row| &row.row_data).collect::<Vec<_>>())
			.expect("serialize large")
			.len();

	eprintln!(
		"snapshot repro payload: small_rows={small_rows} large_rows={large_rows} aspects_per_entity={aspects_per_entity} cols_per_aspect={cols_per_aspect} small_bytes={small_payload_bytes} large_bytes={large_payload_bytes}"
	);

	let small_start = Instant::now();
	engine
		.upsert_batch_raw(source, "entities_small", 1, &small)
		.await
		.expect("small upsert");
	let small_elapsed = small_start.elapsed();

	let large_start = Instant::now();
	engine
		.upsert_batch_raw(source, "entities_large", 1, &large)
		.await
		.expect("large upsert");
	let large_elapsed = large_start.elapsed();

	let tables = TableNames::for_source(source);
	let small_snapshot_rows = count_snapshot_rows(&state_db.client, &tables.snapshot).await;

	eprintln!(
		"snapshot repro result: small_elapsed_ms={} large_elapsed_ms={} snapshot_rows={}",
		small_elapsed.as_millis(),
		large_elapsed.as_millis(),
		small_snapshot_rows
	);

	assert_eq!(small_snapshot_rows, small_rows + large_rows);
}
