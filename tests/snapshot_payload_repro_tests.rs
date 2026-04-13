mod common;

use std::time::Instant;

use common::surreal::TestSurrealContainer;
use oversync_core::TableNames;
use oversync_core::model::RawRow;
use oversync_core::runtime_surreal_url;
use oversync_delta::DeltaEngine;
use serde_json::{Value, json};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;

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
							"email": format!("credit-team-{}@example.test", entity_idx % 12)
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

async fn count_snapshot_rows(client: &Surreal<Any>, table: &str) -> usize {
	let mut response = client
		.query(format!("SELECT count() AS count FROM {table} GROUP ALL"))
		.await
		.expect("snapshot count query");
	let rows: Vec<serde_json::Value> = response.take(0).expect("snapshot count take");
	rows.first()
		.and_then(|row| row.get("count"))
		.and_then(|value| value.as_u64())
		.unwrap_or(0) as usize
}

fn rewrite_scheme(url: &str, secure: bool, websocket: bool) -> String {
	match (secure, websocket) {
		(false, false) => {
			if let Some(rest) = url.strip_prefix("ws://") {
				format!("http://{rest}")
			} else if let Some(rest) = url.strip_prefix("wss://") {
				format!("https://{rest}")
			} else {
				url.to_string()
			}
		}
		(false, true) => runtime_surreal_url(url).into_owned(),
		(true, false) => {
			if let Some(rest) = url.strip_prefix("wss://") {
				format!("https://{rest}")
			} else if let Some(rest) = url.strip_prefix("ws://") {
				format!("http://{rest}")
			} else {
				url.to_string()
			}
		}
		(true, true) => {
			if let Some(rest) = url.strip_prefix("https://") {
				format!("wss://{rest}")
			} else if let Some(rest) = url.strip_prefix("http://") {
				format!("ws://{rest}")
			} else if let Some(rest) = url.strip_prefix("ws://") {
				format!("wss://{rest}")
			} else {
				url.to_string()
			}
		}
	}
}

async fn connect_with_url(url: &str, ns: &str, db: &str) -> Surreal<Any> {
	let client = surrealdb::engine::any::connect(url)
		.await
		.unwrap_or_else(|e| panic!("connect {url}: {e}"));
	client
		.signin(Root {
			username: "root".to_string(),
			password: "root".to_string(),
		})
		.await
		.unwrap_or_else(|e| panic!("signin {url}: {e}"));
	client
		.use_ns(ns)
		.use_db(db)
		.await
		.unwrap_or_else(|e| panic!("use ns/db {url}: {e}"));
	client
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

#[tokio::test]
#[ignore = "diagnostic repro for heavy snapshot batches over HTTP vs WS transport"]
async fn heavy_snapshot_batch_http_hits_payload_limit_while_ws_succeeds() {
	let aspects_per_entity = env_usize("OVERSYNC_REPRO_ASPECTS_PER_ENTITY", 12);
	let cols_per_aspect = env_usize("OVERSYNC_REPRO_COLS_PER_ASPECT", 12);
	let large_rows = env_usize("OVERSYNC_REPRO_LARGE_ROWS", 250);
	let rows = synthetic_rows(large_rows, aspects_per_entity, cols_per_aspect);

	let base_url = TestSurrealContainer::url().await;
	let secure = base_url.starts_with("wss://") || base_url.starts_with("https://");
	let http_url = rewrite_scheme(&base_url, secure, false);
	let ws_url = rewrite_scheme(&base_url, secure, true);

	let http_store = TestSurrealContainer::new_raw().await;
	let ws_store = TestSurrealContainer::new_raw().await;

	let http_client = connect_with_url(&http_url, &http_store.ns, &http_store.db).await;
	let ws_client = connect_with_url(&ws_url, &ws_store.ns, &ws_store.db).await;

	let http_engine = DeltaEngine::single(http_client).for_source("store_pg");
	http_engine
		.ensure_tables()
		.await
		.expect("http ensure tables");
	let ws_engine = DeltaEngine::single(ws_client).for_source("store_pg");
	ws_engine.ensure_tables().await.expect("ws ensure tables");

	let http_err = http_engine
		.upsert_batch_raw("store_pg", "entities_http", 1, &rows)
		.await
		.expect_err("http transport should hit payload ceiling on heavy rows");
	assert!(
		http_err.to_string().contains("413 Payload Too Large"),
		"unexpected http error: {http_err}"
	);

	ws_engine
		.upsert_batch_raw("store_pg", "entities_ws", 1, &rows)
		.await
		.expect("ws transport should accept the same heavy rows");

	let tables = TableNames::for_source("store_pg");
	let ws_snapshot_rows = count_snapshot_rows(&ws_store.client, &tables.snapshot).await;
	assert_eq!(ws_snapshot_rows, large_rows);
}
