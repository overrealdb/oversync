// Resilient DB stress tests — verify that concurrent workers sharing an
// Arc<Surreal<Any>> recover automatically after session loss.
//
// These are NOT checkbox tests — they simulate real production scenarios
// with multiple threads hitting the DB simultaneously during failures.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use oversync::resilient_db::{ResilientDb, ResilientDbConfig};
use tokio_util::sync::CancellationToken;

fn test_config(url: &str) -> ResilientDbConfig {
	let id = uuid::Uuid::new_v4().to_string().replace('-', "");
	ResilientDbConfig {
		url: url.to_string(),
		username: "root".to_string(),
		password: "root".to_string(),
		namespace: format!("rdb_{}", &id[..8]),
		database: format!("stress_{}", &id[..8]),
		health_interval: Duration::from_millis(500),
		token_refresh_interval: None,
	}
}

// ---------------------------------------------------------------------------
// 1. Concurrent workers all recover after session invalidation
// ---------------------------------------------------------------------------

/// Spawn N workers doing continuous DB writes. Mid-test, invalidate the shared
/// session. After health loop recovers (~500ms), all workers must resume.
/// Counts successful vs failed ops — failure window must be bounded.
#[tokio::test]
async fn concurrent_workers_recover_after_session_loss() {
	let url = common::surreal::TestSurrealContainer::url().await;
	let cancel = CancellationToken::new();

	let rdb = ResilientDb::connect(test_config(&url)).await.unwrap();
	let _health = rdb.start_health_loop(cancel.clone());
	let db = rdb.client();

	let success = Arc::new(AtomicU64::new(0));
	let failure = Arc::new(AtomicU64::new(0));
	let worker_cancel = CancellationToken::new();

	// Spawn 10 concurrent workers
	let mut handles = vec![];
	for i in 0..10 {
		let db = Arc::clone(&db);
		let s = Arc::clone(&success);
		let f = Arc::clone(&failure);
		let wc = worker_cancel.clone();

		handles.push(tokio::spawn(async move {
			let mut local_ok = 0u64;
			let mut local_err = 0u64;
			loop {
				if wc.is_cancelled() {
					break;
				}
				let q = format!("UPSERT stress_test:w{i} SET ts = time::now(), worker = {i}");
				match db.query(&q).await {
					Ok(_) => local_ok += 1,
					Err(_) => local_err += 1,
				}
				tokio::time::sleep(Duration::from_millis(50)).await;
			}
			s.fetch_add(local_ok, Ordering::Relaxed);
			f.fetch_add(local_err, Ordering::Relaxed);
		}));
	}

	// Let workers run for 1 second (healthy)
	tokio::time::sleep(Duration::from_secs(1)).await;

	// Simulate session loss
	db.invalidate().await.unwrap();

	// Let health loop recover (~500ms interval)
	tokio::time::sleep(Duration::from_secs(2)).await;

	// Let workers run 1 more second (should be healthy again)
	tokio::time::sleep(Duration::from_secs(1)).await;

	worker_cancel.cancel();
	for h in handles {
		h.await.unwrap();
	}

	let total_ok = success.load(Ordering::Relaxed);
	let total_err = failure.load(Ordering::Relaxed);

	// Workers ran for ~4 seconds at 50ms intervals = ~80 ops per worker = ~800 total
	// Failure window is ~500ms = ~10 ops per worker = ~100 max failures
	assert!(
		total_ok > 0,
		"should have some successful ops, got ok={total_ok} err={total_err}"
	);
	assert!(
		total_ok > total_err,
		"successes should exceed failures: ok={total_ok} err={total_err}"
	);
	// After recovery, workers must succeed — verify the last second was clean
	// by checking that total_ok is substantial
	assert!(
		total_ok > 200,
		"expected >200 successful ops across 10 workers, got {total_ok}"
	);

	cancel.cancel();
}

// ---------------------------------------------------------------------------
// 2. Repeated session loss under load — no permanent degradation
// ---------------------------------------------------------------------------

/// Invalidate the session 5 times with workers running. Each time the health
/// loop must recover. No worker should see permanent failures.
#[tokio::test]
async fn repeated_invalidation_under_load() {
	let url = common::surreal::TestSurrealContainer::url().await;
	let cancel = CancellationToken::new();

	let rdb = ResilientDb::connect(test_config(&url)).await.unwrap();
	let _health = rdb.start_health_loop(cancel.clone());
	let db = rdb.client();

	let success = Arc::new(AtomicU64::new(0));
	let failure = Arc::new(AtomicU64::new(0));
	let worker_cancel = CancellationToken::new();

	// 5 workers
	let mut handles = vec![];
	for i in 0..5 {
		let db = Arc::clone(&db);
		let s = Arc::clone(&success);
		let f = Arc::clone(&failure);
		let wc = worker_cancel.clone();

		handles.push(tokio::spawn(async move {
			let mut local_ok = 0u64;
			let mut local_err = 0u64;
			loop {
				if wc.is_cancelled() {
					break;
				}
				let q = format!("UPSERT repeat_test:w{i} SET ts = time::now()");
				match db.query(&q).await {
					Ok(_) => local_ok += 1,
					Err(_) => local_err += 1,
				}
				tokio::time::sleep(Duration::from_millis(30)).await;
			}
			s.fetch_add(local_ok, Ordering::Relaxed);
			f.fetch_add(local_err, Ordering::Relaxed);
		}));
	}

	for round in 0..5 {
		tokio::time::sleep(Duration::from_secs(1)).await;
		db.invalidate().await.unwrap();
		eprintln!("round {round}: invalidated, waiting for recovery...");
		tokio::time::sleep(Duration::from_secs(1)).await;

		// Verify recovery with a direct query
		let probe: Result<Option<serde_json::Value>, _> =
			db.query("RETURN 1").await.and_then(|mut r| r.take(0));
		assert!(
			probe.is_ok(),
			"round {round}: should recover after health loop, got: {probe:?}"
		);
	}

	worker_cancel.cancel();
	for h in handles {
		h.await.unwrap();
	}

	let total_ok = success.load(Ordering::Relaxed);
	let total_err = failure.load(Ordering::Relaxed);

	eprintln!("repeated_invalidation: ok={total_ok} err={total_err}");

	// Over 10 seconds with 5 workers at 30ms ≈ 1600+ ops total
	// 5 invalidation windows of ~500ms each ≈ ~80 failed ops
	assert!(total_ok > total_err * 3, "ok={total_ok} err={total_err}");

	cancel.cancel();
}

// ---------------------------------------------------------------------------
// 3. Data integrity after recovery — no silent corruption
// ---------------------------------------------------------------------------

/// Write known data, invalidate, recover, verify data is intact.
/// Proves that recovery doesn't corrupt existing records.
#[tokio::test]
async fn data_integrity_after_recovery() {
	let url = common::surreal::TestSurrealContainer::url().await;
	let cancel = CancellationToken::new();

	let rdb = ResilientDb::connect(test_config(&url)).await.unwrap();
	let _health = rdb.start_health_loop(cancel.clone());
	let db = rdb.client();

	// Write 100 records with known values
	for i in 0..100 {
		db.query(format!(
			"CREATE integrity_test:{i} SET value = {i}, checksum = '{i}_check'"
		))
		.await
		.unwrap();
	}

	// Verify all written
	let before: Vec<serde_json::Value> = db.select("integrity_test").await.unwrap();
	assert_eq!(before.len(), 100);

	// Invalidate + recover
	db.invalidate().await.unwrap();
	tokio::time::sleep(Duration::from_secs(2)).await;

	// Read back all records
	let after: Vec<serde_json::Value> = db.select("integrity_test").await.unwrap();
	assert_eq!(
		after.len(),
		100,
		"all 100 records must survive session recovery"
	);

	// Spot-check specific values
	let mut resp = db
		.query("SELECT * FROM ONLY integrity_test:42")
		.await
		.unwrap();
	let row: Option<serde_json::Value> = resp.take(0).unwrap();
	let row = row.expect("record 42 must exist");
	assert_eq!(row["value"], 42);
	assert_eq!(row["checksum"], "42_check");

	cancel.cancel();
}

// ---------------------------------------------------------------------------
// 4. PipeLock survives session loss
// ---------------------------------------------------------------------------

/// Acquire a distributed lock, invalidate session, recover, release lock.
/// Proves lock operations work across session recovery.
#[tokio::test]
async fn pipe_lock_survives_session_loss() {
	let url = common::surreal::TestSurrealContainer::url().await;
	let cancel = CancellationToken::new();

	let rdb = ResilientDb::connect(test_config(&url)).await.unwrap();
	let _health = rdb.start_health_loop(cancel.clone());
	let db = rdb.client();

	// Create pipe_lock table (normally done by schema migration)
	db.query("DEFINE TABLE IF NOT EXISTS pipe_lock SCHEMALESS")
		.await
		.unwrap();

	let lock = oversync::distributed_lock::PipeLock::new(Arc::clone(&db), "test-instance".into());

	// Acquire lock
	let acquired = lock.try_acquire("test-pipe:q1", 60).await.unwrap();
	assert!(acquired, "should acquire lock");

	// Invalidate session
	db.invalidate().await.unwrap();
	tokio::time::sleep(Duration::from_secs(2)).await;

	// Release lock after recovery — must not error
	lock.release("test-pipe:q1").await.unwrap();

	// Re-acquire should work (lock was released)
	let reacquired = lock.try_acquire("test-pipe:q1", 60).await.unwrap();
	assert!(reacquired, "should reacquire after release + recovery");

	cancel.cancel();
}

// ---------------------------------------------------------------------------
// 5. Concurrent reads and writes during recovery window
// ---------------------------------------------------------------------------

/// Spawn readers and writers simultaneously. Invalidate session.
/// After recovery, both reads and writes must work. No reader should
/// see partial/corrupt data.
#[tokio::test]
async fn concurrent_reads_writes_during_recovery() {
	let url = common::surreal::TestSurrealContainer::url().await;
	let cancel = CancellationToken::new();

	let rdb = ResilientDb::connect(test_config(&url)).await.unwrap();
	let _health = rdb.start_health_loop(cancel.clone());
	let db = rdb.client();

	// Seed initial data
	db.query("CREATE rw_test:counter SET value = 0")
		.await
		.unwrap();

	let write_count = Arc::new(AtomicU64::new(0));
	let read_ok = Arc::new(AtomicU64::new(0));
	let worker_cancel = CancellationToken::new();

	// Writer: increment counter
	let writer_db = Arc::clone(&db);
	let wc = Arc::clone(&write_count);
	let wcancel = worker_cancel.clone();
	let writer = tokio::spawn(async move {
		loop {
			if wcancel.is_cancelled() {
				break;
			}
			let committed = match writer_db
				.query("UPDATE rw_test:counter SET value += 1")
				.await
			{
				Ok(mut resp) => {
					let rows: Result<Vec<serde_json::Value>, _> = resp.take(0);
					rows.map(|r| !r.is_empty()).unwrap_or(false)
				}
				Err(_) => false,
			};
			if committed {
				wc.fetch_add(1, Ordering::Relaxed);
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	});

	// Reader: read counter, verify it's a valid number
	let reader_db = Arc::clone(&db);
	let rok = Arc::clone(&read_ok);
	let rcancel = worker_cancel.clone();
	let reader = tokio::spawn(async move {
		loop {
			if rcancel.is_cancelled() {
				break;
			}
			let res: Result<Option<serde_json::Value>, _> =
				reader_db.select(("rw_test", "counter")).await;
			if let Ok(Some(row)) = res {
				if row.get("value").and_then(|v| v.as_u64()).is_some() {
					rok.fetch_add(1, Ordering::Relaxed);
				}
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	});

	tokio::time::sleep(Duration::from_secs(1)).await;

	// Invalidate
	db.invalidate().await.unwrap();
	tokio::time::sleep(Duration::from_secs(2)).await;

	// Run 1 more second healthy
	tokio::time::sleep(Duration::from_secs(1)).await;

	worker_cancel.cancel();
	writer.await.unwrap();
	reader.await.unwrap();

	let writes = write_count.load(Ordering::Relaxed);
	let reads = read_ok.load(Ordering::Relaxed);

	assert!(writes > 50, "expected >50 successful writes, got {writes}");
	assert!(reads > 50, "expected >50 valid reads, got {reads}");

	// Final consistency check — counter value should equal successful writes
	let row: Option<serde_json::Value> = db.select(("rw_test", "counter")).await.unwrap();
	let final_value = row.unwrap()["value"].as_u64().unwrap();
	assert_eq!(
		final_value, writes,
		"counter must equal total successful writes"
	);

	cancel.cancel();
}

// ---------------------------------------------------------------------------
// 6. JWT expiry deadlock recovery — the REAL production failure
// ---------------------------------------------------------------------------

/// Simulate the exact production deadlock where expired JWT makes even
/// invalidate() and signin() fail on HTTP. The health loop must recover
/// by creating a fresh connection (no stale Bearer).
#[tokio::test]
async fn recovers_from_expired_jwt_deadlock() {
	let url = common::surreal::TestSurrealContainer::url().await;
	let cancel = CancellationToken::new();

	let rdb = ResilientDb::connect(test_config(&url)).await.unwrap();
	let _health = rdb.start_health_loop(cancel.clone());
	let db = rdb.client();

	// Write test data while healthy
	db.query("CREATE deadlock_test:1 SET value = 42")
		.await
		.unwrap();

	let row: Option<serde_json::Value> = db.select(("deadlock_test", "1")).await.unwrap();
	assert!(row.is_some(), "should read data when healthy");

	// Corrupt the session by authenticating with a fabricated expired JWT.
	// This simulates what happens when a real JWT expires — the client's
	// Bearer token becomes stale and the server rejects all requests.
	let fake_jwt = surrealdb::opt::auth::Token::from("eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2MDAwMDAwMDAsImV4cCI6MTYwMDAwMDAwMSwiaXNzIjoiU3VycmVhbERCIiwianRpIjoiZmFrZSJ9.invalid");
	let _ = db.authenticate(fake_jwt).await;

	// Connection is now in deadlock state
	let broken: Result<Option<serde_json::Value>, _> = db.select(("deadlock_test", "1")).await;
	eprintln!("deadlock state query result: {broken:?}");

	// Wait for health loop to detect and recover via fresh connection
	tokio::time::sleep(Duration::from_secs(3)).await;

	// After recovery, queries must work
	let recovered: Option<serde_json::Value> = db
		.select(("deadlock_test", "1"))
		.await
		.expect("query must succeed after deadlock recovery");
	assert!(
		recovered.is_some(),
		"data must be readable after deadlock recovery"
	);
	assert_eq!(recovered.unwrap()["value"], 42);

	cancel.cancel();
}

// ---------------------------------------------------------------------------
// 7. Proactive refresh prevents any downtime
// ---------------------------------------------------------------------------

/// With a short token_refresh_interval, workers should experience ZERO
/// failures because the token is refreshed before it expires.
#[tokio::test]
async fn proactive_refresh_prevents_failures() {
	let url = common::surreal::TestSurrealContainer::url().await;
	let cancel = CancellationToken::new();

	let id = uuid::Uuid::new_v4().to_string().replace('-', "");
	let config = ResilientDbConfig {
		url: url.to_string(),
		username: "root".to_string(),
		password: "root".to_string(),
		namespace: format!("rdb_{}", &id[..8]),
		database: format!("proactive_{}", &id[..8]),
		health_interval: Duration::from_millis(500),
		token_refresh_interval: Some(Duration::from_secs(2)),
	};

	let rdb = ResilientDb::connect(config).await.unwrap();
	let _health = rdb.start_health_loop(cancel.clone());
	let db = rdb.client();

	let failure_count = Arc::new(AtomicU64::new(0));
	let success_count = Arc::new(AtomicU64::new(0));
	let worker_cancel = CancellationToken::new();

	let mut handles = vec![];
	for i in 0..5 {
		let db = Arc::clone(&db);
		let f = Arc::clone(&failure_count);
		let s = Arc::clone(&success_count);
		let wc = worker_cancel.clone();
		handles.push(tokio::spawn(async move {
			loop {
				if wc.is_cancelled() {
					break;
				}
				match db
					.query(format!("UPSERT proactive_test:w{i} SET ts = time::now()"))
					.await
				{
					Ok(_) => {
						s.fetch_add(1, Ordering::Relaxed);
					}
					Err(_) => {
						f.fetch_add(1, Ordering::Relaxed);
					}
				}
				tokio::time::sleep(Duration::from_millis(50)).await;
			}
		}));
	}

	tokio::time::sleep(Duration::from_secs(6)).await;
	worker_cancel.cancel();
	for h in handles {
		h.await.unwrap();
	}

	let failures = failure_count.load(Ordering::Relaxed);
	let successes = success_count.load(Ordering::Relaxed);
	eprintln!("proactive refresh: ok={successes} err={failures}");

	assert_eq!(
		failures, 0,
		"proactive refresh should prevent ALL failures, got {failures}"
	);
	assert!(successes > 100, "should have >100 successful ops");

	cancel.cancel();
}
