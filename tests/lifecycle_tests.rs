mod common;

use common::surreal::TestSurrealContainer;
use oversync::config::{SinkDef, SourceDef, SurrealDbDef, SyncConfig};
use oversync::lifecycle::LifecycleManager;
use oversync::registry::PluginRegistry;
use oversync_delta::DeltaEngine;
use oversync_sinks::StdoutTargetFactory;

fn test_registry() -> PluginRegistry {
	let mut r = PluginRegistry::new();
	r.register_sink(Box::new(StdoutTargetFactory));
	r
}

fn make_config(surreal_def: &SurrealDbDef) -> SyncConfig {
	SyncConfig {
		surrealdb: surreal_def.clone(),
		sources: vec![],
		sinks: vec![SinkDef {
			name: "debug".into(),
			sink_type: "stdout".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![],
	}
}

fn make_surreal_def(container: &TestSurrealContainer) -> SurrealDbDef {
	SurrealDbDef {
		url: "mem://".into(),
		username: "root".into(),
		password: "root".into(),
		namespace: container.ns.clone(),
		database: container.db.clone(),
		snapshot: None,
	}
}

#[tokio::test]
async fn start_and_shutdown() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);
	let snap = surrealdb::engine::any::connect("mem://").await.unwrap();
	let engine = DeltaEngine::new(container.client.clone().into(), snap);

	let lm = LifecycleManager::new(engine, test_registry());
	assert!(!lm.is_running().await);

	lm.start(make_config(&def)).await.unwrap();
	assert!(lm.is_running().await);
	assert!(!lm.is_paused().await);

	lm.shutdown().await;
	assert!(!lm.is_running().await);
}

#[tokio::test]
async fn pause_and_resume() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);
	let snap = surrealdb::engine::any::connect("mem://").await.unwrap();
	let engine = DeltaEngine::new(container.client.clone().into(), snap);

	let lm = LifecycleManager::new(engine, test_registry());
	lm.start(make_config(&def)).await.unwrap();
	assert!(lm.is_running().await);

	lm.pause().await;
	assert!(!lm.is_running().await);
	assert!(lm.is_paused().await);

	// Pause is idempotent
	lm.pause().await;
	assert!(lm.is_paused().await);

	lm.resume().await.unwrap();
	assert!(lm.is_running().await);
	assert!(!lm.is_paused().await);

	// Resume is idempotent
	lm.resume().await.unwrap();
	assert!(lm.is_running().await);

	lm.shutdown().await;
}

#[tokio::test]
async fn restart_replaces_scheduler() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);
	let snap = surrealdb::engine::any::connect("mem://").await.unwrap();
	let engine = DeltaEngine::new(container.client.clone().into(), snap);

	let lm = LifecycleManager::new(engine, test_registry());
	lm.start(make_config(&def)).await.unwrap();
	assert!(lm.is_running().await);

	// Restart with new config
	let mut new_config = make_config(&def);
	new_config.sinks = vec![SinkDef {
		name: "debug-2".into(),
		sink_type: "stdout".into(),
		config: serde_json::json!({"pretty": true}),
	}];
	lm.start(new_config.clone()).await.unwrap();
	assert!(lm.is_running().await);

	let stored = lm.current_config().await.unwrap();
	assert_eq!(stored.sinks[0].name, "debug-2");

	lm.shutdown().await;
}

#[tokio::test]
async fn config_update_while_paused_applies_on_resume() {
	let container = TestSurrealContainer::new().await;
	let def = make_surreal_def(&container);
	let snap = surrealdb::engine::any::connect("mem://").await.unwrap();
	let engine = DeltaEngine::new(container.client.clone().into(), snap);

	let lm = LifecycleManager::new(engine, test_registry());
	lm.start(make_config(&def)).await.unwrap();
	lm.pause().await;

	// Update config while paused
	let mut new_config = make_config(&def);
	new_config.sinks = vec![SinkDef {
		name: "updated".into(),
		sink_type: "stdout".into(),
		config: serde_json::json!({}),
	}];
	lm.start(new_config).await.unwrap();

	// Still paused, not running
	assert!(!lm.is_running().await);
	assert!(lm.is_paused().await);

	// Config stored
	let stored = lm.current_config().await.unwrap();
	assert_eq!(stored.sinks[0].name, "updated");

	// Resume applies the updated config
	lm.resume().await.unwrap();
	assert!(lm.is_running().await);

	lm.shutdown().await;
}
