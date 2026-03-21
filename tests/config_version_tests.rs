mod common;

use common::surreal::TestSurrealContainer;
use oversync::config::{SinkDef, SyncConfig, SurrealDbDef};
use oversync::config_version::{save_version, list_versions, get_version};

fn test_config() -> SyncConfig {
	SyncConfig {
		surrealdb: SurrealDbDef {
			url: "mem://".into(),
			username: "root".into(),
			password: "root".into(),
			namespace: "test".into(),
			database: "test".into(),
			snapshot: None,
		},
		sources: vec![],
		sinks: vec![SinkDef {
			name: "stdout".into(),
			sink_type: "stdout".into(),
			config: serde_json::json!({}),
		}],
		pipes: vec![],
	}
}

#[tokio::test]
async fn save_and_list_versions() {
	let surreal = TestSurrealContainer::new().await;

	let v1 = save_version(&surreal.client, &test_config(), "first version")
		.await
		.unwrap();
	assert_eq!(v1, 1);

	let v2 = save_version(&surreal.client, &test_config(), "second version")
		.await
		.unwrap();
	assert_eq!(v2, 2);

	let versions = list_versions(&surreal.client).await.unwrap();
	assert_eq!(versions.len(), 2);
	assert_eq!(versions[0].version, 2); // newest first
	assert_eq!(versions[1].version, 1);
	assert_eq!(versions[0].description, "second version");
}

#[tokio::test]
async fn get_specific_version() {
	let surreal = TestSurrealContainer::new().await;

	save_version(&surreal.client, &test_config(), "v1").await.unwrap();
	save_version(&surreal.client, &test_config(), "v2").await.unwrap();

	let v = get_version(&surreal.client, 1).await.unwrap();
	assert_eq!(v.version, 1);
	assert_eq!(v.description, "v1");
	assert!(v.config_json.is_object());
}

#[tokio::test]
async fn get_nonexistent_version_errors() {
	let surreal = TestSurrealContainer::new().await;

	let err = get_version(&surreal.client, 999).await.unwrap_err();
	assert!(err.to_string().contains("999"));
	assert!(err.to_string().contains("not found"));
}

#[tokio::test]
async fn first_version_is_1() {
	let surreal = TestSurrealContainer::new().await;

	let v = save_version(&surreal.client, &test_config(), "initial")
		.await
		.unwrap();
	assert_eq!(v, 1);
}
