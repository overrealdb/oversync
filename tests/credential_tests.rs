mod common;

use common::surreal::TestSurrealContainer;
use oversync::config::{DeltaDef, OriginDef, PipeConfig, QueryDef, RetryDef, ScheduleDef};
use oversync::credential::{AesGcmStore, resolve_pipe_credentials};

#[tokio::test]
async fn resolve_credential_into_pipe_dsn() {
	let surreal = TestSurrealContainer::new().await;
	let store = AesGcmStore::from_passphrase("test-key");

	// Store an encrypted credential
	let encrypted = store
		.encrypt("postgres://real-user:real-pass@prod:5432/db")
		.unwrap();
	surreal
		.client
		.query(
			"CREATE credential SET name = 'prod-pg', credential_type = 'password', encrypted = $enc",
		)
		.bind(("enc", encrypted))
		.await
		.unwrap();

	// Create a pipe that references the credential
	let mut pipes = vec![PipeConfig {
		name: "test-pipe".into(),
		origin: OriginDef {
			connector: "postgres".into(),
			dsn: "placeholder://".into(),
			credential: Some("prod-pg".into()),
			trino_url: None,
			config: serde_json::json!({}),
		},
		targets: vec![],
		queries: vec![QueryDef {
			id: "q1".into(),
			sql: "SELECT 1".into(),
			key_column: "id".into(),
			sinks: None,
			transform: None,
		}],
		schedule: ScheduleDef::default(),
		delta: DeltaDef::default(),
		retry: RetryDef::default(),
		filters: vec![],
		transforms: vec![],
		enabled: true,
	}];

	// Resolve — should replace DSN with decrypted secret
	resolve_pipe_credentials(&mut pipes, &surreal.client, &store)
		.await
		.unwrap();

	assert_eq!(
		pipes[0].origin.dsn,
		"postgres://real-user:real-pass@prod:5432/db"
	);
}

#[tokio::test]
async fn resolve_missing_credential_errors() {
	let surreal = TestSurrealContainer::new().await;
	let store = AesGcmStore::from_passphrase("test-key");

	let mut pipes = vec![PipeConfig {
		name: "test-pipe".into(),
		origin: OriginDef {
			connector: "postgres".into(),
			dsn: "placeholder://".into(),
			credential: Some("nonexistent".into()),
			trino_url: None,
			config: serde_json::json!({}),
		},
		targets: vec![],
		queries: vec![],
		schedule: ScheduleDef::default(),
		delta: DeltaDef::default(),
		retry: RetryDef::default(),
		filters: vec![],
		transforms: vec![],
		enabled: true,
	}];

	let err = resolve_pipe_credentials(&mut pipes, &surreal.client, &store)
		.await
		.unwrap_err();
	assert!(err.to_string().contains("nonexistent"));
	assert!(err.to_string().contains("not found"));
}

#[tokio::test]
async fn resolve_no_credentials_is_noop() {
	let surreal = TestSurrealContainer::new().await;
	let store = AesGcmStore::from_passphrase("test-key");

	let mut pipes = vec![PipeConfig {
		name: "test-pipe".into(),
		origin: OriginDef {
			connector: "postgres".into(),
			dsn: "postgres://original".into(),
			credential: None,
			trino_url: None,
			config: serde_json::json!({}),
		},
		targets: vec![],
		queries: vec![],
		schedule: ScheduleDef::default(),
		delta: DeltaDef::default(),
		retry: RetryDef::default(),
		filters: vec![],
		transforms: vec![],
		enabled: true,
	}];

	resolve_pipe_credentials(&mut pipes, &surreal.client, &store)
		.await
		.unwrap();

	assert_eq!(pipes[0].origin.dsn, "postgres://original");
}
