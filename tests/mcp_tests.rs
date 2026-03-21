use oversync_connectors::mcp::{McpConfig, McpOriginConnector};
use oversync_core::traits::OriginConnector;

fn mock_server_path() -> String {
	let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
	format!("{manifest}/tests/fixtures/mock_mcp_server.py")
}

#[tokio::test]
async fn mcp_origin_fetch_all_returns_rows() {
	let config = McpConfig {
		dsn: "python3".into(),
		args: vec![mock_server_path()],
		key_field: "id".into(),
		response_path: None,
	};

	let connector = McpOriginConnector::new("mcp-test", config);
	let rows = connector.fetch_all("list_users", "id").await.unwrap();

	assert_eq!(rows.len(), 3);
	assert_eq!(rows[0].row_key, "1");
	assert_eq!(rows[0].row_data["name"], "alice");
	assert_eq!(rows[1].row_key, "2");
	assert_eq!(rows[1].row_data["name"], "bob");
	assert_eq!(rows[2].row_key, "3");
	assert_eq!(rows[2].row_data["score"], 72);
}

#[tokio::test]
async fn mcp_origin_test_connection() {
	let config = McpConfig {
		dsn: "python3".into(),
		args: vec![mock_server_path()],
		key_field: "id".into(),
		response_path: None,
	};

	let connector = McpOriginConnector::new("mcp-test", config);
	connector.test_connection().await.unwrap();
}

#[tokio::test]
async fn mcp_origin_tool_with_args() {
	let config = McpConfig {
		dsn: "python3".into(),
		args: vec![mock_server_path()],
		key_field: "id".into(),
		response_path: None,
	};

	let connector = McpOriginConnector::new("mcp-test", config);
	let rows = connector
		.fetch_all("search:query=rust,limit=10", "id")
		.await
		.unwrap();
	assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn mcp_origin_name() {
	let config = McpConfig {
		dsn: "python3".into(),
		args: vec![mock_server_path()],
		key_field: "id".into(),
		response_path: None,
	};

	let connector = McpOriginConnector::new("my-mcp", config);
	assert_eq!(connector.name(), "my-mcp");
}

#[tokio::test]
async fn mcp_origin_invalid_command_errors() {
	let config = McpConfig {
		dsn: "/nonexistent/binary".into(),
		args: vec![],
		key_field: "id".into(),
		response_path: None,
	};

	let connector = McpOriginConnector::new("bad", config);
	let err = connector.test_connection().await.unwrap_err();
	assert!(err.to_string().contains("mcp spawn"));
}
