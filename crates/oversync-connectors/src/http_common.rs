use oversync_core::error::OversyncError;
use oversync_core::model::RawRow;

pub use oversync_core::model::AuthConfig;

pub fn apply_auth(
	req: reqwest::RequestBuilder,
	auth: &Option<AuthConfig>,
) -> reqwest::RequestBuilder {
	match auth {
		Some(AuthConfig::Bearer { token }) => req.bearer_auth(token),
		Some(AuthConfig::Header { name, value }) => req.header(name, value),
		Some(AuthConfig::Basic { username, password }) => req.basic_auth(username, Some(password)),
		None => req,
	}
}

pub fn navigate_path<'a>(value: &'a serde_json::Value, path: &str) -> &'a serde_json::Value {
	path.split('.').fold(value, |current, key| &current[key])
}

pub fn extract_items(
	body: &serde_json::Value,
	response_path: &Option<String>,
) -> Vec<serde_json::Value> {
	let target = match response_path {
		Some(path) if !path.is_empty() => {
			let resolved = navigate_path(body, path);
			if resolved.is_null() {
				tracing::warn!(path, "response_path resolved to null — check config");
			}
			resolved
		}
		_ => body,
	};
	match target.as_array() {
		Some(arr) => arr.clone(),
		None => {
			if !target.is_null() {
				tracing::warn!(
					"response is not an array (got {}), returning empty",
					match target {
						serde_json::Value::Object(_) => "object",
						serde_json::Value::String(_) => "string",
						serde_json::Value::Number(_) => "number",
						serde_json::Value::Bool(_) => "bool",
						_ => "unknown",
					}
				);
			}
			vec![]
		}
	}
}

pub fn extract_cursor(body: &serde_json::Value, cursor_path: &str) -> Option<String> {
	let val = navigate_path(body, cursor_path);
	match val {
		serde_json::Value::String(s) => Some(s.clone()),
		serde_json::Value::Number(n) => Some(n.to_string()),
		serde_json::Value::Null => None,
		_ => None,
	}
}

pub fn items_to_rows(
	items: &[serde_json::Value],
	key_column: &str,
) -> Result<Vec<RawRow>, OversyncError> {
	let mut rows = Vec::with_capacity(items.len());
	for item in items {
		let key = item.get(key_column).ok_or_else(|| {
			OversyncError::Connector(format!("missing key field '{key_column}' in response"))
		})?;
		let key_str = match key {
			serde_json::Value::String(s) => s.clone(),
			serde_json::Value::Number(n) => n.to_string(),
			other => other.to_string(),
		};
		rows.push(RawRow {
			row_key: key_str,
			row_data: item.clone(),
		});
	}
	Ok(rows)
}
