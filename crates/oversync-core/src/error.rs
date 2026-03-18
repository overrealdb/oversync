#[derive(Debug, thiserror::Error)]
pub enum OversyncError {
	#[error("surrealdb: {0}")]
	SurrealDb(String),

	#[error("connector: {0}")]
	Connector(String),

	#[error("sink: {0}")]
	Sink(String),

	#[error("config: {0}")]
	Config(String),

	#[error("migration: {0}")]
	Migration(String),

	#[error("transform: {0}")]
	Transform(String),

	#[error("plugin: {0}")]
	Plugin(String),

	#[error("internal: {0}")]
	Internal(String),
}

impl From<serde_json::Error> for OversyncError {
	fn from(e: serde_json::Error) -> Self {
		Self::Internal(e.to_string())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn display_all_variants() {
		let cases: Vec<(OversyncError, &str)> = vec![
			(OversyncError::SurrealDb("conn".into()), "surrealdb: conn"),
			(
				OversyncError::Connector("timeout".into()),
				"connector: timeout",
			),
			(OversyncError::Sink("full".into()), "sink: full"),
			(OversyncError::Config("bad".into()), "config: bad"),
			(OversyncError::Migration("v1".into()), "migration: v1"),
			(OversyncError::Transform("bad".into()), "transform: bad"),
			(OversyncError::Plugin("missing".into()), "plugin: missing"),
			(OversyncError::Internal("oops".into()), "internal: oops"),
		];
		for (err, expected) in cases {
			assert_eq!(err.to_string(), expected);
		}
	}

	#[test]
	fn serde_json_error_converts() {
		let bad_json = serde_json::from_str::<serde_json::Value>("not json");
		let oversync_err: OversyncError = bad_json.unwrap_err().into();
		assert!(matches!(oversync_err, OversyncError::Internal(_)));
	}
}
