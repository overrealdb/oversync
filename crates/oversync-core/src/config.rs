#[cfg(feature = "cli")]
use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "cli", derive(Parser))]
#[cfg_attr(
	feature = "cli",
	command(
		name = "oversync",
		about = "Lightweight data sync engine: poll, delta, sink"
	)
)]
pub struct OversyncConfig {
	#[cfg_attr(feature = "cli", arg(long, env = "OVERSYNC_BIND", default_value = "0.0.0.0:4200"))]
	pub bind: String,

	#[cfg_attr(feature = "cli", arg(long, env = "OVERSYNC_LOG_LEVEL", default_value = "info"))]
	pub log_level: String,

	#[cfg_attr(feature = "cli", command(flatten))]
	pub surrealdb: SurrealDbConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "cli", derive(Parser))]
pub struct SurrealDbConfig {
	#[cfg_attr(
		feature = "cli",
		arg(long, env = "OVERSYNC_SURREALDB_URL", default_value = "http://127.0.0.1:8000")
	)]
	pub url: String,

	#[cfg_attr(feature = "cli", arg(long, env = "OVERSYNC_SURREALDB_USER", default_value = "root"))]
	pub user: String,

	#[cfg_attr(feature = "cli", arg(long, env = "OVERSYNC_SURREALDB_PASS", default_value = "root"))]
	pub pass: String,

	#[cfg_attr(feature = "cli", arg(long, env = "OVERSYNC_SURREALDB_NS", default_value = "oversync"))]
	pub ns: String,

	#[cfg_attr(feature = "cli", arg(long, env = "OVERSYNC_SURREALDB_DB", default_value = "sync"))]
	pub db: String,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn config_defaults() {
		let config = OversyncConfig {
			bind: "0.0.0.0:4200".into(),
			log_level: "info".into(),
			surrealdb: SurrealDbConfig {
				url: "http://127.0.0.1:8000".into(),
				user: "root".into(),
				pass: "root".into(),
				ns: "oversync".into(),
				db: "sync".into(),
			},
		};
		assert_eq!(config.bind, "0.0.0.0:4200");
		assert_eq!(config.surrealdb.ns, "oversync");
	}

	#[test]
	fn config_roundtrip_json() {
		let config = OversyncConfig {
			bind: "127.0.0.1:9999".into(),
			log_level: "debug".into(),
			surrealdb: SurrealDbConfig {
				url: "http://localhost:8000".into(),
				user: "admin".into(),
				pass: "secret".into(),
				ns: "test".into(),
				db: "testdb".into(),
			},
		};
		let json = serde_json::to_string(&config).unwrap();
		let back: OversyncConfig = serde_json::from_str(&json).unwrap();
		assert_eq!(config.bind, back.bind);
		assert_eq!(config.surrealdb.url, back.surrealdb.url);
	}
}
