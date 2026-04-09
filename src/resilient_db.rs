//! Dual-connection SurrealDB wrapper with proactive health checking.
//!
//! Maintains two independent connections:
//! - **Primary**: serves all application queries (shared via `Arc`)
//! - **Supervisor**: monitors primary health, provides fresh JWTs for recovery
//!
//! All callers share the **same** `Surreal<Any>` instance (same session) via
//! `Arc`. This ensures that when the health loop re-authenticates the primary,
//! every handler immediately benefits — no stale session clones.
//!
//! A background health loop probes the primary every `health_interval` by
//! reading a sentinel record (`_rdb_probe:health`). If the read fails —
//! meaning auth or ns/db context is lost — it re-authenticates the primary
//! using a **fresh temporary connection** (avoids the HTTP JWT deadlock where
//! expired Bearer tokens block even `signin()` and `invalidate()` requests).
//!
//! Optionally refreshes the token proactively before expiry to prevent any
//! query from ever hitting an expired token.

use std::sync::Arc;
use std::time::Duration;

use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use tokio_util::sync::CancellationToken;

pub struct ResilientDbConfig {
	pub url: String,
	pub username: String,
	pub password: String,
	pub namespace: String,
	pub database: String,
	pub health_interval: Duration,
	/// Proactive token refresh interval. If set, the health loop will
	/// re-authenticate before the token expires (e.g. 50 minutes for a
	/// 1-hour token lifetime). Set to `None` to disable proactive refresh.
	pub token_refresh_interval: Option<Duration>,
}

pub struct ResilientDb {
	primary: Arc<Surreal<Any>>,
	supervisor: Surreal<Any>,
	config: ResilientDbConfig,
}

impl ResilientDb {
	pub async fn connect(config: ResilientDbConfig) -> anyhow::Result<Self> {
		let primary = Self::open_connection(&config).await?;
		let supervisor = Self::open_connection(&config).await?;

		primary
			.query("UPSERT _rdb_probe:health SET ok = true")
			.await?;

		tracing::info!(
			url = %config.url,
			health_interval_ms = config.health_interval.as_millis() as u64,
			proactive_refresh = config.token_refresh_interval.is_some(),
			"ResilientDb: connected (primary + supervisor)"
		);

		Ok(Self {
			primary: Arc::new(primary),
			supervisor,
			config,
		})
	}

	/// Shared handle to the primary connection. All callers get the same
	/// `Surreal<Any>` instance (same session) via Arc.
	pub fn client(&self) -> Arc<Surreal<Any>> {
		Arc::clone(&self.primary)
	}

	pub fn supervisor(&self) -> &Surreal<Any> {
		&self.supervisor
	}

	pub fn start_health_loop(&self, cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
		let primary = Arc::clone(&self.primary);
		let supervisor_clone = self.supervisor.clone();
		let interval = self.config.health_interval;
		let url = self.config.url.clone();
		let username = self.config.username.clone();
		let password = self.config.password.clone();
		let namespace = self.config.namespace.clone();
		let database = self.config.database.clone();
		let refresh_interval = self.config.token_refresh_interval;

		tokio::spawn(async move {
			tracing::info!("ResilientDb health loop: started");
			let mut last_auth = std::time::Instant::now();

			loop {
				tokio::select! {
					_ = tokio::time::sleep(interval) => {}
					_ = cancel.cancelled() => {
						tracing::info!("ResilientDb health loop: shutting down");
						return;
					}
				}

				// Proactive token refresh: re-auth before token expires
				if let Some(refresh) = refresh_interval
					&& last_auth.elapsed() >= refresh
				{
					tracing::info!("ResilientDb: proactive token refresh");
					if Self::reauth_fresh(
						&primary, &url, &username, &password, &namespace, &database,
					)
					.await
					.is_ok()
					{
						last_auth = std::time::Instant::now();
						// Also refresh supervisor
						let _ = Self::reauth_fresh(
							&supervisor_clone,
							&url,
							&username,
							&password,
							&namespace,
							&database,
						)
						.await;
						continue;
					}
				}

				if Self::probe_healthy(&primary).await {
					continue;
				}

				tracing::warn!("ResilientDb: primary probe failed, recovering...");

				// Strategy 1: fresh connection re-auth (avoids HTTP JWT deadlock)
				let recovered = match Self::reauth_fresh(
					&primary, &url, &username, &password, &namespace, &database,
				)
				.await
				{
					Ok(()) => {
						last_auth = std::time::Instant::now();
						Self::ensure_probe(&primary).await;
						if Self::probe_healthy(&primary).await {
							tracing::info!(
								"ResilientDb: primary recovered (fresh connection re-auth)"
							);
							true
						} else {
							false
						}
					}
					Err(e) => {
						tracing::warn!(error = %e, "ResilientDb: fresh connection re-auth failed");
						false
					}
				};

				if recovered {
					if let Err(e) = Self::reauth_fresh(
						&supervisor_clone,
						&url,
						&username,
						&password,
						&namespace,
						&database,
					)
					.await
					{
						tracing::warn!(
							error = %e,
							"ResilientDb: failed to refresh supervisor after primary recovery"
						);
					}
					continue;
				}

				tracing::warn!("ResilientDb: refreshing supervisor session for future recoveries");
				if let Err(e) = Self::reauth_fresh(
					&supervisor_clone,
					&url,
					&username,
					&password,
					&namespace,
					&database,
				)
				.await
				{
					tracing::warn!(
						error = %e,
						"ResilientDb: failed to refresh supervisor session"
					);
				}

				tracing::error!("ResilientDb: primary still unhealthy after recovery attempt");
			}
		})
	}

	/// Re-create the sentinel record after recovery.
	async fn ensure_probe(db: &Surreal<Any>) {
		let _ = db.query("UPSERT _rdb_probe:health SET ok = true").await;
	}

	async fn probe_healthy(db: &Surreal<Any>) -> bool {
		db.select::<Option<serde_json::Value>>(("_rdb_probe", "health"))
			.await
			.ok()
			.flatten()
			.is_some()
	}

	async fn open_connection(config: &ResilientDbConfig) -> anyhow::Result<Surreal<Any>> {
		let conn = surrealdb::engine::any::connect(&config.url).await?;
		conn.signin(Root {
			username: config.username.clone(),
			password: config.password.clone(),
		})
		.await?;
		conn.use_ns(&config.namespace)
			.use_db(&config.database)
			.await?;
		Ok(conn)
	}

	/// Re-authenticate using a **fresh temporary connection**.
	///
	/// This avoids the HTTP JWT expiry deadlock where `invalidate()` and
	/// `signin()` both fail because the expired Bearer token is sent with
	/// the request, and the server middleware rejects it before reaching
	/// the endpoint handler.
	///
	/// Creates a brand new connection (no stale Bearer), signs in to get
	/// a fresh JWT, applies it to the target connection, then drops the
	/// temporary connection.
	async fn reauth_fresh(
		db: &Surreal<Any>,
		url: &str,
		username: &str,
		password: &str,
		namespace: &str,
		database: &str,
	) -> anyhow::Result<()> {
		let fresh = surrealdb::engine::any::connect(url).await?;
		let jwt = fresh
			.signin(Root {
				username: username.to_string(),
				password: password.to_string(),
			})
			.await?;
		db.authenticate(jwt).await?;
		db.use_ns(namespace).use_db(database).await?;
		Ok(())
	}
}
