use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize)]
pub struct PipeStatus {
	pub state: PipeState,
	pub last_cycle_at: Option<DateTime<Utc>>,
	pub last_error: Option<String>,
	pub total_cycles: u64,
	pub total_errors: u64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PipeState {
	Running,
	Idle,
	Errored,
	Disabled,
}

impl std::fmt::Display for PipeState {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Running => write!(f, "running"),
			Self::Idle => write!(f, "idle"),
			Self::Errored => write!(f, "errored"),
			Self::Disabled => write!(f, "disabled"),
		}
	}
}

#[derive(Clone, Default)]
pub struct PipeStatusTracker {
	inner: Arc<RwLock<HashMap<String, PipeStatus>>>,
}

impl PipeStatusTracker {
	pub fn new() -> Self {
		Self::default()
	}

	pub async fn set_running(&self, pipe: &str) {
		let mut map = self.inner.write().await;
		let entry = map.entry(pipe.to_string()).or_insert_with(|| PipeStatus {
			state: PipeState::Idle,
			last_cycle_at: None,
			last_error: None,
			total_cycles: 0,
			total_errors: 0,
		});
		entry.state = PipeState::Running;
	}

	pub async fn set_success(&self, pipe: &str) {
		let mut map = self.inner.write().await;
		let entry = map.entry(pipe.to_string()).or_insert_with(|| PipeStatus {
			state: PipeState::Idle,
			last_cycle_at: None,
			last_error: None,
			total_cycles: 0,
			total_errors: 0,
		});
		entry.state = PipeState::Idle;
		entry.last_cycle_at = Some(Utc::now());
		entry.total_cycles += 1;
		entry.last_error = None;
	}

	pub async fn set_errored(&self, pipe: &str, error: &str) {
		let mut map = self.inner.write().await;
		let entry = map.entry(pipe.to_string()).or_insert_with(|| PipeStatus {
			state: PipeState::Errored,
			last_cycle_at: None,
			last_error: None,
			total_cycles: 0,
			total_errors: 0,
		});
		entry.state = PipeState::Errored;
		entry.last_error = Some(error.to_string());
		entry.total_errors += 1;
	}

	pub async fn set_disabled(&self, pipe: &str) {
		let mut map = self.inner.write().await;
		let entry = map.entry(pipe.to_string()).or_insert_with(|| PipeStatus {
			state: PipeState::Disabled,
			last_cycle_at: None,
			last_error: None,
			total_cycles: 0,
			total_errors: 0,
		});
		entry.state = PipeState::Disabled;
	}

	pub async fn get(&self, pipe: &str) -> Option<PipeStatus> {
		self.inner.read().await.get(pipe).cloned()
	}

	pub async fn all(&self) -> HashMap<String, PipeStatus> {
		self.inner.read().await.clone()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn tracker_lifecycle() {
		let tracker = PipeStatusTracker::new();
		tracker.set_running("pipe-a").await;

		let status = tracker.get("pipe-a").await.unwrap();
		assert_eq!(status.state, PipeState::Running);
		assert_eq!(status.total_cycles, 0);

		tracker.set_success("pipe-a").await;
		let status = tracker.get("pipe-a").await.unwrap();
		assert_eq!(status.state, PipeState::Idle);
		assert_eq!(status.total_cycles, 1);
		assert!(status.last_cycle_at.is_some());
		assert!(status.last_error.is_none());
	}

	#[tokio::test]
	async fn tracker_error() {
		let tracker = PipeStatusTracker::new();
		tracker.set_running("pipe-a").await;
		tracker.set_errored("pipe-a", "connection refused").await;

		let status = tracker.get("pipe-a").await.unwrap();
		assert_eq!(status.state, PipeState::Errored);
		assert_eq!(status.last_error.as_deref(), Some("connection refused"));
		assert_eq!(status.total_errors, 1);
	}

	#[tokio::test]
	async fn tracker_disabled() {
		let tracker = PipeStatusTracker::new();
		tracker.set_disabled("pipe-b").await;

		let status = tracker.get("pipe-b").await.unwrap();
		assert_eq!(status.state, PipeState::Disabled);
	}

	#[tokio::test]
	async fn tracker_unknown_pipe_is_none() {
		let tracker = PipeStatusTracker::new();
		assert!(tracker.get("nonexistent").await.is_none());
	}

	#[tokio::test]
	async fn tracker_all() {
		let tracker = PipeStatusTracker::new();
		tracker.set_running("a").await;
		tracker.set_running("b").await;
		let all = tracker.all().await;
		assert_eq!(all.len(), 2);
	}
}
