use serde::Serialize;
#[cfg(feature = "cli")]
use tracing::error;
use tracing::info;

#[derive(Debug, Serialize)]
pub struct AlertPayload {
	pub pipe: String,
	pub query: String,
	pub alert_type: AlertType,
	pub message: String,
	pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertType {
	CycleFailed,
	FailSafeTriggered,
	AnomalyDetected,
}

/// Send an alert to the configured webhook URL.
/// Non-blocking — failures are logged but don't affect the pipeline.
#[cfg(feature = "cli")]
pub fn send_alert(webhook_url: &str, payload: AlertPayload) {
	let url = webhook_url.to_string();
	tokio::spawn(async move {
		let client = reqwest::Client::new();
		match client
			.post(&url)
			.json(&payload)
			.timeout(std::time::Duration::from_secs(10))
			.send()
			.await
		{
			Ok(resp) if resp.status().is_success() => {
				info!(
					pipe = %payload.pipe,
					alert = ?payload.alert_type,
					"alert sent to webhook"
				);
			}
			Ok(resp) => {
				error!(
					pipe = %payload.pipe,
					status = %resp.status(),
					"alert webhook returned non-success"
				);
			}
			Err(e) => {
				error!(
					pipe = %payload.pipe,
					error = %e,
					"alert webhook request failed"
				);
			}
		}
	});
}

/// Stub when CLI feature is disabled — logs but doesn't send.
#[cfg(not(feature = "cli"))]
pub fn send_alert(_webhook_url: &str, payload: AlertPayload) {
	info!(
		pipe = %payload.pipe,
		alert = ?payload.alert_type,
		"alert would be sent (webhook disabled without cli feature)"
	);
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn alert_payload_serializes() {
		let payload = AlertPayload {
			pipe: "test-pipe".into(),
			query: "q1".into(),
			alert_type: AlertType::CycleFailed,
			message: "connection refused".into(),
			timestamp: chrono::Utc::now(),
		};
		let json = serde_json::to_value(&payload).unwrap();
		assert_eq!(json["pipe"], "test-pipe");
		assert_eq!(json["alert_type"], "cycle_failed");
		assert!(json["timestamp"].is_string());
	}

	#[test]
	fn alert_types_serialize_snake_case() {
		let json = serde_json::to_value(AlertType::FailSafeTriggered).unwrap();
		assert_eq!(json, "fail_safe_triggered");
	}
}
