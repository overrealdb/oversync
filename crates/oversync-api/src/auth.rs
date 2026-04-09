use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use subtle::ConstantTimeEq;

use crate::state::ApiState;

fn api_key_matches(provided: Option<&str>, expected: &str) -> bool {
	provided.is_some_and(|key| bool::from(key.as_bytes().ct_eq(expected.as_bytes())))
}

pub async fn require_api_key(
	State(state): State<Arc<ApiState>>,
	req: Request,
	next: Next,
) -> Result<Response, StatusCode> {
	let Some(ref expected) = state.api_key else {
		return Ok(next.run(req).await);
	};

	let provided = req
		.headers()
		.get("authorization")
		.and_then(|v| v.to_str().ok())
		.and_then(|v| v.strip_prefix("Bearer "))
		.or_else(|| req.headers().get("x-api-key").and_then(|v| v.to_str().ok()));

	if api_key_matches(provided, expected) {
		Ok(next.run(req).await)
	} else {
		Err(StatusCode::UNAUTHORIZED)
	}
}

#[cfg(test)]
mod tests {
	use super::api_key_matches;

	#[test]
	fn matches_expected_key() {
		assert!(api_key_matches(Some("secret-key"), "secret-key"));
	}

	#[test]
	fn rejects_wrong_key() {
		assert!(!api_key_matches(Some("wrong-key"), "secret-key"));
	}

	#[test]
	fn rejects_missing_key() {
		assert!(!api_key_matches(None, "secret-key"));
	}
}
