use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use crate::state::ApiState;

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
		.or_else(|| {
			req.headers()
				.get("x-api-key")
				.and_then(|v| v.to_str().ok())
		});

	match provided {
		Some(key) if key == expected => Ok(next.run(req).await),
		_ => Err(StatusCode::UNAUTHORIZED),
	}
}
