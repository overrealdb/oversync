#[cfg(feature = "api")]
use std::borrow::Cow;

#[cfg(feature = "api")]
use axum::body::Body;
#[cfg(feature = "api")]
use axum::extract::Request;
#[cfg(feature = "api")]
use axum::http::header::{ACCEPT, CACHE_CONTROL, CONTENT_TYPE};
#[cfg(feature = "api")]
use axum::http::{HeaderMap, StatusCode};
#[cfg(feature = "api")]
use axum::middleware::Next;
#[cfg(feature = "api")]
use axum::response::Response;
#[cfg(feature = "api")]
use rust_embed::RustEmbed;

#[cfg(feature = "api")]
#[derive(RustEmbed)]
#[folder = "ui/dist/"]
struct EmbeddedUi;

#[cfg(feature = "api")]
pub async fn embedded_ui_middleware(req: Request, next: Next) -> Response {
	let path = req.uri().path().to_string();

	if req.method() == axum::http::Method::GET || req.method() == axum::http::Method::HEAD {
		if let Some(response) = serve_static_request(&path) {
			return response;
		}

		if should_serve_spa(&path, req.headers()) {
			return serve_index();
		}
	}

	next.run(req).await
}

#[cfg(feature = "api")]
fn should_serve_spa(path: &str, headers: &HeaderMap) -> bool {
	if path == "/" {
		return true;
	}

	accepts_html(headers) && is_spa_route(path)
}

#[cfg(feature = "api")]
fn is_spa_route(path: &str) -> bool {
	matches!(
		path,
		"/pipes" | "/recipes" | "/sinks" | "/history" | "/settings"
	) || path.starts_with("/pipes/")
}

#[cfg(feature = "api")]
fn accepts_html(headers: &HeaderMap) -> bool {
	headers
		.get(ACCEPT)
		.and_then(|value| value.to_str().ok())
		.map(|value| value.contains("text/html"))
		.unwrap_or(false)
}

#[cfg(feature = "api")]
fn serve_static_request(path: &str) -> Option<Response> {
	let relative_path = path.trim_start_matches('/');
	if relative_path.is_empty() {
		return None;
	}

	if relative_path == "index.html" || relative_path.starts_with("assets/") {
		return Some(serve_asset(relative_path));
	}

	if EmbeddedUi::get(relative_path).is_some() {
		return Some(serve_asset(relative_path));
	}

	None
}

#[cfg(feature = "api")]
fn serve_index() -> Response {
	serve_asset("index.html")
}

#[cfg(feature = "api")]
fn serve_asset(path: &str) -> Response {
	match EmbeddedUi::get(path) {
		Some(file) => {
			let mime = mime_guess::from_path(path).first_or_octet_stream();
			let mut response = Response::builder()
				.status(StatusCode::OK)
				.header(CONTENT_TYPE, mime.as_ref())
				.header(
					CACHE_CONTROL,
					if path == "index.html" {
						"no-cache"
					} else {
						"public, max-age=31536000, immutable"
					},
				)
				.body(Body::from(match file.data {
					Cow::Borrowed(bytes) => bytes.to_vec(),
					Cow::Owned(bytes) => bytes,
				}))
				.unwrap_or_else(|_| Response::new(Body::from("failed to build UI response")));
			*response.status_mut() = StatusCode::OK;
			response
		}
		None => Response::builder()
			.status(StatusCode::NOT_FOUND)
			.body(Body::from("UI asset not found"))
			.unwrap_or_else(|_| Response::new(Body::from("UI asset not found"))),
	}
}
