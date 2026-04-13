use std::borrow::Cow;

/// Prefer WebSocket transport for SurrealDB client sessions.
///
/// Surreal's `Any` engine accepts both `http(s)://` and `ws(s)://` URLs. For
/// long-lived authenticated sessions and large batch writes, `ws(s)` avoids the
/// HTTP RPC payload ceiling that can surface as `413 Payload Too Large` on
/// `/rpc`. Keep config backward-compatible by upgrading legacy `http(s)` URLs
/// in-process.
pub fn runtime_surreal_url(url: &str) -> Cow<'_, str> {
	if let Some(rest) = url.strip_prefix("http://") {
		Cow::Owned(format!("ws://{rest}"))
	} else if let Some(rest) = url.strip_prefix("https://") {
		Cow::Owned(format!("wss://{rest}"))
	} else {
		Cow::Borrowed(url)
	}
}

#[cfg(test)]
mod tests {
	use super::runtime_surreal_url;

	#[test]
	fn runtime_surreal_url_upgrades_http_to_ws() {
		assert_eq!(
			runtime_surreal_url("http://127.0.0.1:8000").as_ref(),
			"ws://127.0.0.1:8000"
		);
		assert_eq!(
			runtime_surreal_url("https://surreal.example.test").as_ref(),
			"wss://surreal.example.test"
		);
	}

	#[test]
	fn runtime_surreal_url_keeps_non_http_urls() {
		assert_eq!(
			runtime_surreal_url("ws://127.0.0.1:8000").as_ref(),
			"ws://127.0.0.1:8000"
		);
		assert_eq!(runtime_surreal_url("mem://").as_ref(), "mem://");
	}
}
