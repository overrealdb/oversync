use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

pub fn env_var(name: &str, default: &str) -> String {
	std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

pub fn unique_name(prefix: &str) -> String {
	let uid = uuid::Uuid::new_v4().to_string().replace('-', "");
	format!("{prefix}_{}", &uid[..8])
}

pub async fn retry_async<T, E, Fut, F>(
	label: &str,
	attempts: usize,
	delay: Duration,
	mut operation: F,
) -> T
where
	E: Display,
	F: FnMut() -> Fut,
	Fut: Future<Output = Result<T, E>>,
{
	assert!(attempts > 0, "retry attempts must be greater than zero");

	for attempt in 1..=attempts {
		match operation().await {
			Ok(value) => return value,
			Err(err) if attempt < attempts => {
				eprintln!("waiting for {label} ({attempt}/{attempts}): {err}");
				tokio::time::sleep(delay).await;
			}
			Err(err) => {
				panic!("failed waiting for {label} after {attempts} attempts: {err}");
			}
		}
	}

	unreachable!("retry loop should always return or panic")
}
