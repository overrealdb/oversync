use std::time::{Duration, Instant};

/// Token bucket rate limiter.
///
/// Refills one token per `interval` duration, up to `capacity`.
/// `acquire()` blocks until a token is available.
pub struct RateLimiter {
	capacity: u32,
	tokens: f64,
	interval: Duration,
	last_refill: Instant,
}

impl RateLimiter {
	/// Create a rate limiter allowing `max_per_minute` requests per minute.
	pub fn per_minute(max_per_minute: u32) -> Self {
		let interval = Duration::from_secs_f64(60.0 / max_per_minute as f64);
		Self {
			capacity: max_per_minute,
			tokens: max_per_minute as f64,
			interval,
			last_refill: Instant::now(),
		}
	}

	/// Wait until a token is available, then consume it.
	pub async fn acquire(&mut self) {
		self.refill();
		if self.tokens >= 1.0 {
			self.tokens -= 1.0;
			return;
		}

		let wait = self.interval.mul_f64(1.0 - self.tokens);
		tokio::time::sleep(wait).await;
		self.refill();
		self.tokens = (self.tokens - 1.0).max(0.0);
	}

	fn refill(&mut self) {
		let now = Instant::now();
		let elapsed = now.duration_since(self.last_refill);
		let new_tokens = elapsed.as_secs_f64() / self.interval.as_secs_f64();
		self.tokens = (self.tokens + new_tokens).min(self.capacity as f64);
		self.last_refill = now;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn limiter_allows_burst_up_to_capacity() {
		let mut limiter = RateLimiter::per_minute(60);
		let start = Instant::now();
		for _ in 0..10 {
			limiter.acquire().await;
		}
		// Should complete almost instantly (tokens available)
		assert!(start.elapsed() < Duration::from_millis(100));
	}

	#[tokio::test]
	async fn limiter_throttles_when_exhausted() {
		let mut limiter = RateLimiter::per_minute(600); // 10/sec
		// Drain all tokens
		for _ in 0..600 {
			limiter.tokens -= 1.0;
		}
		limiter.tokens = 0.0;
		let start = Instant::now();
		limiter.acquire().await;
		// Should wait ~100ms for 1 token at 10/sec
		assert!(start.elapsed() >= Duration::from_millis(50));
	}

	#[test]
	fn limiter_refills_over_time() {
		let mut limiter = RateLimiter::per_minute(60);
		limiter.tokens = 0.0;
		limiter.last_refill = Instant::now() - Duration::from_secs(30);
		limiter.refill();
		assert!(limiter.tokens >= 29.0);
		assert!(limiter.tokens <= 31.0);
	}

	#[test]
	fn limiter_caps_at_capacity() {
		let mut limiter = RateLimiter::per_minute(10);
		limiter.tokens = 0.0;
		limiter.last_refill = Instant::now() - Duration::from_secs(600);
		limiter.refill();
		assert_eq!(limiter.tokens, 10.0);
	}
}
