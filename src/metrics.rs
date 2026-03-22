use std::time::Instant;

use metrics::{counter, gauge, histogram};

pub fn record_cycle_start(pipe: &str, query: &str) -> Instant {
	counter!("oversync_cycles_total", "pipe" => pipe.to_string(), "query" => query.to_string(), "status" => "started").increment(1);
	gauge!("oversync_pipe_running", "pipe" => pipe.to_string()).set(1.0);
	Instant::now()
}

pub fn record_cycle_success(pipe: &str, query: &str, start: Instant, created: usize, updated: usize, deleted: usize) {
	let duration = start.elapsed().as_secs_f64();
	counter!("oversync_cycles_total", "pipe" => pipe.to_string(), "query" => query.to_string(), "status" => "success").increment(1);
	histogram!("oversync_cycle_duration_seconds", "pipe" => pipe.to_string(), "query" => query.to_string()).record(duration);
	counter!("oversync_events_total", "pipe" => pipe.to_string(), "query" => query.to_string(), "op" => "created").increment(created as u64);
	counter!("oversync_events_total", "pipe" => pipe.to_string(), "query" => query.to_string(), "op" => "updated").increment(updated as u64);
	counter!("oversync_events_total", "pipe" => pipe.to_string(), "query" => query.to_string(), "op" => "deleted").increment(deleted as u64);
}

pub fn record_cycle_failure(pipe: &str, query: &str, start: Instant) {
	let duration = start.elapsed().as_secs_f64();
	counter!("oversync_cycles_total", "pipe" => pipe.to_string(), "query" => query.to_string(), "status" => "failed").increment(1);
	histogram!("oversync_cycle_duration_seconds", "pipe" => pipe.to_string(), "query" => query.to_string()).record(duration);
	counter!("oversync_errors_total", "pipe" => pipe.to_string(), "query" => query.to_string()).increment(1);
}

pub fn record_rows_fetched(pipe: &str, query: &str, count: usize) {
	counter!("oversync_rows_fetched_total", "pipe" => pipe.to_string(), "query" => query.to_string()).increment(count as u64);
}

pub fn record_pipe_stopped(pipe: &str) {
	gauge!("oversync_pipe_running", "pipe" => pipe.to_string()).set(0.0);
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn record_cycle_metrics_no_panic() {
		let start = record_cycle_start("test-pipe", "q1");
		record_rows_fetched("test-pipe", "q1", 100);
		record_cycle_success("test-pipe", "q1", start, 5, 3, 1);
	}

	#[test]
	fn record_failure_no_panic() {
		let start = record_cycle_start("test-pipe", "q1");
		record_cycle_failure("test-pipe", "q1", start);
	}

	#[test]
	fn record_pipe_stopped_no_panic() {
		record_pipe_stopped("test-pipe");
	}
}
