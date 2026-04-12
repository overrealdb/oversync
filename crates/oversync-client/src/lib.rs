pub mod client;
pub mod types;

#[allow(
	clippy::all,
	missing_docs,
	unused_qualifications,
	dead_code,
	non_camel_case_types,
	non_snake_case,
	non_upper_case_globals
)]
pub mod generated {
	include!(concat!(env!("OUT_DIR"), "/generated.rs"));
}

pub use client::{OversyncClient, OversyncClientBuilder, OversyncClientError};
pub use generated::Client as GeneratedClient;

#[cfg(test)]
mod tests {
	use super::GeneratedClient;

	#[test]
	fn generated_client_builds_from_base_url() {
		let _client = GeneratedClient::new("http://localhost:4200/api");
	}
}
