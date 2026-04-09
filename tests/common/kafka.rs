use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use tokio::sync::OnceCell;

use super::stack::{env_var, retry_async};

struct SharedKafkaEndpoint {
	broker: String,
}

static SHARED_KAFKA: OnceCell<SharedKafkaEndpoint> = OnceCell::const_new();

async fn shared_kafka() -> &'static SharedKafkaEndpoint {
	SHARED_KAFKA
		.get_or_init(|| async {
			let broker = env_var("OVERSYNC_TEST_KAFKA_BROKER", "127.0.0.1:59092");

			retry_async("kafka", 30, Duration::from_secs(1), || {
				let broker = broker.clone();
				async move {
					let consumer: BaseConsumer = ClientConfig::new()
						.set("bootstrap.servers", &broker)
						.set("group.id", "oversync-test-health")
						.create()
						.map_err(|e| e.to_string())?;

					consumer
						.fetch_metadata(None, Duration::from_secs(5))
						.map_err(|e| e.to_string())?;

					Ok::<_, String>(())
				}
			})
			.await;

			SharedKafkaEndpoint { broker }
		})
		.await
}

pub struct TestKafka {
	pub broker: String,
}

impl TestKafka {
	pub async fn new() -> Self {
		let shared = shared_kafka().await;
		Self {
			broker: shared.broker.clone(),
		}
	}
}
