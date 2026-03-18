use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use tokio::sync::OnceCell;

struct SharedKafkaContainer {
	broker: String,
	_container: ContainerAsync<Kafka>,
}

static SHARED_KAFKA: OnceCell<SharedKafkaContainer> = OnceCell::const_new();

async fn shared_kafka() -> &'static SharedKafkaContainer {
	SHARED_KAFKA
		.get_or_init(|| async {
			let container = Kafka::default()
				.start()
				.await
				.expect("failed to start Kafka container");

			let host = container.get_host().await.expect("kafka host");
			let port = container
				.get_host_port_ipv4(KAFKA_PORT)
				.await
				.expect("kafka port");

			SharedKafkaContainer {
				broker: format!("{host}:{port}"),
				_container: container,
			}
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
