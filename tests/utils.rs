use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_redpanda_rs::Redpanda;

use schema_registry::SchemaRegistryClient;

pub struct TestEnvironment {
    kafka_schema_registry: TestKafkaSchemaRegistry,
}

impl TestEnvironment {
    pub async fn start() -> Self {
        let kafka_schema_registry = TestKafkaSchemaRegistry::init().await;

        Self {
            kafka_schema_registry,
        }
    }

    async fn schema_registry_url(&self) -> String {
        let schema_registry_host = self
            .kafka_schema_registry
            .0
            .get_host()
            .await
            .unwrap()
            .to_string();
        let schema_registry_port = self
            .kafka_schema_registry
            .0
            .get_host_port_ipv4(8081)
            .await
            .unwrap();

        format!("http://{}:{}", schema_registry_host, schema_registry_port)
    }

    pub async fn create_schema_registry_client(&self) -> SchemaRegistryClient {
        let url = self.schema_registry_url().await;

        SchemaRegistryClient::from_url(&url).expect("to create client")
    }
}

pub struct TestKafkaSchemaRegistry(ContainerAsync<Redpanda>);

impl TestKafkaSchemaRegistry {
    pub async fn init() -> Self {
        let kafka_schema_registry = Redpanda::default().start().await.unwrap();

        Self(kafka_schema_registry)
    }
}

pub fn load_schema(path: &str) -> String {
    std::fs::read_to_string(path).expect("to read schema")
}
