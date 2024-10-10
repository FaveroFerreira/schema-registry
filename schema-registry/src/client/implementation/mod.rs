use crate::api::SchemaRegistryAPI;
use crate::client::SchemaRegistryClient;

pub mod compatibility;
pub mod configuration;
pub mod exporter;
pub mod mode;
pub mod schema;
pub mod subject;

#[async_trait::async_trait]
impl SchemaRegistryAPI for SchemaRegistryClient {}
