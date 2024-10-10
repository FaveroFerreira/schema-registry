use std::collections::HashMap;

use crate::error::SchemaRegistryError;
use crate::types::{ExporterConfig, ExporterStatus};

#[async_trait::async_trait]
pub trait ExporterAPI {
    /// Get the list of exporters currently registered in the schema registry
    async fn get_exporters(&self) -> Result<Vec<String>, SchemaRegistryError>;

    /// Gets a list of contexts. The list will always include the default context,
    /// and any custom contexts that were created in the registry.
    async fn get_contexts(&self) -> Result<Vec<String>, SchemaRegistryError>;

    /// Create a new exporter
    async fn create_exporter(&self, config: &ExporterConfig)
        -> Result<String, SchemaRegistryError>;

    /// Updates the information or configuration of an existing exporter
    async fn update_exporter(
        &self,
        name: &str,
        config: &ExporterConfig,
    ) -> Result<String, SchemaRegistryError>;

    /// Updates only the configuration of an existing exporter
    async fn update_exporter_config(
        &self,
        name: &str,
        config: &HashMap<String, String>,
    ) -> Result<String, SchemaRegistryError>;

    /// Get an existing exporter
    async fn get_exporter(&self, name: &str) -> Result<ExporterConfig, SchemaRegistryError>;

    /// Get the configuration of an existing exporter
    async fn get_exporter_config(
        &self,
        name: &str,
    ) -> Result<HashMap<String, String>, SchemaRegistryError>;

    /// Get the status of an existing exporter
    async fn get_exporter_status(&self, name: &str) -> Result<ExporterStatus, SchemaRegistryError>;

    /// Pause an existing exporter
    async fn pause_exporter(&self, name: &str) -> Result<(), SchemaRegistryError>;

    /// Reset an existing exporter
    async fn reset_exporter(&self, name: &str) -> Result<(), SchemaRegistryError>;

    /// Resume a paused exporter
    async fn resume_exporter(&self, name: &str) -> Result<(), SchemaRegistryError>;

    /// Delete an existing exporter
    async fn delete_exporter(&self, name: &str) -> Result<(), SchemaRegistryError>;
}
