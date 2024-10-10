use crate::types::Mode;
use crate::SchemaRegistryError;
use async_trait::async_trait;

#[async_trait]
pub trait ModeAPI {
    /// Get the global resource mode of the schema registry
    async fn get_global_resource_mode(&self) -> Result<Mode, SchemaRegistryError>;

    /// Set the global resource mode of the schema registry
    async fn update_global_resource_mode(
        &self,
        mode: Mode,
        force: bool,
    ) -> Result<Mode, SchemaRegistryError>;

    /// Get subject resource mode
    async fn get_subject_resource_mode(&self, subject: &str) -> Result<Mode, SchemaRegistryError>;

    /// Set subject resource mode
    async fn update_subject_resource_mode(
        &self,
        subject: &str,
        mode: Mode,
        force: bool,
    ) -> Result<Mode, SchemaRegistryError>;

    /// Delete the subject resource mode
    async fn delete_subject_mode(&self, subject: &str) -> Result<Mode, SchemaRegistryError>;
}
