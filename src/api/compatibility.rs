use crate::error::SchemaRegistryError;
use crate::{UnregisteredSchema, Version};

#[async_trait::async_trait]
pub trait CompatibilityAPI: Send + Sync {
    /// Checks if a schema is compatible with the provided subject version
    async fn is_compatible(
        &self,
        subject: &str,
        version: Version,
        schema: &UnregisteredSchema,
    ) -> Result<bool, SchemaRegistryError>;

    /// Checks if a schema is compatible with all versions of the provided subject
    async fn is_full_compatible(
        &self,
        subject: &str,
        schema: &UnregisteredSchema,
    ) -> Result<bool, SchemaRegistryError>;
}
