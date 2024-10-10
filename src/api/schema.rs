use crate::{Schema, SchemaRegistryError, SchemaType, StringSchema};
use async_trait::async_trait;

#[async_trait]
pub trait SchemaAPI {
    /// Get the schema identified by the provided id
    async fn get_schema_by_id(&self, id: u32) -> Result<Schema, SchemaRegistryError>;

    /// Get the raw schema identified by the provided id
    async fn get_schema_by_id_raw(&self, id: u32) -> Result<StringSchema, SchemaRegistryError>;

    /// Get all schema types currently registered in the schema registry
    async fn get_schemas_types(&self) -> Result<Vec<SchemaType>, SchemaRegistryError>;
}
