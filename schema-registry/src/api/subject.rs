use crate::error::SchemaRegistryError;
use crate::types::{StringSchema, Subject, SubjectVersion, UnregisteredSchema, Version};

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait SubjectAPI {
    /// Get the subject-version pairs for the provided schema id
    async fn get_schema_subject_versions(
        &self,
        id: u32,
    ) -> Result<Vec<SubjectVersion>, SchemaRegistryError>;

    /// Get all subjects currently registered in the schema registry
    async fn get_subjects(&self, deleted: bool) -> Result<Vec<String>, SchemaRegistryError>;

    /// Get the latest version of the schema for the provided subject
    async fn get_subject_versions(&self, subject: &str) -> Result<Vec<u32>, SchemaRegistryError>;

    /// Delete a subject, it's versions and associated compatibility level if it exists
    async fn delete_subject(
        &self,
        subject: &str,
        permanent: bool,
    ) -> Result<Vec<u32>, SchemaRegistryError>;

    /// Get a specific version of the subject
    async fn get_subject_version(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<Subject, SchemaRegistryError>;

    /// Get the raw schema for a specific version of the subject
    async fn get_subject_version_raw(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<StringSchema, SchemaRegistryError>;

    /// Post a new schema to the schema registry
    async fn post_new_subject_version(
        &self,
        subject: &str,
        schema: &UnregisteredSchema,
        normalize: bool,
    ) -> Result<u32, SchemaRegistryError>;

    /// Lookup if a schema is registered under a subject
    async fn lookup_subject_schema(
        &self,
        subject: &str,
        schema: &UnregisteredSchema,
        normalize: bool,
    ) -> Result<Subject, SchemaRegistryError>;

    /// Delete a specific version of the subject
    async fn delete_subject_version(
        &self,
        subject: &str,
        version: Version,
        permanent: bool,
    ) -> Result<u32, SchemaRegistryError>;

    /// Get IDs of schemas that reference the provided subject version
    async fn get_subject_version_references(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<Vec<u32>, SchemaRegistryError>;
}
