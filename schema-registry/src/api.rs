use crate::error::SchemaRegistryError;
use crate::types::{
    ClusterConfig, ExporterConfig, ExporterStatus, Mode, Schema, SchemaType, StringSchema, Subject,
    SubjectConfig, SubjectVersion, UnregisteredSchema, Version,
};
use std::collections::HashMap;

/// The Schema Registry API trait
///
/// This trait conforms with the [Confluent Schema Registry API documentation](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait SchemaRegistryAPI: Send + Sync {
    /// Checks if a schema is compatible with the provided subject version
    async fn is_compatible(
        &self,
        subject: &str,
        version: Version,
        schema: &UnregisteredSchema,
    ) -> Result<bool, SchemaRegistryError>;

    /// Checks if a schema is compatible with all versions of the provided subject
    async fn is_fully_compatible(
        &self,
        subject: &str,
        schema: &UnregisteredSchema,
    ) -> Result<bool, SchemaRegistryError>;

    /// Get the global configuration for the cluster
    async fn get_configuration(&self) -> Result<ClusterConfig, SchemaRegistryError>;

    /// Update the global configuration for the cluster
    async fn update_configuration(
        &self,
        configuration: &ClusterConfig,
    ) -> Result<ClusterConfig, SchemaRegistryError>;

    /// Get the configuration for a specific subject
    async fn get_subject_configuration(
        &self,
        subject: &str,
    ) -> Result<SubjectConfig, SchemaRegistryError>;

    /// Update the configuration for a specific subject
    async fn update_subject_configuration(
        &self,
        subject: &str,
        configuration: &SubjectConfig,
    ) -> Result<SubjectConfig, SchemaRegistryError>;

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

    /// Get the schema identified by the provided id
    async fn get_schema_by_id(&self, id: u32) -> Result<Schema, SchemaRegistryError>;

    /// Get the raw schema identified by the provided id
    async fn get_schema_by_id_raw(&self, id: u32) -> Result<StringSchema, SchemaRegistryError>;

    /// Get all schema types currently registered in the schema registry
    async fn get_schemas_types(&self) -> Result<Vec<SchemaType>, SchemaRegistryError>;

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
