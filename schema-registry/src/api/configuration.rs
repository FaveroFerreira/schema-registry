use crate::error::SchemaRegistryError;
use crate::types::{ClusterConfig, SubjectConfig};

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait ConfigurationAPI: Send + Sync {
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
}
