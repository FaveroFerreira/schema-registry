//! This module contains the API traits for interacting with the schema registry
//!
//! All traits represented here are according to the schema registry api reference
//!
//! https://docs.confluent.io/platform/current/schema-registry/develop/api.html

pub use crate::api::compatibility::CompatibilityAPI;
pub use crate::api::configuration::ConfigurationAPI;
pub use crate::api::exporter::ExporterAPI;
pub use crate::api::mode::ModeAPI;
pub use crate::api::schema::SchemaAPI;
pub use crate::api::subject::SubjectAPI;

pub mod compatibility;
pub mod configuration;
pub mod exporter;
pub mod mode;
pub mod schema;
pub mod subject;

#[async_trait::async_trait]
pub trait SchemaRegistryAPI:
    SchemaAPI + SubjectAPI + CompatibilityAPI + ConfigurationAPI + ModeAPI + ExporterAPI + Send + Sync
{
}
