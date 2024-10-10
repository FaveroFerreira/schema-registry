//! This module contains the API traits for interacting with the schema registry
//!
//! All traits represented here are according to the schema registry api reference
//!
//! https://docs.confluent.io/platform/current/schema-registry/develop/api.html

use crate::api::compatibility::CompatibilityAPI;
use crate::api::configuration::ConfigurationAPI;
use crate::api::exporter::ExporterAPI;
use crate::api::mode::ModeAPI;
use crate::api::schema::SchemaAPI;
use crate::api::subject::SubjectAPI;

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
