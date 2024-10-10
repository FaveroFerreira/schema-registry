mod api;
mod client;
mod error;
mod types;

mod prelude {
    pub use crate::api::SchemaRegistryAPI;
    pub use crate::client::config::SchemaRegistryConfig;
    pub use crate::client::SchemaRegistryClient;
    pub use crate::error::SchemaRegistryError;
    pub use crate::types::{
        CompatibilityLevel, Reference, Schema, SchemaType, StringSchema, Subject, SubjectVersion,
        UnregisteredSchema, Version,
    };
}

pub use prelude::*;
