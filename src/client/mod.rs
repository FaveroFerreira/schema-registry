use std::sync::Arc;

use crate::api::SchemaRegistryAPI;
use crate::client::config::SchemaRegistryConfig;
use crate::error::SchemaRegistryError;
use reqwest::Client;

pub mod config;
mod http_util;
pub mod implementation;

/// A simple client for interacting with a Confluent Schema Registry.
///
/// This client is a thin wrapper around the `reqwest` HTTP client.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    urls: Arc<[String]>,
    http: Client,
}

impl SchemaRegistryClient {
    /// Create a new `SchemaRegistryClient` from a URL.
    ///
    /// This is the simplest way to create a new `SchemaRegistryClient`.
    /// However, if you need to customize the client, you should use `from_conf` instead.
    pub fn from_url(url: &str) -> Result<Self, SchemaRegistryError> {
        let urls = Arc::from([url.to_owned()]);
        let http = config::build_http_client(&SchemaRegistryConfig::new().url(url))?;

        Ok(Self { http, urls })
    }

    /// Create a new `SchemaRegistryClient` from a `SchemaRegistryConfig`.
    ///
    /// # Errors
    ///
    /// Returns an error if the `SchemaRegistryConfig` is invalid or if the HTTP client cannot be created.
    pub fn from_conf(conf: SchemaRegistryConfig) -> Result<Self, SchemaRegistryError> {
        let urls = Arc::from(conf.urls.clone());
        let http = config::build_http_client(&conf)?;

        Ok(Self { http, urls })
    }
}

#[async_trait::async_trait]
impl SchemaRegistryAPI for SchemaRegistryClient {}
