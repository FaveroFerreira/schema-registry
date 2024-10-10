use std::sync::Arc;

use async_trait::async_trait;
use futures::FutureExt;
use reqwest::header;
use reqwest::Client;

use crate::types::{RegisteredSchema, Subject};
use crate::{
    Compatibility, CompatibilityLevel, IsCompatible, Schema, SchemaRegistryAPI,
    SchemaRegistryConfig, SchemaRegistryError, UnregisteredSchema, Version,
};

pub mod config;
mod http;

const APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON: &str = "application/vnd.schemaregistry.v1+json";

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
        let http = http::build_http_client(&SchemaRegistryConfig::new().url(url))?;

        Ok(Self { http, urls })
    }

    /// Create a new `SchemaRegistryClient` from a `SchemaRegistryConfig`.
    ///
    /// # Errors
    ///
    /// Returns an error if the `SchemaRegistryConfig` is invalid or if the HTTP client cannot be created.
    pub fn from_conf(conf: SchemaRegistryConfig) -> Result<Self, SchemaRegistryError> {
        let urls = Arc::from(conf.urls.clone());
        let http = http::build_http_client(&conf)?;

        Ok(Self { http, urls })
    }
}

#[async_trait]
impl SchemaRegistryAPI for SchemaRegistryClient {
    async fn fetch_subjects(&self) -> Result<Vec<String>, SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects", url);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                http::parse_response::<Vec<String>>(response).await
            }
            .boxed();

            calls.push(call);
        }

        let subjects = http::exec_http_calls(calls).await?;

        Ok(subjects)
    }

    async fn fetch_schema_by_id(&self, id: u32) -> Result<Schema, SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/schemas/ids/{}", url, id);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                http::parse_response::<Schema>(response).await
            }
            .boxed();

            calls.push(call);
        }

        let schema = http::exec_http_calls(calls).await?;

        Ok(schema)
    }

    async fn lookup_subject_by_schema(
        &self,
        subject: &str,
        schema: &UnregisteredSchema,
    ) -> Result<Subject, SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}", url, subject);

            let call = async move {
                let response = http
                    .post(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(&schema)
                    .send()
                    .await?;

                http::parse_response::<Schema>(response).await
            }
            .boxed();

            calls.push(call);
        }

        let schema = http::exec_http_calls(calls).await?;

        Ok(schema)
    }

    async fn delete_subject_schemas(&self, subject: &str) -> Result<Vec<u32>, SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}", url, subject);

            let call = async move {
                let response = http
                    .delete(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                http::parse_response::<Vec<u32>>(response).await
            }
            .boxed();

            calls.push(call);
        }

        let versions = http::exec_http_calls(calls).await?;

        Ok(versions)
    }

    async fn register_subject_schema(
        &self,
        subject: &str,
        schema: &UnregisteredSchema,
    ) -> Result<RegisteredSchema, SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}/versions", url, subject);

            let call = async move {
                let response = http
                    .post(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(&schema)
                    .send()
                    .await?;

                http::parse_response::<RegisteredSchema>(response).await
            }
            .boxed();

            calls.push(call);
        }

        let registered_schema = http::exec_http_calls(calls).await?;

        Ok(registered_schema)
    }

    async fn fetch_subject_versions(&self, subject: &str) -> Result<Vec<u32>, SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}/versions", url, subject);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                http::parse_response::<Vec<u32>>(response).await
            }
            .boxed();

            calls.push(call);
        }

        let versions = http::exec_http_calls(calls).await?;

        Ok(versions)
    }

    async fn fetch_schema_by_subject_version(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<Subject, SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}/versions/{}", url, subject, version);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                http::parse_response::<Subject>(response).await
            }
            .boxed();

            calls.push(call);
        }

        let schema = http::exec_http_calls(calls).await?;

        Ok(schema)
    }

    async fn delete_subject_version(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<u32, SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}/versions/{}", url, subject, version);

            let call = async move {
                let response = http
                    .delete(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                http::parse_response::<u32>(response).await
            }
            .boxed();

            calls.push(call);
        }

        let version = http::exec_http_calls(calls).await?;

        Ok(version)
    }

    async fn is_compatible(
        &self,
        subject: &str,
        version: Version,
        schema: &UnregisteredSchema,
    ) -> Result<IsCompatible, SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!(
                "{}/compatibility/subjects/{}/versions/{}",
                url, subject, version
            );

            let call = async move {
                let response = http
                    .post(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(&schema)
                    .send()
                    .await?;

                http::parse_response::<IsCompatible>(response).await
            }
            .boxed();

            calls.push(call);
        }

        let compatibility = http::exec_http_calls(calls).await?;

        Ok(compatibility)
    }

    async fn fetch_config(&self) -> Result<CompatibilityLevel, SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/config", url);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                http::parse_response::<Compatibility>(response).await
            }
            .boxed();

            calls.push(call);
        }

        let compatibility = http::exec_http_calls(calls).await?;

        Ok(compatibility.compatibility)
    }

    async fn update_config(
        &self,
        compatibility: CompatibilityLevel,
    ) -> Result<(), SchemaRegistryError> {
        let compatibility = Compatibility { compatibility };

        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/config", url);

            let call = async move {
                let response = http
                    .put(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(&compatibility)
                    .send()
                    .await?;

                http::parse_response::<()>(response).await
            }
            .boxed();

            calls.push(call);
        }

        http::exec_http_calls(calls).await?;

        Ok(())
    }

    async fn fetch_subject_config(
        &self,
        subject: &str,
    ) -> Result<CompatibilityLevel, SchemaRegistryError> {
        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/config/{}", url, subject);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                http::parse_response::<Compatibility>(response).await
            }
            .boxed();

            calls.push(call);
        }

        let compatibility = http::exec_http_calls(calls).await?;

        Ok(compatibility.compatibility)
    }

    async fn update_subject_config(
        &self,
        subject: &str,
        compatibility: CompatibilityLevel,
    ) -> Result<(), SchemaRegistryError> {
        let compatibility = Compatibility { compatibility };

        let mut calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/config/{}", url, subject);

            let call = async move {
                let response = http
                    .put(&url)
                    .header(header::ACCEPT, APPLICATION_VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(&compatibility)
                    .send()
                    .await?;

                http::parse_response::<()>(response).await
            }
            .boxed();

            calls.push(call);
        }

        http::exec_http_calls(calls).await?;

        Ok(())
    }
}
