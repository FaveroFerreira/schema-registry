use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use http::header;

use crate::api::SchemaRegistryAPI;
use crate::client::config::SchemaRegistryConfig;
use crate::client::http_util::{exec_calls, parse_response, VND_SCHEMA_REGISTRY_V1_JSON};
use crate::error::SchemaRegistryError;
use crate::types::{
    ClusterConfig, CompatibilityCheck, ExporterConfig, ExporterStatus, Id, Mode, ResourceMode,
    Schema, SchemaType, StringSchema, Subject, SubjectConfig, SubjectVersion, UnregisteredSchema,
    Version,
};

pub mod config;
mod http_util;

/// A simple client for interacting with a Confluent Schema Registry.
///
/// This client is a thin wrapper around the `reqwest` HTTP client.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    urls: Arc<[String]>,
    http: reqwest::Client,
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

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
impl SchemaRegistryAPI for SchemaRegistryClient {
    async fn is_compatible(
        &self,
        subject: &str,
        version: Version,
        schema: &UnregisteredSchema,
    ) -> Result<bool, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!(
                "{}/compatibility/subjects/{}/versions/{}",
                url, subject, version
            );

            let call = async move {
                let response = http
                    .post(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(schema)
                    .send()
                    .await?;

                parse_response::<CompatibilityCheck>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result.is_compatible)
    }

    async fn is_fully_compatible(
        &self,
        subject: &str,
        schema: &UnregisteredSchema,
    ) -> Result<bool, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/compatibility/subjects/{}/versions", url, subject);

            let call = async move {
                let response = http
                    .post(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(schema)
                    .send()
                    .await?;

                parse_response::<CompatibilityCheck>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result.is_compatible)
    }

    async fn get_configuration(&self) -> Result<ClusterConfig, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/config", url);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<ClusterConfig>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn update_configuration(
        &self,
        configuration: &ClusterConfig,
    ) -> Result<ClusterConfig, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/config", url);

            let call = async move {
                let response = http
                    .put(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(configuration)
                    .send()
                    .await?;

                parse_response::<ClusterConfig>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_subject_configuration(
        &self,
        subject: &str,
    ) -> Result<SubjectConfig, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/config/{}", url, subject);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<SubjectConfig>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn update_subject_configuration(
        &self,
        subject: &str,
        configuration: &SubjectConfig,
    ) -> Result<SubjectConfig, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/config/{}", url, subject);

            let call = async move {
                let response = http
                    .put(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(configuration)
                    .send()
                    .await?;

                parse_response::<SubjectConfig>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_exporters(&self) -> Result<Vec<String>, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/exporters", url);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<Vec<String>>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_contexts(&self) -> Result<Vec<String>, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/contexts", url);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<Vec<String>>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn create_exporter(
        &self,
        config: &ExporterConfig,
    ) -> Result<String, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/exporters", url);

            let call = async move {
                let response = http
                    .post(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(config)
                    .send()
                    .await?;

                parse_response::<String>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn update_exporter(
        &self,
        name: &str,
        config: &ExporterConfig,
    ) -> Result<String, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/exporters/{}", url, name);

            let call = async move {
                let response = http
                    .put(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(config)
                    .send()
                    .await?;

                parse_response::<String>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn update_exporter_config(
        &self,
        name: &str,
        config: &HashMap<String, String>,
    ) -> Result<String, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/exporters/{}/config", url, name);

            let call = async move {
                let response = http
                    .put(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(config)
                    .send()
                    .await?;

                parse_response::<String>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_exporter(&self, name: &str) -> Result<ExporterConfig, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/exporters/{}", url, name);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<ExporterConfig>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_exporter_config(
        &self,
        name: &str,
    ) -> Result<HashMap<String, String>, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/exporters/{}/config", url, name);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<HashMap<String, String>>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_exporter_status(&self, name: &str) -> Result<ExporterStatus, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/exporters/{}/status", url, name);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<ExporterStatus>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn pause_exporter(&self, name: &str) -> Result<(), SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/exporters/{}/pause", url, name);

            let call = async move {
                let response = http
                    .put(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<()>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        exec_calls(http_calls).await?;

        Ok(())
    }

    async fn reset_exporter(&self, name: &str) -> Result<(), SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/exporters/{}/reset", url, name);

            let call = async move {
                let response = http
                    .put(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<()>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        exec_calls(http_calls).await?;

        Ok(())
    }

    async fn resume_exporter(&self, name: &str) -> Result<(), SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/exporters/{}/resume", url, name);

            let call = async move {
                let response = http
                    .put(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<()>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        exec_calls(http_calls).await?;

        Ok(())
    }

    async fn delete_exporter(&self, name: &str) -> Result<(), SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/exporters/{}", url, name);

            let call = async move {
                let response = http
                    .delete(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<()>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        exec_calls(http_calls).await?;

        Ok(())
    }

    async fn get_global_resource_mode(&self) -> Result<Mode, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/mode", url);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<ResourceMode>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result.mode)
    }

    async fn update_global_resource_mode(
        &self,
        mode: Mode,
        force: bool,
    ) -> Result<Mode, SchemaRegistryError> {
        let body = ResourceMode { mode };

        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/mode?force={}", url, force);

            let call = async move {
                let response = http
                    .put(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(&body)
                    .send()
                    .await?;

                parse_response::<ResourceMode>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result.mode)
    }

    async fn get_subject_resource_mode(&self, subject: &str) -> Result<Mode, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/mode/{}", url, subject);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<ResourceMode>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result.mode)
    }

    async fn update_subject_resource_mode(
        &self,
        subject: &str,
        mode: Mode,
        force: bool,
    ) -> Result<Mode, SchemaRegistryError> {
        let body = ResourceMode { mode };

        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/mode/{}?force={}", url, subject, force);

            let call = async move {
                let response = http
                    .put(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(&body)
                    .send()
                    .await?;

                parse_response::<ResourceMode>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result.mode)
    }

    async fn delete_subject_mode(&self, subject: &str) -> Result<Mode, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/mode/{}", url, subject);

            let call = async move {
                let response = http
                    .delete(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<ResourceMode>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result.mode)
    }

    async fn get_schema_by_id(&self, id: u32) -> Result<Schema, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/schemas/ids/{}", url, id);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<Schema>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_schema_by_id_raw(&self, id: u32) -> Result<StringSchema, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/schemas/ids/{}/schema", url, id);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<StringSchema>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_schemas_types(&self) -> Result<Vec<SchemaType>, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/schemas/types", url);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<Vec<SchemaType>>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_schema_subject_versions(
        &self,
        id: u32,
    ) -> Result<Vec<SubjectVersion>, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/schemas/ids/{}/versions", url, id);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<Vec<SubjectVersion>>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_subjects(&self, deleted: bool) -> Result<Vec<String>, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects?deleted={}", url, deleted);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<Vec<String>>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_subject_versions(&self, subject: &str) -> Result<Vec<u32>, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}/versions", url, subject);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<Vec<u32>>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn delete_subject(
        &self,
        subject: &str,
        permanent: bool,
    ) -> Result<Vec<u32>, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}?permanent={}", url, subject, permanent);

            let call = async move {
                let response = http
                    .delete(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<Vec<u32>>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_subject_version(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<Subject, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}/versions/{}", url, subject, version);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<Subject>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_subject_version_raw(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<StringSchema, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}/versions/{}/schema", url, subject, version);

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<StringSchema>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn post_new_subject_version(
        &self,
        subject: &str,
        schema: &UnregisteredSchema,
        normalize: bool,
    ) -> Result<u32, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}/versions?={}", url, subject, normalize);

            let call = async move {
                let response = http
                    .post(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(schema)
                    .send()
                    .await?;

                parse_response::<Id>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result.id)
    }

    async fn lookup_subject_schema(
        &self,
        subject: &str,
        schema: &UnregisteredSchema,
        normalize: bool,
    ) -> Result<Subject, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!("{}/subjects/{}?normalize={}", url, subject, normalize);

            let call = async move {
                let response = http
                    .post(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .json(schema)
                    .send()
                    .await?;

                parse_response::<Subject>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn delete_subject_version(
        &self,
        subject: &str,
        version: Version,
        permanent: bool,
    ) -> Result<u32, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!(
                "{}/subjects/{}/versions/{}?permanent={}",
                url, subject, version, permanent
            );

            let call = async move {
                let response = http
                    .delete(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<u32>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }

    async fn get_subject_version_references(
        &self,
        subject: &str,
        version: Version,
    ) -> Result<Vec<u32>, SchemaRegistryError> {
        let mut http_calls = Vec::with_capacity(self.urls.len());

        for url in self.urls.iter() {
            let http = self.http.clone();
            let url = format!(
                "{}/subjects/{}/versions/{}/referencedBy",
                url, subject, version
            );

            let call = async move {
                let response = http
                    .get(&url)
                    .header(header::ACCEPT, VND_SCHEMA_REGISTRY_V1_JSON)
                    .send()
                    .await?;

                parse_response::<Vec<u32>>(response).await
            }
            .boxed();

            http_calls.push(call);
        }

        let result = exec_calls(http_calls).await?;

        Ok(result)
    }
}
