use std::collections::HashMap;

use futures::FutureExt;
use http::header;

use crate::api::exporter::ExporterAPI;
use crate::client::http_util::{exec_calls, parse_response, VND_SCHEMA_REGISTRY_V1_JSON};
use crate::client::SchemaRegistryClient;
use crate::error::SchemaRegistryError;
use crate::types::{ExporterConfig, ExporterStatus};

#[async_trait::async_trait]
impl ExporterAPI for SchemaRegistryClient {
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

    async fn create_exporter(&self, config: &ExporterConfig) -> Result<String, SchemaRegistryError> {
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

    async fn update_exporter(&self, name: &str, config: &ExporterConfig) -> Result<String, SchemaRegistryError> {
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

    async fn update_exporter_config(&self, name: &str, config: &HashMap<String, String>) -> Result<String, SchemaRegistryError> {
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

    async fn get_exporter_config(&self, name: &str) -> Result<HashMap<String, String>, SchemaRegistryError> {
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
}