use futures::FutureExt;
use http::header;

use crate::api::configuration::ConfigurationAPI;
use crate::client::http_util::{exec_calls, parse_response, VND_SCHEMA_REGISTRY_V1_JSON};
use crate::client::SchemaRegistryClient;
use crate::error::SchemaRegistryError;
use crate::types::{ClusterConfig, SubjectConfig};

#[async_trait::async_trait]
impl ConfigurationAPI for SchemaRegistryClient {
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
}
