use futures::FutureExt;
use http::header;

use crate::api::mode::ModeAPI;
use crate::client::http_util::{exec_calls, parse_response, VND_SCHEMA_REGISTRY_V1_JSON};
use crate::client::SchemaRegistryClient;
use crate::error::SchemaRegistryError;
use crate::types::{Mode, ResourceMode};

#[async_trait::async_trait]
impl ModeAPI for SchemaRegistryClient {
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
}
