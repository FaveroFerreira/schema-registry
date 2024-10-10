use futures::FutureExt;
use http::header;

use crate::api::compatibility::CompatibilityAPI;
use crate::client::http_util::{exec_calls, parse_response, VND_SCHEMA_REGISTRY_V1_JSON};
use crate::client::SchemaRegistryClient;
use crate::error::SchemaRegistryError;
use crate::types::{CompatibilityCheck, UnregisteredSchema, Version};

#[async_trait::async_trait]
impl CompatibilityAPI for SchemaRegistryClient {
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
}
