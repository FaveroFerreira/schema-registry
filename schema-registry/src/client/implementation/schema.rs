use futures::FutureExt;
use http::header;

use crate::api::schema::SchemaAPI;
use crate::client::http_util::{exec_calls, parse_response, VND_SCHEMA_REGISTRY_V1_JSON};
use crate::client::SchemaRegistryClient;
use crate::error::SchemaRegistryError;
use crate::types::{Schema, SchemaType, StringSchema};

#[async_trait::async_trait]
impl SchemaAPI for SchemaRegistryClient {
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
}
