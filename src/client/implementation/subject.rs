use futures::FutureExt;
use http::header;

use crate::api::subject::SubjectAPI;
use crate::client::http_util::{exec_calls, parse_response, VND_SCHEMA_REGISTRY_V1_JSON};
use crate::client::SchemaRegistryClient;
use crate::error::SchemaRegistryError;
use crate::types::{Id, StringSchema, Subject, SubjectVersion, UnregisteredSchema, Version};

#[async_trait::async_trait]
impl SubjectAPI for SchemaRegistryClient {
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
