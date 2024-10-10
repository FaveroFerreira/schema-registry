use futures::future::BoxFuture;
use serde::de::DeserializeOwned;

use crate::error::HttpCallError;

pub const VND_SCHEMA_REGISTRY_V1_JSON: &str = "application/vnd.schemaregistry.v1+json";

/// Execute a collection of async calls and return the first successful result.
/// If all calls fail, return the last error.
pub async fn exec_calls<T>(
    calls: Vec<BoxFuture<'_, Result<T, HttpCallError>>>,
) -> Result<T, HttpCallError> {
    let (result, remaining) = futures::future::select_ok(calls.into_iter()).await?;
    remaining.into_iter().for_each(drop);
    Ok(result)
}

/// Parse a response into a JSON value and return the result or an error.
///
/// If the response is successful, tries to parse the JSON value into the desired type.
/// If the response is not successful, tries to parse the JSON value into a `JsonValue` and return an error.
pub async fn parse_response<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, HttpCallError> {
    let status = response.status();
    let host = response.url().to_string();
    let bytes = response.bytes().await?;

    match status.as_u16() {
        200..=299 => match serde_json::from_slice::<T>(&bytes) {
            Ok(parsed) => Ok(parsed),
            Err(source) => {
                let body = String::from_utf8_lossy(&bytes);

                Err(HttpCallError::JsonParse {
                    body: String::from(body),
                    target: std::any::type_name::<T>(),
                    source: Box::new(source),
                })
            }
        },
        _ => Err(HttpCallError::UpstreamError {
            url: host,
            status: status.as_u16(),
            body: String::from_utf8_lossy(&bytes).to_string(),
        }),
    }
}
