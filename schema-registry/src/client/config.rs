use std::collections::HashMap;
use std::fmt;
use std::io::Write;
use std::str::FromStr;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::write::EncoderWriter;
use http::{header, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Proxy};
use tracing::warn;

use crate::error::ConfigurationError;

#[derive(Clone, Eq, PartialEq)]
pub enum Authentication {
    Bearer {
        token: String,
    },
    Basic {
        username: String,
        password: Option<String>,
    },
}

impl fmt::Debug for Authentication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Authentication::Bearer { .. } => {
                write!(f, "BearerAuthentication {{ token: ****** }}")
            }
            Authentication::Basic { username, .. } => {
                write!(
                    f,
                    "BasicAuthentication {{ username: {}, password ****** }}",
                    username
                )
            }
        }
    }
}

impl fmt::Display for Authentication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Authentication::Bearer { .. } => write!(f, "Bearer ******"),
            Authentication::Basic { username, .. } => {
                write!(f, "Basic {}:******", username)
            }
        }
    }
}

#[derive(Default, Clone)]
pub struct SchemaRegistryConfig {
    /// Comma separated list of schema registry urls
    pub(crate) urls: Vec<String>,
    /// Optional authentication configuration
    pub(crate) authentication: Option<Authentication>,
    /// Optional proxy configuration
    pub(crate) proxy: Option<String>,
    /// Optional headers to be included in every request
    pub(crate) headers: Option<HashMap<String, String>>,
}

impl SchemaRegistryConfig {
    /// Create a new schema registry client configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a URL to the configuration
    pub fn url<S>(mut self, url: S) -> Self
    where
        S: Into<String>,
    {
        self.urls.push(url.into());
        self
    }

    /// Set the authentication configuration with basic authentication
    pub fn basic_auth<'a, S>(mut self, username: S, password: S) -> Self
    where
        S: Into<Option<&'a String>>,
    {
        if self.authentication.is_some() {
            warn!("Overwriting existing authentication configuration");
        }

        let Some(username) = username.into() else {
            warn!("Basic auth not applied, provided username is none");
            return self;
        };

        let username = username.to_owned();
        let password = password.into().map(|password| password.to_owned());

        self.authentication = Some(Authentication::Basic { username, password });
        self
    }

    /// Set the authentication configuration with bearer authentication
    pub fn bearer_auth<'a, S>(mut self, token: S) -> Self
    where
        S: Into<Option<&'a String>>,
    {
        if self.authentication.is_some() {
            warn!("Overwriting existing authentication configuration");
        }

        let Some(token) = token.into() else {
            warn!("Bearer auth not applied, provided token is none");
            return self;
        };

        let token = token.to_owned();

        self.authentication = Some(Authentication::Bearer { token });
        self
    }

    /// Set the proxy configuration
    pub fn proxy<'a, S>(mut self, proxy: S) -> Self
    where
        S: Into<Option<&'a String>>,
    {
        self.proxy = proxy.into().map(|proxy| proxy.to_owned());
        self
    }

    /// Set the headers to be included in every request
    pub fn headers<S, I>(mut self, headers: I) -> Self
    where
        S: Into<String>,
        I: IntoIterator<Item = (S, S)>,
    {
        self.headers = Some(
            headers
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        self
    }
}

pub fn build_auth_headers(
    auth: &Authentication,
) -> Result<(HeaderName, HeaderValue), ConfigurationError> {
    match auth {
        Authentication::Bearer { token } => bearer_auth(token),
        Authentication::Basic { username, password } => basic_auth(username, password.as_ref()),
    }
}

pub fn bearer_auth(token: &str) -> Result<(HeaderName, HeaderValue), ConfigurationError> {
    let header_name = header::AUTHORIZATION;
    let mut header = HeaderValue::from_str(&format!("Bearer {}", token))?;
    header.set_sensitive(true);
    Ok((header_name, header))
}

pub fn basic_auth<U, P>(
    username: U,
    password: Option<P>,
) -> Result<(HeaderName, HeaderValue), ConfigurationError>
where
    U: fmt::Display,
    P: fmt::Display,
{
    let mut buf = b"Basic ".to_vec();
    {
        let mut encoder = EncoderWriter::new(&mut buf, &BASE64_STANDARD);

        write!(encoder, "{}:", username)?;
        if let Some(password) = password {
            write!(encoder, "{}", password)?;
        }
    }

    let header_name = header::AUTHORIZATION;
    let mut header_value = HeaderValue::from_bytes(&buf)?;
    header_value.set_sensitive(true);
    Ok((header_name, header_value))
}

pub(crate) fn build_headers(headers: &HashMap<String, String>) -> Result<HeaderMap, ConfigurationError> {
    let mut header_map = HeaderMap::new();

    for (name, value) in headers {
        let header_name = HeaderName::from_str(name)?;
        let header_value = HeaderValue::from_str(value)?;
        header_map.insert(header_name, header_value);
    }

    Ok(header_map)
}

pub(crate) fn build_proxy(proxy: &String) -> Result<Proxy, ConfigurationError> {
    let proxy = Proxy::all(proxy)?;
    Ok(proxy)
}

pub(crate) fn build_http_client(conf: &SchemaRegistryConfig) -> Result<Client, ConfigurationError> {
    let mut default_headers = HeaderMap::new();

    if let Some(headers) = &conf.headers {
        default_headers = build_headers(headers)?;
    }

    if let Some(auth) = &conf.authentication {
        let (header_name, header_value) = build_auth_headers(auth)?;
        default_headers.insert(header_name, header_value);
    }

    let proxy = conf.proxy.as_ref().map(build_proxy).transpose()?;

    let mut client_builder = Client::builder().default_headers(default_headers);

    if let Some(proxy) = proxy {
        client_builder = client_builder.proxy(proxy);
    }

    let http_client = client_builder.build().map_err(ConfigurationError::from)?;

    Ok(http_client)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use factori::factori;
    use factori::factori_impl::create;

    use crate::client::config::{Authentication, SchemaRegistryConfig};

    fn headers() -> HashMap<String, String> {
        HashMap::from([("x_app_id".to_owned(), "my-app".to_owned())])
    }

    struct MockEnvRequiredVars {
        pub schema_registry_url: String,
        pub schema_registry_username: String,
        pub schema_registry_password: String,
        pub schema_registry_proxy: String,
        pub schema_registry_headers: HashMap<String, String>,
    }

    factori!(MockEnvRequiredVars, {
        default {
            schema_registry_url = "http://localhost:8081".to_owned(),
            schema_registry_username = "sr-username".to_owned(),
            schema_registry_password = "sr-password".to_owned(),
            schema_registry_proxy = "http://localhost:9999".to_owned(),
            schema_registry_headers = headers(),
        }
    });

    struct MockEnvOptionalVars {
        pub schema_registry_url: String,
        pub schema_registry_username: Option<String>,
        pub schema_registry_password: Option<String>,
        pub schema_registry_token: Option<String>,
        pub schema_registry_proxy: Option<String>,
        pub schema_registry_headers: HashMap<String, String>,
    }

    factori!(MockEnvOptionalVars, {
        default {
            schema_registry_url = "http://localhost:8081".to_owned(),
            schema_registry_username = None,
            schema_registry_password = None,
            schema_registry_token = None,
            schema_registry_proxy = None,
            schema_registry_headers = HashMap::new(),
        }

        mixin with_username {
            schema_registry_username = Some("sr-username".to_owned()),
        }

        mixin with_password {
            schema_registry_password = Some("sr-password".to_owned()),
        }

        mixin with_token {
            schema_registry_token = Some("sr-token".to_owned()),
        }

        mixin with_proxy {
            schema_registry_proxy = Some("http://localhost:9999".to_owned())
        }

        mixin with_headers {
            schema_registry_headers = headers(),
        }
    });

    #[test]
    fn create_full_config_with_required_vars_and_basic_auth() {
        let app = create!(MockEnvRequiredVars);

        let config = SchemaRegistryConfig::new()
            .url(&app.schema_registry_url)
            .basic_auth(&app.schema_registry_username, &app.schema_registry_password)
            .proxy(&app.schema_registry_proxy)
            .headers(&app.schema_registry_headers);

        assert_eq!(config.urls[0], app.schema_registry_url);
        assert!(matches!(
            config.authentication.unwrap(),
            Authentication::Basic { .. }
        ));
        assert_eq!(config.proxy.unwrap(), app.schema_registry_proxy);
        assert_eq!(config.headers.unwrap(), app.schema_registry_headers);
    }

    #[test]
    fn create_full_config_with_optional_vars_and_basic_auth() {
        let app = create!(MockEnvOptionalVars, :with_username, :with_password, :with_proxy, :with_headers);

        let config = SchemaRegistryConfig::new()
            .url(&app.schema_registry_url)
            .basic_auth(
                app.schema_registry_username.as_ref(),
                app.schema_registry_password.as_ref(),
            )
            .proxy(app.schema_registry_proxy.as_ref())
            .headers(&app.schema_registry_headers);

        assert_eq!(config.urls[0], app.schema_registry_url);
        assert!(matches!(
            config.authentication.unwrap(),
            Authentication::Basic { .. }
        ));
        assert_eq!(config.proxy.unwrap(), app.schema_registry_proxy.unwrap());
        assert_eq!(config.headers.unwrap(), app.schema_registry_headers);
    }

    #[test]
    fn create_config_with_basic_auth() {
        let app = create!(MockEnvOptionalVars, :with_username, :with_password);

        let config = SchemaRegistryConfig::new()
            .url(&app.schema_registry_url)
            .basic_auth(
                app.schema_registry_username.as_ref(),
                app.schema_registry_password.as_ref(),
            );

        assert_eq!(config.urls[0], app.schema_registry_url);
        assert!(matches!(
            config.authentication.unwrap(),
            Authentication::Basic { .. }
        ));
    }

    #[test]
    fn ignore_basic_auth_if_provided_variables_are_empty() {
        let app = create!(MockEnvOptionalVars);

        let config = SchemaRegistryConfig::new()
            .url(&app.schema_registry_url)
            .basic_auth(
                app.schema_registry_username.as_ref(),
                app.schema_registry_password.as_ref(),
            );

        assert_eq!(config.urls[0], app.schema_registry_url);
        assert!(config.authentication.is_none());
    }

    #[test]
    fn create_config_with_bearer_auth() {
        let app = create!(MockEnvOptionalVars, :with_token);

        let config = SchemaRegistryConfig::new()
            .url(&app.schema_registry_url)
            .bearer_auth(app.schema_registry_token.as_ref());

        assert_eq!(config.urls[0], app.schema_registry_url);
        assert!(matches!(
            config.authentication.unwrap(),
            Authentication::Bearer { .. }
        ))
    }

    #[test]
    fn add_additional_headers() {
        let app = create!(MockEnvOptionalVars, :with_headers);

        let config = SchemaRegistryConfig::new()
            .url(&app.schema_registry_url)
            .headers(&app.schema_registry_headers);

        assert_eq!(config.urls[0], app.schema_registry_url);
        assert!(config.authentication.is_none());
        assert_eq!(config.headers.unwrap().len(), 1);
    }
}
