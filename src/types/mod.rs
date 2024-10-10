use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::SchemaRegistryError;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExporterStatus {
    pub name: String,
    pub state: String,
    pub offset: i64,
    pub ts: i64,
    pub trace: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExporterConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subjects: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject_rename_format: Option<String>,
    /// always required
    pub config: HashMap<String, String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalize: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility_level: Option<CompatibilityLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub override_metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_rule_set: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub override_rule_set: Option<HashMap<String, String>>,
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubjectConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalize: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility_level: Option<CompatibilityLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub override_metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_rule_set: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub override_rule_set: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct Id {
    pub id: u32,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceMode {
    pub mode: Mode,
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum Mode {
    #[default]
    #[serde(rename = "READWRITE")]
    ReadWrite,
    #[serde(rename = "READONLY")]
    ReadOnly,
    #[serde(rename = "IMPORT")]
    Import,
}

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityLevel {
    #[default]
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
    None,
}

impl fmt::Display for CompatibilityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompatibilityLevel::Backward => write!(f, "BACKWARD"),
            CompatibilityLevel::BackwardTransitive => write!(f, "BACKWARD_TRANSITIVE"),
            CompatibilityLevel::Forward => write!(f, "FORWARD"),
            CompatibilityLevel::ForwardTransitive => write!(f, "FORWARD_TRANSITIVE"),
            CompatibilityLevel::Full => write!(f, "FULL"),
            CompatibilityLevel::FullTransitive => write!(f, "FULL_TRANSITIVE"),
            CompatibilityLevel::None => write!(f, "NONE"),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq)]
pub enum Version {
    #[default]
    Latest,
    Number(u32),
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Version::Latest => write!(f, "latest"),
            Version::Number(version) => write!(f, "{}", version),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaType {
    #[default]
    Avro,
    Protobuf,
    Json,
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaType::Avro => write!(f, "AVRO"),
            SchemaType::Protobuf => write!(f, "PROTOBUF"),
            SchemaType::Json => write!(f, "JSON"),
        }
    }
}

impl FromStr for SchemaType {
    type Err = SchemaRegistryError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str {
            s if s.eq_ignore_ascii_case("AVRO") => Ok(SchemaType::Avro),
            s if s.eq_ignore_ascii_case("PROTOBUF") => Ok(SchemaType::Protobuf),
            s if s.eq_ignore_ascii_case("JSON") => Ok(SchemaType::Json),
            _ => Err(SchemaRegistryError::invalid_schema_type(str)),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LookupSubject {
    pub schema: Cow<'static, str>,
    pub schema_type: Option<SchemaType>,
    pub references: Option<Vec<Reference>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StringSchema(Cow<'static, str>);

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubjectVersion {
    pub subject: String,
    pub version: u32,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    #[serde(default)]
    pub schema_type: SchemaType,
    pub schema: Cow<'static, str>,
    pub references: Option<Vec<Reference>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subject {
    pub id: u32,
    pub subject: String,
    pub version: u32,
    #[serde(default)]
    pub schema_type: SchemaType,
    pub schema: Cow<'static, str>,
    pub references: Option<Vec<Reference>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reference {
    pub name: String,
    pub subject: String,
    pub version: u32,
}

impl Reference {
    pub fn new(name: &str, subject: &str) -> Self {
        Self {
            name: name.to_string(),
            subject: subject.to_string(),
            version: 1,
        }
    }

    pub fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnregisteredSchema {
    pub schema: String,
    pub schema_type: SchemaType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<Reference>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredSchema {
    pub id: u32,
}

impl UnregisteredSchema {
    pub fn schema<T>(schema: T) -> Self
    where
        T: Into<String>,
    {
        Self {
            schema: schema.into(),
            schema_type: SchemaType::Avro,
            references: None,
        }
    }

    pub fn schema_type(mut self, schema_type: SchemaType) -> Self {
        self.schema_type = schema_type;
        self
    }

    pub fn reference(mut self, reference: Reference) -> Self {
        if let Some(references) = self.references.as_mut() {
            references.push(reference);
        } else {
            self.references = Some(vec![reference]);
        }

        self
    }

    pub fn references<I>(mut self, references: I) -> Self
    where
        I: IntoIterator<Item = Reference>,
    {
        if let Some(refs) = self.references.as_mut() {
            refs.extend(references);
        } else {
            self.references = Some(references.into_iter().collect());
        }

        self
    }
}
