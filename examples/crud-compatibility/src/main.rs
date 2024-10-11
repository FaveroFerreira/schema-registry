use schema_registry::api::SchemaRegistryAPI;
use schema_registry::client::SchemaRegistryClient;
use schema_registry::types::{
    CompatibilityLevel, SchemaType, SubjectConfig, UnregisteredSchema, Version,
};

const SUBJECT: &str = "compatibility-example";
const SCHEMA: &str = r#"
{
  "type": "record",
  "name": "User",
  "fields": [
    {
      "name": "name",
      "type": "string"
    }
  ]
}
"#;

const FORWARD_COMPATIBLE_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "User",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "age",
      "type": "int"
    }
  ]
}
"#;

const BACKWARD_COMPATIBLE_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "User",
  "fields": [
    {
        "name": "name",
        "type": "string"
    },
    {
        "name": "email",
        "type": ["null", "string"],
        "default": null
    }
  ]
}
"#;

const NORMALIZE: bool = false;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = SchemaRegistryClient::from_url("http://localhost:8081")?;

    // cleanup subject, gotta delete it twice because the first time it will be soft deleted
    client.delete_subject(SUBJECT, false).await.ok();
    client.delete_subject(SUBJECT, true).await.ok();

    let unregistered_schema = UnregisteredSchema::schema(SCHEMA).schema_type(SchemaType::Avro);
    client
        .post_new_subject_version(SUBJECT, &unregistered_schema, NORMALIZE)
        .await?;

    let forward_compatible_schema =
        UnregisteredSchema::schema(FORWARD_COMPATIBLE_SCHEMA).schema_type(SchemaType::Avro);
    let is_forward_compatible = client
        .is_compatible(SUBJECT, Version::Latest, &forward_compatible_schema)
        .await?;

    // by default, the schema is BACKWARD compatible, so forwards compatibility should be false
    assert!(
        !is_forward_compatible,
        "Forward compatibility should be false"
    );

    let backward_compatible_schema =
        UnregisteredSchema::schema(BACKWARD_COMPATIBLE_SCHEMA).schema_type(SchemaType::Avro);
    let is_backward_compatible = client
        .is_compatible(SUBJECT, Version::Latest, &backward_compatible_schema)
        .await?;

    // by default, the schema is backward compatible, so backwards compatibility should be true
    assert!(
        is_backward_compatible,
        "Backward compatibility should be true"
    );

    // lets change the compatibility level of the subject to FORWARD to test the forward compatibility
    let config = SubjectConfig::new().compatibility_level(CompatibilityLevel::Forward);
    client
        .update_subject_configuration(SUBJECT, &config)
        .await?;

    let is_forward_compatible = client
        .is_compatible(SUBJECT, Version::Latest, &forward_compatible_schema)
        .await?;

    // now the schema is FORWARD compatible, so forwards compatibility should be true
    assert!(
        is_forward_compatible,
        "Forward compatibility should be true"
    );

    // cleanup subject
    client.delete_subject(SUBJECT, false).await?;
    client.delete_subject(SUBJECT, true).await?;

    Ok(())
}
