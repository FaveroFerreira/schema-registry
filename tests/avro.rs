use schema_registry::{Reference, SchemaRegistryAPI, UnregisteredSchema, Version};

mod utils;

const BOOK_KEY_SUBJECT: &str = "test.avro.book-key";
const BOOK_VALUE_SUBJECT: &str = "test.avro.book-value";
const AUTHOR_VALUE_SUBJECT: &str = "test.avro.author-value";

const BOOK_KEY_SCHEMA: &str = "./tools/schemas/avro/book-key.avsc";
const BOOK_VALUE_SCHEMA: &str = "./tools/schemas/avro/book-value.avsc";
const AUTHOR_VALUE_SCHEMA: &str = "./tools/schemas/avro/author-value.avsc";

#[tokio::test]
#[serial_test::serial]
async fn can_register_schema() {
    let env = utils::TestEnvironment::start().await;

    let client = env.create_schema_registry_client().await;

    let book_key_schema = utils::load_schema(BOOK_KEY_SCHEMA);
    let schema = UnregisteredSchema::schema(&book_key_schema);

    let registered_schema = client
        .register_subject_schema(BOOK_KEY_SUBJECT, &schema)
        .await
        .expect("to register schema successfully");
    let schema = client.fetch_schema_by_id(registered_schema.id).await;

    assert!(schema.is_ok());
}

#[tokio::test]
#[serial_test::serial]
async fn can_register_schema_with_references() {
    let env = utils::TestEnvironment::start().await;

    let client = env.create_schema_registry_client().await;

    let author_schema = utils::load_schema(AUTHOR_VALUE_SCHEMA);
    let unregistered_author = UnregisteredSchema::schema(&author_schema);
    client
        .register_subject_schema(AUTHOR_VALUE_SUBJECT, &unregistered_author)
        .await
        .expect("to register author schema successfully");

    let book_schema = utils::load_schema(BOOK_VALUE_SCHEMA);

    let unregistered_book = UnregisteredSchema::schema(&book_schema)
        .references([Reference::new("Author", AUTHOR_VALUE_SUBJECT)]);

    let registered_schema = client
        .register_subject_schema(BOOK_VALUE_SUBJECT, &unregistered_book)
        .await
        .expect("to register book schema successfully");
    let schema = client.fetch_schema_by_id(registered_schema.id).await;

    assert!(schema.is_ok());
}

#[tokio::test]
#[serial_test::serial]
async fn can_list_subjects() {
    let env = utils::TestEnvironment::start().await;

    let client = env.create_schema_registry_client().await;

    let book_key_schema = utils::load_schema(BOOK_KEY_SCHEMA);
    let schema = UnregisteredSchema::schema(&book_key_schema);
    client
        .register_subject_schema(BOOK_KEY_SUBJECT, &schema)
        .await
        .expect("to register schema successfully");

    let author_value_schema = utils::load_schema(AUTHOR_VALUE_SCHEMA);
    let schema = UnregisteredSchema::schema(&author_value_schema);
    client
        .register_subject_schema(AUTHOR_VALUE_SUBJECT, &schema)
        .await
        .expect("to register schema successfully");

    let subjects = client
        .fetch_subjects()
        .await
        .expect("to fetch subjects successfully");

    assert!(subjects.contains(&BOOK_KEY_SUBJECT.to_string()));
    assert!(subjects.contains(&AUTHOR_VALUE_SUBJECT.to_string()));
}

#[tokio::test]
#[serial_test::serial]
async fn can_fetch_schema_by_subject_version() {
    let env = utils::TestEnvironment::start().await;

    let client = env.create_schema_registry_client().await;

    let book_key_schema = utils::load_schema(BOOK_KEY_SCHEMA);
    let schema = UnregisteredSchema::schema(&book_key_schema);
    client
        .register_subject_schema(BOOK_KEY_SUBJECT, &schema)
        .await
        .expect("to register schema successfully");

    let version_by_number = client
        .fetch_schema_by_subject_version(BOOK_KEY_SUBJECT, Version::Number(1))
        .await
        .expect("to fetch schema successfully");
    let version_latest = client
        .fetch_schema_by_subject_version(BOOK_KEY_SUBJECT, Version::Latest)
        .await
        .expect("to fetch schema successfully");

    assert_eq!(version_by_number, version_latest);
}

#[tokio::test]
#[serial_test::serial]
async fn can_fetch_subject_versions() {
    let env = utils::TestEnvironment::start().await;

    let client = env.create_schema_registry_client().await;

    let book_key_schema = utils::load_schema(BOOK_KEY_SCHEMA);
    let schema = UnregisteredSchema::schema(&book_key_schema);
    client
        .register_subject_schema(BOOK_KEY_SUBJECT, &schema)
        .await
        .expect("to register schema successfully");

    let versions = client
        .fetch_subject_versions(BOOK_KEY_SUBJECT)
        .await
        .expect("to fetch versions successfully");

    assert_eq!(versions, vec![1]);
}

#[tokio::test]
#[serial_test::serial]
async fn can_lookup_subject_schema() {
    let env = utils::TestEnvironment::start().await;

    let client = env.create_schema_registry_client().await;

    let author_schema = utils::load_schema(AUTHOR_VALUE_SCHEMA);
    let unregistered_author = UnregisteredSchema::schema(&author_schema);
    client
        .register_subject_schema(AUTHOR_VALUE_SUBJECT, &unregistered_author)
        .await
        .expect("to register author schema successfully");

    let book_schema = utils::load_schema(BOOK_VALUE_SCHEMA);

    let unregistered_book = UnregisteredSchema::schema(&book_schema)
        .references([Reference::new("Author", AUTHOR_VALUE_SUBJECT)]);

    client
        .register_subject_schema(BOOK_VALUE_SUBJECT, &unregistered_book)
        .await
        .expect("to register book schema successfully");

    let book_schema = utils::load_schema(BOOK_VALUE_SCHEMA);
    let unregistered_book = UnregisteredSchema::schema(&book_schema).references([Reference::new(
        "Author",
        AUTHOR_VALUE_SUBJECT,
    )
    .version(1)]);

    let schema = client
        .lookup_subject_by_schema(BOOK_VALUE_SUBJECT, &unregistered_book)
        .await
        .expect("to lookup schema successfully");
}
