use schema_registry::api::SchemaRegistryAPI;
use schema_registry::client::SchemaRegistryClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sr = SchemaRegistryClient::from_url("http://localhost:8081")?;

    let state = AppState {
        sr: Box::new(sr)
    };

    let subjects = get_subjects(&state).await?;

    Ok(())
}


pub struct AppState {
    sr: Box<dyn SchemaRegistryAPI>
}

async fn get_subjects(state: &AppState) -> anyhow::Result<Vec<String>> {
    let subjects = state.sr.get_subjects(true).await?;

    Ok(subjects)
}

#[cfg(test)]
mod tests {
    use schema_registry::api::MockSchemaRegistryAPI;

    use crate::{AppState};

    #[tokio::test]
    async fn should_at_some_point_call_get_subjects() {
        let mut sr = MockSchemaRegistryAPI::new();
        sr.expect_get_subjects().returning(|_| Ok(vec![]));

        let state = AppState {
            sr: Box::new(sr)
        };

        let subjects = super::get_subjects(&state).await.unwrap();

        assert_eq!(subjects.len(), 0);
    }
}