use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cluster {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub state: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RancherCollection<T> {
    data: Vec<T>,
}

#[async_trait]
pub trait RancherApi: Send + Sync {
    async fn list_clusters(&self) -> Result<Vec<Cluster>>;
}

pub struct RancherClient {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

impl RancherClient {
    pub fn new(base_url: String, token: String, tls_verify: bool) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(!tls_verify)
            .build()
            .context("failed to build HTTP client")?;

        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            token,
        })
    }
}

#[async_trait]
impl RancherApi for RancherClient {
    async fn list_clusters(&self) -> Result<Vec<Cluster>> {
        let url = format!("{}/v3/clusters", self.base_url);
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context("failed to reach Rancher")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("Rancher returned {status} listing clusters: {body}");
        }

        let collection: RancherCollection<Cluster> = resp
            .json()
            .await
            .context("failed to parse clusters response")?;
        Ok(collection.data)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn client(server: &MockServer) -> RancherClient {
        RancherClient::new(server.uri(), "test-token".to_string(), true).unwrap()
    }

    #[tokio::test]
    async fn list_clusters_parses_all_fields() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/clusters"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [{
                    "id": "c-m-abc123",
                    "name": "prod",
                    "state": "active",
                    "description": "Production cluster"
                }]
            })))
            .mount(&mock)
            .await;

        let clusters = client(&mock).await.list_clusters().await.unwrap();

        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].id, "c-m-abc123");
        assert_eq!(clusters[0].name, "prod");
        assert_eq!(clusters[0].state.as_deref(), Some("active"));
        assert_eq!(
            clusters[0].description.as_deref(),
            Some("Production cluster")
        );
    }

    #[tokio::test]
    async fn list_clusters_handles_missing_optional_fields() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/clusters"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [{ "id": "local", "name": "local" }]
            })))
            .mount(&mock)
            .await;

        let clusters = client(&mock).await.list_clusters().await.unwrap();

        assert_eq!(clusters.len(), 1);
        assert!(clusters[0].state.is_none());
        assert!(clusters[0].description.is_none());
    }

    #[tokio::test]
    async fn list_clusters_returns_empty_vec_for_empty_data() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/clusters"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "data": [] })),
            )
            .mount(&mock)
            .await;

        let clusters = client(&mock).await.list_clusters().await.unwrap();
        assert!(clusters.is_empty());
    }

    #[tokio::test]
    async fn list_clusters_returns_error_on_non_200() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/clusters"))
            .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
            .mount(&mock)
            .await;

        let err = client(&mock).await.list_clusters().await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("401"), "error: {msg}");
        assert!(msg.contains("Unauthorized"), "error: {msg}");
    }
}
