use anyhow::Result;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod cluster;
mod credentials;
mod longhorn;
mod model;
mod rancher;
mod render;
mod server;

use credentials::ClientFactory;
use server::StorageMcpServer;

#[tokio::main]
async fn main() -> Result<()> {
    // Logs must go to stderr: on the stdio transport, anything on stdout
    // corrupts the JSON-RPC stream.
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    cluster::install_crypto_provider();

    // Optional: when both are absent, every tool call must instead carry
    // `R_url` and `R_token` headers, so one deployment can serve callers with
    // different Rancher identities. See `StorageMcpServer::call_tool`.
    let rancher_url = std::env::var("RANCHER_URL").ok();
    let rancher_token = std::env::var("RANCHER_TOKEN").ok();
    let tls_verify = std::env::var("RANCHER_TLS_VERIFY")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);

    // Namespace Longhorn is installed into on the downstream clusters.
    let longhorn_namespace =
        std::env::var("LONGHORN_NAMESPACE").unwrap_or_else(|_| "longhorn-system".to_string());

    info!(tls_verify, %longhorn_namespace, "Starting rancher-storage-mcp");

    let factory = ClientFactory::new(tls_verify, longhorn_namespace);
    let server = match (rancher_url, rancher_token) {
        (Some(url), Some(token)) => {
            info!(%url, "Using static Rancher credentials from the environment");
            StorageMcpServer::configured(factory.build(url, token)?, factory)
        }
        _ => {
            info!("No static Rancher credentials; R_url and R_token headers required per request");
            StorageMcpServer::unconfigured(factory)
        }
    };

    match std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
    {
        Some(port) => serve_http(server, port).await,
        None => serve_stdio(server).await,
    }
}

async fn serve_stdio(server: StorageMcpServer) -> Result<()> {
    use rmcp::ServiceExt;
    info!("Transport: stdio");
    let transport = rmcp::transport::io::stdio();
    server.serve(transport).await?.waiting().await?;
    Ok(())
}

async fn serve_http(server: StorageMcpServer, port: u16) -> Result<()> {
    use axum::{routing::get, Router};
    use rmcp::transport::streamable_http_server::{
        session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    };
    use tokio_util::sync::CancellationToken;

    info!(port, "Transport: streamable HTTP");

    let ct = CancellationToken::new();
    let svc = StreamableHttpService::new(
        move || Ok(server.clone()),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default()
            .with_cancellation_token(ct.child_token())
            .disable_allowed_hosts(),
    );
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .fallback_service(svc);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    info!(port, "rancher-storage-mcp listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down");
            ct.cancel();
        })
        .await?;
    Ok(())
}
