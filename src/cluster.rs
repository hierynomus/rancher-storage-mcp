//! Builds a `kube::Client` for a downstream cluster through Rancher's
//! cluster proxy at `{RANCHER_URL}/k8s/clusters/<cluster-id>`.
//!
//! The sibling `rancher-helm-mcp` calls `?action=generateKubeconfig` because it
//! has to hand a kubeconfig *file* to a `helm` binary. We speak to the
//! Kubernetes API directly, so we point `kube-rs` at the proxy and reuse the
//! Rancher token we already have. That avoids minting a fresh Rancher token on
//! every call, and it means authorization is exactly the token's own RBAC on
//! the downstream cluster.

use anyhow::{Context, Result};
use http::{HeaderName, HeaderValue};
use kube::{Client, Config};

/// rustls 0.23 requires a process-level `CryptoProvider`, and it only picks one
/// automatically when exactly one of its `ring` / `aws-lc-rs` features is on.
/// Both end up enabled here — `reqwest` and `kube-client` each pull rustls with
/// a different provider — so the choice has to be made explicitly, once, before
/// any TLS client is built. Without this, constructing a client panics inside
/// rustls rather than returning an error.
///
/// Idempotent: a second call is a no-op, which is what makes it safe to call
/// from both `main` and every test.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[derive(Clone)]
pub struct ClusterClients {
    rancher_url: String,
    token: String,
    tls_verify: bool,
    namespace: String,
}

impl ClusterClients {
    pub fn new(rancher_url: String, token: String, tls_verify: bool, namespace: String) -> Self {
        Self {
            rancher_url: rancher_url.trim_end_matches('/').to_string(),
            token,
            tls_verify,
            namespace,
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// A client bound to one downstream cluster. Cheap enough to build per tool
    /// call at this call volume; caching by cluster id is a possible follow-up.
    pub fn client(&self, cluster_id: &str) -> Result<Client> {
        let url = format!("{}/k8s/clusters/{}", self.rancher_url, cluster_id);
        let mut cfg = Config::new(
            url.parse()
                .with_context(|| format!("invalid cluster proxy URL '{url}'"))?,
        );
        cfg.default_namespace = self.namespace.clone();
        cfg.accept_invalid_certs = !self.tls_verify;

        // The bearer token goes in as an extra header rather than via
        // `auth_info.token`, which would need `secrecy::SecretString` — a
        // direct dependency we would have to keep version-locked to kube's
        // own. With an empty `AuthInfo` kube installs no auth layer, so this
        // header reaches the API server untouched.
        cfg.headers.push((
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {}", self.token))
                .context("Rancher token is not a valid HTTP header value")?,
        ));

        Client::try_from(cfg).with_context(|| {
            format!("failed to build Kubernetes client for cluster '{cluster_id}'")
        })
    }
}
