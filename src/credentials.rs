//! Building both Rancher-backed clients from one set of credentials.
//!
//! Unlike `rancher-helm-mcp`, this server uses the Rancher URL and token in two
//! places: the `/v3` REST client that resolves cluster names, and the cluster
//! proxy that every Longhorn request goes through. A per-request credential
//! override therefore has to rebuild *both*, which is what this factory is for.

use std::sync::Arc;

use anyhow::Result;

use crate::cluster::ClusterClients;
use crate::longhorn::{LonghornApi, LonghornClient};
use crate::rancher::{RancherApi, RancherClient};

/// The pair of clients a single Rancher identity needs.
pub struct Clients {
    pub rancher: Arc<dyn RancherApi>,
    pub longhorn: Arc<dyn LonghornApi>,
}

/// The settings that do *not* vary per request, plus the ability to mint a
/// `Clients` for whichever credentials a request supplies.
#[derive(Clone)]
pub struct ClientFactory {
    tls_verify: bool,
    longhorn_namespace: String,
}

impl ClientFactory {
    pub fn new(tls_verify: bool, longhorn_namespace: String) -> Self {
        Self {
            tls_verify,
            longhorn_namespace,
        }
    }

    pub fn build(&self, rancher_url: String, rancher_token: String) -> Result<Clients> {
        let rancher =
            RancherClient::new(rancher_url.clone(), rancher_token.clone(), self.tls_verify)?;
        let clusters = ClusterClients::new(
            rancher_url,
            rancher_token,
            self.tls_verify,
            self.longhorn_namespace.clone(),
        );
        Ok(Clients {
            rancher: Arc::new(rancher),
            longhorn: Arc::new(LonghornClient::new(clusters)),
        })
    }
}
