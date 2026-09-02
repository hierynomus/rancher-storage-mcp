//! The MCP surface: five tools over Longhorn on any Rancher-managed cluster.
//!
//! Every tool returns a rendered `String`; `rmcp` wraps it as a single text
//! block. Longhorn status is sprawling and mostly irrelevant, so a curated
//! summary is both smaller and more useful to a model than raw CR JSON.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use rmcp::{
    handler::server::wrapper::Parameters,
    model::{Implementation, InitializeResult, ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ServerHandler,
};
use serde::Deserialize;
use tracing::info;

use crate::longhorn::LonghornApi;
use crate::model::{Replica, Volume};
use crate::rancher::RancherApi;
use crate::render;

/// Longhorn's own validating webhook derives these bounds from the
/// `default-replica-count` setting's int range. They are *not* in the OpenAPI
/// schema, so a bad value would come back as a bare 422 — check client-side.
const MIN_REPLICAS: i64 = 1;
const MAX_REPLICAS: i64 = 20;

/// Nothing in Longhorn is synchronous. Mutating tools submit an intent, then
/// poll a specific status predicate for a bounded time and report what they
/// actually observed — never a bare "success".
const POLL_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct StorageMcpServer {
    rancher: Arc<dyn RancherApi>,
    longhorn: Arc<dyn LonghornApi>,
    poll_timeout: Duration,
    poll_interval: Duration,
}

impl StorageMcpServer {
    pub fn new(rancher: impl RancherApi + 'static, longhorn: impl LonghornApi + 'static) -> Self {
        Self {
            rancher: Arc::new(rancher),
            longhorn: Arc::new(longhorn),
            poll_timeout: POLL_TIMEOUT,
            poll_interval: POLL_INTERVAL,
        }
    }

    /// Shortens the polling loops so tests can exercise the timeout branches.
    #[cfg(test)]
    fn with_poll(mut self, timeout: Duration, interval: Duration) -> Self {
        self.poll_timeout = timeout;
        self.poll_interval = interval;
        self
    }
}

// ── Tool inputs ───────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListClustersInput {}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListVolumesInput {
    #[schemars(description = "Rancher cluster ID (e.g. c-m-abcd1234), cluster name, or 'local'")]
    pub cluster: String,
    #[schemars(description = "Only volumes bound to a PVC in this Kubernetes namespace")]
    pub namespace: Option<String>,
    #[schemars(
        description = "Only volumes in this state: creating, attached, detached, attaching, detaching, deleting"
    )]
    pub state: Option<String>,
    #[schemars(
        description = "Only volumes with this robustness: healthy, degraded, faulted, unknown"
    )]
    pub robustness: Option<String>,
    #[schemars(
        description = "Only volumes whose Longhorn name or PVC name contains this substring"
    )]
    pub name_contains: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct InspectVolumeInput {
    #[schemars(description = "Rancher cluster ID (e.g. c-m-abcd1234), cluster name, or 'local'")]
    pub cluster: String,
    #[schemars(
        description = "Longhorn volume name (pvc-<uuid>), a 'namespace/pvc-name' reference, or a \
        bare PVC name if it is unambiguous"
    )]
    pub volume: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetReplicaCountInput {
    #[schemars(description = "Rancher cluster ID (e.g. c-m-abcd1234), cluster name, or 'local'")]
    pub cluster: String,
    #[schemars(
        description = "Longhorn volume name (pvc-<uuid>), a 'namespace/pvc-name' reference, or a \
        bare PVC name if it is unambiguous"
    )]
    pub volume: String,
    #[schemars(description = "Desired number of replicas, between 1 and 20")]
    pub replica_count: i64,
    #[schemars(
        description = "Allow the change on a volume that is not attached. Longhorn accepts it but \
        defers all rebuild work until the volume is next attached, so it is refused by default."
    )]
    pub allow_detached: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CreateSnapshotInput {
    #[schemars(description = "Rancher cluster ID (e.g. c-m-abcd1234), cluster name, or 'local'")]
    pub cluster: String,
    #[schemars(
        description = "Longhorn volume name (pvc-<uuid>), a 'namespace/pvc-name' reference, or a \
        bare PVC name if it is unambiguous"
    )]
    pub volume: String,
    #[schemars(
        description = "Snapshot name; this is the Longhorn snapshot name itself. Omit for \
        <volume>-mcp-<utc-timestamp>. Re-using a name is idempotent — no second snapshot is taken."
    )]
    pub snapshot_name: Option<String>,
    #[schemars(description = "Labels to attach to the snapshot, as a string-to-string map")]
    pub labels: Option<BTreeMap<String, String>>,
}

// ── Tools ─────────────────────────────────────────────────────────────────────

#[tool_router]
impl StorageMcpServer {
    #[tool(description = "List all Rancher-managed clusters available to the configured API token")]
    async fn list_clusters(
        &self,
        Parameters(_): Parameters<ListClustersInput>,
    ) -> Result<String, rmcp::ErrorData> {
        let clusters = self.rancher.list_clusters().await.map_err(internal)?;

        if clusters.is_empty() {
            return Ok("No clusters found.".to_string());
        }

        let lines: Vec<String> = clusters
            .iter()
            .map(|c| {
                format!(
                    "- id={} name={} state={} {}",
                    c.id,
                    c.name,
                    c.state.as_deref().unwrap_or("unknown"),
                    c.description
                        .as_deref()
                        .map(|d| format!("({d})"))
                        .unwrap_or_default(),
                )
            })
            .collect();

        Ok(lines.join("\n"))
    }

    #[tool(
        description = "List Longhorn volumes on a Rancher-managed cluster, with their state, \
        robustness, size, healthy/desired replica counts and the PVC they back. Use this to find \
        the Longhorn volume name (pvc-<uuid>) behind a workload, and to spot degraded volumes."
    )]
    async fn list_volumes(
        &self,
        Parameters(input): Parameters<ListVolumesInput>,
    ) -> Result<String, rmcp::ErrorData> {
        let cluster_id = self.resolve_cluster_id(&input.cluster).await?;
        let volumes = self
            .longhorn
            .list_volumes(&cluster_id)
            .await
            .map_err(internal)?;
        let all_replicas = self
            .longhorn
            .list_all_replicas(&cluster_id)
            .await
            .map_err(internal)?;

        let total = volumes.len();
        let matched: Vec<&Volume> = volumes
            .iter()
            .filter(|v| matches_filters(v, &input))
            .collect();

        if matched.is_empty() {
            return Ok(format!(
                "No Longhorn volumes matched in namespace '{}' of cluster {cluster_id} \
                 ({total} volume(s) present before filtering).",
                self.longhorn.namespace(),
            ));
        }

        let mut out = format!(
            "{} of {total} Longhorn volume(s) in namespace '{}' of cluster {cluster_id}:\n",
            matched.len(),
            self.longhorn.namespace(),
        );
        for v in matched {
            let replicas = replicas_of(&all_replicas, &v.name);
            out.push_str(&render::volume_line(v, &replicas));
            out.push('\n');
        }
        out.push_str("\nUse inspect_volume for replica, snapshot and backup detail.");
        Ok(out)
    }

    #[tool(
        description = "Inspect one Longhorn volume in depth: spec and status, conditions, every \
        replica with its node/disk/mode/health, any in-flight rebuild or restore, snapshots and \
        backups. Accepts a Longhorn volume name, a 'namespace/pvc-name' reference, or a bare PVC name."
    )]
    async fn inspect_volume(
        &self,
        Parameters(input): Parameters<InspectVolumeInput>,
    ) -> Result<String, rmcp::ErrorData> {
        let cluster_id = self.resolve_cluster_id(&input.cluster).await?;
        let name = self.resolve_volume_name(&cluster_id, &input.volume).await?;

        let volume = self
            .longhorn
            .get_volume(&cluster_id, &name)
            .await
            .map_err(internal)?;
        let engines = self
            .longhorn
            .list_engines(&cluster_id, &name)
            .await
            .map_err(internal)?;
        let replicas = self
            .longhorn
            .list_replicas(&cluster_id, &name)
            .await
            .map_err(internal)?;

        // Snapshots and backups are best-effort: a token without RBAC on those
        // kinds, or a cluster with no backup target, must not blank out the
        // volume and replica detail the caller actually asked for.
        let mut notes = Vec::new();
        let snapshots = match self.longhorn.list_snapshots(&cluster_id, &name).await {
            Ok(s) => s,
            Err(e) => {
                notes.push(format!("could not list snapshots: {}", chain(&e)));
                Vec::new()
            }
        };
        let target = volume.spec.backup_target_name.as_deref();
        let backups = match self.longhorn.list_backups(&cluster_id, &name, target).await {
            Ok(b) => b,
            Err(e) => {
                notes.push(format!("could not list backups: {}", chain(&e)));
                Vec::new()
            }
        };
        let backup_volume = match self.longhorn.get_backup_volume(&cluster_id, &name).await {
            Ok(bv) => bv,
            Err(e) => {
                notes.push(format!("could not read BackupVolume: {}", chain(&e)));
                None
            }
        };

        let mut out = format!("Cluster: {cluster_id}\n");
        out.push_str(&render::volume_report(
            &volume,
            &engines,
            &replicas,
            &snapshots,
            &backups,
            backup_volume.as_ref(),
        ));
        if !notes.is_empty() {
            out.push_str("\nNotes:\n");
            for n in notes {
                out.push_str(&format!("  - {n}\n"));
            }
        }
        Ok(out)
    }

    #[tool(
        description = "Change the number of replicas of a Longhorn volume by patching \
        spec.numberOfReplicas. Scaling up triggers a rebuild from a healthy replica and leaves the \
        volume degraded until it finishes; scaling down removes excess replicas one per reconcile. \
        Returns the state observed after a short wait — re-run inspect_volume to follow a rebuild."
    )]
    async fn set_volume_replica_count(
        &self,
        Parameters(input): Parameters<SetReplicaCountInput>,
    ) -> Result<String, rmcp::ErrorData> {
        let cluster_id = self.resolve_cluster_id(&input.cluster).await?;
        let name = self.resolve_volume_name(&cluster_id, &input.volume).await?;
        let volume = self
            .longhorn
            .get_volume(&cluster_id, &name)
            .await
            .map_err(internal)?;
        let desired = input.replica_count;

        check_replica_count_preconditions(&volume, desired, input.allow_detached.unwrap_or(false))?;

        let current = volume.spec.number_of_replicas.unwrap_or(0);
        if current == desired {
            let replicas = self
                .longhorn
                .list_replicas(&cluster_id, &name)
                .await
                .map_err(internal)?;
            return Ok(format!(
                "Volume {name} already has spec.numberOfReplicas={desired}; no change made.\n{}",
                replica_summary(&replicas, desired),
            ));
        }

        info!(cluster_id, volume = %name, current, desired, "Patching replica count");
        self.longhorn
            .patch_replica_count(&cluster_id, &name, desired)
            .await
            .map_err(internal)?;

        // Poll on the usable replica count, which is what Longhorn itself
        // reconciles towards. Scale-up makes new replica CRs appear quickly;
        // the rebuild that follows runs for minutes to hours.
        let deadline = std::time::Instant::now() + self.poll_timeout;
        let mut replicas: Vec<Replica>;
        let mut converged = false;
        loop {
            replicas = self
                .longhorn
                .list_replicas(&cluster_id, &name)
                .await
                .map_err(internal)?;
            let usable = replicas
                .iter()
                .filter(|r| render::replica_usable(r))
                .count() as i64;
            if usable == desired {
                converged = true;
                break;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(self.poll_interval).await;
        }

        let mut out = format!(
            "Patched spec.numberOfReplicas on volume {name} from {current} to {desired}.\n{}",
            replica_summary(&replicas, desired),
        );
        if converged {
            if desired > current {
                out.push_str(
                    "\nReplica count reached. New replicas rebuild from a healthy replica, so the \
                     volume stays 'degraded' until every replica reports healthy — re-run \
                     inspect_volume to follow the rebuild progress.",
                );
            } else {
                out.push_str("\nReplica count reached.");
            }
        } else {
            out.push_str(&format!(
                "\nStill converging after {}s. Scale-down removes at most one excess replica per \
                 reconcile, and scale-up waits on scheduling — re-run inspect_volume to check again.",
                self.poll_timeout.as_secs(),
            ));
        }
        if volume.spec.replica_auto_balance.as_deref() == Some("best-effort") {
            out.push_str(
                "\nNote: replicaAutoBalance is 'best-effort', so Longhorn may keep creating and \
                 deleting replicas after the count converges. Replica names are not stable.",
            );
        }
        Ok(out)
    }

    #[tool(
        description = "Take a snapshot of a Longhorn volume by creating a Snapshot CR. This is the \
        primitive both the CSI 'type: snap' path and the Longhorn UI use. Idempotent per snapshot \
        name: re-running with the same name never takes a second snapshot. Returns whether the \
        snapshot became ready within a short wait."
    )]
    async fn create_volume_snapshot(
        &self,
        Parameters(input): Parameters<CreateSnapshotInput>,
    ) -> Result<String, rmcp::ErrorData> {
        let cluster_id = self.resolve_cluster_id(&input.cluster).await?;
        let name = self.resolve_volume_name(&cluster_id, &input.volume).await?;
        let volume = self
            .longhorn
            .get_volume(&cluster_id, &name)
            .await
            .map_err(internal)?;

        let snapshot_name = match input.snapshot_name.as_deref().filter(|s| !s.is_empty()) {
            Some(s) => s.to_string(),
            None => render::default_snapshot_name(&name, Utc::now()),
        };

        info!(cluster_id, volume = %name, snapshot = %snapshot_name, "Creating Snapshot CR");
        let mut reused = false;
        if let Err(create_err) = self
            .longhorn
            .create_snapshot(&cluster_id, &snapshot_name, &name, input.labels.clone())
            .await
        {
            // Most likely AlreadyExists. `createSnapshot: true` is idempotent
            // once status.creationTime is set, so an existing CR with this name
            // is a successful retry, not a failure. Only report the create
            // error if there is genuinely no such snapshot.
            match self
                .longhorn
                .get_snapshot(&cluster_id, &snapshot_name)
                .await
            {
                Ok(_) => reused = true,
                Err(_) => return Err(internal(create_err)),
            }
        }

        let deadline = std::time::Instant::now() + self.poll_timeout;
        let mut snapshot = None;
        loop {
            match self
                .longhorn
                .get_snapshot(&cluster_id, &snapshot_name)
                .await
            {
                Ok(s) => {
                    let ready = s.is_ready();
                    snapshot = Some(s);
                    if ready {
                        break;
                    }
                }
                // The CR was accepted; a read failing right after creation is
                // normally propagation, not absence.
                Err(e) => info!("snapshot not readable yet: {}", chain(&e)),
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(self.poll_interval).await;
        }

        let mut out = format!(
            "Snapshot: {snapshot_name}\n  Volume:  {name} (state={})\n",
            render::dash(volume.status.state.as_deref()),
        );
        if reused {
            out.push_str(
                "  Note:    a snapshot with this name already existed; no second snapshot was \
                 taken (createSnapshot is idempotent).\n",
            );
        }
        match snapshot {
            Some(s) if s.is_ready() => {
                out.push_str(&format!(
                    "  Ready:   yes\n  Created: {}\n  Size:    {}\n",
                    render::dash(s.status.creation_time.as_deref()),
                    render::size_int(s.status.size),
                ));
            }
            Some(s) => {
                out.push_str("  Ready:   not yet\n");
                if let Some(err) = s.status.error.as_deref().filter(|e| !e.is_empty()) {
                    out.push_str(&format!("  Status:  {err}\n"));
                    out.push_str(
                        "  This is usually transient rather than a failure: if the volume is \
                         detached, Longhorn first raises a VolumeAttachment ticket and retries \
                         once the volume is attached.\n",
                    );
                } else {
                    out.push_str("  The controller has not reported creationTime yet.\n");
                }
                out.push_str(&format!(
                    "  Re-run create_volume_snapshot with snapshot_name=\"{snapshot_name}\" to \
                     re-check without taking another snapshot, or inspect_volume to see the \
                     snapshot list.\n"
                ));
            }
            None => {
                out.push_str(&format!(
                    "  Ready:   unknown — the CR was accepted but could not be read back within \
                     {}s. Check with inspect_volume.\n",
                    self.poll_timeout.as_secs(),
                ));
            }
        }
        Ok(out)
    }
}

// ── Non-tool helpers (separate impl block so #[tool_router] ignores them) ─────

impl StorageMcpServer {
    /// Accepts a cluster ID (`c-…`, `local`) or a human-readable name.
    async fn resolve_cluster_id(&self, cluster: &str) -> Result<String, rmcp::ErrorData> {
        if cluster == "local" || cluster.starts_with("c-") {
            return Ok(cluster.to_string());
        }

        let clusters = self.rancher.list_clusters().await.map_err(internal)?;

        clusters
            .into_iter()
            .find(|c| c.name.eq_ignore_ascii_case(cluster))
            .map(|c| c.id)
            .ok_or_else(|| {
                rmcp::ErrorData::internal_error(
                    format!(
                        "cluster '{cluster}' not found; use list_clusters to see available clusters"
                    ),
                    None,
                )
            })
    }

    /// Resolves a Longhorn volume name, a `namespace/pvc-name` reference, or a
    /// bare PVC name. A bare PVC name that matches more than one volume is an
    /// error listing the candidates rather than an arbitrary pick.
    async fn resolve_volume_name(
        &self,
        cluster_id: &str,
        reference: &str,
    ) -> Result<String, rmcp::ErrorData> {
        let volumes = self
            .longhorn
            .list_volumes(cluster_id)
            .await
            .map_err(internal)?;

        if volumes.iter().any(|v| v.name == reference) {
            return Ok(reference.to_string());
        }

        let (ns, pvc) = match reference.split_once('/') {
            Some((ns, pvc)) => (Some(ns), pvc),
            None => (None, reference),
        };

        let matches: Vec<&Volume> = volumes
            .iter()
            .filter(|v| match v.status.pvc_ref() {
                Some((vns, vpvc)) => vpvc == pvc && ns.is_none_or(|ns| ns == vns),
                None => false,
            })
            .collect();

        match matches.as_slice() {
            [only] => Ok(only.name.clone()),
            [] => Err(rmcp::ErrorData::internal_error(
                format!(
                    "no Longhorn volume '{reference}' in namespace '{}' of cluster {cluster_id}; \
                     use list_volumes to see what is available",
                    self.longhorn.namespace()
                ),
                None,
            )),
            many => Err(rmcp::ErrorData::internal_error(
                format!(
                    "'{reference}' is ambiguous — {} volumes back a PVC with that name: {}. \
                     Pass 'namespace/{pvc}' or the Longhorn volume name.",
                    many.len(),
                    many.iter()
                        .map(|v| {
                            let (ns, pvc) = v.status.pvc_ref().unwrap_or_default();
                            format!("{} ({ns}/{pvc})", v.name)
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                ),
                None,
            )),
        }
    }
}

// ── Free helpers ──────────────────────────────────────────────────────────────

/// Flattens the whole `anyhow` cause chain, so a Kubernetes webhook's rejection
/// message survives all the way to the caller instead of being hidden behind
/// our own context string.
fn chain(e: &anyhow::Error) -> String {
    e.chain()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(": ")
}

fn internal(e: anyhow::Error) -> rmcp::ErrorData {
    rmcp::ErrorData::internal_error(chain(&e), None)
}

fn refuse(msg: String) -> rmcp::ErrorData {
    rmcp::ErrorData::internal_error(msg, None)
}

fn replicas_of(all: &[Replica], volume: &str) -> Vec<Replica> {
    all.iter()
        .filter(|r| r.spec.volume_name.as_deref() == Some(volume))
        .cloned()
        .collect()
}

fn matches_filters(v: &Volume, input: &ListVolumesInput) -> bool {
    let pvc = v.status.pvc_ref();

    if let Some(ns) = input.namespace.as_deref().filter(|s| !s.is_empty()) {
        if pvc.as_ref().map(|(n, _)| n.as_str()) != Some(ns) {
            return false;
        }
    }
    if let Some(state) = input.state.as_deref().filter(|s| !s.is_empty()) {
        if !v
            .status
            .state
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case(state))
        {
            return false;
        }
    }
    if let Some(rob) = input.robustness.as_deref().filter(|s| !s.is_empty()) {
        if !v
            .status
            .robustness
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case(rob))
        {
            return false;
        }
    }
    if let Some(needle) = input.name_contains.as_deref().filter(|s| !s.is_empty()) {
        let needle = needle.to_ascii_lowercase();
        let in_name = v.name.to_ascii_lowercase().contains(&needle);
        let in_pvc = pvc
            .as_ref()
            .is_some_and(|(_, p)| p.to_ascii_lowercase().contains(&needle));
        if !in_name && !in_pvc {
            return false;
        }
    }
    true
}

fn replica_summary(replicas: &[Replica], desired: i64) -> String {
    format!(
        "  Replicas: desired={desired} usable={} healthy={} total={}",
        replicas
            .iter()
            .filter(|r| render::replica_usable(r))
            .count(),
        replicas
            .iter()
            .filter(|r| render::replica_healthy(r))
            .count(),
        replicas.len(),
    )
}

/// The webhook enforces only the range and the strict-local rule. The remaining
/// three checks mirror Longhorn's own `VolumeManager.UpdateReplicaCount`, which
/// lives in its HTTP API layer — a direct PATCH bypasses them, so we reproduce
/// them here rather than letting a caller put a volume in a confusing state.
fn check_replica_count_preconditions(
    volume: &Volume,
    desired: i64,
    allow_detached: bool,
) -> Result<(), rmcp::ErrorData> {
    let name = &volume.name;

    if !(MIN_REPLICAS..=MAX_REPLICAS).contains(&desired) {
        return Err(refuse(format!(
            "replica_count must be between {MIN_REPLICAS} and {MAX_REPLICAS}, got {desired}"
        )));
    }

    if volume.spec.data_locality.as_deref() == Some("strict-local") && desired != 1 {
        return Err(refuse(format!(
            "volume {name} has dataLocality=strict-local, which requires exactly 1 replica; \
             Longhorn's webhook would reject replica_count={desired}"
        )));
    }

    let state = volume.status.state.as_deref().unwrap_or("");
    if state != "attached" && !allow_detached {
        return Err(refuse(format!(
            "volume {name} is '{state}', not 'attached'. Longhorn accepts the change but defers \
             all rebuild work until the volume is next attached, which is why its own UI refuses \
             it. Attach the volume, or pass allow_detached=true to patch it anyway."
        )));
    }

    let spec_image = volume.spec.image.as_deref().unwrap_or("");
    let current_image = volume.status.current_image.as_deref().unwrap_or("");
    if !spec_image.is_empty() && !current_image.is_empty() && spec_image != current_image {
        return Err(refuse(format!(
            "volume {name} is mid engine upgrade (spec.image={spec_image}, \
             status.currentImage={current_image}); cannot change the replica count until it finishes"
        )));
    }

    if !volume
        .spec
        .migration_node_id
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        return Err(refuse(format!(
            "volume {name} is migrating to node '{}'; cannot change the replica count during a \
             live migration",
            volume.spec.migration_node_id.as_deref().unwrap_or("")
        )));
    }

    Ok(())
}

// ── ServerHandler ─────────────────────────────────────────────────────────────

#[tool_handler]
impl ServerHandler for StorageMcpServer {
    fn get_info(&self) -> ServerInfo {
        InitializeResult::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            ))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use crate::model::{
        Backup, BackupStatus, BackupVolume, Engine, EngineSpec, EngineStatus, KubernetesStatus,
        ReplicaSpec, Snapshot, SnapshotSpec, SnapshotStatus, VolumeSpec, VolumeStatus,
    };
    use crate::rancher::Cluster;

    const CLUSTER: &str = "c-m-abc123";
    const VOL: &str = "pvc-840804d8-6f11-49fd-afae-54bc5be639de";

    // ── Stubs ─────────────────────────────────────────────────────────────

    struct StubRancher {
        clusters: Vec<Cluster>,
    }

    impl StubRancher {
        fn new() -> Self {
            Self {
                clusters: vec![cluster("c-m-abc123", "prod"), cluster("c-m-def456", "dev")],
            }
        }
    }

    #[async_trait::async_trait]
    impl RancherApi for StubRancher {
        async fn list_clusters(&self) -> Result<Vec<Cluster>> {
            Ok(self.clusters.clone())
        }
    }

    #[derive(Default)]
    struct Recorded {
        patches: Vec<(String, i64)>,
        creates: Vec<(String, String, Option<BTreeMap<String, String>>)>,
    }

    struct StubLonghorn {
        volumes: Vec<Volume>,
        engines: Vec<Engine>,
        replicas: Mutex<Vec<Replica>>,
        snapshots: Vec<Snapshot>,
        backups: Vec<Backup>,
        /// Successive `get_snapshot` results; the last one repeats forever.
        snapshot_states: Mutex<VecDeque<Snapshot>>,
        /// When set, a patch immediately reconciles the replica list to the
        /// requested count — the "converges quickly" case.
        converge: bool,
        create_error: Option<String>,
        snapshot_readable: bool,
        snapshots_list_error: Option<String>,
        calls: Mutex<Recorded>,
    }

    impl Default for StubLonghorn {
        fn default() -> Self {
            Self {
                volumes: vec![attached_volume(VOL, 3)],
                engines: vec![engine(VOL, &["r-1", "r-2", "r-3"])],
                replicas: Mutex::new(vec![
                    replica("r-1", VOL, true),
                    replica("r-2", VOL, true),
                    replica("r-3", VOL, true),
                ]),
                snapshots: Vec::new(),
                backups: Vec::new(),
                snapshot_states: Mutex::new(VecDeque::new()),
                converge: true,
                create_error: None,
                snapshot_readable: true,
                snapshots_list_error: None,
                calls: Mutex::new(Recorded::default()),
            }
        }
    }

    #[async_trait::async_trait]
    impl LonghornApi for StubLonghorn {
        fn namespace(&self) -> &str {
            "longhorn-system"
        }

        async fn list_volumes(&self, _c: &str) -> Result<Vec<Volume>> {
            Ok(self.volumes.clone())
        }

        async fn get_volume(&self, _c: &str, name: &str) -> Result<Volume> {
            self.volumes
                .iter()
                .find(|v| v.name == name)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("volumes.longhorn.io '{name}' not found"))
        }

        async fn list_all_replicas(&self, _c: &str) -> Result<Vec<Replica>> {
            Ok(self.replicas.lock().unwrap().clone())
        }

        async fn list_engines(&self, _c: &str, volume: &str) -> Result<Vec<Engine>> {
            Ok(self
                .engines
                .iter()
                .filter(|e| e.spec.volume_name.as_deref() == Some(volume))
                .cloned()
                .collect())
        }

        async fn list_replicas(&self, _c: &str, volume: &str) -> Result<Vec<Replica>> {
            Ok(self
                .replicas
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.spec.volume_name.as_deref() == Some(volume))
                .cloned()
                .collect())
        }

        async fn list_snapshots(&self, _c: &str, volume: &str) -> Result<Vec<Snapshot>> {
            if let Some(e) = &self.snapshots_list_error {
                anyhow::bail!("{e}");
            }
            Ok(self
                .snapshots
                .iter()
                .filter(|s| s.spec.volume.as_deref() == Some(volume))
                .cloned()
                .collect())
        }

        async fn list_backups(
            &self,
            _c: &str,
            _volume: &str,
            _target: Option<&str>,
        ) -> Result<Vec<Backup>> {
            Ok(self.backups.clone())
        }

        async fn get_backup_volume(&self, _c: &str, _volume: &str) -> Result<Option<BackupVolume>> {
            Ok(None)
        }

        async fn patch_replica_count(&self, _c: &str, name: &str, count: i64) -> Result<Volume> {
            self.calls
                .lock()
                .unwrap()
                .patches
                .push((name.to_string(), count));

            if self.converge {
                let mut replicas = self.replicas.lock().unwrap();
                replicas.retain(|r| r.spec.volume_name.as_deref() != Some(name));
                for i in 0..count {
                    replicas.push(replica(&format!("r-{}", i + 1), name, true));
                }
            }

            let mut v = self.get_volume(_c, name).await?;
            v.spec.number_of_replicas = Some(count);
            Ok(v)
        }

        async fn create_snapshot(
            &self,
            _c: &str,
            snapshot_name: &str,
            volume: &str,
            labels: Option<BTreeMap<String, String>>,
        ) -> Result<Snapshot> {
            self.calls.lock().unwrap().creates.push((
                snapshot_name.to_string(),
                volume.to_string(),
                labels.clone(),
            ));
            if let Some(e) = &self.create_error {
                anyhow::bail!("{e}");
            }
            Ok(pending_snapshot(snapshot_name, volume))
        }

        async fn get_snapshot(&self, _c: &str, name: &str) -> Result<Snapshot> {
            if !self.snapshot_readable {
                anyhow::bail!("snapshots.longhorn.io '{name}' not found");
            }
            let mut states = self.snapshot_states.lock().unwrap();
            if states.len() > 1 {
                states.pop_front();
            }
            Ok(states
                .front()
                .cloned()
                .unwrap_or_else(|| ready_snapshot(name, VOL)))
        }
    }

    // ── Fixtures ──────────────────────────────────────────────────────────

    fn cluster(id: &str, name: &str) -> Cluster {
        Cluster {
            id: id.to_string(),
            name: name.to_string(),
            description: None,
            state: Some("active".to_string()),
        }
    }

    fn attached_volume(name: &str, replicas: i64) -> Volume {
        Volume {
            name: name.to_string(),
            spec: VolumeSpec {
                size: Some("10737418240".to_string()),
                number_of_replicas: Some(replicas),
                access_mode: Some("rwo".to_string()),
                data_locality: Some("disabled".to_string()),
                image: Some("longhornio/longhorn-engine:v1.9.2".to_string()),
                ..Default::default()
            },
            status: VolumeStatus {
                state: Some("attached".to_string()),
                robustness: Some("healthy".to_string()),
                current_node_id: Some("node-1".to_string()),
                current_image: Some("longhornio/longhorn-engine:v1.9.2".to_string()),
                actual_size: Some(2_147_483_648),
                kubernetes_status: Some(KubernetesStatus {
                    namespace: Some("postgres".to_string()),
                    pvc_name: Some("data-postgres-0".to_string()),
                    pv_name: Some(name.to_string()),
                    pv_status: Some("Bound".to_string()),
                    workloads_status: None,
                }),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn volume_for_pvc(name: &str, ns: &str, pvc: &str) -> Volume {
        let mut v = attached_volume(name, 3);
        let ks = v.status.kubernetes_status.as_mut().unwrap();
        ks.namespace = Some(ns.to_string());
        ks.pvc_name = Some(pvc.to_string());
        v
    }

    fn engine(volume: &str, replicas: &[&str]) -> Engine {
        Engine {
            name: format!("{volume}-e-0"),
            spec: EngineSpec {
                volume_name: Some(volume.to_string()),
                active: Some(true),
                ..Default::default()
            },
            status: EngineStatus {
                current_state: Some("running".to_string()),
                endpoint: Some(format!("/dev/longhorn/{volume}")),
                replica_mode_map: Some(
                    replicas
                        .iter()
                        .map(|r| (r.to_string(), "RW".to_string()))
                        .collect(),
                ),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn replica(name: &str, volume: &str, healthy: bool) -> Replica {
        Replica {
            name: name.to_string(),
            spec: ReplicaSpec {
                volume_name: Some(volume.to_string()),
                node_id: Some("node-1".to_string()),
                disk_id: Some("disk-uuid".to_string()),
                active: Some(true),
                failed_at: Some(String::new()),
                healthy_at: Some(if healthy {
                    "2026-09-01T10:00:00Z".to_string()
                } else {
                    String::new()
                }),
                eviction_requested: Some(false),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn pending_snapshot(name: &str, volume: &str) -> Snapshot {
        Snapshot {
            name: name.to_string(),
            spec: SnapshotSpec {
                volume: Some(volume.to_string()),
                create_snapshot: Some(true),
                ..Default::default()
            },
            status: SnapshotStatus {
                creation_time: Some(String::new()),
                ready_to_use: Some(false),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn ready_snapshot(name: &str, volume: &str) -> Snapshot {
        let mut s = pending_snapshot(name, volume);
        s.status.creation_time = Some("2026-09-02T14:30:12Z".to_string());
        s.status.ready_to_use = Some(true);
        s.status.size = Some(1_048_576);
        s.status.user_created = Some(true);
        s
    }

    fn detached_snapshot_error(name: &str, volume: &str) -> Snapshot {
        let mut s = pending_snapshot(name, volume);
        s.status.error = Some(
            "failed to take snapshot because the volume engine is not running. Waiting for the \
             volume to be attached"
                .to_string(),
        );
        s
    }

    fn server(longhorn: StubLonghorn) -> StorageMcpServer {
        // Sub-second polling keeps the timeout branches fast to test.
        StorageMcpServer::new(StubRancher::new(), longhorn)
            .with_poll(Duration::from_millis(120), Duration::from_millis(20))
    }

    // ── list_clusters ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn list_clusters_renders_id_name_and_state() {
        let out = server(StubLonghorn::default())
            .list_clusters(Parameters(ListClustersInput {}))
            .await
            .unwrap();
        assert!(out.contains("c-m-abc123"), "output: {out}");
        assert!(out.contains("name=prod"), "output: {out}");
        assert!(out.contains("state=active"), "output: {out}");
    }

    // ── resolve_cluster_id ────────────────────────────────────────────────

    #[tokio::test]
    async fn cluster_ids_and_local_short_circuit_the_lookup() {
        let s = server(StubLonghorn::default());
        assert_eq!(s.resolve_cluster_id("c-m-zzz").await.unwrap(), "c-m-zzz");
        assert_eq!(s.resolve_cluster_id("local").await.unwrap(), "local");
    }

    #[tokio::test]
    async fn cluster_names_resolve_case_insensitively_and_report_misses() {
        let s = server(StubLonghorn::default());
        assert_eq!(s.resolve_cluster_id("PROD").await.unwrap(), "c-m-abc123");

        let err = s.resolve_cluster_id("staging").await.unwrap_err();
        assert!(
            format!("{err:?}").contains("list_clusters"),
            "error: {err:?}"
        );
    }

    // ── list_volumes ──────────────────────────────────────────────────────

    fn list_input(cluster: &str) -> ListVolumesInput {
        ListVolumesInput {
            cluster: cluster.to_string(),
            namespace: None,
            state: None,
            robustness: None,
            name_contains: None,
        }
    }

    #[tokio::test]
    async fn list_volumes_renders_pvc_state_and_replica_counts() {
        let out = server(StubLonghorn::default())
            .list_volumes(Parameters(list_input(CLUSTER)))
            .await
            .unwrap();

        assert!(out.contains(VOL), "output: {out}");
        assert!(
            out.contains("pvc=postgres/data-postgres-0"),
            "output: {out}"
        );
        assert!(out.contains("state=attached"), "output: {out}");
        assert!(out.contains("replicas=3/3"), "output: {out}");
        assert!(out.contains("size=10Gi"), "output: {out}");
    }

    #[tokio::test]
    async fn list_volumes_counts_only_healthy_replicas_against_the_desired_count() {
        let stub = StubLonghorn {
            replicas: Mutex::new(vec![
                replica("r-1", VOL, true),
                replica("r-2", VOL, true),
                // Rebuilding: usable but not yet healthy.
                replica("r-3", VOL, false),
            ]),
            ..Default::default()
        };
        let out = server(stub)
            .list_volumes(Parameters(list_input(CLUSTER)))
            .await
            .unwrap();
        assert!(out.contains("replicas=2/3"), "output: {out}");
    }

    #[tokio::test]
    async fn list_volumes_filters_by_namespace_state_robustness_and_substring() {
        let stub = StubLonghorn {
            volumes: vec![
                volume_for_pvc("pvc-aaa", "postgres", "data-postgres-0"),
                volume_for_pvc("pvc-bbb", "redis", "data-redis-0"),
            ],
            ..Default::default()
        };
        let s = server(stub);

        let out = s
            .list_volumes(Parameters(ListVolumesInput {
                namespace: Some("redis".to_string()),
                ..list_input(CLUSTER)
            }))
            .await
            .unwrap();
        assert!(out.contains("pvc-bbb"), "output: {out}");
        assert!(!out.contains("pvc-aaa"), "output: {out}");

        let out = s
            .list_volumes(Parameters(ListVolumesInput {
                name_contains: Some("POSTGRES".to_string()),
                ..list_input(CLUSTER)
            }))
            .await
            .unwrap();
        assert!(
            out.contains("pvc-aaa"),
            "substring match is case-insensitive: {out}"
        );
        assert!(!out.contains("pvc-bbb"), "output: {out}");

        let out = s
            .list_volumes(Parameters(ListVolumesInput {
                robustness: Some("degraded".to_string()),
                ..list_input(CLUSTER)
            }))
            .await
            .unwrap();
        assert!(out.contains("No Longhorn volumes matched"), "output: {out}");
        assert!(out.contains("2 volume(s) present"), "output: {out}");
    }

    // ── resolve_volume_name ───────────────────────────────────────────────

    fn inspect_input(volume: &str) -> InspectVolumeInput {
        InspectVolumeInput {
            cluster: CLUSTER.to_string(),
            volume: volume.to_string(),
        }
    }

    #[tokio::test]
    async fn volumes_resolve_by_longhorn_name_namespaced_pvc_and_bare_pvc() {
        let s = server(StubLonghorn::default());
        for reference in [VOL, "postgres/data-postgres-0", "data-postgres-0"] {
            let out = s
                .inspect_volume(Parameters(inspect_input(reference)))
                .await
                .unwrap_or_else(|e| panic!("resolving '{reference}' failed: {e:?}"));
            assert!(out.contains(&format!("Volume: {VOL}")), "output: {out}");
        }
    }

    #[tokio::test]
    async fn an_ambiguous_bare_pvc_name_lists_the_candidates_instead_of_guessing() {
        let stub = StubLonghorn {
            volumes: vec![
                volume_for_pvc("pvc-aaa", "postgres", "data"),
                volume_for_pvc("pvc-bbb", "redis", "data"),
            ],
            ..Default::default()
        };
        let err = server(stub)
            .inspect_volume(Parameters(inspect_input("data")))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("ambiguous"), "error: {msg}");
        assert!(
            msg.contains("postgres/data") && msg.contains("redis/data"),
            "error: {msg}"
        );
    }

    #[tokio::test]
    async fn a_namespace_qualifier_disambiguates() {
        let stub = StubLonghorn {
            volumes: vec![
                volume_for_pvc("pvc-aaa", "postgres", "data"),
                volume_for_pvc("pvc-bbb", "redis", "data"),
            ],
            engines: vec![engine("pvc-bbb", &[])],
            ..Default::default()
        };
        let out = server(stub)
            .inspect_volume(Parameters(inspect_input("redis/data")))
            .await
            .unwrap();
        assert!(out.contains("Volume: pvc-bbb"), "output: {out}");
    }

    #[tokio::test]
    async fn an_unknown_volume_points_at_list_volumes() {
        let err = server(StubLonghorn::default())
            .inspect_volume(Parameters(inspect_input("nope")))
            .await
            .unwrap_err();
        assert!(
            format!("{err:?}").contains("list_volumes"),
            "error: {err:?}"
        );
    }

    // ── inspect_volume ────────────────────────────────────────────────────

    #[tokio::test]
    async fn inspect_volume_reports_replica_mode_from_the_engine() {
        let mut e = engine(VOL, &["r-1", "r-2"]);
        e.status
            .replica_mode_map
            .as_mut()
            .unwrap()
            .insert("r-3".to_string(), "ERR".to_string());
        let stub = StubLonghorn {
            engines: vec![e],
            ..Default::default()
        };

        let out = server(stub)
            .inspect_volume(Parameters(inspect_input(VOL)))
            .await
            .unwrap();

        assert!(out.contains("Replicas (3)"), "output: {out}");
        assert!(
            out.contains("r-3") && out.contains("mode=ERR"),
            "output: {out}"
        );
        assert!(out.contains("desired=3 healthy=3"), "output: {out}");
        assert!(
            out.contains(&format!("/dev/longhorn/{VOL}")),
            "output: {out}"
        );
    }

    #[tokio::test]
    async fn inspect_volume_surfaces_conditions_that_are_not_true() {
        let mut v = attached_volume(VOL, 3);
        v.status.robustness = Some("degraded".to_string());
        v.status.conditions = Some(vec![crate::model::Condition {
            condition_type: Some("Scheduled".to_string()),
            status: Some("False".to_string()),
            reason: Some("ReplicaSchedulingFailure".to_string()),
            message: Some("no available disk".to_string()),
            last_transition_time: None,
        }]);
        let stub = StubLonghorn {
            volumes: vec![v],
            ..Default::default()
        };

        let out = server(stub)
            .inspect_volume(Parameters(inspect_input(VOL)))
            .await
            .unwrap();
        assert!(out.contains("Conditions (not True)"), "output: {out}");
        assert!(out.contains("Scheduled=False"), "output: {out}");
        assert!(out.contains("no available disk"), "output: {out}");
    }

    #[tokio::test]
    async fn inspect_volume_falls_back_to_engine_snapshots_and_filters_volume_head() {
        let mut e = engine(VOL, &["r-1"]);
        e.status.snapshots = Some(
            [
                (
                    "snap-1".to_string(),
                    serde_json::from_value(serde_json::json!({
                        "name": "snap-1", "created": "2026-09-01T10:00:00Z",
                        "size": "1048576", "usercreated": true, "removed": false
                    }))
                    .unwrap(),
                ),
                (
                    "snap-gone".to_string(),
                    serde_json::from_value(serde_json::json!({
                        "name": "snap-gone", "removed": true
                    }))
                    .unwrap(),
                ),
                (
                    "volume-head".to_string(),
                    serde_json::from_value(serde_json::json!({ "name": "volume-head" })).unwrap(),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let stub = StubLonghorn {
            engines: vec![e],
            snapshots: Vec::new(),
            ..Default::default()
        };

        let out = server(stub)
            .inspect_volume(Parameters(inspect_input(VOL)))
            .await
            .unwrap();

        assert!(
            out.contains("read from engine.status.snapshots"),
            "output: {out}"
        );
        assert!(out.contains("snap-1"), "output: {out}");
        assert!(
            !out.contains("volume-head"),
            "volume-head is synthetic: {out}"
        );
        assert!(
            !out.contains("snap-gone"),
            "removed snapshots are hidden: {out}"
        );
    }

    #[tokio::test]
    async fn inspect_volume_prefers_snapshot_crs_when_present() {
        let stub = StubLonghorn {
            snapshots: vec![ready_snapshot("snap-cr", VOL)],
            ..Default::default()
        };
        let out = server(stub)
            .inspect_volume(Parameters(inspect_input(VOL)))
            .await
            .unwrap();
        assert!(out.contains("Snapshots (1)"), "output: {out}");
        assert!(
            out.contains("snap-cr") && out.contains("ready=true"),
            "output: {out}"
        );
        assert!(
            !out.contains("read from engine.status.snapshots"),
            "output: {out}"
        );
    }

    #[tokio::test]
    async fn inspect_volume_degrades_gracefully_when_snapshots_cannot_be_listed() {
        let stub = StubLonghorn {
            snapshots_list_error: Some("snapshots.longhorn.io is forbidden".to_string()),
            ..Default::default()
        };
        let out = server(stub)
            .inspect_volume(Parameters(inspect_input(VOL)))
            .await
            .unwrap();

        // The volume and replica detail the caller asked for must still arrive.
        assert!(out.contains("Replicas (3)"), "output: {out}");
        assert!(out.contains("Notes:"), "output: {out}");
        assert!(out.contains("could not list snapshots"), "output: {out}");
        assert!(
            out.contains("is forbidden"),
            "the cause must survive: {out}"
        );
    }

    #[tokio::test]
    async fn inspect_volume_renders_backups_with_their_state() {
        let stub = StubLonghorn {
            backups: vec![Backup {
                name: "backup-1".to_string(),
                status: BackupStatus {
                    state: Some("Completed".to_string()),
                    snapshot_name: Some("snap-1".to_string()),
                    size: Some("1048576".to_string()),
                    backup_created_at: Some("2026-09-01T11:00:00Z".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let out = server(stub)
            .inspect_volume(Parameters(inspect_input(VOL)))
            .await
            .unwrap();
        assert!(out.contains("Backups (1)"), "output: {out}");
        assert!(out.contains("state=Completed"), "output: {out}");
        assert!(out.contains("size=1Mi"), "output: {out}");
    }

    // ── set_volume_replica_count ──────────────────────────────────────────

    fn replica_input(count: i64) -> SetReplicaCountInput {
        SetReplicaCountInput {
            cluster: CLUSTER.to_string(),
            volume: VOL.to_string(),
            replica_count: count,
            allow_detached: None,
        }
    }

    async fn expect_refusal(stub: StubLonghorn, input: SetReplicaCountInput, needle: &str) {
        let s = server(stub);
        let err = s
            .set_volume_replica_count(Parameters(input))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains(needle),
            "error should mention '{needle}': {msg}"
        );
    }

    #[tokio::test]
    async fn replica_count_outside_one_to_twenty_is_refused_before_any_request() {
        for bad in [0, -1, 21, 100] {
            let stub = StubLonghorn::default();
            let s = server(stub);
            let err = s
                .set_volume_replica_count(Parameters(replica_input(bad)))
                .await
                .unwrap_err();
            let msg = format!("{err:?}");
            assert!(msg.contains("between 1 and 20"), "count {bad}: {msg}");
        }
    }

    #[tokio::test]
    async fn strict_local_volumes_must_keep_exactly_one_replica() {
        let mut v = attached_volume(VOL, 1);
        v.spec.data_locality = Some("strict-local".to_string());
        expect_refusal(
            StubLonghorn {
                volumes: vec![v],
                ..Default::default()
            },
            replica_input(3),
            "strict-local",
        )
        .await;
    }

    #[tokio::test]
    async fn a_detached_volume_is_refused_by_default_and_allowed_on_request() {
        let mut v = attached_volume(VOL, 3);
        v.status.state = Some("detached".to_string());
        v.status.current_node_id = Some(String::new());

        expect_refusal(
            StubLonghorn {
                volumes: vec![v.clone()],
                ..Default::default()
            },
            replica_input(2),
            "allow_detached=true",
        )
        .await;

        let stub = StubLonghorn {
            volumes: vec![v],
            ..Default::default()
        };
        let s = server(stub);
        let out = s
            .set_volume_replica_count(Parameters(SetReplicaCountInput {
                allow_detached: Some(true),
                ..replica_input(2)
            }))
            .await
            .unwrap();
        assert!(out.contains("from 3 to 2"), "output: {out}");
    }

    #[tokio::test]
    async fn an_engine_upgrade_in_flight_blocks_the_change() {
        let mut v = attached_volume(VOL, 3);
        v.spec.image = Some("longhornio/longhorn-engine:v1.9.2".to_string());
        v.status.current_image = Some("longhornio/longhorn-engine:v1.8.1".to_string());
        expect_refusal(
            StubLonghorn {
                volumes: vec![v],
                ..Default::default()
            },
            replica_input(2),
            "engine upgrade",
        )
        .await;
    }

    #[tokio::test]
    async fn a_live_migration_in_flight_blocks_the_change() {
        let mut v = attached_volume(VOL, 3);
        v.spec.migration_node_id = Some("node-2".to_string());
        expect_refusal(
            StubLonghorn {
                volumes: vec![v],
                ..Default::default()
            },
            replica_input(2),
            "migrating to node 'node-2'",
        )
        .await;
    }

    #[tokio::test]
    async fn a_no_op_change_skips_the_patch_entirely() {
        let stub = StubLonghorn::default();
        let s = server(stub);
        let out = s
            .set_volume_replica_count(Parameters(replica_input(3)))
            .await
            .unwrap();
        assert!(
            out.contains("already has spec.numberOfReplicas=3"),
            "output: {out}"
        );
        assert!(out.contains("no change made"), "output: {out}");
        assert!(
            out.contains("desired=3 usable=3 healthy=3"),
            "output: {out}"
        );
    }

    #[tokio::test]
    async fn scaling_up_warns_that_the_volume_stays_degraded_during_rebuild() {
        let stub = StubLonghorn {
            volumes: vec![attached_volume(VOL, 2)],
            replicas: Mutex::new(vec![replica("r-1", VOL, true), replica("r-2", VOL, true)]),
            ..Default::default()
        };
        let out = server(stub)
            .set_volume_replica_count(Parameters(replica_input(3)))
            .await
            .unwrap();

        assert!(out.contains("from 2 to 3"), "output: {out}");
        assert!(out.contains("Replica count reached"), "output: {out}");
        assert!(out.contains("degraded"), "output: {out}");
        assert!(out.contains("inspect_volume"), "output: {out}");
    }

    #[tokio::test]
    async fn a_slow_scale_down_reports_that_it_is_still_converging() {
        let stub = StubLonghorn {
            // The stub does not reconcile, so the usable count never reaches 2.
            converge: false,
            ..Default::default()
        };
        let out = server(stub)
            .set_volume_replica_count(Parameters(replica_input(2)))
            .await
            .unwrap();

        assert!(out.contains("Still converging"), "output: {out}");
        assert!(out.contains("one excess replica per"), "output: {out}");
        assert!(out.contains("inspect_volume"), "output: {out}");
        // Must not claim success it did not observe.
        assert!(!out.contains("Replica count reached"), "output: {out}");
    }

    #[tokio::test]
    async fn best_effort_auto_balance_is_called_out() {
        let mut v = attached_volume(VOL, 3);
        v.spec.replica_auto_balance = Some("best-effort".to_string());
        let stub = StubLonghorn {
            volumes: vec![v],
            ..Default::default()
        };
        let out = server(stub)
            .set_volume_replica_count(Parameters(replica_input(2)))
            .await
            .unwrap();
        assert!(
            out.contains("replicaAutoBalance is 'best-effort'"),
            "output: {out}"
        );
        assert!(out.contains("names are not stable"), "output: {out}");
    }

    // ── create_volume_snapshot ────────────────────────────────────────────

    fn snapshot_input(name: Option<&str>) -> CreateSnapshotInput {
        CreateSnapshotInput {
            cluster: CLUSTER.to_string(),
            volume: VOL.to_string(),
            snapshot_name: name.map(str::to_string),
            labels: None,
        }
    }

    #[tokio::test]
    async fn a_ready_snapshot_reports_its_creation_time_and_size() {
        let stub = StubLonghorn {
            snapshot_states: Mutex::new(VecDeque::from(vec![ready_snapshot("snap-1", VOL)])),
            ..Default::default()
        };
        let out = server(stub)
            .create_volume_snapshot(Parameters(snapshot_input(Some("snap-1"))))
            .await
            .unwrap();

        assert!(out.contains("Snapshot: snap-1"), "output: {out}");
        assert!(out.contains("Ready:   yes"), "output: {out}");
        assert!(out.contains("2026-09-02T14:30:12Z"), "output: {out}");
        assert!(out.contains("Size:    1Mi"), "output: {out}");
    }

    #[tokio::test]
    async fn polling_waits_for_ready_rather_than_reporting_the_first_read() {
        let stub = StubLonghorn {
            snapshot_states: Mutex::new(VecDeque::from(vec![
                pending_snapshot("snap-1", VOL),
                detached_snapshot_error("snap-1", VOL),
                ready_snapshot("snap-1", VOL),
            ])),
            ..Default::default()
        };
        let out = server(stub)
            .create_volume_snapshot(Parameters(snapshot_input(Some("snap-1"))))
            .await
            .unwrap();
        assert!(
            out.contains("Ready:   yes"),
            "a transient error must not end the poll: {out}"
        );
    }

    #[tokio::test]
    async fn a_transient_engine_error_is_reported_as_waiting_not_as_failure() {
        let stub = StubLonghorn {
            snapshot_states: Mutex::new(VecDeque::from(vec![detached_snapshot_error(
                "snap-1", VOL,
            )])),
            ..Default::default()
        };
        let out = server(stub)
            .create_volume_snapshot(Parameters(snapshot_input(Some("snap-1"))))
            .await
            .expect("a pending snapshot is not an error");

        assert!(out.contains("Ready:   not yet"), "output: {out}");
        assert!(out.contains("usually transient"), "output: {out}");
        assert!(out.contains("VolumeAttachment ticket"), "output: {out}");
        assert!(
            out.contains("snapshot_name=\"snap-1\""),
            "it must tell the caller how to re-check idempotently: {out}"
        );
    }

    #[tokio::test]
    async fn an_omitted_name_gets_the_volume_and_a_utc_timestamp() {
        let stub = StubLonghorn::default();
        let out = server(stub)
            .create_volume_snapshot(Parameters(snapshot_input(None)))
            .await
            .unwrap();

        let line = out.lines().next().unwrap();
        assert!(
            line.starts_with(&format!("Snapshot: {VOL}-mcp-")),
            "output: {out}"
        );
        assert!(
            line.trim_start_matches("Snapshot: ")
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "generated names must be DNS-1123 safe: {line}"
        );
    }

    #[tokio::test]
    async fn an_existing_snapshot_with_the_same_name_is_a_successful_retry() {
        let stub = StubLonghorn {
            create_error: Some(
                "snapshots.longhorn.io \"snap-1\" already exists (AlreadyExists)".to_string(),
            ),
            snapshot_states: Mutex::new(VecDeque::from(vec![ready_snapshot("snap-1", VOL)])),
            ..Default::default()
        };
        let out = server(stub)
            .create_volume_snapshot(Parameters(snapshot_input(Some("snap-1"))))
            .await
            .unwrap();

        assert!(out.contains("already existed"), "output: {out}");
        assert!(out.contains("no second snapshot"), "output: {out}");
        assert!(out.contains("Ready:   yes"), "output: {out}");
    }

    #[tokio::test]
    async fn a_genuine_create_failure_is_propagated() {
        let stub = StubLonghorn {
            create_error: Some("admission webhook denied the request: bad volume".to_string()),
            // Nothing to fall back to: the snapshot really does not exist.
            snapshot_readable: false,
            ..Default::default()
        };
        let err = server(stub)
            .create_volume_snapshot(Parameters(snapshot_input(Some("snap-1"))))
            .await
            .unwrap_err();
        assert!(
            format!("{err:?}").contains("admission webhook denied"),
            "the webhook message must survive: {err:?}"
        );
    }

    #[tokio::test]
    async fn snapshots_of_an_unknown_volume_are_refused_before_creating_anything() {
        let err = server(StubLonghorn::default())
            .create_volume_snapshot(Parameters(CreateSnapshotInput {
                volume: "nope".to_string(),
                ..snapshot_input(None)
            }))
            .await
            .unwrap_err();
        assert!(
            format!("{err:?}").contains("list_volumes"),
            "error: {err:?}"
        );
    }

    // ── Recorded calls ────────────────────────────────────────────────────

    /// The stub records what reached the Longhorn layer, so these assert the
    /// wire-level intent rather than the rendered text.
    #[tokio::test]
    async fn recorded_calls_carry_exactly_the_intended_mutation() {
        // Sharing the stub with the server requires reading the recording back
        // out through a channel of our own: build the stub, keep a handle to
        // its `Arc`-free interior by constructing the server from a wrapper.
        struct Shared(Arc<StubLonghorn>);

        #[async_trait::async_trait]
        impl LonghornApi for Shared {
            fn namespace(&self) -> &str {
                self.0.namespace()
            }
            async fn list_volumes(&self, c: &str) -> Result<Vec<Volume>> {
                self.0.list_volumes(c).await
            }
            async fn get_volume(&self, c: &str, n: &str) -> Result<Volume> {
                self.0.get_volume(c, n).await
            }
            async fn list_all_replicas(&self, c: &str) -> Result<Vec<Replica>> {
                self.0.list_all_replicas(c).await
            }
            async fn list_engines(&self, c: &str, v: &str) -> Result<Vec<Engine>> {
                self.0.list_engines(c, v).await
            }
            async fn list_replicas(&self, c: &str, v: &str) -> Result<Vec<Replica>> {
                self.0.list_replicas(c, v).await
            }
            async fn list_snapshots(&self, c: &str, v: &str) -> Result<Vec<Snapshot>> {
                self.0.list_snapshots(c, v).await
            }
            async fn list_backups(&self, c: &str, v: &str, t: Option<&str>) -> Result<Vec<Backup>> {
                self.0.list_backups(c, v, t).await
            }
            async fn get_backup_volume(&self, c: &str, v: &str) -> Result<Option<BackupVolume>> {
                self.0.get_backup_volume(c, v).await
            }
            async fn patch_replica_count(&self, c: &str, n: &str, k: i64) -> Result<Volume> {
                self.0.patch_replica_count(c, n, k).await
            }
            async fn create_snapshot(
                &self,
                c: &str,
                s: &str,
                v: &str,
                l: Option<BTreeMap<String, String>>,
            ) -> Result<Snapshot> {
                self.0.create_snapshot(c, s, v, l).await
            }
            async fn get_snapshot(&self, c: &str, n: &str) -> Result<Snapshot> {
                self.0.get_snapshot(c, n).await
            }
        }

        let stub = Arc::new(StubLonghorn {
            snapshot_states: Mutex::new(VecDeque::from(vec![ready_snapshot("snap-1", VOL)])),
            ..Default::default()
        });
        let s = StorageMcpServer::new(StubRancher::new(), Shared(Arc::clone(&stub)))
            .with_poll(Duration::from_millis(120), Duration::from_millis(20));

        s.set_volume_replica_count(Parameters(replica_input(2)))
            .await
            .unwrap();

        let mut labels = BTreeMap::new();
        labels.insert("purpose".to_string(), "pre-upgrade".to_string());
        s.create_volume_snapshot(Parameters(CreateSnapshotInput {
            snapshot_name: Some("snap-1".to_string()),
            labels: Some(labels.clone()),
            ..snapshot_input(None)
        }))
        .await
        .unwrap();

        let calls = stub.calls.lock().unwrap();
        assert_eq!(calls.patches, vec![(VOL.to_string(), 2)]);
        assert_eq!(
            calls.creates,
            vec![("snap-1".to_string(), VOL.to_string(), Some(labels))],
        );
    }
}
