//! The Longhorn CRD layer: `longhorn.io/v1beta2` via `kube`'s dynamic API.
//!
//! Pinned to `v1beta2` for every kind. `v1beta1` is `served: false` from
//! Longhorn 1.9 and would 404; `v1beta2` is the storage version on 1.7, 1.8 and
//! 1.9 alike.
//!
//! `Engine` and `Replica` are strictly read-only here — the volume controller
//! owns their spec. The only writes this module makes are a JSON-merge patch of
//! `volume.spec.numberOfReplicas` and the creation of a `Snapshot` CR.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use kube::api::{Api, DynamicObject, ListParams, Patch, PatchParams, PostParams};
use kube::core::{ApiResource, GroupVersionKind};
use kube::Client;
use tracing::{debug, warn};

use crate::cluster::ClusterClients;
use crate::model::{Backup, BackupVolume, Engine, Replica, Snapshot, Volume};

const GROUP: &str = "longhorn.io";
const VERSION: &str = "v1beta2";

/// Label Longhorn puts on the engines, replicas and snapshots of a volume.
/// Bare key — there is no `longhorn.io/` prefix.
const LABEL_VOLUME: &str = "longhornvolume";
/// Label linking a `Backup` to its volume. `Backup` has no `spec.volumeName`,
/// so this label is the only queryable link.
const LABEL_BACKUP_VOLUME: &str = "backup-volume";
const LABEL_BACKUP_TARGET: &str = "backup-target";

/// Plurals are given explicitly: naive pluralization is wrong for some kinds,
/// and `nodes`, `volumeattachments` and `settings` collide with core and
/// `storage.k8s.io` plurals, so the group must always qualify them.
fn resource(kind: &str, plural: &str) -> ApiResource {
    ApiResource::from_gvk_with_plural(&GroupVersionKind::gvk(GROUP, VERSION, kind), plural)
}

fn volumes() -> ApiResource {
    resource("Volume", "volumes")
}
fn engines() -> ApiResource {
    resource("Engine", "engines")
}
fn replicas() -> ApiResource {
    resource("Replica", "replicas")
}
fn snapshots() -> ApiResource {
    resource("Snapshot", "snapshots")
}
fn backups() -> ApiResource {
    resource("Backup", "backups")
}
fn backup_volumes() -> ApiResource {
    resource("BackupVolume", "backupvolumes")
}

#[async_trait]
pub trait LonghornApi: Send + Sync {
    fn namespace(&self) -> &str;

    async fn list_volumes(&self, cluster_id: &str) -> Result<Vec<Volume>>;
    async fn get_volume(&self, cluster_id: &str, name: &str) -> Result<Volume>;
    /// Every replica in the namespace, so a listing can group by volume without
    /// one request per volume.
    async fn list_all_replicas(&self, cluster_id: &str) -> Result<Vec<Replica>>;
    async fn list_engines(&self, cluster_id: &str, volume: &str) -> Result<Vec<Engine>>;
    async fn list_replicas(&self, cluster_id: &str, volume: &str) -> Result<Vec<Replica>>;
    async fn list_snapshots(&self, cluster_id: &str, volume: &str) -> Result<Vec<Snapshot>>;
    async fn list_backups(
        &self,
        cluster_id: &str,
        volume: &str,
        backup_target: Option<&str>,
    ) -> Result<Vec<Backup>>;
    async fn get_backup_volume(
        &self,
        cluster_id: &str,
        volume: &str,
    ) -> Result<Option<BackupVolume>>;

    /// JSON-merge patch of `spec.numberOfReplicas`.
    async fn patch_replica_count(&self, cluster_id: &str, name: &str, count: i64)
        -> Result<Volume>;

    /// Creates a `Snapshot` CR. The CR name *is* the Longhorn snapshot name.
    async fn create_snapshot(
        &self,
        cluster_id: &str,
        snapshot_name: &str,
        volume: &str,
        labels: Option<BTreeMap<String, String>>,
    ) -> Result<Snapshot>;

    async fn get_snapshot(&self, cluster_id: &str, name: &str) -> Result<Snapshot>;
}

pub struct LonghornClient {
    clusters: ClusterClients,
}

impl LonghornClient {
    pub fn new(clusters: ClusterClients) -> Self {
        Self { clusters }
    }

    fn api(&self, cluster_id: &str, ar: &ApiResource) -> Result<Api<DynamicObject>> {
        let client: Client = self.clusters.client(cluster_id)?;
        Ok(Api::namespaced_with(client, self.clusters.namespace(), ar))
    }

    async fn list_raw(
        &self,
        cluster_id: &str,
        ar: &ApiResource,
        selector: Option<&str>,
    ) -> Result<Vec<DynamicObject>> {
        let api = self.api(cluster_id, ar)?;
        let mut lp = ListParams::default();
        if let Some(sel) = selector {
            lp = lp.labels(sel);
        }
        let list = api.list(&lp).await.with_context(|| {
            format!(
                "failed to list {} in namespace '{}' of cluster '{}'",
                ar.plural,
                self.clusters.namespace(),
                cluster_id
            )
        })?;
        Ok(list.items)
    }
}

/// One request by label, and — only if that comes back empty — a full list
/// filtered client-side.
macro_rules! list_for_volume {
    ($self:expr, $cluster:expr, $ar:expr, $label:expr, $ty:ty, $owner:expr) => {{
        let ar = $ar;
        let selector = $label;
        let mut items = $self.list_raw($cluster, &ar, Some(&selector)).await?;
        if items.is_empty() {
            debug!(%selector, plural = %ar.plural, "label selector matched nothing; falling back to a full list");
            items = $self.list_raw($cluster, &ar, None).await?;
        }
        let owner: &dyn Fn(&$ty) -> bool = &$owner;
        let mut out = Vec::new();
        for obj in items {
            let name = obj.metadata.name.clone().unwrap_or_default();
            match <$ty>::from_dynamic(obj) {
                Ok(decoded) => {
                    if owner(&decoded) {
                        out.push(decoded);
                    }
                }
                // One malformed object must not blank out the whole listing.
                Err(e) => warn!(%name, plural = %ar.plural, "skipping undecodable object: {e:#}"),
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }};
}

#[async_trait]
impl LonghornApi for LonghornClient {
    fn namespace(&self) -> &str {
        self.clusters.namespace()
    }

    async fn list_volumes(&self, cluster_id: &str) -> Result<Vec<Volume>> {
        let mut out = Vec::new();
        for obj in self.list_raw(cluster_id, &volumes(), None).await? {
            let name = obj.metadata.name.clone().unwrap_or_default();
            match Volume::from_dynamic(obj) {
                Ok(v) => out.push(v),
                Err(e) => warn!(%name, "skipping undecodable Volume: {e:#}"),
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn get_volume(&self, cluster_id: &str, name: &str) -> Result<Volume> {
        let obj = self
            .api(cluster_id, &volumes())?
            .get(name)
            .await
            .with_context(|| format!("failed to get Longhorn volume '{name}'"))?;
        Volume::from_dynamic(obj)
    }

    async fn list_all_replicas(&self, cluster_id: &str) -> Result<Vec<Replica>> {
        let mut out = Vec::new();
        for obj in self.list_raw(cluster_id, &replicas(), None).await? {
            let name = obj.metadata.name.clone().unwrap_or_default();
            match Replica::from_dynamic(obj) {
                Ok(r) => out.push(r),
                Err(e) => warn!(%name, "skipping undecodable Replica: {e:#}"),
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn list_engines(&self, cluster_id: &str, volume: &str) -> Result<Vec<Engine>> {
        list_for_volume!(
            self,
            cluster_id,
            engines(),
            format!("{LABEL_VOLUME}={volume}"),
            Engine,
            |e: &Engine| e.spec.volume_name.as_deref() == Some(volume)
        )
    }

    async fn list_replicas(&self, cluster_id: &str, volume: &str) -> Result<Vec<Replica>> {
        list_for_volume!(
            self,
            cluster_id,
            replicas(),
            format!("{LABEL_VOLUME}={volume}"),
            Replica,
            |r: &Replica| r.spec.volume_name.as_deref() == Some(volume)
        )
    }

    async fn list_snapshots(&self, cluster_id: &str, volume: &str) -> Result<Vec<Snapshot>> {
        // Note `Snapshot` uses `spec.volume`, not `spec.volumeName`.
        list_for_volume!(
            self,
            cluster_id,
            snapshots(),
            format!("{LABEL_VOLUME}={volume}"),
            Snapshot,
            |s: &Snapshot| s.spec.volume.as_deref() == Some(volume)
        )
    }

    async fn list_backups(
        &self,
        cluster_id: &str,
        volume: &str,
        backup_target: Option<&str>,
    ) -> Result<Vec<Backup>> {
        // Multiple backup targets can hold same-named volumes (1.8+), so
        // constrain by target too when the volume names one.
        let mut selector = format!("{LABEL_BACKUP_VOLUME}={volume}");
        if let Some(target) = backup_target.filter(|t| !t.is_empty()) {
            selector.push_str(&format!(",{LABEL_BACKUP_TARGET}={target}"));
        }

        let mut out = Vec::new();
        for obj in self
            .list_raw(cluster_id, &backups(), Some(&selector))
            .await?
        {
            let name = obj.metadata.name.clone().unwrap_or_default();
            match Backup::from_dynamic(obj) {
                Ok(b) => out.push(b),
                Err(e) => warn!(%name, "skipping undecodable Backup: {e:#}"),
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn get_backup_volume(
        &self,
        cluster_id: &str,
        volume: &str,
    ) -> Result<Option<BackupVolume>> {
        // `BackupVolume` CR names are not reliably equal to the volume name
        // across Longhorn versions, but it does carry `spec.volumeName`.
        for obj in self.list_raw(cluster_id, &backup_volumes(), None).await? {
            let name = obj.metadata.name.clone().unwrap_or_default();
            match BackupVolume::from_dynamic(obj) {
                Ok(bv) if bv.spec.volume_name.as_deref() == Some(volume) => {
                    return Ok(Some(bv));
                }
                Ok(_) => {}
                Err(e) => warn!(%name, "skipping undecodable BackupVolume: {e:#}"),
            }
        }
        Ok(None)
    }

    async fn patch_replica_count(
        &self,
        cluster_id: &str,
        name: &str,
        count: i64,
    ) -> Result<Volume> {
        let api = self.api(cluster_id, &volumes())?;
        // A minimal merge patch, never GET-mutate-PUT: the volume validator
        // rejects an empty `spec.backupTargetName`, so round-tripping a volume
        // created under Longhorn 1.7 (where the field does not exist) would
        // submit `""` and get a 422 on a field we never touched.
        let patch = serde_json::json!({ "spec": { "numberOfReplicas": count } });

        // 409 is routine here — Longhorn's controllers write these objects
        // constantly, and Longhorn wraps its own updates in RetryOnConflict.
        let mut attempt = 0;
        loop {
            attempt += 1;
            match api
                .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                Ok(obj) => return Volume::from_dynamic(obj),
                Err(kube::Error::Api(e)) if e.code == 409 && attempt < 4 => {
                    debug!(%name, attempt, "conflict patching volume; retrying");
                    tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e)).with_context(|| {
                        format!("failed to patch spec.numberOfReplicas={count} on volume '{name}'")
                    })
                }
            }
        }
    }

    async fn create_snapshot(
        &self,
        cluster_id: &str,
        snapshot_name: &str,
        volume: &str,
        labels: Option<BTreeMap<String, String>>,
    ) -> Result<Snapshot> {
        let ar = snapshots();
        let mut obj = DynamicObject::new(snapshot_name, &ar);

        // Deliberately no `metadata.labels["longhornvolume"]` and no
        // ownerReferences: the Longhorn mutator sets both and they are
        // immutable once present.
        let mut spec = serde_json::Map::new();
        spec.insert("volume".to_string(), serde_json::json!(volume));
        spec.insert("createSnapshot".to_string(), serde_json::json!(true));
        if let Some(labels) = labels.filter(|l| !l.is_empty()) {
            spec.insert("labels".to_string(), serde_json::json!(labels));
        }
        obj.data = serde_json::json!({ "spec": spec });

        let created = self
            .api(cluster_id, &ar)?
            .create(&PostParams::default(), &obj)
            .await
            .with_context(|| {
                format!("failed to create Snapshot '{snapshot_name}' for volume '{volume}'")
            })?;
        Snapshot::from_dynamic(created)
    }

    async fn get_snapshot(&self, cluster_id: &str, name: &str) -> Result<Snapshot> {
        let obj = self
            .api(cluster_id, &snapshots())?
            .get(name)
            .await
            .with_context(|| format!("failed to get Snapshot '{name}'"))?;
        Snapshot::from_dynamic(obj)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{bearer_token, method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const CLUSTER: &str = "c-m-abc123";
    const NS: &str = "longhorn-system";
    const VOL: &str = "pvc-abc";

    fn client(mock: &MockServer) -> LonghornClient {
        crate::cluster::install_crypto_provider();
        LonghornClient::new(ClusterClients::new(
            mock.uri(),
            "test-token".to_string(),
            true,
            NS.to_string(),
        ))
    }

    /// The path every request must be prefixed with: Rancher's cluster proxy,
    /// then the Longhorn API group at v1beta2 in the Longhorn namespace.
    fn api_path(plural: &str) -> String {
        format!("/k8s/clusters/{CLUSTER}/apis/longhorn.io/v1beta2/namespaces/{NS}/{plural}")
    }

    fn list_body(kind: &str, items: serde_json::Value) -> serde_json::Value {
        json!({
            "apiVersion": "longhorn.io/v1beta2",
            "kind": format!("{kind}List"),
            "metadata": { "resourceVersion": "1" },
            "items": items,
        })
    }

    fn volume_json(name: &str, replicas: i64) -> serde_json::Value {
        json!({
            "apiVersion": "longhorn.io/v1beta2",
            "kind": "Volume",
            "metadata": { "name": name, "namespace": NS },
            "spec": { "size": "10737418240", "numberOfReplicas": replicas },
            "status": { "state": "attached", "robustness": "healthy" }
        })
    }

    fn replica_json(name: &str, volume: &str) -> serde_json::Value {
        json!({
            "apiVersion": "longhorn.io/v1beta2",
            "kind": "Replica",
            "metadata": { "name": name, "namespace": NS, "labels": { "longhornvolume": volume } },
            "spec": { "volumeName": volume, "nodeID": "node-1", "active": true, "failedAt": "" },
            "status": { "currentState": "running" }
        })
    }

    // ── Reads ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn list_volumes_goes_through_the_rancher_proxy_to_v1beta2() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(api_path("volumes")))
            .and(bearer_token("test-token"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(list_body("Volume", json!([volume_json(VOL, 3)]))),
            )
            .expect(1)
            .mount(&mock)
            .await;

        let volumes = client(&mock).list_volumes(CLUSTER).await.unwrap();

        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].name, VOL);
        assert_eq!(volumes[0].spec.number_of_replicas, Some(3));
        assert_eq!(volumes[0].status.state.as_deref(), Some("attached"));
    }

    #[tokio::test]
    async fn get_volume_reports_the_api_error_for_a_missing_volume() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("{}/{VOL}", api_path("volumes"))))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "message": "volumes.longhorn.io \"pvc-abc\" not found",
                "reason": "NotFound", "code": 404
            })))
            .mount(&mock)
            .await;

        let err = client(&mock).get_volume(CLUSTER, VOL).await.unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("failed to get Longhorn volume"),
            "error: {msg}"
        );
        assert!(
            msg.contains("not found"),
            "the API message must survive: {msg}"
        );
    }

    #[tokio::test]
    async fn list_replicas_selects_by_the_bare_longhornvolume_label() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(api_path("replicas")))
            .and(query_param(
                "labelSelector",
                format!("longhornvolume={VOL}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(list_body(
                "Replica",
                json!([replica_json("r-2", VOL), replica_json("r-1", VOL)]),
            )))
            .expect(1)
            .mount(&mock)
            .await;

        let replicas = client(&mock).list_replicas(CLUSTER, VOL).await.unwrap();

        // Sorted by name so rendered output is stable between calls.
        assert_eq!(
            replicas.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
            vec!["r-1", "r-2"]
        );
    }

    #[tokio::test]
    async fn an_unlabelled_replica_is_still_found_via_the_unfiltered_fallback() {
        let mock = MockServer::start().await;
        // The labelled query finds nothing ...
        Mock::given(method("GET"))
            .and(path(api_path("replicas")))
            .and(query_param(
                "labelSelector",
                format!("longhornvolume={VOL}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(list_body("Replica", json!([]))))
            .expect(1)
            .mount(&mock)
            .await;
        // ... so a full list is fetched and filtered on spec.volumeName.
        Mock::given(method("GET"))
            .and(path(api_path("replicas")))
            .and(query_param_is_missing("labelSelector"))
            .respond_with(ResponseTemplate::new(200).set_body_json(list_body(
                "Replica",
                json!([
                    json!({
                        "metadata": { "name": "r-1", "namespace": NS },
                        "spec": { "volumeName": VOL, "active": true, "failedAt": "" }
                    }),
                    replica_json("r-other", "pvc-other"),
                ]),
            )))
            .expect(1)
            .mount(&mock)
            .await;

        let replicas = client(&mock).list_replicas(CLUSTER, VOL).await.unwrap();

        assert_eq!(
            replicas.len(),
            1,
            "another volume's replicas must be filtered out"
        );
        assert_eq!(replicas[0].name, "r-1");
    }

    #[tokio::test]
    async fn list_backups_selects_by_the_backup_volume_label() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(api_path("backups")))
            .and(query_param("labelSelector", format!("backup-volume={VOL}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(list_body("Backup", json!([]))))
            .expect(1)
            .mount(&mock)
            .await;

        // `Backup` has no spec.volumeName — the label is the only queryable link.
        client(&mock)
            .list_backups(CLUSTER, VOL, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn list_backups_also_constrains_the_target_when_the_volume_names_one() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(api_path("backups")))
            .and(query_param(
                "labelSelector",
                format!("backup-volume={VOL},backup-target=default"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(list_body("Backup", json!([]))))
            .expect(1)
            .mount(&mock)
            .await;

        client(&mock)
            .list_backups(CLUSTER, VOL, Some("default"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_backup_volume_matches_on_spec_volume_name_not_on_the_cr_name() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(api_path("backupvolumes")))
            .respond_with(ResponseTemplate::new(200).set_body_json(list_body(
                "BackupVolume",
                json!([
                    json!({
                        "metadata": { "name": "some-other-name", "namespace": NS },
                        "spec": { "volumeName": VOL, "backupTargetName": "default" },
                        "status": { "lastBackupName": "backup-1", "dataStored": "1048576" }
                    }),
                    json!({
                        "metadata": { "name": VOL, "namespace": NS },
                        "spec": { "volumeName": "pvc-other" }
                    }),
                ]),
            )))
            .mount(&mock)
            .await;

        let bv = client(&mock)
            .get_backup_volume(CLUSTER, VOL)
            .await
            .unwrap()
            .expect("the BackupVolume for this volume");

        assert_eq!(bv.name, "some-other-name");
        assert_eq!(bv.status.last_backup_name.as_deref(), Some("backup-1"));
    }

    // ── Writes ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn patch_replica_count_sends_a_minimal_json_merge_patch() {
        let mock = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path(format!("{}/{VOL}", api_path("volumes"))))
            .and(bearer_token("test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(volume_json(VOL, 2)))
            .expect(1)
            .mount(&mock)
            .await;

        let v = client(&mock)
            .patch_replica_count(CLUSTER, VOL, 2)
            .await
            .unwrap();
        assert_eq!(v.spec.number_of_replicas, Some(2));

        let requests = mock.received_requests().await.unwrap();
        let req = requests.iter().find(|r| r.method == "PATCH").unwrap();

        // Merge patch, not strategic-merge and not a full PUT: a full-object
        // update would resubmit fields like an empty spec.backupTargetName and
        // be rejected by the validating webhook.
        assert_eq!(
            req.headers.get("content-type").unwrap(),
            "application/merge-patch+json"
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&req.body).unwrap(),
            json!({ "spec": { "numberOfReplicas": 2 } }),
        );
    }

    #[tokio::test]
    async fn patch_replica_count_retries_a_conflict() {
        let mock = MockServer::start().await;
        // 409 is routine on Longhorn objects: its controllers write them
        // constantly, which is why Longhorn wraps its own updates in a retry.
        Mock::given(method("PATCH"))
            .and(path(format!("{}/{VOL}", api_path("volumes"))))
            .respond_with(ResponseTemplate::new(409).set_body_json(json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "message": "the object has been modified", "reason": "Conflict", "code": 409
            })))
            .up_to_n_times(1)
            .with_priority(1)
            .expect(1)
            .mount(&mock)
            .await;
        Mock::given(method("PATCH"))
            .and(path(format!("{}/{VOL}", api_path("volumes"))))
            .respond_with(ResponseTemplate::new(200).set_body_json(volume_json(VOL, 2)))
            .with_priority(2)
            .expect(1)
            .mount(&mock)
            .await;

        let v = client(&mock)
            .patch_replica_count(CLUSTER, VOL, 2)
            .await
            .unwrap();
        assert_eq!(v.spec.number_of_replicas, Some(2));
    }

    #[tokio::test]
    async fn patch_replica_count_surfaces_a_webhook_rejection_verbatim() {
        let mock = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path(format!("{}/{VOL}", api_path("volumes"))))
            .respond_with(ResponseTemplate::new(422).set_body_json(json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "message": "admission webhook \"validator.longhorn.io\" denied the request: \
                            replica count value 25 must between 1 to 20",
                "reason": "Invalid", "code": 422
            })))
            .mount(&mock)
            .await;

        let err = client(&mock)
            .patch_replica_count(CLUSTER, VOL, 25)
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("must between 1 to 20"),
            "webhook message lost: {msg}"
        );
        assert!(
            msg.contains("numberOfReplicas=25"),
            "our own context is lost: {msg}"
        );
    }

    #[tokio::test]
    async fn create_snapshot_requests_a_new_snapshot_and_sets_no_volume_label() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(api_path("snapshots")))
            .and(bearer_token("test-token"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "apiVersion": "longhorn.io/v1beta2",
                "kind": "Snapshot",
                "metadata": { "name": "snap-1", "namespace": NS },
                "spec": { "volume": VOL, "createSnapshot": true },
                "status": { "creationTime": "", "readyToUse": false }
            })))
            .expect(1)
            .mount(&mock)
            .await;

        let snap = client(&mock)
            .create_snapshot(CLUSTER, "snap-1", VOL, None)
            .await
            .unwrap();
        assert_eq!(snap.name, "snap-1");
        assert!(!snap.is_ready(), "a fresh snapshot is not ready yet");

        let requests = mock.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();

        assert_eq!(body["apiVersion"], "longhorn.io/v1beta2");
        assert_eq!(body["kind"], "Snapshot");
        assert_eq!(body["metadata"]["name"], "snap-1");
        assert_eq!(body["spec"]["volume"], VOL);
        assert_eq!(
            body["spec"]["createSnapshot"], true,
            "without this the CR is only a read-only mirror of an existing snapshot"
        );
        assert!(
            body["spec"].get("labels").is_none(),
            "no labels were requested, so none should be sent: {body}"
        );
        // The Longhorn mutator owns these and they are immutable once set.
        assert!(
            body["metadata"].get("labels").is_none(),
            "must not set longhornvolume ourselves: {body}"
        );
        assert!(
            body["metadata"].get("ownerReferences").is_none(),
            "must not set ownerReferences ourselves: {body}"
        );
    }

    #[tokio::test]
    async fn create_snapshot_forwards_requested_labels() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(api_path("snapshots")))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "metadata": { "name": "snap-1", "namespace": NS },
                "spec": { "volume": VOL, "createSnapshot": true }
            })))
            .mount(&mock)
            .await;

        let mut labels = BTreeMap::new();
        labels.insert("purpose".to_string(), "pre-upgrade".to_string());
        client(&mock)
            .create_snapshot(CLUSTER, "snap-1", VOL, Some(labels))
            .await
            .unwrap();

        let requests = mock.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["spec"]["labels"]["purpose"], "pre-upgrade");
    }
}
