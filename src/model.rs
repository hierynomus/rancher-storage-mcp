//! Narrow `serde` views over the Longhorn CRDs (`longhorn.io/v1beta2`).
//!
//! Only the fields we actually render are modelled. Everything is `Option` +
//! `#[serde(default)]` because Longhorn marks most fields `+nullable`, which
//! serializes as `null` rather than as an absent key or an empty collection.
//!
//! Size fields are the trap in this API: Longhorn uses three different
//! encodings for byte counts (`json:"...,string"` quoted decimals, genuine
//! `string` fields, and plain integers). Rather than track which is which at
//! every call site, `num_or_string` / `int_or_string` accept either form.

// These are API views: every field below is a documented part of the Longhorn
// CRD schema and decodes correctly, but only the subset the renderers touch is
// read today. Keeping the rest costs nothing and saves re-deriving the field
// names (and their casing traps) next time a tool needs one.
#![allow(dead_code)]

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use kube::api::DynamicObject;
use serde::{Deserialize, Deserializer};

// ── Lenient scalar decoding ───────────────────────────────────────────────────

/// Accepts `"123"` or `123`, yielding `Some("123")`. `null` yields `None`.
fn num_or_string<'de, D>(de: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match Option::<serde_json::Value>::deserialize(de)? {
        Some(serde_json::Value::String(s)) => Some(s),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        _ => None,
    })
}

/// Accepts `123` or `"123"`, yielding `Some(123)`. `null`/unparsable yields `None`.
fn int_or_string<'de, D>(de: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match Option::<serde_json::Value>::deserialize(de)? {
        Some(serde_json::Value::Number(n)) => n.as_i64(),
        Some(serde_json::Value::String(s)) => s.parse().ok(),
        _ => None,
    })
}

// ── Shared sub-objects ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    #[serde(rename = "type", default)]
    pub condition_type: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub last_transition_time: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadStatus {
    #[serde(default)]
    pub pod_name: Option<String>,
    #[serde(default)]
    pub pod_status: Option<String>,
    #[serde(default)]
    pub workload_name: Option<String>,
    #[serde(default)]
    pub workload_type: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubernetesStatus {
    #[serde(default)]
    pub pv_name: Option<String>,
    #[serde(default)]
    pub pv_status: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub pvc_name: Option<String>,
    #[serde(default)]
    pub workloads_status: Option<Vec<WorkloadStatus>>,
}

/// Common shape of `engine.status.{rebuild,restore,purge,clone}Status` entries.
/// The per-kind booleans differ; unknown fields are ignored.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineProgress {
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub progress: Option<i64>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub from_replica_address: Option<String>,
    #[serde(default)]
    pub is_rebuilding: Option<bool>,
    #[serde(default)]
    pub is_purging: Option<bool>,
    #[serde(default)]
    pub is_restoring: Option<bool>,
    #[serde(default)]
    pub is_cloning: Option<bool>,
}

impl EngineProgress {
    /// True while the operation is actually running, for any of the four kinds.
    pub fn active(&self) -> bool {
        self.is_rebuilding.unwrap_or(false)
            || self.is_purging.unwrap_or(false)
            || self.is_restoring.unwrap_or(false)
            || self.is_cloning.unwrap_or(false)
    }
}

/// An entry of `engine.status.snapshots`. Note `usercreated` is all-lowercase
/// here, unlike the `Snapshot` CR's `userCreated`, and `size` is a string.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineSnapshot {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub children: Option<BTreeMap<String, bool>>,
    #[serde(default)]
    pub removed: Option<bool>,
    #[serde(rename = "usercreated", default)]
    pub user_created: Option<bool>,
    #[serde(default)]
    pub created: Option<String>,
    #[serde(default, deserialize_with = "num_or_string")]
    pub size: Option<String>,
    #[serde(default)]
    pub labels: Option<BTreeMap<String, String>>,
}

/// The synthetic entry Longhorn keeps in `engine.status.snapshots` for the live
/// volume head. Never a real snapshot.
pub const VOLUME_HEAD: &str = "volume-head";

// ── Volume ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSpec {
    #[serde(default, deserialize_with = "num_or_string")]
    pub size: Option<String>,
    #[serde(default)]
    pub number_of_replicas: Option<i64>,
    #[serde(default)]
    pub frontend: Option<String>,
    #[serde(default)]
    pub access_mode: Option<String>,
    #[serde(default)]
    pub data_locality: Option<String>,
    #[serde(default)]
    pub data_engine: Option<String>,
    #[serde(default)]
    pub replica_auto_balance: Option<String>,
    #[serde(default)]
    pub snapshot_data_integrity: Option<String>,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub backing_image: Option<String>,
    #[serde(default)]
    pub migration_node_id: Option<String>,
    #[serde(default)]
    pub from_backup: Option<String>,
    #[serde(default)]
    pub backup_target_name: Option<String>,
    #[serde(default)]
    pub node_selector: Option<Vec<String>>,
    #[serde(default)]
    pub disk_selector: Option<Vec<String>>,
    #[serde(default)]
    pub encrypted: Option<bool>,
    #[serde(default)]
    pub migratable: Option<bool>,
    #[serde(default)]
    pub revision_counter_disabled: Option<bool>,
    #[serde(default)]
    pub stale_replica_timeout: Option<i64>,
    #[serde(default)]
    pub snapshot_max_count: Option<i64>,
    #[serde(default, deserialize_with = "num_or_string")]
    pub snapshot_max_size: Option<String>,
    /// `json:"Standby"` — the one capital-letter field in the whole API.
    #[serde(rename = "Standby", default)]
    pub standby: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeStatus {
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub robustness: Option<String>,
    #[serde(default)]
    pub current_node_id: Option<String>,
    #[serde(default)]
    pub current_image: Option<String>,
    #[serde(default)]
    pub owner_id: Option<String>,
    #[serde(default, deserialize_with = "int_or_string")]
    pub actual_size: Option<i64>,
    #[serde(default)]
    pub kubernetes_status: Option<KubernetesStatus>,
    #[serde(default)]
    pub conditions: Option<Vec<Condition>>,
    #[serde(default)]
    pub last_backup: Option<String>,
    #[serde(default)]
    pub last_backup_at: Option<String>,
    #[serde(default)]
    pub last_degraded_at: Option<String>,
    #[serde(default)]
    pub expansion_required: Option<bool>,
    #[serde(default)]
    pub restore_required: Option<bool>,
    #[serde(default)]
    pub is_standby: Option<bool>,
    #[serde(default)]
    pub share_endpoint: Option<String>,
    #[serde(default)]
    pub share_state: Option<String>,
}

impl VolumeStatus {
    pub fn pvc_ref(&self) -> Option<(String, String)> {
        let ks = self.kubernetes_status.as_ref()?;
        let ns = ks.namespace.as_deref().filter(|s| !s.is_empty())?;
        let pvc = ks.pvc_name.as_deref().filter(|s| !s.is_empty())?;
        Some((ns.to_string(), pvc.to_string()))
    }
}

// ── Engine ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineSpec {
    #[serde(default)]
    pub volume_name: Option<String>,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub desire_state: Option<String>,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub frontend: Option<String>,
    #[serde(default)]
    pub active: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineStatus {
    #[serde(default)]
    pub current_state: Option<String>,
    #[serde(default)]
    pub current_image: Option<String>,
    #[serde(default)]
    pub instance_manager_name: Option<String>,
    #[serde(default)]
    pub owner_id: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default, deserialize_with = "num_or_string")]
    pub current_size: Option<String>,
    /// Keyed by *replica CR name*; values are exactly `RW`, `WO` or `ERR`.
    #[serde(default)]
    pub replica_mode_map: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub snapshots: Option<BTreeMap<String, EngineSnapshot>>,
    #[serde(default)]
    pub snapshots_error: Option<String>,
    #[serde(default)]
    pub rebuild_status: Option<BTreeMap<String, EngineProgress>>,
    #[serde(default)]
    pub restore_status: Option<BTreeMap<String, EngineProgress>>,
    #[serde(default)]
    pub purge_status: Option<BTreeMap<String, EngineProgress>>,
    #[serde(default)]
    pub clone_status: Option<BTreeMap<String, EngineProgress>>,
    #[serde(default)]
    pub is_expanding: Option<bool>,
    #[serde(default)]
    pub last_restored_backup: Option<String>,
}

// ── Replica ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSpec {
    #[serde(default)]
    pub volume_name: Option<String>,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub disk_id: Option<String>,
    #[serde(default)]
    pub disk_path: Option<String>,
    #[serde(default)]
    pub data_directory_name: Option<String>,
    #[serde(default)]
    pub engine_name: Option<String>,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub active: Option<bool>,
    /// Cleared before a rebuild; `""` is the unset sentinel, not `null`.
    #[serde(default)]
    pub healthy_at: Option<String>,
    #[serde(default)]
    pub last_healthy_at: Option<String>,
    #[serde(default)]
    pub failed_at: Option<String>,
    #[serde(default)]
    pub last_failed_at: Option<String>,
    #[serde(default)]
    pub eviction_requested: Option<bool>,
    #[serde(default)]
    pub rebuild_retry_count: Option<i64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaStatus {
    #[serde(default)]
    pub current_state: Option<String>,
    #[serde(default)]
    pub instance_manager_name: Option<String>,
    #[serde(default)]
    pub owner_id: Option<String>,
    #[serde(default)]
    pub current_image: Option<String>,
    #[serde(default)]
    pub started: Option<bool>,
}

// ── Snapshot ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotSpec {
    #[serde(default)]
    pub volume: Option<String>,
    #[serde(default)]
    pub create_snapshot: Option<bool>,
    #[serde(default)]
    pub labels: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotStatus {
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub children: Option<BTreeMap<String, bool>>,
    #[serde(default)]
    pub mark_removed: Option<bool>,
    #[serde(default)]
    pub user_created: Option<bool>,
    /// `""` until the controller has actually taken the snapshot.
    #[serde(default)]
    pub creation_time: Option<String>,
    #[serde(default, deserialize_with = "int_or_string")]
    pub size: Option<i64>,
    #[serde(default, deserialize_with = "int_or_string")]
    pub restore_size: Option<i64>,
    #[serde(default)]
    pub ready_to_use: Option<bool>,
    #[serde(default)]
    pub checksum: Option<String>,
    /// Non-empty while the snapshot is *not* yet usable. Frequently transient
    /// (the volume is being attached) rather than terminal.
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub owner_id: Option<String>,
    #[serde(default)]
    pub labels: Option<BTreeMap<String, String>>,
}

// ── Backup / BackupVolume ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSpec {
    #[serde(default)]
    pub snapshot_name: Option<String>,
    #[serde(default)]
    pub backup_mode: Option<String>,
    #[serde(default)]
    pub labels: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupStatus {
    /// Capitalized, unlike volume state: `Pending`/`InProgress`/`Completed`/…
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub progress: Option<i64>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub snapshot_name: Option<String>,
    #[serde(default)]
    pub snapshot_created_at: Option<String>,
    #[serde(default)]
    pub backup_created_at: Option<String>,
    #[serde(default, deserialize_with = "num_or_string")]
    pub size: Option<String>,
    #[serde(default)]
    pub volume_name: Option<String>,
    #[serde(default, deserialize_with = "num_or_string")]
    pub volume_size: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub backup_target_name: Option<String>,
    #[serde(default)]
    pub compression_method: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupVolumeSpec {
    #[serde(default)]
    pub volume_name: Option<String>,
    #[serde(default)]
    pub backup_target_name: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupVolumeStatus {
    #[serde(default)]
    pub last_backup_name: Option<String>,
    #[serde(default)]
    pub last_backup_at: Option<String>,
    #[serde(default, deserialize_with = "num_or_string")]
    pub size: Option<String>,
    #[serde(default, deserialize_with = "num_or_string")]
    pub data_stored: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub storage_class_name: Option<String>,
    #[serde(default)]
    pub backing_image_name: Option<String>,
}

// ── DynamicObject → typed view ────────────────────────────────────────────────

macro_rules! longhorn_object {
    ($name:ident, $spec:ty, $status:ty) => {
        #[derive(Debug, Clone, Default)]
        pub struct $name {
            pub name: String,
            pub labels: BTreeMap<String, String>,
            pub spec: $spec,
            pub status: $status,
        }

        impl $name {
            pub fn from_dynamic(obj: DynamicObject) -> Result<Self> {
                #[derive(Default, Deserialize)]
                struct Body {
                    #[serde(default)]
                    spec: $spec,
                    #[serde(default)]
                    status: $status,
                }

                let name = obj.metadata.name.clone().unwrap_or_default();
                let labels = obj.metadata.labels.clone().unwrap_or_default();
                let body: Body = serde_json::from_value(obj.data).with_context(|| {
                    format!("failed to decode {} '{}'", stringify!($name), name)
                })?;

                Ok(Self {
                    name,
                    labels,
                    spec: body.spec,
                    status: body.status,
                })
            }
        }
    };
}

longhorn_object!(Volume, VolumeSpec, VolumeStatus);
longhorn_object!(Engine, EngineSpec, EngineStatus);
longhorn_object!(Replica, ReplicaSpec, ReplicaStatus);
longhorn_object!(Snapshot, SnapshotSpec, SnapshotStatus);
longhorn_object!(Backup, BackupSpec, BackupStatus);
longhorn_object!(BackupVolume, BackupVolumeSpec, BackupVolumeStatus);

impl Snapshot {
    /// Success predicate: the controller has taken the snapshot *and* it is usable.
    pub fn is_ready(&self) -> bool {
        self.status
            .creation_time
            .as_deref()
            .is_some_and(|t| !t.is_empty())
            && self.status.ready_to_use.unwrap_or(false)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dynamic(value: serde_json::Value) -> DynamicObject {
        serde_json::from_value(value).expect("valid DynamicObject")
    }

    #[test]
    fn volume_decodes_the_three_size_encodings() {
        let v = Volume::from_dynamic(dynamic(json!({
            "apiVersion": "longhorn.io/v1beta2",
            "kind": "Volume",
            "metadata": { "name": "pvc-abc", "labels": { "longhornvolume": "pvc-abc" } },
            // spec.size is a quoted decimal (Go `json:"size,string"`) ...
            "spec": { "size": "10737418240", "numberOfReplicas": 3 },
            // ... while status.actualSize is a plain integer.
            "status": { "actualSize": 2147483648i64, "state": "attached", "robustness": "healthy" }
        })))
        .unwrap();

        assert_eq!(v.name, "pvc-abc");
        assert_eq!(
            v.labels.get("longhornvolume").map(String::as_str),
            Some("pvc-abc")
        );
        assert_eq!(v.spec.size.as_deref(), Some("10737418240"));
        assert_eq!(v.spec.number_of_replicas, Some(3));
        assert_eq!(v.status.actual_size, Some(2_147_483_648));
        assert_eq!(v.status.state.as_deref(), Some("attached"));
    }

    #[test]
    fn size_fields_accept_either_json_form() {
        // Defensive: a cluster emitting a bare number where Longhorn documents
        // a string (or vice versa) must not fail the whole decode.
        let v = Volume::from_dynamic(dynamic(json!({
            "metadata": { "name": "pvc-abc" },
            "spec": { "size": 1024 },
            "status": { "actualSize": "512" }
        })))
        .unwrap();
        assert_eq!(v.spec.size.as_deref(), Some("1024"));
        assert_eq!(v.status.actual_size, Some(512));
    }

    #[test]
    fn volume_spec_standby_is_capital_s() {
        let v = Volume::from_dynamic(dynamic(json!({
            "metadata": { "name": "pvc-abc" },
            "spec": { "Standby": true },
            "status": { "isStandby": true }
        })))
        .unwrap();
        assert_eq!(v.spec.standby, Some(true));
        assert_eq!(v.status.is_standby, Some(true));

        // The camelCase spelling is NOT the field name and must not bind.
        let v = Volume::from_dynamic(dynamic(json!({
            "metadata": { "name": "pvc-abc" },
            "spec": { "standby": true }
        })))
        .unwrap();
        assert_eq!(v.spec.standby, None);
    }

    #[test]
    fn nullable_fields_tolerate_explicit_null() {
        // Longhorn's `+nullable` fields serialize as null, not as {} or [].
        let v = Volume::from_dynamic(dynamic(json!({
            "metadata": { "name": "pvc-abc" },
            "spec": { "nodeSelector": null, "diskSelector": null },
            "status": { "conditions": null, "kubernetesStatus": null }
        })))
        .unwrap();
        assert!(v.status.conditions.is_none());
        assert!(v.status.kubernetes_status.is_none());
        assert!(v.spec.node_selector.is_none());
    }

    #[test]
    fn missing_spec_and_status_decode_to_defaults() {
        let v =
            Volume::from_dynamic(dynamic(json!({ "metadata": { "name": "pvc-abc" } }))).unwrap();
        assert!(v.spec.size.is_none());
        assert!(v.status.state.is_none());
        assert!(v.labels.is_empty());
    }

    #[test]
    fn engine_snapshot_map_uses_lowercase_usercreated_and_string_size() {
        let e = Engine::from_dynamic(dynamic(json!({
            "metadata": { "name": "e-1" },
            "spec": { "volumeName": "pvc-abc", "active": true },
            "status": {
                "currentState": "running",
                "replicaModeMap": { "r-1": "RW", "r-2": "ERR" },
                "snapshots": {
                    "snap-1": {
                        "name": "snap-1", "parent": "", "children": { "volume-head": true },
                        "removed": false, "usercreated": true,
                        "created": "2026-09-01T10:00:00Z", "size": "1048576"
                    },
                    "volume-head": { "name": "volume-head", "size": "0" }
                }
            }
        })))
        .unwrap();

        let snaps = e.status.snapshots.as_ref().unwrap();
        let s = &snaps["snap-1"];
        assert_eq!(s.user_created, Some(true));
        assert_eq!(s.size.as_deref(), Some("1048576"));
        assert!(
            snaps.contains_key(VOLUME_HEAD),
            "volume-head must decode too, then be filtered"
        );
        assert_eq!(e.status.replica_mode_map.as_ref().unwrap()["r-1"], "RW");
    }

    #[test]
    fn snapshot_cr_uses_camelcase_user_created_and_integer_sizes() {
        let s = Snapshot::from_dynamic(dynamic(json!({
            "metadata": { "name": "snap-1" },
            "spec": { "volume": "pvc-abc", "createSnapshot": true },
            "status": {
                "userCreated": true, "creationTime": "2026-09-01T10:00:00Z",
                "size": 1048576, "restoreSize": 10737418240i64, "readyToUse": true
            }
        })))
        .unwrap();

        assert_eq!(s.status.user_created, Some(true));
        assert_eq!(s.status.size, Some(1_048_576));
        assert_eq!(s.status.restore_size, Some(10_737_418_240));
        assert!(s.is_ready());
    }

    #[test]
    fn snapshot_is_ready_requires_both_creation_time_and_ready_to_use() {
        let mk = |creation: &str, ready: bool| Snapshot {
            status: SnapshotStatus {
                creation_time: Some(creation.to_string()),
                ready_to_use: Some(ready),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(mk("2026-09-01T10:00:00Z", true).is_ready());
        // Empty string, not null, is Longhorn's "not taken yet" sentinel.
        assert!(!mk("", true).is_ready());
        assert!(!mk("2026-09-01T10:00:00Z", false).is_ready());
        assert!(!Snapshot::default().is_ready());
    }

    #[test]
    fn replica_health_timestamps_live_in_spec() {
        let r = Replica::from_dynamic(dynamic(json!({
            "metadata": { "name": "r-1" },
            "spec": {
                "volumeName": "pvc-abc", "nodeID": "node-1", "diskID": "disk-uuid",
                "healthyAt": "2026-09-01T10:00:00Z", "failedAt": "",
                "evictionRequested": false, "active": true
            },
            "status": { "currentState": "running" }
        })))
        .unwrap();

        assert_eq!(r.spec.healthy_at.as_deref(), Some("2026-09-01T10:00:00Z"));
        assert_eq!(r.spec.failed_at.as_deref(), Some(""));
        assert_eq!(r.status.current_state.as_deref(), Some("running"));
    }

    #[test]
    fn backup_status_state_is_capitalized_and_sizes_are_strings() {
        let b = Backup::from_dynamic(dynamic(json!({
            "metadata": { "name": "backup-1", "labels": { "backup-volume": "pvc-abc" } },
            "spec": { "snapshotName": "snap-1" },
            "status": {
                "state": "Completed", "size": "1048576", "volumeSize": "10737418240",
                "volumeName": "pvc-abc", "url": "s3://bucket/backup-1"
            }
        })))
        .unwrap();

        assert_eq!(b.status.state.as_deref(), Some("Completed"));
        assert_eq!(b.status.size.as_deref(), Some("1048576"));
        assert_eq!(
            b.labels.get("backup-volume").map(String::as_str),
            Some("pvc-abc")
        );
    }

    #[test]
    fn kubernetes_status_exposes_the_pvc_reference() {
        let v = Volume::from_dynamic(dynamic(json!({
            "metadata": { "name": "pvc-abc" },
            "status": { "kubernetesStatus": {
                "namespace": "postgres", "pvcName": "data-postgres-0",
                "pvName": "pvc-abc", "pvStatus": "Bound",
                "workloadsStatus": [{
                    "podName": "postgres-0", "podStatus": "Running",
                    "workloadName": "postgres", "workloadType": "StatefulSet"
                }]
            }}
        })))
        .unwrap();

        assert_eq!(
            v.status.pvc_ref(),
            Some(("postgres".to_string(), "data-postgres-0".to_string()))
        );
        assert_eq!(
            v.status
                .kubernetes_status
                .unwrap()
                .workloads_status
                .unwrap()[0]
                .workload_type
                .as_deref(),
            Some("StatefulSet")
        );
    }

    #[test]
    fn pvc_ref_is_none_when_unbound_or_empty() {
        assert_eq!(VolumeStatus::default().pvc_ref(), None);
        let v = Volume::from_dynamic(dynamic(json!({
            "metadata": { "name": "pvc-abc" },
            "status": { "kubernetesStatus": { "namespace": "", "pvcName": "" } }
        })))
        .unwrap();
        assert_eq!(v.status.pvc_ref(), None);
    }

    #[test]
    fn engine_progress_active_covers_all_four_operation_kinds() {
        let rebuilding: EngineProgress =
            serde_json::from_value(json!({ "isRebuilding": true, "progress": 42 })).unwrap();
        assert!(rebuilding.active());
        assert_eq!(rebuilding.progress, Some(42));

        let idle: EngineProgress =
            serde_json::from_value(json!({ "isRebuilding": false, "progress": 0 })).unwrap();
        assert!(!idle.active());

        assert!(
            serde_json::from_value::<EngineProgress>(json!({ "isPurging": true }))
                .unwrap()
                .active()
        );
        assert!(
            serde_json::from_value::<EngineProgress>(json!({ "isRestoring": true }))
                .unwrap()
                .active()
        );
    }

    #[test]
    fn condition_type_binds_the_reserved_word_field() {
        let v = Volume::from_dynamic(dynamic(json!({
            "metadata": { "name": "pvc-abc" },
            "status": { "conditions": [
                { "type": "Scheduled", "status": "False", "reason": "ReplicaSchedulingFailure",
                  "message": "no available disk" },
                { "type": "Restore", "status": "True" }
            ]}
        })))
        .unwrap();

        let conds = v.status.conditions.unwrap();
        assert_eq!(conds[0].condition_type.as_deref(), Some("Scheduled"));
        assert_eq!(conds[0].status.as_deref(), Some("False"));
        assert_eq!(conds[0].reason.as_deref(), Some("ReplicaSchedulingFailure"));
    }
}
