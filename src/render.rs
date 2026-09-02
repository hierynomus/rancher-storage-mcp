//! Pure formatting: byte-size decoding, name sanitization, replica health
//! predicates and the text renderers behind every tool's output.
//!
//! Nothing here touches the network, so it is all directly unit-testable.

use chrono::{DateTime, Utc};

use crate::model::{
    Backup, BackupVolume, Engine, EngineSnapshot, Replica, Snapshot, Volume, VOLUME_HEAD,
};

// ── Scalars ───────────────────────────────────────────────────────────────────

/// `"-"` for absent or empty strings, so rendered tables never have holes.
pub fn dash(v: Option<&str>) -> &str {
    match v {
        Some(s) if !s.is_empty() => s,
        _ => "-",
    }
}

pub fn human_bytes(bytes: i64) -> String {
    const UNITS: [(&str, i64); 5] = [
        ("Pi", 1 << 50),
        ("Ti", 1 << 40),
        ("Gi", 1 << 30),
        ("Mi", 1 << 20),
        ("Ki", 1 << 10),
    ];
    if bytes < 0 {
        return bytes.to_string();
    }
    for (unit, scale) in UNITS {
        if bytes >= scale {
            let whole = bytes / scale;
            let frac = (bytes % scale) * 10 / scale;
            return if frac == 0 {
                format!("{whole}{unit}")
            } else {
                format!("{whole}.{frac}{unit}")
            };
        }
    }
    format!("{bytes}B")
}

/// Longhorn encodes byte counts as quoted decimals in most places. Renders the
/// human form, or `"-"` when absent or unparsable.
pub fn size_str(v: Option<&String>) -> String {
    match v.and_then(|s| s.parse::<i64>().ok()) {
        Some(n) => human_bytes(n),
        None => "-".to_string(),
    }
}

/// The handful of size fields that are genuine integers.
pub fn size_int(v: Option<i64>) -> String {
    match v {
        Some(n) => human_bytes(n),
        None => "-".to_string(),
    }
}

/// Converts arbitrary strings to a valid DNS-1123 name segment (lowercase
/// alphanumeric + hyphens), truncated so a suffix still fits.
pub fn sanitize_k8s_name(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    out.trim_matches('-').chars().take(60).collect()
}

/// The `Snapshot` CR's name *is* the Longhorn snapshot name — there is no
/// separate field — so it has to be deterministic and DNS-1123 safe.
pub fn default_snapshot_name(volume: &str, now: DateTime<Utc>) -> String {
    format!(
        "{}-mcp-{}",
        sanitize_k8s_name(volume),
        now.format("%Y%m%d-%H%M%S")
    )
}

// ── Replica health ────────────────────────────────────────────────────────────

/// Mirrors Longhorn's own `getReplenishReplicasCount`: a replica counts toward
/// the desired replica count only if it has not failed, is not being evicted,
/// and is active.
pub fn replica_usable(r: &Replica) -> bool {
    r.spec.failed_at.as_deref().unwrap_or("").is_empty()
        && !r.spec.eviction_requested.unwrap_or(false)
        && r.spec.active.unwrap_or(true)
}

/// A usable replica that has actually completed a rebuild at least once.
pub fn replica_healthy(r: &Replica) -> bool {
    replica_usable(r) && !r.spec.healthy_at.as_deref().unwrap_or("").is_empty()
}

// ── Renderers ─────────────────────────────────────────────────────────────────

/// Picks the engine that is currently serving the volume.
pub fn active_engine(engines: &[Engine]) -> Option<&Engine> {
    engines
        .iter()
        .find(|e| e.spec.active.unwrap_or(false))
        .or_else(|| engines.first())
}

fn pvc_label(v: &Volume) -> String {
    match v.status.pvc_ref() {
        Some((ns, pvc)) => format!("{ns}/{pvc}"),
        None => "-".to_string(),
    }
}

pub fn volume_line(v: &Volume, replicas: &[Replica]) -> String {
    let healthy = replicas.iter().filter(|r| replica_healthy(r)).count();
    let desired = v.spec.number_of_replicas.unwrap_or(0);
    format!(
        "- {}  pvc={}  state={}  robustness={}  size={}  replicas={}/{}  node={}",
        v.name,
        pvc_label(v),
        dash(v.status.state.as_deref()),
        dash(v.status.robustness.as_deref()),
        size_str(v.spec.size.as_ref()),
        healthy,
        desired,
        dash(v.status.current_node_id.as_deref()),
    )
}

pub fn volume_report(
    v: &Volume,
    engines: &[Engine],
    replicas: &[Replica],
    snapshots: &[Snapshot],
    backups: &[Backup],
    backup_volume: Option<&BackupVolume>,
) -> String {
    let engine = active_engine(engines);
    let mut out = String::new();

    // ── Volume ────────────────────────────────────────────────────────────
    out.push_str(&format!("Volume: {}\n", v.name));
    out.push_str(&format!("  PVC:            {}\n", pvc_label(v)));
    if let Some(ks) = v.status.kubernetes_status.as_ref() {
        out.push_str(&format!(
            "  PV:             {}",
            dash(ks.pv_name.as_deref())
        ));
        if let Some(s) = ks.pv_status.as_deref().filter(|s| !s.is_empty()) {
            out.push_str(&format!(" ({s})"));
        }
        out.push('\n');
        let workloads: Vec<String> = ks
            .workloads_status
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|w| {
                format!(
                    "{}/{} pod={} ({})",
                    dash(w.workload_type.as_deref()),
                    dash(w.workload_name.as_deref()),
                    dash(w.pod_name.as_deref()),
                    dash(w.pod_status.as_deref()),
                )
            })
            .collect();
        if !workloads.is_empty() {
            out.push_str(&format!("  Workloads:      {}\n", workloads.join(", ")));
        }
    }
    out.push_str(&format!(
        "  State:          {}\n",
        dash(v.status.state.as_deref())
    ));
    out.push_str(&format!(
        "  Robustness:     {}\n",
        dash(v.status.robustness.as_deref())
    ));
    out.push_str(&format!(
        "  Size:           {} (actual {})\n",
        size_str(v.spec.size.as_ref()),
        size_int(v.status.actual_size),
    ));
    out.push_str(&format!(
        "  Replicas:       desired={} healthy={} usable={} total={}\n",
        v.spec.number_of_replicas.unwrap_or(0),
        replicas.iter().filter(|r| replica_healthy(r)).count(),
        replicas.iter().filter(|r| replica_usable(r)).count(),
        replicas.len(),
    ));
    out.push_str(&format!(
        "  Access mode:    {}   Data locality: {}   Auto-balance: {}\n",
        dash(v.spec.access_mode.as_deref()),
        dash(v.spec.data_locality.as_deref()),
        dash(v.spec.replica_auto_balance.as_deref()),
    ));
    out.push_str(&format!(
        "  Data engine:    {}   Frontend: {}   Encrypted: {}\n",
        dash(v.spec.data_engine.as_deref()),
        dash(v.spec.frontend.as_deref()),
        v.spec.encrypted.unwrap_or(false),
    ));
    out.push_str(&format!(
        "  Attached to:    {}   Owner (manager): {}\n",
        dash(v.status.current_node_id.as_deref()),
        dash(v.status.owner_id.as_deref()),
    ));
    out.push_str(&format!(
        "  Engine image:   {} (spec {})\n",
        dash(v.status.current_image.as_deref()),
        dash(v.spec.image.as_deref()),
    ));
    if let Some(e) = engine {
        out.push_str(&format!(
            "  Endpoint:       {}   Engine state: {}\n",
            dash(e.status.endpoint.as_deref()),
            dash(e.status.current_state.as_deref()),
        ));
    }
    if v.spec.migration_node_id.as_deref().unwrap_or("") != "" {
        out.push_str(&format!(
            "  Migrating to:   {}\n",
            dash(v.spec.migration_node_id.as_deref())
        ));
    }
    if v.status.expansion_required.unwrap_or(false) {
        out.push_str("  ! Expansion required\n");
    }
    if v.status.restore_required.unwrap_or(false) {
        out.push_str("  ! Restore required\n");
    }

    // ── Conditions (only the ones that are not True) ──────────────────────
    // Read `Scheduled` from here rather than from `kubectl get lhv` — the CRD's
    // printer column filters on `Schedulable`, which is not a Volume condition
    // type, so that column is always empty.
    let bad: Vec<&crate::model::Condition> = v
        .status
        .conditions
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|c| c.status.as_deref() != Some("True"))
        .collect();
    if bad.is_empty() {
        out.push_str("\nConditions: all True\n");
    } else {
        out.push_str("\nConditions (not True):\n");
        for c in bad {
            out.push_str(&format!(
                "  - {}={} reason={} {}\n",
                dash(c.condition_type.as_deref()),
                dash(c.status.as_deref()),
                dash(c.reason.as_deref()),
                dash(c.message.as_deref()),
            ));
        }
    }

    // ── Replicas ──────────────────────────────────────────────────────────
    out.push_str(&format!("\nReplicas ({}):\n", replicas.len()));
    if replicas.is_empty() {
        out.push_str("  (none)\n");
    }
    let modes = engine.and_then(|e| e.status.replica_mode_map.as_ref());
    for r in replicas {
        let mode = modes
            .and_then(|m| m.get(&r.name))
            .map(String::as_str)
            .unwrap_or("-");
        let mut flags = Vec::new();
        if !r.spec.failed_at.as_deref().unwrap_or("").is_empty() {
            flags.push(format!("failedAt={}", r.spec.failed_at.as_deref().unwrap()));
        }
        if r.spec.eviction_requested.unwrap_or(false) {
            flags.push("evictionRequested".to_string());
        }
        if !r.spec.active.unwrap_or(true) {
            flags.push("inactive".to_string());
        }
        if let Some(n) = r.spec.rebuild_retry_count.filter(|n| *n > 0) {
            flags.push(format!("rebuildRetries={n}"));
        }
        out.push_str(&format!(
            "  - {}  node={}  disk={}  state={}  mode={}  healthyAt={}{}\n",
            r.name,
            dash(r.spec.node_id.as_deref()),
            dash(r.spec.disk_id.as_deref()),
            dash(r.status.current_state.as_deref()),
            mode,
            dash(r.spec.healthy_at.as_deref()),
            if flags.is_empty() {
                String::new()
            } else {
                format!("  [{}]", flags.join(", "))
            },
        ));
    }

    // ── In-flight engine operations ───────────────────────────────────────
    let mut progress = Vec::new();
    if let Some(e) = engine {
        for (label, map) in [
            ("rebuild", e.status.rebuild_status.as_ref()),
            ("restore", e.status.restore_status.as_ref()),
            ("purge", e.status.purge_status.as_ref()),
            ("clone", e.status.clone_status.as_ref()),
        ] {
            for (addr, p) in map.into_iter().flatten() {
                if !p.active() && p.error.as_deref().unwrap_or("").is_empty() {
                    continue;
                }
                progress.push(format!(
                    "  - {label} {addr}: state={} progress={}%{}",
                    dash(p.state.as_deref()),
                    p.progress.unwrap_or(0),
                    match p.error.as_deref().filter(|s| !s.is_empty()) {
                        Some(e) => format!(" error={e}"),
                        None => String::new(),
                    },
                ));
            }
        }
    }
    if !progress.is_empty() {
        out.push_str("\nIn progress:\n");
        for line in progress {
            out.push_str(&line);
            out.push('\n');
        }
    }

    // ── Snapshots ─────────────────────────────────────────────────────────
    out.push_str(&format!("\nSnapshots ({}):\n", snapshots.len()));
    if snapshots.is_empty() {
        // The CRs can lag, and they are absent entirely for snapshots taken
        // before the volume was managed here. Fall back to the engine's map.
        let engine_snaps: Vec<&EngineSnapshot> = engine
            .and_then(|e| e.status.snapshots.as_ref())
            .into_iter()
            .flatten()
            .map(|(_, s)| s)
            .filter(|s| s.name.as_deref() != Some(VOLUME_HEAD) && !s.removed.unwrap_or(false))
            .collect();
        if engine_snaps.is_empty() {
            out.push_str("  (none)\n");
        } else {
            out.push_str("  (no Snapshot CRs; read from engine.status.snapshots)\n");
            for s in engine_snaps {
                out.push_str(&format!(
                    "  - {}  created={}  size={}  userCreated={}  parent={}\n",
                    dash(s.name.as_deref()),
                    dash(s.created.as_deref()),
                    size_str(s.size.as_ref()),
                    s.user_created.unwrap_or(false),
                    dash(s.parent.as_deref()),
                ));
            }
        }
    }
    for s in snapshots {
        let mut flags = Vec::new();
        if s.status.mark_removed.unwrap_or(false) {
            flags.push("markRemoved".to_string());
        }
        if let Some(e) = s.status.error.as_deref().filter(|e| !e.is_empty()) {
            flags.push(format!("error={e}"));
        }
        out.push_str(&format!(
            "  - {}  created={}  ready={}  size={}  restoreSize={}  userCreated={}{}\n",
            s.name,
            dash(s.status.creation_time.as_deref()),
            s.status.ready_to_use.unwrap_or(false),
            size_int(s.status.size),
            size_int(s.status.restore_size),
            s.status.user_created.unwrap_or(false),
            if flags.is_empty() {
                String::new()
            } else {
                format!("  [{}]", flags.join(", "))
            },
        ));
    }

    // ── Backups ───────────────────────────────────────────────────────────
    out.push_str(&format!("\nBackups ({}):\n", backups.len()));
    if let Some(bv) = backup_volume {
        out.push_str(&format!(
            "  BackupVolume: lastBackup={} at {} stored={} target={}\n",
            dash(bv.status.last_backup_name.as_deref()),
            dash(bv.status.last_backup_at.as_deref()),
            size_str(bv.status.data_stored.as_ref()),
            dash(bv.spec.backup_target_name.as_deref()),
        ));
    }
    if backups.is_empty() {
        out.push_str("  (none)\n");
    }
    for b in backups {
        out.push_str(&format!(
            "  - {}  state={}  snapshot={}  size={}  created={}\n",
            b.name,
            dash(b.status.state.as_deref()),
            dash(
                b.status
                    .snapshot_name
                    .as_deref()
                    .or(b.spec.snapshot_name.as_deref())
            ),
            size_str(b.status.size.as_ref()),
            dash(b.status.backup_created_at.as_deref()),
        ));
    }
    if v.status.last_backup.as_deref().unwrap_or("") != "" {
        out.push_str(&format!(
            "  volume.status.lastBackup: {} at {}\n",
            dash(v.status.last_backup.as_deref()),
            dash(v.status.last_backup_at.as_deref()),
        ));
    }

    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn human_bytes_uses_binary_units() {
        assert_eq!(human_bytes(0), "0B");
        assert_eq!(human_bytes(512), "512B");
        assert_eq!(human_bytes(1024), "1Ki");
        assert_eq!(human_bytes(10 * (1 << 30)), "10Gi");
        assert_eq!(human_bytes(1536), "1.5Ki");
        assert_eq!(human_bytes(10_737_418_240), "10Gi");
    }

    #[test]
    fn size_str_parses_longhorns_quoted_decimals() {
        assert_eq!(size_str(Some(&"10737418240".to_string())), "10Gi");
        assert_eq!(size_str(None), "-");
        assert_eq!(size_str(Some(&"".to_string())), "-");
        assert_eq!(size_str(Some(&"not-a-number".to_string())), "-");
    }

    #[test]
    fn size_int_handles_the_plain_integer_fields() {
        assert_eq!(size_int(Some(2_147_483_648)), "2Gi");
        assert_eq!(size_int(None), "-");
    }

    #[test]
    fn dash_replaces_empty_and_absent() {
        assert_eq!(dash(Some("node-1")), "node-1");
        assert_eq!(dash(Some("")), "-");
        assert_eq!(dash(None), "-");
    }

    #[test]
    fn sanitize_k8s_name_produces_dns1123_segments() {
        assert_eq!(sanitize_k8s_name("Postgres_Data/0"), "postgres-data-0");
        assert_eq!(
            sanitize_k8s_name("--leading-and-trailing--"),
            "leading-and-trailing"
        );
        assert_eq!(sanitize_k8s_name(&"x".repeat(100)).len(), 60);
    }

    #[test]
    fn default_snapshot_name_is_deterministic_and_safe() {
        let now = Utc.with_ymd_and_hms(2026, 9, 2, 14, 30, 12).unwrap();
        let name = default_snapshot_name("pvc-840804d8-6f11-49fd-afae-54bc5be639de", now);
        assert_eq!(
            name,
            "pvc-840804d8-6f11-49fd-afae-54bc5be639de-mcp-20260902-143012"
        );
        assert!(name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
    }

    fn replica(failed: &str, healthy: &str, evict: bool, active: bool) -> Replica {
        Replica {
            name: "r-1".to_string(),
            spec: crate::model::ReplicaSpec {
                failed_at: Some(failed.to_string()),
                healthy_at: Some(healthy.to_string()),
                eviction_requested: Some(evict),
                active: Some(active),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn replica_predicates_match_longhorns_own_rule() {
        assert!(replica_usable(&replica(
            "",
            "2026-09-01T00:00:00Z",
            false,
            true
        )));
        assert!(replica_healthy(&replica(
            "",
            "2026-09-01T00:00:00Z",
            false,
            true
        )));

        // Rebuilding: healthyAt is cleared, but the replica is still usable.
        assert!(replica_usable(&replica("", "", false, true)));
        assert!(!replica_healthy(&replica("", "", false, true)));

        assert!(!replica_usable(&replica(
            "2026-09-01T00:00:00Z",
            "",
            false,
            true
        )));
        assert!(!replica_usable(&replica("", "x", true, true)));
        assert!(!replica_usable(&replica("", "x", false, false)));

        // Absent fields must not be read as failure.
        assert!(replica_usable(&Replica::default()));
    }

    #[test]
    fn active_engine_prefers_the_active_one() {
        let mk = |name: &str, active: bool| Engine {
            name: name.to_string(),
            spec: crate::model::EngineSpec {
                active: Some(active),
                ..Default::default()
            },
            ..Default::default()
        };
        let engines = vec![mk("e-old", false), mk("e-new", true)];
        assert_eq!(active_engine(&engines).unwrap().name, "e-new");

        // No engine flagged active (e.g. detached volume): fall back to the first.
        let engines = vec![mk("e-only", false)];
        assert_eq!(active_engine(&engines).unwrap().name, "e-only");
        assert!(active_engine(&[]).is_none());
    }
}
