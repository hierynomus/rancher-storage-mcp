# rancher-storage-mcp

An MCP server that gives an AI agent read and write access to **Longhorn** block storage on
any Rancher-managed Kubernetes cluster. It talks to the Longhorn CRDs directly, so it can
explain why a volume is degraded, change a volume's replica count, and take snapshots —
without anyone opening the Longhorn UI.

Sibling to [`rancher-helm-mcp`](https://github.com/hierynomus/rancher-helm-mcp), which does the
same for Helm releases.

---

## How it works

```
┌────────────┐    MCP (stdio or streamable HTTP)
│  AI agent  │──────────────────────────────┐
└────────────┘                              │
                                            ▼
                              ┌──────────────────────────┐
                              │   rancher-storage-mcp    │
                              └──────────────────────────┘
                                   │                │
                GET /v3/clusters   │                │  Kubernetes API via
                (name → cluster id)│                │  /k8s/clusters/<id>
                                   ▼                ▼
                          ┌────────────────────────────────────┐
                          │          Rancher Manager           │
                          └────────────────────────────────────┘
                                                    │  cluster proxy
                                                    ▼
                            ┌────────────────────────────────────────┐
                            │  Downstream cluster · longhorn-system  │
                            │                                        │
                            │  volumes.longhorn.io      (read+patch) │
                            │  engines.longhorn.io      (read)       │
                            │  replicas.longhorn.io     (read)       │
                            │  snapshots.longhorn.io    (read+create)│
                            │  backups.longhorn.io      (read)       │
                            │  backupvolumes.longhorn.io(read)       │
                            └────────────────────────────────────────┘
```

Everything is pinned to **`longhorn.io/v1beta2`**. `v1beta1` is no longer served as of
Longhorn 1.9; `v1beta2` is the storage version on 1.7, 1.8 and 1.9 alike.

Unlike `rancher-helm-mcp`, this server does **not** call
`?action=generateKubeconfig`. It points `kube-rs` straight at Rancher's cluster proxy
(`{RANCHER_URL}/k8s/clusters/<cluster-id>`) and reuses the Rancher token it already has.
That avoids minting a fresh Rancher token on every call, and it means authorization is
exactly the token's own RBAC on the downstream cluster — see [RBAC](#rbac).

---

## MCP tools

| Tool | Purpose |
|---|---|
| `list_clusters` | List Rancher-managed clusters visible to the configured token. |
| `list_volumes` | List Longhorn volumes with state, robustness, size, healthy/desired replica counts and the PVC they back. Filters by `namespace`, `state`, `robustness`, `name_contains`. |
| `inspect_volume` | Full detail for one volume: spec and status, conditions that are not True, every replica with node/disk/mode/health, in-flight rebuild or restore progress, snapshots and backups. |
| `set_volume_replica_count` | Patch `spec.numberOfReplicas`, after checking Longhorn's own preconditions. |
| `set_volume_replica_countPlan` | Describe that change without making it, for human confirmation. |
| `create_volume_snapshot` | Take a snapshot by creating a `Snapshot` CR. Idempotent per snapshot name. |
| `create_volume_snapshotPlan` | Describe that snapshot without taking it, for human confirmation. |

The three volume tools accept a Longhorn volume name (`pvc-<uuid>`), a `namespace/pvc-name`
reference, or a bare PVC name when it is unambiguous — an ambiguous name returns the
candidates rather than picking one.

### Human-in-the-loop

Both mutating tools are listed in `ai-agent-config.yaml` under
`spec.humanValidationTools`, and each has a `<tool_name>Plan` companion. The Rancher AI
front-end calls the Plan tool first and renders its output as the confirmation prompt the
human approves or rejects, then calls the real tool only on approval — so a mutating tool
without its Plan companion would leave the human nothing to approve.

Plan tools take the **same arguments** as the tool they describe and return structured JSON:

```json
[{
  "type": "update",
  "resource": { "kind": "Volume", "name": "pvc-840804d8-…",
                "namespace": "longhorn-system", "cluster": "c-m-abcd1234" },
  "payload": { "original": { "numberOfReplicas": 3 },
               "patched":  { "numberOfReplicas": 2 } }
}]
```

They read from the cluster but never write. Two consequences worth knowing:

- The `original` values are the volume's **real** current values, so the prompt renders an
  actual delta rather than a placeholder.
- The Plan tool runs the same preconditions as the real tool, so an impossible change is
  refused *before* a human is asked to approve it. Asking for approval of something that will
  then be rejected is worse than refusing up front.

When `snapshot_name` is omitted, the plan shows the pattern
`<volume>-mcp-<utc-timestamp>` rather than a concrete name, because the timestamp is generated
at execution time and a concrete name in the plan would not match it.

### Mutating tools submit an intent, then poll

Nothing in Longhorn is synchronous. Both mutating tools patch or create, then poll a specific
status predicate for up to 30 seconds and report **what they actually observed**:

- `create_volume_snapshot` waits for `status.creationTime != "" && status.readyToUse == true`.
  A non-empty `status.error` in the meantime is usually *not* a failure — on a detached volume
  Longhorn first raises a VolumeAttachment ticket and retries — so it is reported as waiting.
- `set_volume_replica_count` waits for the usable replica count to reach the target. A
  scale-up then rebuilds for minutes to hours; a scale-down removes at most one excess replica
  per reconcile. Neither tool claims success it did not see: it tells the caller to re-run
  `inspect_volume`.

### Replica-count preconditions

`set_volume_replica_count` refuses before touching the cluster when:

| Condition | Why |
|---|---|
| `replica_count` outside 1–20 | Longhorn's validating webhook rejects it, and the bound is not in the OpenAPI schema, so the raw failure is an opaque 422. |
| `dataLocality=strict-local` and `replica_count != 1` | Webhook-enforced. |
| Volume is not `attached` | Longhorn accepts the patch but defers all rebuild work until the next attach — which is why its own UI refuses it. Pass `allow_detached: true` to override. |
| Engine upgrade in flight (`spec.image != status.currentImage`) | Mirrors Longhorn's own `UpdateReplicaCount`. |
| Live migration in flight (`spec.migrationNodeID` set) | Mirrors Longhorn's own `UpdateReplicaCount`. |

The last three are enforced only in Longhorn's HTTP API layer, not in its webhook, so a direct
`PATCH` would bypass them.

---

## Typical AI workflow

> **User:** the postgres pod is complaining about its disk, what's going on?

1. `list_volumes(cluster="prod", namespace="postgres")` → `pvc-840804d8-… state=attached robustness=degraded replicas=2/3`
2. `inspect_volume(cluster="prod", volume="postgres/data-postgres-0")` → replica `r-3` has `mode=ERR` and a `failedAt`; `Scheduled=False` with reason `ReplicaSchedulingFailure`
3. Agent explains: one replica failed and Longhorn cannot place a replacement because no other node has schedulable disk space.

> **User:** snapshot it before I resize the disks

4. `create_volume_snapshot(cluster="prod", volume="postgres/data-postgres-0", snapshot_name="pre-disk-resize")` → `Ready: yes`

---

## Configuration

All configuration is by environment variable. There are no CLI flags.

| Variable | Required | Default | Description |
|---|---|---|---|
| `RANCHER_URL` | yes | — | Rancher Manager base URL, e.g. `https://rancher.example.com`. |
| `RANCHER_TOKEN` | yes | — | Rancher API token. Used as the bearer token for both `/v3` and the cluster proxy. |
| `RANCHER_TLS_VERIFY` | no | `true` | `false` or `0` skips TLS verification (self-signed lab certs). |
| `LONGHORN_NAMESPACE` | no | `longhorn-system` | Namespace Longhorn is installed into on the downstream clusters. |
| `PORT` | no | — | When set, serve streamable HTTP on this port. When unset, serve stdio. |
| `RUST_LOG` | no | `info` | `tracing` `EnvFilter` directive. Logs always go to stderr. |

### Generating a Rancher API token

In the Rancher UI: avatar menu → **Account & API Keys** → **Create API Key**. Leave the scope
unset for access to every cluster the user can see, or scope it to one cluster. The token's
own permissions are what this server gets — it does not escalate.

---

## RBAC

**The pod needs no Kubernetes RBAC in its own cluster.** It never calls the local API server,
which is why this chart contains no `Role`, no `RoleBinding`, and sets
`automountServiceAccountToken: false`.

Authorization is entirely the Rancher token's user's RBAC **on each downstream cluster**. Bind
this `Role` there — a namespaced `Role` is enough, because every Longhorn CRD is namespaced:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: rancher-storage-mcp
  namespace: longhorn-system
rules:
  - apiGroups: ["longhorn.io"]
    resources:
      - volumes
      - engines
      - replicas
      - snapshots
      - backups
      - backupvolumes
      - backuptargets
      - nodes
      - settings
      - volumeattachments
    verbs: ["get", "list", "watch"]
  - apiGroups: ["longhorn.io"]
    resources: ["volumes"]
    verbs: ["patch"]
  - apiGroups: ["longhorn.io"]
    resources: ["snapshots"]
    verbs: ["create"]
```

> Note the deliberate omissions. There are **no `/status` subresource verbs** — Longhorn's
> controllers own every `status` field and this server never writes one. There is no `update`
> on `snapshots`, because `createSnapshot` is idempotent. There is no `delete` anywhere. And
> emphatically do not reuse Longhorn's own `longhorn-role` ClusterRole, which is
> `verbs: ["*"]` over everything.
>
> Read access degrades gracefully: if the token cannot list `snapshots` or `backups`,
> `inspect_volume` still returns the volume and replica detail and notes what it could not read.

---

## Deployment

```bash
helm install rancher-storage-mcp oci://ghcr.io/hierynomus/rancher-storage-mcp/charts/rancher-storage-mcp \
  --namespace rancher-storage --create-namespace \
  --set rancher.url=https://rancher.example.com \
  --set rancher.token=token-abcde:xxxxxxxxxxxx
```

For production, keep the token out of Helm values by creating the Secret yourself with keys
`RANCHER_URL` and `RANCHER_TOKEN`, then:

```bash
helm install rancher-storage-mcp oci://ghcr.io/hierynomus/rancher-storage-mcp/charts/rancher-storage-mcp \
  --namespace rancher-storage --create-namespace \
  --set rancher.existingSecret=rancher-credentials
```

Longhorn installed somewhere other than `longhorn-system`:

```bash
helm upgrade rancher-storage-mcp ... --set longhorn.namespace=longhorn
```

Expose it over Ingress:

```yaml
ingress:
  enabled: true
  className: traefik
  host: rancher-storage-mcp.example.com
  tls:
    enabled: true
    certManager:
      enabled: true
      clusterIssuer: letsencrypt-prod
```

### As a Rancher AI agent

`ai-agent-config.yaml` registers this server with Rancher's AI agent supervisor, including a
system prompt and `humanValidationTools: [set_volume_replica_count, create_volume_snapshot]`
so both mutating tools require human approval:

```bash
kubectl apply -f ai-agent-config.yaml
```

---

## Client configuration

Streamable HTTP (the deployed server):

```json
{
  "mcpServers": {
    "rancher-storage": {
      "url": "https://rancher-storage-mcp.example.com/mcp"
    }
  }
}
```

stdio (a local binary):

```json
{
  "mcpServers": {
    "rancher-storage": {
      "command": "/usr/local/bin/rancher-storage-mcp",
      "env": {
        "RANCHER_URL": "https://rancher.example.com",
        "RANCHER_TOKEN": "token-abcde:xxxxxxxxxxxx"
      }
    }
  }
}
```

---

## Development

```bash
mise exec -- cargo test
```

```bash
mise exec -- cargo build --release
```

Run locally over stdio and drive it from Claude Code:

```bash
claude mcp add rancher-storage -- env RANCHER_URL=https://rancher.example.com RANCHER_TOKEN=token-abcde:xxxx ./target/release/rancher-storage-mcp
```

Lint and inspect the chart:

```bash
mise exec -- helm lint charts/rancher-storage-mcp --set rancher.url=https://x --set rancher.token=y
```

See [CLAUDE.md](CLAUDE.md) for the SDK version pins and the Longhorn field-decoding traps.
