# CLAUDE.md

See [README.md](README.md) for what this is, how to configure it and how to run it. This file
covers the things that are easy to get wrong.

Sibling projects, worth reading when changing shared idioms:
[`../rancher-helm-mcp/CLAUDE.md`](../rancher-helm-mcp/CLAUDE.md) (the Helm MCP server this one
mirrors) and `../rancher-mcp-proxy/CLAUDE.md`.

## Project structure

```
src/
├── main.rs        Env-var config, crypto provider, transport selection (stdio vs HTTP)
├── credentials.rs ClientFactory: builds BOTH clients from one Rancher URL + token
├── rancher.rs     Rancher v3 REST client — only `GET /v3/clusters`, for name → id resolution
├── cluster.rs     Builds a kube::Client per downstream cluster via Rancher's cluster proxy
├── longhorn.rs  The CRD layer: LonghornApi trait + LonghornClient (kube dynamic API)
├── model.rs     Narrow serde views of Volume/Engine/Replica/Snapshot/Backup/BackupVolume
├── render.rs    Pure formatting, size decoding, replica health predicates
└── server.rs    StorageMcpServer: five tools, preconditions, polling, ServerHandler
```

`RancherApi` and `LonghornApi` are traits so `server.rs` can be tested against stubs. That is
the whole reason they exist — keep tool logic above the trait, not below it.

## Upstream drift

`rancher-helm-mcp` is the reference for shared idioms, and it moves. **Fetch and read
`origin/main`, not a local checkout** — the local clone was eight commits behind while this
project was first written, and the missing commits contained the whole `*Plan` tool convention
above. In this shell `git` is a `scmpuff` function that needs `$SCMPUFF_GIT_CMD`, so call
`/usr/bin/git` directly:

```bash
/usr/bin/git -C ../rancher-helm-mcp fetch origin && /usr/bin/git -C ../rancher-helm-mcp log --oneline HEAD..origin/main
```

## rustls needs an explicit crypto provider

`reqwest` and `kube-client` each pull `rustls` 0.23 with a *different* provider feature, so
both `ring` and `aws-lc-rs` end up enabled and rustls refuses to pick one. Building any TLS
client then **panics** inside rustls rather than returning an error. `cluster::install_crypto_provider()`
resolves it once, and is called from `main` and from every test that builds a client. If you
ever see `Could not automatically determine the process-level CryptoProvider`, a code path is
constructing a client without having called it. Upstream hit the same thing — see
`hierynomus/rancher-helm-mcp` branch `fix/rustls-crypto-provider`.

## rmcp API (v1.7)

Three things that cost time if rediscovered:

- Put `#[tool_router]` on the inherent `impl` and `#[tool_handler]` on a **separate**
  `impl ServerHandler` block. Do *not* use `#[tool_router(server_handler)]` when you also
  write your own `get_info()` — the generated impl conflicts.
- Use `rmcp::ErrorData`. `rmcp::Error` is a deprecated alias.
- `ServerInfo` is a type alias for `InitializeResult`, which is `#[non_exhaustive]`. Use
  `InitializeResult::new(..).with_server_info(..)`, not struct-literal syntax.

Tools return `Result<String, rmcp::ErrorData>`; rmcp wraps the `String` as a single text
content block. There is no structured output anywhere, matching the sibling.

Non-tool helper methods must live in their own un-annotated `impl` block or `#[tool_router]`
will try to register them.

To read the SDK source: `find ~/.cargo/registry/src -maxdepth 1 -iname "rmcp-*"`.

## Per-request credentials

`RANCHER_URL`/`RANCHER_TOKEN` are **optional**. `call_tool` is overridden to look for `R_url`
and `R_token` headers in `cx.extensions.get::<http::request::Parts>()`; when present it mints a
scoped clone of the server and dispatches through
`Self::tool_router().call(ToolCallContext::new(&scoped, request, cx))`. With no headers and no
static credentials it returns `invalid_request`.

Things that will bite:

- `credentials::ClientFactory` exists because the Rancher URL and token are needed in **two**
  places here — `RancherClient` for `/v3`, and `ClusterClients` for the cluster proxy. An
  override must rebuild both. `rancher-helm-mcp` only rebuilds its REST client, so do not copy
  its single-client shape.
- The `rancher_client` / `longhorn_client` fields are `Option`, reached through the
  `rancher()` / `longhorn()` accessors which `expect`. That is sound *only* because
  `call_tool` rejects the uncredentialed case before dispatching. Any new entry point that
  reaches a tool body without going through `call_tool` breaks that invariant.
- `#[tool_handler]` stays on the `impl ServerHandler` block even though `call_tool` is written
  by hand — the macro only fills in what is missing. `list_tools` is still generated, which is
  why `tools/list` works without credentials.
- Only the HTTP transport has headers. A stdio server must have the env vars.
- Header lookup is case-insensitive (`HeaderMap::get` normalises), so `r_url` works too.
  Blank header values are ignored rather than used to build a broken client.
- Testing this needs the real `StreamableHttpService`, because nothing else injects
  `http::request::Parts` into `cx.extensions`. The tests in `server.rs` spin up the service on
  an ephemeral port and fire raw JSON-RPC at it, pointing `R_url` at a wiremock Rancher that
  only answers for one specific bearer token — which is what proves the header's credentials
  were the ones used. Note the transport may answer as SSE, so responses are parsed by
  stripping a `data:` prefix.

## Human-in-the-loop: the `*Plan` tool convention

Every tool listed in `ai-agent-config.yaml`'s `spec.humanValidationTools` **must** have a
companion tool named `<tool_name>Plan`. The Rancher AI front-end calls the Plan tool first and
renders its output as the confirmation prompt, then calls the real tool only on approval. A
mutating tool without its Plan companion silently skips the human-in-the-loop step — there is
nothing for the human to approve. This is half of the safety mechanism, not an optional extra.

The convention comes from `../rancher-helm-mcp/src/server.rs`:

- Declare the name explicitly with `#[tool(name = "set_volume_replica_countPlan")]` — the Rust
  method name is snake_case, the tool name is not.
- Take the **same input struct** as the tool it describes, so the agent can pass identical
  arguments through after approval.
- Return a JSON **string** via `serde_json::to_string` of an array of
  `{"type": "create"|"update", "resource": {"kind","name","namespace","cluster"}, "payload": {…}}`.
  For an update, `payload` carries `original` and `patched`. **Returning prose instead of this
  JSON stops the confirmation prompt from rendering** — that was the bug fixed upstream in
  `hierynomus/rancher-helm-mcp` PR #6.
- List only the real tools in `humanValidationTools`, never the Plan tools.

Two deliberate divergences from the Helm server, whose Plan tools are pure functions:

- Ours read from the cluster, so `original` holds the volume's real current values and the
  prompt shows a true delta instead of a placeholder like the Helm server's
  `{"revision": "current"}`.
- Ours run the same preconditions as the real tool and error out, so an impossible change is
  refused before a human is asked to approve it.

They must never write. `plan_tools_record_no_writes` in `server.rs` asserts that.

## kube crate (v0.99)

- `kube` and `k8s-openapi` versions are coupled. Check with `cargo tree -i k8s-openapi` before
  bumping either.
- Longhorn types are not in `k8s-openapi`, so everything goes through
  `Api::<DynamicObject>::namespaced_with(client, ns, &api_resource)`.
- **Give plurals explicitly** via `ApiResource::from_gvk_with_plural`. Naive pluralization is
  wrong for some kinds, and `nodes`, `volumeattachments` and `settings` collide with core and
  `storage.k8s.io` plurals — always qualify by group (`nodes.longhorn.io`).
- `kube::Config` has public fields and a `Config::new(Uri)` constructor, which is what lets
  `cluster.rs` build a client for an arbitrary URL. The bearer token goes in `cfg.headers`
  rather than `cfg.auth_info.token`, which would require `secrecy::SecretString` as a direct
  dependency version-locked to kube's own.
- k8s-openapi structs are `#[non_exhaustive]`; finish them with `..Default::default()`.

## Longhorn API (v1beta2)

Pinned to `longhorn.io/v1beta2` everywhere. `v1beta1` is `served: false` from Longhorn 1.9 and
returns 404; `v1beta2` is the storage version on 1.7, 1.8 and 1.9.

### Field-decoding traps

These are the bugs `model.rs` exists to prevent. All of them are covered by tests in
`model.rs` — if you touch that file, keep them passing.

- **Byte counts have three encodings.** Quoted decimals from Go's `json:"...,string"`
  (`volume.spec.size`, `engine.status.currentSize`, …); genuine strings
  (`engine.status.snapshots[*].size`, `backup.status.size`); and plain integers
  (`volume.status.actualSize`, `snapshot.status.size`/`restoreSize`). `model.rs` uses lenient
  `num_or_string` / `int_or_string` deserializers so a call site never has to know which.
- **`volume.spec.Standby` is capital-S** (`json:"Standby"`) — unique in the whole API.
  `status.isStandby` is normal camelCase.
- **`usercreated` vs `userCreated`.** All-lowercase in `engine.status.snapshots[*]`, camelCase
  in the `Snapshot` CR. Same concept, two spellings.
- **Replica health timestamps are in `spec`, not `status`**: `spec.healthyAt`, `spec.failedAt`,
  `spec.lastHealthyAt`, `spec.lastFailedAt`. `healthyAt` and `failedAt` are *cleared* before a
  rebuild.
- **Most timestamps are opaque strings and `""` is the unset sentinel**, not `null` and not
  `metav1.Time`. Never test them with `is_none()`; test for empty.
- **`+nullable` fields serialize as `null`**, not `{}` / `[]`. Everything is `Option` +
  `#[serde(default)]`.
- `Setting` has no `spec` — its value is top-level. Don't write code that assumes every
  Longhorn CR has a `spec`.
- Version deltas to tolerate: `volume.spec.backupTargetName` absent on 1.7,
  `volume.status.pendingNodeID` removed in 1.9, `spec.engineImage` and `spec.backendStoreDriver`
  removed in 1.9 (use `spec.image` / `spec.dataEngine`).
- `engine.status.snapshots` contains a synthetic `volume-head` entry — filter it, and filter
  `removed == true`. It is only populated while the engine is *running*; check
  `status.snapshotsError`.
- The `Volume` CRD's `Scheduled` printer column filters on `@.type=='Schedulable'`, which is
  not a Volume condition type, so `kubectl get lhv` always shows it empty. Read
  `status.conditions[type=="Scheduled"]` directly.

### Labels are bare — no `longhorn.io/` prefix

| Resource | Selector |
|---|---|
| engines, replicas, snapshots | `longhornvolume=<volume>` |
| backups | `backup-volume=<volume>`, plus `backup-target=<target>` on 1.8+ |

`Backup` has **no** `spec.volumeName`, so the label is the only queryable link.
`BackupVolume` *does* have `spec.volumeName`, but its CR name is not reliably the volume name,
so `get_backup_volume` lists and filters on the spec field.

`list_engines` / `list_replicas` / `list_snapshots` query by label and fall back to an
unfiltered list filtered client-side, so an unlabelled object is still found. The fallback
costs an extra request only when the selector genuinely matches nothing.

### Writes

Only two, and both are deliberate:

- **`PATCH` `{"spec":{"numberOfReplicas":N}}` as a JSON merge patch.** Never GET-mutate-PUT: the
  volume validator rejects an empty `spec.backupTargetName`, so round-tripping a volume created
  under Longhorn 1.7 (where that field does not exist) submits `""` and gets a 422 on a field
  you never touched. 409 Conflict is routine — Longhorn's controllers write these objects
  constantly, which is why `patch_replica_count` retries.
- **`POST` a `Snapshot` CR** with `spec.volume` + `createSnapshot: true`. The CR name *is* the
  Longhorn snapshot name; there is no separate field. Do not set
  `metadata.labels["longhornvolume"]` or `ownerReferences` — the Longhorn mutator adds both and
  they are immutable once present, as is `spec.volume`.

`Engine` and `Replica` are **read-only**. The volume controller owns
`engine.spec.{replicaAddressMap,desireState,active}` and the replicas' `failedAt`/`healthyAt`.
Never write `status` on any Longhorn CR — the `/status` subresource silently discards it on the
main resource, and writing via `/status` fights the controllers. Never set `volume.spec.nodeID`
to attach or detach: since Longhorn 1.5 that is arbitrated by `volumeattachments.longhorn.io`
tickets, and writing behind them is how volumes get stuck in `attaching`/`detaching`.

### Not used, on purpose

- The Longhorn HTTP API (`POST /v1/volumes/<n>?action=snapshotCreate`) is an unauthenticated
  Service endpoint with no Kubernetes RBAC and no audit trail, and its per-volume actions are
  proxied to the manager pod on the volume's owner node.
- CSI `VolumeSnapshot` needs the external snapshot controller and `snapshot.storage.k8s.io`
  CRDs, which many distros omit — and an empty `parameters.type` silently defaults to `bak`
  (an off-cluster backup), not `snap`. Its `type: snap` path is only a wrapper around the same
  `Snapshot` CR we create directly.

## Eventual consistency

Design every mutating tool as *submit intent, then poll a specific status predicate* — never
"fire and report success". Concretely:

- Snapshot: accepted immediately; `status.creationTime`/`readyToUse` appear after reconcile,
  and on a detached volume Longhorn first raises an attachment ticket. The transient
  `status.error` in between is **not** a failure.
- Replica scale-up: CRs appear fast, `robustness` goes `degraded`, rebuild takes minutes to
  hours. Track `engine.status.rebuildStatus[*].progress`.
- Replica scale-down: at most one excess replica removed per reconcile.
- `status.ownerID` names the manager pod reconciling the object; reconciliation pauses during
  handover, which is a common source of "my patch did nothing for 30s".
- With `replicaAutoBalance: best-effort`, replicas churn *after* the count converges — replica
  CR names are not stable.

Poll durations live in `server.rs` (`POLL_TIMEOUT`, `POLL_INTERVAL`) and are overridable via
`with_poll` so tests can exercise the timeout branches in milliseconds.

## Helm chart conventions

- No `Role` and no `RoleBinding`, and `automountServiceAccountToken: false`: this server never
  calls its own cluster's API. RBAC lives on the *downstream* clusters and belongs to the
  Rancher token's user — documented in the README, not shipped by the chart.
- Rancher credentials come from a Secret, either generated from `rancher.url`/`rancher.token`
  or an `existingSecret`; `secret.yaml` uses `fail` guards so a missing value is a clear error.
- `PORT=3000` is set in the Deployment, which is what selects streamable HTTP over stdio.
- Keep the chart in step with the sibling's, and keep the README's RBAC table in step with the
  tool set — those two drift most easily when tools are added.

## Common tasks

```bash
mise exec -- cargo test
```

```bash
mise exec -- helm template rs charts/rancher-storage-mcp --set rancher.url=https://x --set rancher.token=y
```

Check what a real cluster returns for a field you are unsure about:

```bash
kubectl -n longhorn-system get volumes.longhorn.io -o yaml
```
