# Isolated Kind E2E

Status: 0.4 target suite contract. A scenario is evidence only after its product path is implemented and the
script completes without direct status success patches or host-side replacement effects.

The suite uses one control-plane and at least two Linux workers backed by different temporary host directories.
A shared directory cannot prove node-local storage behavior.

```text
control-plane
├── worker-a ── /mnt/shiftpv-a
└── worker-b ── /srv/shiftpv-b
```

## Requirements

- healthy Docker-compatible engine
- kind 0.33.0 or newer
- kubectl and Helm 3
- space for pinned node, CSI sidecar, and ShiftPV images

```bash
./test/e2e/kind/run.sh
```

## Required scenarios

| Area | Evidence |
|---|---|
| Pool | existing ordinary directories, exact identity, generation-fenced complete inventory |
| Provision | `WaitForFirstConsumer`, exact topology, Volume owner hold before directory effect |
| Publish | RWO owner-only bind mount and scan/publish race serialization |
| Capacity | filesystem pressure plus Volume/Move holds; no release before cleanup closure |
| Retain/Delete | data preservation, explicit retirement, Volume journal/finalizer convergence |
| Mobility | bidirectional cold move, stable PVC/PV/handle/checksum, one owner |
| Restart | Controller and Node restart at every intent/effect/receipt boundary |
| Node outage | source and destination stop/start before and after owner commit |
| Filesystem fault | partial copy, ENOSPC, inode exhaustion, read-only, checksum mismatch |
| GC | parent-owned cleanup only; unknown orphan report-only and Pool removal blocked |
| Removal | mounted, retained, moving, deleting, hold, stale inventory and API error all fail closed; successful Helm removal leaves no release-owned workload, RBAC, storage or webhook object |

Every scenario checks API state, actual paths and mounts, capacity holds, logs/events, restart count and final
fixture cleanup. One successful Move or checksum is not sufficient.

## Focused runs

```bash
POOL_CAPACITY_ONLY=1 CLUSTER_NAME=shiftpv-capacity-focused ./test/e2e/kind/run.sh
DIRECTORY_POOL_ONLY=1 CLUSTER_NAME=shiftpv-directory-focused ./test/e2e/kind/run.sh
VOLUME_DELETE_CLEANUP_ONLY=1 CLUSTER_NAME=shiftpv-delete-focused ./test/e2e/kind/run.sh
CLEANUP_EXECUTOR_RETRY_ONLY=1 CLUSTER_NAME=shiftpv-cleanup-retry-focused ./test/e2e/kind/run.sh
MOBILITY_FILESYSTEM_FAULTS_ONLY=1 CLUSTER_NAME=shiftpv-mobility-fs-focused ./test/e2e/kind/run.sh
MOBILITY_NODE_RESTARTS_ONLY=1 CLUSTER_NAME=shiftpv-mobility-restart-focused ./test/e2e/kind/run.sh
MOBILITY_NODE_RESTARTS_ONLY=1 MOBILITY_NODE_RESTART_CASE=source,destination \
  CLUSTER_NAME=shiftpv-mobility-restart-cases ./test/e2e/kind/run.sh
```

`MOBILITY_NODE_RESTART_CASE` takes one case or a comma/space-separated list out of `source`, `destination`,
`copying-source`, `copying-destination`, `promoting-destination`, `committed-destination`, `cleaning-source`
and `cleaning-destination`; CI runs those eight in parallel jobs instead of one serialized sweep.

Each run owns a unique cluster name, kubeconfig, image tag and host directories. It removes exactly those
resources on success or failure. `KEEP_CLUSTER=1` is for bounded diagnosis only.

## Related suites

| Suite | Contract |
|---|---|
| [`mobility/`](mobility/README.md) | closed-loop planned move, restart and fault coverage |
| [`argocd/`](argocd/README.md) | Argo CD Application removal through the uninstall guard |
| [`artifact/`](artifact/README.md) | provenance of the published chart and digest-pinned images |
| [`upgrade/`](upgrade/README.md) | published release upgraded in place to this checkout, plus the documented StorageClass replacement |

A fresh install only ever sees one chart. `upgrade/` is the suite that observes an immutable StorageClass field
changing between releases and a webhook rule that contradicts the migration documentation.
