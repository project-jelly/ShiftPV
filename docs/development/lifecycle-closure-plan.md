# Lifecycle closure stabilization

Baseline: chart0.6.13 (`9b4b768`). Fixes and staged qualification are recorded in
[PR #237](https://github.com/project-jelly/ShiftPV/pull/237) and its preceding stages.
Use each PR's reviewed commit and required CI artifacts for execution evidence.
The final candidate must pass the integrated gates below before release readiness.
Publishing images/charts and Home rollout are separate operations.

## Goal and invariants

Once a supported fault recovers and external changes stop, the operation must
converge to success or cancellation settlement. Contradictory identity or missing
proof retains data and accounting with an explicit review reason and recovery
prerequisites. Reaching `Blocked` alone does not establish settlement.

- Only exact owner CAS transfers authority; postcommit recovery never returns it
  to the source.
- Destructive effects require exact identity, durable intent/receipt and fresh,
  generation-fenced absence evidence as applicable.
- Settlement stops executors, proves cleanup, releases only the Move's temporary
  holds, clears its gates and `activeMove`, then retires protection/history.
  A retained Volume continues to account for its authoritative copy.
- A workload binding, an executor instance and copy authority have different
  lifetimes. Missing or replaced PVCs do not identify a new owner or grant delete
  authority. API errors are unknown observations, never absence proof.
- Node/API recovery and fair processing are assumptions for convergence.
  Permanent authoritative disk/node loss remains operator recovery.

## Stage and event contracts

Explicit creation cancellation has the following boundaries:

- Spec-only intent uses exact UID/resourceVersion deletion and finalizer CAS.
- Pending/NodeCreating cancellation races completion on the same Volume CAS.
  Only the original initial copy is eligible; legacy helper responsibility or an
  unknown observation preserves the reservation.
- `Deleting` with `cancel-create-<VolumeUID>` and cleanup reason
  `CreationCancelled` fences creation. Older journal readers reject this reason.
  Cancellation never invents Ready or a successful creation receipt.
- The Node uses the creator's filesystem lock and checks copy/placement/inode
  evidence across serving, create-stage and retired paths. An unplaced stage
  must be empty. Ambiguous data is retained for review.
- A cleanup receipt and fresh generation-fenced complete absence precede Volume
  finalization and reservation release.
- Automatic retirement uses only immutable original-claim provenance, a monotonic
  end of that PVC incarnation and no PV handoff. Ready completion, unknown source,
  legacy helper responsibility or an API error never grant cleanup authority.
  CSI and background retirement share creation-only cancellation and exact cleanup
  settlement. Background work does not wait for an RPC effect.

| Event | Before execution approval | Approved, before owner CAS | After owner CAS |
|---|---|---|---|
| Placement Pod recreated | Revalidate preparation; cancel an obsolete reservation | Same Move ownership/role and fixed destination permit recreation | Same Move ownership permits retiring the reservation |
| Replacement Pod recreated | Revalidate the held workload | Reobserve exact binding and held replacement before pinning | Reobserve exact binding; safely pin/release the current held consumer |
| Placement replaced by foreign owner | Preserve foreign object; stop and diagnose | Preserve foreign object; stop and diagnose | Preserve foreign object; stop and diagnose |
| PVC deleted | Cancel preparation without requiring the PVC to survive | Quiesce and settle destination effects; preserve source authority | Settle forward at destination; never roll back owner |
| Same-name PVC with new UID | Never attach the old operation to the new request | Settle only recorded identities | Settle only recorded identities |
| Pool becomes read-only | Recheck operation-specific capability | Allow non-writing observation/unpublish; preserve effects requiring writes | Allow non-writing observation/unpublish; preserve cleanup until provable |
| Effect succeeded, response lost | Reobserve durable state and exact object identities | Reobserve receipts and owner; do not duplicate authority | Reobserve commit/receipts; do not revert owner |
| Controller/executor restart | Reload persisted intent and reservations | Resume only the recorded operation | Continue settlement under destination authority |

Move settlement preserves the authoritative copy and its Volume reservation for
both Delete and Retain. Ordinary CSI reclaim acts afterwards according to PV
policy. It never fabricates publication to replace a retired workload.

## Delivery

Each change follows implementation + regression evidence -> PR -> review and
required CI -> merge -> refreshed main. Regression tests ship with the fix;
record the failing baseline rather than merging a permanently failing test.

| Order | Scope | Acceptance |
|---|---|---|
| 1 | Pod lifetime policy: D3, cancellation and replacement release | New UID on recreation; same-operation progress; foreign-object rejection; exact UID mutation and observed absence |
| 2 | D7 unpublish on read-only backing | Actual Linux remount/Pod deletion; mutual exclusion; unknown identity preserved |
| 3 | 035 move/deletion settlement | PVC deletion before/after commit, Delete/Retain, name reuse, restart and owner/delete races |
| 4 | Confirmed unfinished-creation and read-only selection gaps | Cancellation settles exact partial effects/accounting without inventing a successful creation |
| 5 | Admission and background-work progress | Independent webhook budget; bounded deferred reevaluation; active operations progress |
| 6 | Integrated candidate qualification | Model, real reconciler, Linux and isolated Kind evidence; release-readiness report |

Share operation policy only where callers need the same contract. Preserve the
existing observe/decide/effect/read-back architecture and authority boundaries.
Do not introduce a generic transaction framework or a new protocol by default.

## Evidence

| Category | Required check |
|---|---|
| Safety | Foreign UID/owner/copy cannot be adopted or destroyed |
| Progress | Supported recovery/recreation advances without manual Pod deletion |
| Concurrency | Commit/delete races, CAS conflict, response loss and executor replacement |
| Settlement | Executors, receipts/absence, temporary holds, gates and protection |

Replay critical event traces in both the abstract model and actual reconciler.
Model abort requests must map to real deletion/recovery observations. Recoverable
Blocked cases need a convergence check; genuine contradictions remain preserved.
Kind qualification asserts data checksums and original identities together with
settlement, not only Pod readiness. Physical reboot/soak and load measurements
remain separately reported. This goal ends at release readiness; publishing
images/charts and Home rollout are separate operations.

### Workload-retirement trace coverage

`test/model/workload_settlement_model_test.go` independently models retirement
without importing the production FSM or cleanup predicates. Its state keeps
claim lifetime, owner CAS, publication, effect evidence and capacity settlement
separate. The normal mobility model continues to require destination publication;
retirement instead verifies the fenced, unpublished retained owner, with a causal
Pool scan for destination-owner retirement. Unlock clears the API gate; the model
does not introduce a permanent local fence.

| Boundary | Independent model event | Actual Registry/reconciler coverage |
|---|---|---|
| Original claim absent/replaced; timeout or terminating original is insufficient | `retireClaim`, `observeClaimEnd`, `observeUnknownClaim` | `TestWorkloadSettlementRegistryTrace`; `TestWorkloadRetirementRequiresAbsenceRatherThanAPIFailure` |
| Commit competes with retirement on exact current copy | `commitBeforeFence`, `fenceOwner` | `TestWorkloadSettlementAndOwnerCommitCompeteOnExactVolumeCAS` |
| Fence/intent/unlock stored but response lost; restart reads durable state | Separate fence, intent and unlock transitions | `TestWorkloadSettlementRegistryTrace`, all three accepted-write boundaries |
| Quiesce and verify owner without restarting service | `quiesceEffects`, `verifyOwner`, `observeUnpublishedOwner` | Registry trace preserves exact UID/copy/capacity and empty publication |
| Effect or API receipt alone cannot release capacity | `removeOtherCopy`, `recordLocalPurge`, `reportPurge` | Registry trace holds the actual embedded journal Pending across retries |
| Only a later complete scan closes cleanup | `requestAbsenceScan`, `observeFreshAbsence` | Registry trace withholds the target Pool scan for three retries |
| Release temporary hold, remove verifier, unlock, retire metadata | Final four settlement transitions | Registry trace checks per-Pool reservations, jobs, activeMove and finalizer |

The automatic and exact-UID entry each replay 24 Registry traces: both owners,
absent/replaced PVC, Delete/Retain and each accepted-write loss, with a fresh
reconciler each pass. Its effect and
scanner are injected; it proves API settlement, not native filesystem cleanup or
subsequent CSI Delete. The required Kind `retirement` group supplies that layer:
both owners × Delete/Retain, actual copy/purge, receipt loss and Controller restart,
later generation-fenced absence, retained payload or normal CSI deletion. The
required upgrade `move-recovery` scenario starts all four cases on pinned
published0.6.13, reproduces its blocked state, then uses exact-UID retirement while
normal writers are stopped. Admission-only serving preserves Pool scan validation.
A missing API receipt survives command restart; strict `upgrade-check` must become
SAFE before normal Controller cutover. Unrelated Ready data and all original
identities remain asserted. See [maintenance](versioning.md#원본-pvc가-끝난-native-move의-전환-정산).

### Final candidate gates

| Contract | Persisted execution evidence |
|---|---|
| D3 placement/replacement recreation | Required mobility `g3`, plus Registry/UID and cancellation boundary tests |
| D7 read-only unpublish | Linux real remount/bind-mount test; Kind `g2` makes the same published Pod's backing RO before deleting it and checks real mount absence before RW restore |
| 035 retirement | Independent model (328 reachable states), automatic/explicit Registry traces, native four-case `retirement` and pinned old-release `move-recovery` |
| Creation | Required `node-rpc` partial-copy retirement and real deferred original-PV handoff; pinned `creation-recovery` upgrade |
| Admission/progress | HTTP budget/deadline tests; bounded background retirement and deferred-discovery tests |
| Delivery | Every required exact-head check passes; merged main tree equals reviewed tree; original data/identity proof retained |

The deferred-discovery fixture changes 900 to140 typed/dynamic fake-client API
actions (84.4% fewer) across20 scan/work cycles for four unchanged volumes in one
30s retry interval, with zero writes. This measures request amplification through
the actual Registry/reconciler, not production network traffic or Move latency.

An independent model or fake executor cannot establish native cleanup. Kind is
not physical reboot, permanent disk-loss recovery, Home load or real KubeVirt
qualification. An inaccessible original VM fixture remains unverified until it
can be inspected without destroying its RAM-backed data.
