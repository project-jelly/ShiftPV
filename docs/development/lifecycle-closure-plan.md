# Lifecycle closure stabilization

Status: in progress, based on chart 0.6.13 (`9b4b768`).

Step 1: placement/cancellation [PR #222](https://github.com/project-jelly/ShiftPV/pull/222)
and consumer replacement release [PR #223](https://github.com/project-jelly/ShiftPV/pull/223)
and read-only Unpublish [PR #224](https://github.com/project-jelly/ShiftPV/pull/224)
merged; exact-head CI and local two-node Kind passed. Step 3 workload-retirement
settlement [PR #225](https://github.com/project-jelly/ShiftPV/pull/225) merged after
31 successful checks and precommit Delete/postcommit Retain Kind qualification.
Step 4 read-only allocation admission [PR #226](https://github.com/project-jelly/ShiftPV/pull/226)
merged after 30 successful checks and real Linux/Kind RO-to-RW qualification.
Unstarted creation cancellation [PR #227](https://github.com/project-jelly/ShiftPV/pull/227)
merged after required CI and real API deletion-fence qualification. Native partial
creation cancellation [PR #228](https://github.com/project-jelly/ShiftPV/pull/228)
merged after 31 successful checks, Linux boundary tests and Kind receipt/absence,
full source-quota reuse and normal cleanup proof. Original claim provenance is
under verification; automatic retirement and the remaining matrix are outstanding.

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

Move retirement passed the diagonal Kind cases; integrated qualification remains
outstanding. Explicit creation cancellation has the following boundaries:

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
  finalization and reservation release. This does not yet detect a missing
  original PVC automatically.

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

For PVC deletion, distinguish an explicit Delete request from Retain. Determine
whether existing durable records can represent that intent before adding fields.
Never fabricate publication or cleanup evidence to bypass a service-recovery
prerequisite. Unfinished creation needs the same intent/effect/settlement analysis
with its own executor and exact partial-copy evidence.

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
