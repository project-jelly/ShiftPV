# Volume Mobility Contract

This describes the current implementation. Qualification requirements are in [Testing](../development/testing.md).

ShiftPV moves one RWO Filesystem PVC from its current node-local owner to another
registered Pool. For a continuing workload, PVC, PV, and CSI volume handle identity remain stable. The
operation is eventually consistent when interrupted nodes or disks later return;
permanent authoritative disk or node loss is outside this contract.

## Authority

| Rule | Contract |
|---|---|
| Commit | Exact owner CAS is the only commit boundary. |
| Before commit | Source remains authoritative; abort or recovery returns to source. |
| After commit | Destination is authoritative; recovery only moves forward. |
| Partial copies | Incoming or incomplete copies never promote and never become owner. |
| Publish proof | Destination actual-publish proof is required before source cleanup. |
| Cleanup owner | The Move journal and finalizer own source and rollback cleanup decisions. |
| Executors | Jobs copy, verify and promote; resident Nodes execute new creation and cleanup. Read-only measurement helpers remain. Parent journals own truth. |
| Identity contradiction | Move enters `Blocked`; cleanup subjournals may enter `NeedsReview`. |

`activeMove` is the per-volume lock. The Move keeps it until source cleanup has
an API purge receipt and a fresh absence proof. Before commit, capacity is held
for the source owner and the destination incoming copy. After commit, the Volume
holds destination capacity and the Move continues holding the retained source
until both API purge receipt and fresh absence proof exist.

Destination approval records the Pool, incoming/destination copy identities,
operation IDs and planned copy Job name in one status write. An interruption
after approval therefore retains both the capacity hold and its recovery plan.

### Preparation and execution

Only `spec.protocolVersion: prepared-v1` executes in this controller. The registry
and API require it for new Moves. Legacy records remain readable for settled-history
validation and metadata retirement; reader/status compatibility does not permit execution.
Preparation later records a `status.preparation` logical reservation identified by Pool name/UID, node and
requested bytes. The shared capacity ledger counts it once; it protects its Pool
from deletion and never authorizes a disk effect. Updates use exact Move UID and
resourceVersion CAS, preserving the embedded cleanup journal.

Preparation keeps the source `Ready + activeMove`, so the original consumer can
keep using its copy. A non-preempting placement Pod reserves scheduler resources
before the destination Pool grants requested bytes. This reservation uses the
chart-provided, non-global PriorityClass at priority 0 with `preemptionPolicy: Never`;
it may wait behind higher-priority work even when the consumer has higher priority.
The controller writes `StartingDisruption`, re-observes the binding, source and placement, and only then
changes the source to `Moving` and evicts the recorded consumer UID.

After source unpublish, it measures actual source usage and destination free
space. One CAS converts the logical hold to execution approval with the complete
disk plan. The bytes stay counted once, even if the Pool limit was reduced after
the hold was granted. Logical reservations are not quotas: source growth or an
external writer can still make the physical-space check fail.

Before `StartingDisruption`, changed prerequisites can cancel preparation. That
durable intent marks possible eviction even when the API response was lost.
From this point, temporary Node/Pool unavailability, PDB denial, consumer
ambiguity or insufficient physical space retains the same Move, placement and
capacity hold for retry. Identity or placement contradictions enter `Blocked`;
they never authorize a different reservation or disk plan. A fresh PDB check
still precedes eviction of a live original Pod. An already terminating original
is observed as disrupted without re-eviction.

Confirmed retirement of the recorded original PVC UID can still settle an
unapproved Move with no destination effect intent. This requires its exact
reserved source authority. The controller first persists `Canceling` to fence
late approvals, then restores the exact source (including one blocked by that
Move) to Ready,
confirms the owned placement Pod is absent, then records `Cancelled` and releases
the logical hold. A later pass clears `activeMove` before retiring the journal.
Existing cancellation journals remain settleable without a surviving consumer;
they do not automatically reopen. Deleting a Move after disruption preserves its
authority in `Blocked`; it does not authorize stopping the workload or bypassing
the original-claim retirement check.

The recorded placement Pod UID fences preparation through execution approval.
After approval, the controller may recreate the same Move-owned reservation on
the persisted destination; this does not change the Pool, copy plan or owner.
Recreation keeps the same priority-0, non-preempting policy used for preparation.
Cancellation may remove a recreated reservation owned by the exact Move UID.
Removal uses the current observed Pod UID as a DELETE precondition and waits for
actual API absence before releasing its hold. Foreign ownership remains a conflict.

A replacement consumer can also be recreated while copying, promoting or waiting
after commit. On the committed owner, release rechecks the recorded PV/PVC UIDs,
current copy authority and the observed Pod UID. A new instance must be managed,
held, unassigned and owned by a live supported workload controller. The same Pod
update pins the destination and removes only ShiftPV's gate, preserving user
constraints. Publication wait and source cleanup revisit held replacements;
recreation does not require returning to the Copying phase or changing owner.

If multiple live consumers appear after approval, execution keeps observing its
Jobs and cleanup. Before owner commit it waits with `MultipleConsumers` until one
consumer remains; it never selects an arbitrary replacement. After commit,
destination publication and approved cleanup progress from their own evidence;
consumer ambiguity cannot hide Job failure or settled cleanup. Before approval
this shape waits after disruption intent, or defers/cancels earlier preparation.
A terminal Move-owned rsync source
Pod is deleted with its exact UID and recreated; copying waits for its readiness.

Before switching, settle active legacy work with its compatible old controller
and helper images. Exclude new work through cutover, run the candidate image's
read-only `upgrade-check`, and require a complete report with no blockers before
replacing the controller with `Recreate`. The inspection is not an atomic fence.
The candidate never resumes legacy effects or converts Job bindings to Nodes;
unknown or unsettled records are retained. Journal-bearing effects require exact
receipt and absence proof. A prepared source recovery without cleanup intent can
close through exact CAS settlement and its causal rollback fence; helper quiescence
and no Volume references remain required for history acceptance. See
[versioning](../development/versioning.md#planned-mobility-upgrades).

## Flow

```text
source owner
  -> persist binding/copy/consumer identities
  -> claim Ready + activeMove (source remains publishable)
  -> reserve scheduler placement and logical Pool capacity
  -> persist StartingDisruption, re-observe, lock Moving and evict original UID
  -> wait for source unpublish and held replacement
  -> check physical capacity and atomically convert the logical hold
  -> copy to destination incoming
  -> promote destination serving copy
  -> owner CAS commit
  -> release destination placement
  -> prove destination is actually published
  -> source cleanup
  -> fresh absence proof
  -> release Move/finalizer/source capacity
```

## Preflight And Placement

0.4 keeps preflight small. A Move may start only for a Ready RWO Filesystem
Volume with one authoritative owner, no active Move, no contradictory PV/PVC
identity, and at least one destination Pool whose inventory is fresh and
complete. Scheduler fit and disruption policy are inputs to the Move journal;
they are not source cleanup authority.

### Held Pods without a Move

Admission can add `shiftpv.io/placement-hold` before a Move exists. The same
controller loop revisits managed, unassigned held Pods. If the current owner is
Ready and uncordoned, the Volume is Ready with no active Move or cleanup, and no
unsettled Move owns the work, it verifies the exact PV/PVC binding and current
serving copy. One Pod update adds the owner hostname selector and removes only
the ShiftPV gate. Other gates, selectors and affinity remain intact; a conflicting
hostname selector is not overwritten. Unbound companion claims are left to the
scheduler and their provisioner.

Adding a selector while gated follows Kubernetes'
[Pod Scheduling Readiness](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-scheduling-readiness/#mutable-pod-scheduling-directives) rules.

Pod update conflicts trigger fresh binding, Volume, Move and Node reads. An
identity change aborts that attempt. Existing held Pods need no new annotation or
recreation. These reads and the Pod update are not a multi-object transaction;
NodePublish still enforces the current phase and owner before mounting.

## Reconcile Loop

Each reconcile cycle observes current metadata and node evidence, chooses one
action, persists the journal result, and observes again. API timeouts and lost
responses are resolved by read-back of deterministic names, UID ownership, CAS
results, and purge receipts. Repeated reconciliation is valid only when every
failure exit either retries automatically, waits for a recoverable node, or
preserves data in `Blocked` or a cleanup subjournal `NeedsReview`.

Events and periodic relists route discovery, Move advancement, and held-Pod
recovery to a queue keyed by Volume ID. Two workers process different Volumes;
one Volume has at most one worker. Routing hints are advisory: the worker reads
the current object and verifies its UID and Volume binding before acting.
Startup relists persisted work, so queue contents are not durable authority.
Each Volume pass shares a 30-second default context across discovery, Move and
held-Pod work; an earlier caller deadline wins. Expiry stops subsequent references
and status diagnostics, preserving accepted writes for a fresh read on retry. It
does not cancel a Move or release its holds. Context-aware API calls return the
worker; this is not a hard deadline for arbitrary code or conflict-retry sleeps.
Bulk API lists remain a shared discovery boundary: their delay can postpone new
queue entries while already queued work continues. The budget also does not
guarantee fairness between multiple historical Moves within the same Volume.

When another operation holds the destination Pool UID lock, mobility yields and
retries without cancelling placement or releasing a hold. Capacity measurement,
fresh ledger admission, and owner commit still use that shared lock. Admission
and external writers remain outside the Volume queue and require the existing
CAS and publication fences. Cleanup binds an executor and observes its journal
once, returning the worker while the resident Node executes. Pending cleanup
preserves the Move phase and holds. Source usage uses one Move UID-owned read-only
Pod with a fixed deadline. Each pass observes once. A successful result is stored
by exact Move CAS after rechecking the unpublished source authority; only then
is the Pod retired. Lost replies retain the result for re-observation. Cancellation
restores source authority before waiting for this read-only helper to disappear.

Routine Pool scan generations do not restart an unchanged source measurement.
The helper rechecks current inventory and live backing after reading, waiting only
for stale successful observations while retaining the measured bytes. Changed
configuration, copy or publication and explicit failed evidence discard the result.
Destination statfs uses a separate resident RPC/API probe with one four-second
budget covering discovery, reads and response validation. It has no helper-Pod
fallback; private lock contention returns immediately. Temporary unavailability
preserves the current phase, placement and hold for a fresh retry. CSI statfs may
use a measurement helper fallback that observes the host mount access mode and
rejects read-only allocation. Existing-copy usage keeps its read-only bind. Creation and cleanup never fall back
to helpers. The probe budget fits inside the Volume pass budget.

New preparation candidates must advertise resident capacity probing. Immediately
before changing a Ready source to Moving, a fresh nonce probe verifies that the
held destination responds, under the shared Pool UID lock. Its free-space result
is discarded; execution approval measures again after source quiescence. A retry
already in Moving does not repeat this preflight, and existing execution approval
is not revoked by a missing capability flag. Upgrade Nodes before enabling this
controller path; an older or unavailable destination waits without releasing its
hold. The budget bounds controller I/O waits, not a Node kernel statfs syscall.

## Copy Contract

Copy uses an exact, identity-bound source and destination path. The source opens
read-only after source publication has quiesced. The destination writes to an
incoming copy and promotes only after verification.

Required copy command semantics:

```text
rsync -aHAXS --numeric-ids --one-file-system --no-devices --delete --fsync \
  --compress --compress-choice=zstd --compress-level=1
```

Compression applies only to copy transport. Both helper endpoints need
zstd-capable rsync; custom images without it fail the copy before promotion.
Checksum verification keeps its uncompressed command. Sparse, bandwidth-limited
copies can transfer fewer bytes; incompressible data still incurs codec CPU work.
This does not bound total Move time or change the Job deadline.

The copy must be followed by checksum verification before promotion. `--fsync`
reduces the crash window, but real power-loss durability still needs dedicated
fault validation on the target filesystem and host configuration.

Nested filesystem traversal and device-node recreation are explicitly excluded
from the Volume data contract by the copy options. The supported data set is
verified with the same exclusions before promotion.

Settle active work before changing helper images or execution profiles. Source
Pods and copy/verify/promote Jobs compare their image and execution shape exactly.
Legacy work must finish under its old compatible writer and helper images before
the candidate controller starts.

## State Table

One decision model selects the next `phase` and `recoveryPhase` for preparation,
execution, cancellation and recovery. The controller observes only the facts each
stage needs. Effects persist exact evidence or report pending; they do not choose
lifecycle phases. A pending effect retains accepted evidence and does not advance
its phase. `Canceling` releases the hold and becomes `Cancelled` in one status CAS.

| State | Entry condition | Allowed next step |
|---|---|---|
| `Pending` | Volume is Ready, RWO Filesystem, one active owner, no active Move | Validate preconditions |
| `Preparing` | Exact binding, consumer and serving copy recorded | Claim source, reserve placement, grant logical hold |
| `StartingDisruption` | Protected logical hold and exact placement UID recorded | Recheck conditions, fence source publication and evict recorded consumer UID |
| `Canceling` | No destination execution authority | Restore source Ready; confirm placement absence; release logical hold |
| `Cancelled` | Logical hold released | Clear the exact source lock, then retain the settled journal for its retention period |
| `WaitingForUnpublish` | Original consumer eviction was accepted or the consumer is gone | Wait until source publication is absent |
| `WaitingForReplacement` | Source is unpublished | Wait for a replacement claim/placement object |
| `WaitingForDestination` | Replacement exists and its hold is intact | Ensure destination placement and node readiness |
| `WaitingForCapacity` | Destination placement is scheduled and usable | Verify quiesced physical usage; convert the prepared hold |
| `Copying` | Destination capacity hold is approved | Copy into destination incoming path |
| `Promoting` | Checksum and identities match | Promote incoming to destination serving copy |
| `Committing` | Destination serving copy exists | CAS owner/currentCopy to destination |
| `ReleasingDestination` | Owner CAS read-back proves destination owner | Delete/release the temporary destination placement |
| `WaitingForDestinationPublish` | Destination placement is released | Wait for actual destination publish proof |
| `CleaningSource` | Destination publish proof exists | Retire and purge exact source copy |
| `Completing` | Cleanup API receipt and post-receipt absence proof are complete | Release `activeMove` and retained source capacity |
| `Succeeded` | Cleanup settled, capacity released, `activeMove` cleared | Terminal |
| `Blocked` | Identity, inventory, receipt, or authority contradiction | Preserve data; no destructive action |

## Publication And Inventory Fence

Cleanup absence proof comes from a valid, complete Pool scan whose
`status.observedGeneration` matches the current Pool `metadata.generation` and is
at or after the cleanup journal's post-receipt `requiredGeneration` fence.
Superseded observations cannot prove absence.

Publication proof uses two signals. `status.publishedNodes` is the API-side
intent and CAS fence maintained by the node service after inspecting real mount
references. The actual destination publication proof is the destination Pool's
fresh valid complete inventory entry for the exact destination serving copy with
`published=true`. The scanner observes mount references; it is not the same
critical section as the CSI publish/unpublish filesystem lock, so freshness and
generation fences remain part of the authority.

Fresh proof requires all of the following:

| Proof | Required evidence |
|---|---|
| Source quiesced | `NodeUnpublish` inspected the real mount under the local lock and cleared the matching API publication fence |
| Destination published | `publishedNodes` contains destination and fresh Pool inventory marks the exact destination serving copy as published |
| Source cleanup-ready | Source copy is not current owner, not in-flight, and has no live source publication |
| Cleanup settled | API purge receipt plus fresh complete absence proof |

If a scan is incomplete, stale, truncated, or generation-ambiguous, the Move or
cleanup subjournal waits; it does not infer absence.

## Recovery Matrix

| Failure point | Recovery rule |
|---|---|
| Source down before commit | Wait. When source returns, source owner is still authoritative. |
| Destination down before commit | Wait or abort to source. Incoming copy is not authoritative. |
| Copy partial or checksum mismatch | Preserve source; mark the Move `Blocked` or retry before promotion only. |
| API response lost before commit | Read back owner/currentCopy; source remains owner unless CAS is proven. |
| API response lost during owner CAS | Read back exact owner/currentCopy. CAS success means postcommit path. |
| Destination down after commit | Wait. Destination owner remains authoritative and resumes forward. |
| Source down after commit | Wait for source return; cleanup resumes after authority and publication checks. |
| Cleanup unlink happened before receipt | Re-run the exact idempotent effect, reconstruct its local result, then require API receipt and a later fresh absence proof. |
| Identity contradiction | Move `Blocked` or cleanup subjournal `NeedsReview`; no automatic delete, rollback, or owner switch. |
| Unknown orphan | Report only; never auto-delete. |
| Permanent authoritative disk/node loss | Out of scope; operator recovery required. |

## Blocked Recovery

Service recovery never guesses a new owner. Before owner CAS, recovery verifies
the source owner and either resumes from source or aborts the Move. After owner
CAS, recovery verifies the destination owner and only converges forward through
destination publish, source cleanup, receipt settlement, and capacity release.
If either side returns with contradictory identity, the Move remains
`Blocked`, or the relevant cleanup subjournal remains `NeedsReview`.

Recovery has its own subphase journal on the blocked Move:
`Quiescing -> Verifying -> Retiring -> Resuming -> Completing -> Recovered`.
Precommit recovery does not create a separate `Aborted` Move phase. Its terminal
representation is the original Move remaining `Blocked` with
`status.recoveryPhase=Recovered`, while the Volume is back to source `Ready` and
`activeMove` is cleared.

Before using destination inventory in source rollback, `Retiring` requests a new
Pool `spec.scanEpoch` and persists the returned generation in
`status.rollbackRequiredGeneration`. Capacity remains held until the exact Pool
UID reports a valid, complete inventory at its current generation, at or beyond
that fence. This applies even when neither transaction copy exists and no
cleanup journal was created. Node/controller timestamps only bound freshness;
they do not establish scan ordering. A lost request or unrecorded fence causes a
new scan request, while a persisted fence survives controller restart.

Approval without its recorded full destination plan requires review and preserves
the hold. Recovery does not infer copy identities, request a scan from an inferred
plan, or fabricate cleanup authority.

### Workload retirement

For an approved prepared Move, confirmed absence of the original PVC UID (including
same-name replacement) automatically records `status.settlementRequested`. An
exact-copy Volume CAS fences execution and competes with owner commit. This
intent is monotonic, has no legacy default, and does not authorize Volume deletion.

Retirement reuses quiesce, owner verification, cleanup and hold settlement, skipping
service resumption. Before commit, retain the source and settle exact destination
effects. After commit, retain the destination; source cleanup requires its verified
blocked authority, empty publication and a fresh post-Retiring inventory fence.
Ordinary service recovery still requires actual destination publication.

Completion releases only Move holds and `activeMove`; the authoritative Volume and
its bytes remain accounted. Delete policy then uses the independent CSI deletion
fence; Retain preserves the owner copy. Accepted CSI deletion after unlock cannot
strand Move metadata: recorded cleanup/absence evidence and verifier termination
must still close. Unknown API reads or changed owner/copy remain errors.

A still-present original UID remains subject to PVC protection and normal workload
teardown. No replacement claim or workload is adopted. Before approval, the existing
preparation cancellation path applies. This feature needs the updated CRD,
Controller, Node and helper; older writers cannot erase its stored intent.

### Operator procedure

For planned maintenance, use **cordon → Move completion → drain**. Confirm the
consumer is Ready on the destination, the Move has settled cleanup and the Volume
has no `activeMove` before draining the source. Draining first can remove the only
controlled source consumer before discovery; that is outside automatic migration
support. Uncordon a healthy source to recover an unassigned held replacement when
no unsettled Move owns it, then retry the planned sequence.

For `Blocked` after execution approval on a new `prepared-v1` Move:

1. Inspect the Move, Volume current owner/copy, Pool readiness and embedded cleanup
   reason. Keep the PV/PVC and workload; deleting or recreating a claim changes its
   UID and cannot repair the recorded binding.
2. Restore the prerequisite named by the reason (node/Pool availability, exact
   workload placement, or cleanup evidence). Preserve transaction artifacts and
   finalizers. Precommit recovery needs the original binding and a controlled
   consumer so it can verify the owner and restore publication.
3. Request recovery on that Move with
   `kubectl patch shiftpvmove <move> --type=merge -p '{"spec":{"recovery":"ResumeOwner"}}'`.
   Observe `status.recoveryReason`, `status.recoveryPhase`, current owner and Pod
   readiness. Completion is `Recovered` with cleared `activeMove` and settled
   cleanup/holds, not merely acceptance of the patch.

`ResumeOwner` restores service only for the recorded live binding. Confirmed claim
retirement follows the separate settlement path above. Legacy Moves must settle
on their compatible old writer before upgrade.

## Safety Invariants

| Invariant | Meaning |
|---|---|
| One owner | Only one committed owner copy exists in metadata at a time. |
| One commit | Only owner CAS changes authoritative ownership. |
| Forward after commit | Postcommit recovery never rolls back to source. |
| Source retirement | A continuing workload requires destination publish; claim retirement requires separately fenced and verified destination authority. |
| Fresh absence | Completion and capacity release require fresh absence proof after purge receipt. |
| Bounded destructiveness | Unknown paths, stale observations, and contradictory identity preserve data. |

The corresponding cleanup rules are defined in
[`source-cleanup.md`](source-cleanup.md).
