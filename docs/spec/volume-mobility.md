# Volume Mobility Contract

Status: target contract, not a runtime guarantee until implementation gates pass.

ShiftPV moves one RWO Filesystem PVC from its current node-local owner to another
registered Pool. PVC, PV, and CSI volume handle identity remain stable. The
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
| Executors | Helper Jobs perform effects only; they own no truth. |
| Identity contradiction | Move enters `Blocked`; cleanup subjournals may enter `NeedsReview`. |

`activeMove` is the per-volume lock. The Move keeps it until source cleanup has
an API purge receipt and a fresh absence proof. Before commit, capacity is held
for the source owner and the destination incoming copy. After commit, the Volume
holds destination capacity and the Move continues holding the retained source
until both API purge receipt and fresh absence proof exist.

## Flow

```text
source owner
  -> lock volume and evict consumer
  -> wait for source unpublish
  -> create replacement placement
  -> reserve destination capacity
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

## Reconcile Loop

Each reconcile cycle observes current metadata and node evidence, chooses one
action, persists the journal result, and observes again. API timeouts and lost
responses are resolved by read-back of deterministic names, UID ownership, CAS
results, and purge receipts. Repeated reconciliation is valid only when every
failure exit either retries automatically, waits for a recoverable node, or
preserves data in `Blocked` or a cleanup subjournal `NeedsReview`.

## Copy Contract

Copy uses an exact, identity-bound source and destination path. The source opens
read-only after source publication has quiesced. The destination writes to an
incoming copy and promotes only after verification.

Required copy command semantics:

```text
rsync -aHAXS --numeric-ids --one-file-system --no-devices --delete --fsync
```

The copy must be followed by checksum verification before promotion. `--fsync`
reduces the crash window, but real power-loss durability still needs dedicated
fault validation on the target filesystem and host configuration.

Nested filesystem traversal and device-node recreation are explicitly excluded
from the Volume data contract by the copy options. The supported data set is
verified with the same exclusions before promotion.

## State Table

| State | Entry condition | Allowed next step |
|---|---|---|
| `Pending` | Volume is Ready, RWO Filesystem, one active owner, no active Move | Validate preconditions |
| `Locking` | Preconditions pass | Acquire `activeMove` by CAS |
| `Evicting` | Move lock held, source still owner | Evict the current consumer or wait if eviction is already requested |
| `WaitingForUnpublish` | Consumer is gone | Wait until source publication is absent |
| `WaitingForReplacement` | Source is unpublished | Wait for a replacement claim/placement object |
| `WaitingForDestination` | Replacement exists and its hold is intact | Ensure destination placement and node readiness |
| `WaitingForCapacity` | Destination placement is scheduled and usable | Reserve destination capacity |
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

Blocked recovery never guesses a new owner. Before owner CAS, recovery verifies
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

## Safety Invariants

| Invariant | Meaning |
|---|---|
| One owner | Only one committed owner copy exists in metadata at a time. |
| One commit | Only owner CAS changes authoritative ownership. |
| Forward after commit | Postcommit recovery never rolls back to source. |
| Delete after publish | Source cleanup cannot start before destination actual-publish proof. |
| Fresh absence | Completion and capacity release require fresh absence proof after purge receipt. |
| Bounded destructiveness | Unknown paths, stale observations, and contradictory identity preserve data. |

The corresponding cleanup rules are defined in
[`source-cleanup.md`](source-cleanup.md).
