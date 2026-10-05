# Cleanup And GC Contract

This describes the current implementation. Qualification requirements are in [Testing](../development/testing.md).

Cleanup removes only exact non-authoritative ShiftPV copies. There is no
standalone durable cleanup CR. Durable truth lives in `status.cleanup` on the
parent `ShiftPVVolume` delete journal/finalizer or the parent `ShiftPVMove`
rollback/source-cleanup journal/finalizer. Helper Jobs execute filesystem
effects and own no truth.

## Authority

| Owner | Scope |
|---|---|
| Volume journal/finalizer | Delete the committed Volume copy during `DeleteVolume`. |
| Move journal/finalizer | Delete rollback artifacts before commit and source copy after commit. |
| Helper Job | Execute retire, purge, and local receipt effects for the parent journal. |
| Pool inventory | Report observations; never grants deletion authority by itself. |

Every destructive operation must be derived from a durable exact parent intent.
Unknown paths are report-only, stale or truncated observations wait, and missing
intent preserves data. Identity contradiction or terminal executor failure moves
only the cleanup subjournal to `NeedsReview`.

## Exact Identity

Cleanup identity includes installation, Pool UID, Volume UID, copy ID, node, role,
operation ID, and the local marker's device/inode binding. Symlink replacement,
Pool re-registration, Volume reincarnation, executor replacement, or missing
marker proof invalidates destructive authority.

Valid destructive authority requires all of the following:

| Evidence | Requirement |
|---|---|
| Parent intent | Exact Volume or Move journal entry exists and still owns the operation |
| Volume authority | Target is not a live current owner unless this is the Volume delete path |
| Move authority | Destination owner CAS and actual-publish proof exist before source cleanup |
| Publication | No live kubelet mount references the target copy |
| Pool identity | Pool name/UID/node still match and the protection finalizer is present |
| Local marker | Copy marker and directory device/inode match the target |

## State Table

| State | Entry condition | Allowed next step |
|---|---|---|
| `Pending` | Parent journal contains exact cleanup intent | Recheck live authority |
| `Running` | Executor is known and authority still matches | Retire and purge the exact target |
| `Verifying` | API receipt records the exact purged local effect | Request a post-receipt Pool scan |
| `ConfirmingAbsence` | Post-receipt `scanEpoch` generation fence is recorded | Wait for fresh valid complete absence proof |
| `Completed` | Receipt and absence proof match operation | Release parent finalizer and capacity hold |
| `NeedsReview` | Identity contradiction or terminal executor failure | Preserve data; no destructive action |

`Completed` is terminal for that exact cleanup operation. It proves one
operation's receipt and later absence fence; it is not a license to delete again.
If the exact copy reappears after settlement, it is a report-only anomaly for
operator review; cleanup does not reopen or auto-delete from the completed
operation.

## Terminal Move Journal Retention

Terminal Move GC deletes only the Kubernetes `ShiftPVMove` metadata object. It
never deletes a filesystem path or reuses a completed cleanup intent. The
default minimum retention is seven days and the Helm value
`mobility.journalRetention` accepts whole hours with a minimum of one hour.

A journal is eligible only when every gate below is true:

- the Move is `Succeeded` with cleanup `Completed`, or `Blocked` with recovery
  `Recovered`, capacity approval released, and reason `RecoverySettled`;
- the controller protection finalizer has already been removed in an earlier
  reconcile and no other finalizer remains;
- `lastTransitionTime` is valid, is not in the future, and the retention has
  elapsed;
- the current Volume is absent or does not reference this Move as `activeMove`;
- deletion uses the exact Move name and UID precondition.

Missing or contradictory evidence retains the journal. Journal GC is therefore
bounded metadata retention after cleanup closure, not a second data GC path.

## Volume Delete Cleanup

Volume deletion is a parent-owned transaction. `DeleteVolume` first records an
exact delete intent in the Volume journal and keeps the Volume finalizer until
cleanup is settled.

| Gate | Requirement |
|---|---|
| Delete intent | Operation ID and target currentCopy are durable before filesystem effect |
| Publish quiesce | Node has rechecked actual unpublish and `publishedNodes` is empty |
| Pre-effect authority | Parent UID, Pool UID/finalizer, target marker, and executor ownership match |
| Purge receipt | API receipt records the exact operation result |
| Absence proof | Post-receipt fresh valid complete Pool scan proves the exact copy is absent |
| Capacity release | Volume capacity releases only after receipt and absence proof |

If the target is already absent and the Volume journal contains durable exact
intent plus exact local effect evidence, the controller may reconstruct an
idempotent local result and then record the API receipt. It must still request a
new post-receipt `scanEpoch` fence and wait for that generation's absence proof.
If the target is absent without prior exact authority and local effect evidence,
absence is a contradiction, not proof; the cleanup subjournal enters
`NeedsReview`.

## Move Cleanup

Move cleanup is parent-owned by the Move journal and finalizer.

| Case | Rule |
|---|---|
| Precommit abort | Destination incoming or promoted non-owner copy may be purged only from exact Move intent. |
| Postcommit source cleanup | Source copy may be purged only after destination owner CAS and destination actual-publish proof. |
| Rollback artifact | Partial copy never promotes; cleanup removes only the exact non-authoritative artifact. |
| Source capacity | After commit, the Move holds retained source capacity until receipt and fresh absence proof. |
| Forward recovery | Postcommit cleanup waits and resumes; it never rolls ownership back to source. |

If precommit recovery settles rollback artifacts, the Move stays `Blocked` with
`status.recoveryPhase=Recovered` as the terminal recovery representation, and the
Volume returns to source `Ready` with `activeMove` cleared. If postcommit source
cleanup cannot prove authority, destination remains owner and the Move waits or
the cleanup subjournal enters `NeedsReview`.

## Already-Absent Rule

Absence is meaningful only when tied to a durable exact intent.

```text
durable exact intent + exact executor/effect evidence + observed absence
  -> reconstruct idempotent local result
  -> record API purge receipt
  -> request post-receipt scanEpoch
  -> require later fresh absence proof from that generation
  -> require every per-copy marker, identity, presence, publication and problem field to be internally consistent

no prior exact intent + absent target
  -> cleanup NeedsReview
  -> never treat absence as cleanup proof
```

This distinction closes the unlink-before-receipt crash window without allowing
inventory alone to erase accountability. A fresh aggregate inventory is still
insufficient when any individual copy observation is missing identity, reports a
problem, conflicts with its Pool or marker, or has contradictory
`present`/`published` evidence; cleanup waits without releasing its finalizer or
capacity hold.

## Recovery Matrix

| Failure point | Recovery rule |
|---|---|
| Node down before cleanup | Wait; resume when the node returns. |
| Helper Job lost before effect | Recreate from parent journal after live authority recheck. |
| Helper Job lost after retire | Same Job UID may rebind a new Pod and continue if exact; a different Job UID enters cleanup `NeedsReview`. |
| Unlink succeeds, receipt lost | Reconstruct result only with prior exact intent and local effect evidence, then request the post-receipt absence fence. |
| API timeout recording receipt | Read back receipt; retry idempotently if absent. |
| Copy reappears after completion | Report anomaly only; do not reopen the completed operation or auto-delete. |
| Current owner or active Move references target | Cleanup `NeedsReview`; preserve data. |
| Unknown marker or unmarked path | Report only; preserve data. |
| Permanent authoritative disk/node loss | Out of scope; operator recovery required. |

## Orphan Policy

Inventory can report orphan candidates, but cannot authorize deletion.

| Observation | Disposition |
|---|---|
| Exact parent-owned target | Automatic cleanup may proceed after all gates pass. |
| Superseded copy with complete identity and no live reference | Report for explicit review. |
| Unknown or unmarked directory | Report only. |
| Stale or truncated inventory | Wait; do not infer absence or orphanhood. |
| Identity contradiction in a parent-owned cleanup | Cleanup `NeedsReview`; no destructive action. |

The cleanup loop is intentionally conservative: it may leave data behind for
operator review, but it must not delete a copy whose identity or authority is
ambiguous.

## Verification Requirements

Before this contract can be treated as operating evidence, tests must cover:

| Test area | Required proof |
|---|---|
| Model | Precommit abort to source, postcommit forward convergence, stale inventory rejection |
| Unit | Volume and Move journal ownership, receipt idempotency, capacity release after settlement |
| Node | Exact marker/device/inode checks and published mount observation |
| Fault | Unlink-before-receipt recovery and node outage resume |
| Kind | Destination publish before source cleanup and fresh absence before completion |

The mobility sequence is defined in
[`volume-mobility.md`](volume-mobility.md). Metrics are defined in
[`metrics.md`](metrics.md).
