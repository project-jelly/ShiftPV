# Cleanup And GC Contract

This describes the current implementation. Qualification requirements are in [Testing](../development/testing.md).

Cleanup removes only exact non-authoritative ShiftPV copies. There is no
standalone durable cleanup CR. Durable truth lives in `status.cleanup` on the
parent `ShiftPVVolume` delete journal/finalizer or the parent `ShiftPVMove`
rollback/source-cleanup journal/finalizer. New creation, Volume deletion and Move
cleanup execute only on resident Nodes. Copy, owner verification and promotion
still use Jobs; read-only measurement helpers remain.

## Authority

| Owner | Scope |
|---|---|
| Volume journal/finalizer | Delete the committed Volume copy during `DeleteVolume`. |
| Move journal/finalizer | Delete rollback artifacts before commit and source copy after commit. |
| Resident Node | Execute retire, purge, and local receipt effects for the parent journal. |
| Pool inventory | Report observations; never grants deletion authority by itself. |

Every destructive operation must be derived from a durable exact parent intent.
Unknown paths are report-only, stale or truncated observations wait, and missing
intent preserves data. Identity contradiction stops cleanup in `NeedsReview`.
Only `prepared-v1` Moves execute. Legacy Move records are retained without
execution unless independently settled history qualifies for metadata GC.

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
| Move authority | Destination owner CAS plus actual publish or separately fenced [workload-retirement proof](volume-mobility.md#workload-retirement) |
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
| `NeedsReview` | Identity or receipt authority contradiction | Preserve data; no destructive action |

`Completed` is terminal for that exact cleanup operation. It proves one
operation's receipt and later absence fence; it is not a license to delete again.
If the exact copy reappears after settlement, it is a report-only anomaly for
operator review; cleanup does not reopen or auto-delete from the completed
operation.

An unfinished legacy creation Pod or Job-bound cleanup, including `Verifying`
and `ConfirmingAbsence`, must settle on its compatible old writer and helper
images before upgrade. The candidate neither resumes that executor nor converts
its binding to a Node.

Completed legacy cleanup history requires valid receipt and absence proof plus
the exact Job UID, parent authority and execution termination. A retained Job
must be Complete, inactive and not deleting or Failed; all related Pods must
have explicit termination evidence for every main, init and ephemeral container.
If the Job is absent, any remaining Job-owned or receipt-writing Pod blocks
settlement, even if terminal. Mutable labels do not prove absence. A resident
Node Pod need not terminate to settle a Node receipt.

A prepared source recovery without cleanup intent closes through its exact
Retiring CAS settlement and causal rollback fence; helper quiescence is still
required. Recorded cleanup keeps its receipt/absence requirements, and legacy
full-plan recovery without independent cleanup proof remains blocked.

## Terminal Move Journal Retention

Terminal Move GC deletes only the Kubernetes `ShiftPVMove` metadata object. It
never deletes a filesystem path or reuses a completed cleanup intent. The
default minimum retention is seven days and the Helm value
`mobility.journalRetention` accepts whole hours with a minimum of one hour.

A journal is eligible only when every gate below is true:

- the Move is `Succeeded` with completed cleanup, `Blocked` with completed
  recovery and released capacity, or `Cancelled` without execution or cleanup intent;
- legacy history additionally passes independent settled-transaction validation,
  no Volume references it, and exact owned, recorded or deterministic helpers
  have complete quiescence evidence;
- the controller protection finalizer has already been removed in an earlier
  reconcile and no other finalizer remains;
- `lastTransitionTime` is valid, is not in the future, and the retention has
  elapsed;
- the current Volume is absent or does not reference this Move as `activeMove`;
- deletion uses the exact Move name and UID precondition.

Missing or contradictory evidence retains the journal. Reading old status does
not authorize runtime execution, intent repair or filesystem GC. Unsupported or
unsettled legacy records remain unchanged.

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

An unbound Move cleanup requires a resident Node advertising both
`shiftpv.io/node-effects: v1` and `shiftpv.io/node-move-cleanup: v1`. Missing
capability retries the same intent and reservation without creating a Job.
Existing legacy Job bindings or accepted unbound Jobs block candidate execution
until the old compatible writer settles them.

The Node advertises this capability only after both Volume and Move watches
have synced. RPC and watch recovery share the same per-Volume execution gate.
Every effect and receipt replay rechecks the exact Move UID, active Volume,
target copy, executor Pod UID and current authority. Rollback uses the recorded
incoming or destination copy, not the Volume's authoritative source copy.

A Node-bound operation never falls back to a Job on timeout or replacement.
After the old Pod is absent, another capable Node Pod on the same node may bind
the same operation and replay its durable local receipt. The Node writes the
API receipt; the controller still requests a later Pool scan and requires fresh
absence before releasing the hold. Mobility observes cleanup once per reconcile
and returns while the executor runs. Node work starts through its synchronized
watch; CSI deletion retains the direct RPC and bounded wait path.

Apply Node RBAC for Move get/list/watch/status updates before rolling out the new
Node binary. Finish old execution responsibilities before changing writers or
helper images; the candidate provides history inspection, not old Job execution.

| Case | Rule |
|---|---|
| Precommit abort | Destination incoming or promoted non-owner copy may be purged only from exact Move intent. |
| Postcommit source cleanup | Requires committed destination authority and actual publish, or the separate verified workload-retirement proof. |
| Rollback artifact | Partial copy never promotes; cleanup removes only the exact non-authoritative artifact. |
| Source capacity | After commit, the Move holds retained source capacity until receipt and fresh absence proof. |
| Forward recovery | Postcommit cleanup waits and resumes; it never rolls ownership back to source. |

If precommit recovery settles rollback artifacts, the Move stays `Blocked` with
`status.recoveryPhase=Recovered` as the terminal recovery representation, and the
Volume returns to source `Ready` with `activeMove` cleared. If postcommit source
cleanup cannot prove authority, destination remains owner and the Move waits or
the cleanup subjournal enters `NeedsReview`.

An approved Move missing its full recorded copy/operation plan stops recovery
with `CleanupNeedsReview` and retains its copies, lock and capacity holds. It
does not infer an approval or reconstruct the plan by scanning storage.

## Node Metadata Retention

The node metadata collector runs at startup and hourly. Completed reclaim
`receipt-*.json` and matching `cleanup-*.json` are retained for at least seven
days (`--metadata-retention`, minimum one hour). Collection requires a canonical
completed receipt, an unchanged ready Pool and backing filesystem, fresh complete
inventory, no Volume or Move reference, and no data/copy/placement path. API
failure, a busy lock, malformed metadata or contradictory evidence retains the
records. Unlink the redundant intent and sync before unlinking the receipt;
replay after an interrupted pass remains safe. Historical records at a
re-registered path require the same installation and no remaining old Pool.
API checks keep current-Pool candidates volume-locked; the Pool-wide marker lock covers
only the final local recheck and unlink, so API latency cannot block other
volumes' marker writes. GC uses a separate API client and rate limiter from
foreground publish and readiness work.

`lock-<volumeID>` stays for the entire Pool lifetime: unlinking an active lock
can create two independently locked inodes at one path. Only controller-approved
empty Pool deregistration removes these locks, while holding `identity.lock`
and every volume lock. Pool identity removal is synced before lock removal;
`identity.lock` remains across re-registration. Copy/promotion receipts and
unknown metadata are preserved; this collector never deletes volume data or
releases capacity.

Empty Pool deregistration also removes abandoned `placements/pending-<UUIDv4>`
marker temporary files under `identity.lock`, which all marker writers hold.
Only regular files owned by the node process, mode `0600`, one link and at most
8 KiB qualify. Unknown names, symlinks and other contradictions block removal.
This does not authorize deleting copy data or completed placement markers.

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
| Resident Node Pod replaced | After old Pod absence, bind a capable replacement to the same journal; recheck authority and replay the exact local receipt. |
| Unfinished legacy helper responsibility | Keep its journal and holds; settle with the compatible old writer before transition. |
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
