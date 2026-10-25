# Closed-loop Mobility E2E

The suite exercises the product admission, controller, scheduler and node paths in disposable Kind clusters.

```bash
./test/e2e/kind/mobility/run.sh
```

`KIND_MOBILITY_GROUP=node-cleanup` exercises native source cleanup with rejected
API receipt writes, postcommit source drain, Node Pod replacement and controller
restart. It requires the same local receipt and operation to finish with a later
absence scan, unchanged PVC/PV/checksum, and no cleanup Job. The `g2` completion
case replaces the completed Node executor while terminal status writes fail and
CSI deletion races. The `g3` case supplies an invalid local receipt before cleanup,
then requires real Node NeedsReview, preserved source data and capacity hold.
The restart suite also replaces a pre-receipt Node while its existing volume
lock is held; the same operation must complete after the lock is released.

`KIND_MOBILITY_GROUP=retirement` deletes the original PVC before/after owner CAS
with Delete/Retain policies. Both real copies must exist first. A rejected native
purge receipt must retain the exact owner and temporary hold across Controller
restart. Replay, fresh absence, hold/gate/artifact release and normal CSI deletion
must finish without a `ResumeOwner` request or invented publication. Retain also
requires the original owner identity, payload and capacity to survive settlement.

`g1` also exercises an early targeted drain with no eligible destination: admission
holds the replacement, and uncordon must recover that same Pod UID without a Move.
The ordinary case waits for Move success, cleanup settlement, empty ActiveMove and
a Ready destination before draining the now-empty source workload set. PVC/PV/Volume/Move
identity and unique payload must remain unchanged. These are fixture-selector tests,
not whole-node eviction or drain-first automatic migration qualification.

The product Controller must create and advance the Move. Tests may inject API, process, node, network and
filesystem failures, but must not patch a successful status or perform the product copy/purge from the host.

## Preconditions

- source and destination Pools have fresh, valid, complete inventory
- Volume is Ready, RWO Filesystem, and has one exact owner
- workload selector, affinity, taints/tolerations and PDB permit a destination
- source publication can quiesce without an in-flight NodePublish race

An unsatisfied preflight preserves the same consumer UID and data and creates no filesystem effect.

## Transaction evidence

```text
Ready source + activeMove
  -> destination placement + durable logical hold
  -> StartingDisruption -> source publish fence
  -> observed empty publication -> source usage
  -> fresh destination capacity -> execution approval
  -> partial copy + full verification
  -> destination promotion
  -> owner CAS
  -> actual destination publish proof
  -> source cleanup intent/effect/API receipt
  -> later generation-fenced absence proof
  -> source hold release and terminal Move
```

| Fault boundary | Required result after node/process recovery |
|---|---|
| Before owner CAS | Source remains owner; retry or recovery returns to source |
| Owner CAS response lost | Read-back determines the side of commit; never guess |
| After owner CAS | Destination remains owner; only forward convergence |
| Partial copy / checksum failure | No promotion; source and both holds remain safe |
| Destination unavailable postcommit | Before cleanup effect the source is preserved; after effect/receipt, settlement and hold release wait |
| Source unavailable postcommit | Destination serves; cleanup resumes when source returns |
| Unlink before receipt | Exact prior intent reconstructs result and later absence proof closes cleanup |
| Stale/invalid/incomplete scan | No promote, purge, completion or hold release |
| Identity contradiction | Move `Blocked` or cleanup `NeedsReview`; no destructive transition |

## Assertions

The suites cover these assertions at the relevant boundaries:

1. unchanged PVC UID, PV and CSI volume handle
2. exactly one metadata owner and no dual writable mount
3. authoritative payload plus hardlink/symlink/FIFO/UID/GID/mode/sparse/ACL/xattr semantics
4. exact Pool/Volume/Move/copy/operation identities
5. correct source and destination capacity holds at every boundary
6. parent journal/finalizer persistence across Controller, helper and node restart
7. source purge requires actual destination publication or fenced retired-workload verification
8. API purge receipt and a later fresh absence proof before completion
9. no parentless executor or unexplained physical copy after terminal success/recovery

Permanent loss of the authoritative disk/node is not a recovery-success scenario. The suite must show a safe,
observable wait or review state without inventing another owner.
