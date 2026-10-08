# Node CREATE attribution — 2026-10-08

Instrumentation only: the Node API budgets, live authority checks, shared
RPC/Watch gate and durable receipt rules were unchanged. Source tree was
`80a86d7` (rebased as `6905967`), on top of the probe budget change.
Both runs used isolated three-node Kind 1.35.8 installations and directory
Pools. These are direct CSI timings, excluding external-provisioner and CDI.

## Completed qualification

A fresh installation passed authenticated RPC create, Pod replacement and fenced
delete. Then ten new calls, ten completed-intent retries and five concurrent
pairs in the same Pool completed with empty stderr. The caller checked Ready
receipts, unchanged retry state and Pool membership; deletion left zero Volumes.

| Timed case | Calls | Median / max |
|---|---:|---|
| New volume | 10 | 0.586s / 0.724s |
| Completed-intent retry | 10 | 0.020s / 0.203s |
| Same Pool, two concurrent callers | 10 | 0.916s / 1.450s |

For new calls, correlated Node logs gave these per-volume medians:

| Node stage | Median |
|---|---:|
| Creation effect total | 400ms |
| Four authority checks, summed per volume | 345ms |
| Prepare local, excluding authority | 5.86ms |
| Receipt local, excluding authority | 3.23ms |
| API receipt recording | 39.5ms |

Authority checks occupied a median 87.6% of each creation effect. The maximum
volume gate wait within each creation window had median 396ms, overlapping the
effect performed by the other RPC/Watch caller. It is not additional filesystem
time. Controller CREATE RPC median was 415ms. Stage medians cannot be added.

All twenty-one created copies (including the retry seed) emitted four authority
checks each. The Node metrics endpoint exposed 21 creation effects and 84
authority observations. Timed completed-intent retries emitted zero creation
stages. Same-Pool concurrent calls retained separate copy identities.

## Earlier aborted diagnostic run

The initial thirty-call attempt stopped at call 24 with `Unavailable`. The
23 successful responses had median 0.720s; Node creation total was 534ms,
authority checks 469ms, prepare local 7.75ms and receipt local 3.46ms.
This partial run did not reach the caller's final Ready checks and is not a
successful thirty-call qualification.

The rejected volume was `shiftpv-adda44cab7c318f19e99cdfc981cf3fa`. At
04:31:59 UTC, Watch and RPC authority checks rejected Pool `InventoryInvalid`.
Watch later recorded the same operation's creation receipt at 04:32:01 UTC;
the controller had already returned the retryable error. The saved API snapshot
retained `NodeCreating`, the exact copy and its receipt. The failed cluster was
removed after preserving logs, API objects and its host fixture.

The transient inventory reason had already cleared in the later Pool snapshot,
so its underlying scan failure was not captured. Investigation of inventory
observation during concurrent creation remains a separate candidate; this run
does not establish a new safety regression or a successful retry convergence.

## Interpretation and provenance

The remaining delay is concentrated in live authority validation. This includes
API client waiting and read-only backing checks, not an isolated measurement of
rate-limiter waiting. Preserve the four mutation-boundary checks when evaluating
duplicate reads or client budgets. No performance improvement or production SLO
is claimed for this instrumentation; burst state and Watch scheduling differ
between runs.

Controller and Node container `imageID` for both runs:
`sha256:62468da6f544f9afa89b3666db2b95019a106e481cd69ffabba86b3a60958e75`.
Raw JSONL, timestamped logs, metrics, snapshots and summaries are preserved
locally in `.tmp/rpc-attribution/`. Both owned Kind clusters were removed.
