# Capacity probe API budget comparison

Baseline: `3e1492f` (#173). Only the controller's live-capacity API budget changed
from 5 QPS / 10 burst to an independent 50 QPS / 100 burst limiter.
Kubernetes 1.35.8, three-node Kind cluster, one eligible 10Gi directory Pool per
worker. Controller image replaced in place; Node images and configuration fixed.
These are direct CSI timings, excluding PVC/provisioner/scheduler/CDI latency.

| Case | Samples per version | Before median / max | After median / max |
|---|---:|---|---|
| New volume | 30 | 0.797s / 0.890s | 0.719s / 0.817s |
| Completed-intent retry | 10 | 0.0117s / 0.067s | 0.0108s / 0.202s |
| Same Pool, two concurrent calls | 10 calls / 5 pairs | 0.913s / 1.604s | 0.959s / 1.440s |
| Both calls in a pair completed | 5 pairs | 1.539s median | 1.050s median |

New-volume capacity-step median fell from 602ms to 118ms. Overall creation
improved about 10%; pair completion improved about 32%. Per-call concurrent
median increased about 5%, and retry maximum increased. These small samples
do not establish tail-latency or production SLO improvements. The last twenty
serial calls had medians 0.804s before and 0.720s after, beyond startup bursts.

The new-volume CREATE RPC median grew from 46ms to 551ms under faster admission,
with Node code unchanged. Its internal API/lock/filesystem costs need attribution
before another budget or architecture change. Nested durations are not additive.

Both runs validated Ready creation receipts, selected node/group, identical
retry durable state and deletion. The updated controller passed authenticated
Node capacity/create, Pod restart/data preservation and fenced deletion E2E.
Same-node independent disks and full CDI remain separate qualification.

DI regression uses real REST clients with an exhausted inherited limiter:
GET + peer LIST still run before and after RPC (four HTTP requests). A Pool
generation change or post-RPC API rejection returns no capacity and cannot
switch to fallback. Retry retains its independent 5 QPS / 10 burst budget.
`make verify` now runs `src/cmd/...` race tests, including these wiring checks.

Tradeoff: live-capacity requests can place more load on the API server. The new
limiter remains bounded and separate from CSI metadata, effect and observation
clients; fresh readiness/peer/backing verification and node/Pool locks remain.

Imported container `imageID` digests:

- Baseline controller and unchanged Nodes: `sha256:71f800aafa7382ff40db5931ce7d285a5ec50553a795e6ef0a2ed078c3a6a824`
- Updated controller: `sha256:288cbd79d916cdc47004a93d44c4dae1f6fab5dd59fc543b30f98c8589750287`
