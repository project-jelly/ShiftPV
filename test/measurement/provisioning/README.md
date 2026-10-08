# Direct CSI provisioning profile

Run only in an isolated ShiftPV installation. This caller creates 8Mi volumes,
checks their Ready creation receipts and deletes them. It bypasses PVCs,
external-provisioner, scheduler and CDI; its timings are not VM startup latency.
After a failed call, remove the isolated cluster to clear any unresolved intent.

## Run

Keep a focused Kind installation and its printed kubeconfig:

```bash
KEEP_CLUSTER=1 NODE_EFFECTS_ONLY=1 CLUSTER_NAME=shiftpv-profile ./test/e2e/kind/run.sh
```

Set `KUBECONFIG` to that installation. Build for the node architecture (`arm64`
or `amd64`), then copy the binary into the controller:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -tags shiftpv_profile -o .tmp/provisioning-profile ./test/measurement/provisioning
kubectl -n shiftpv-system exec -i deployment/shiftpv-controller -c shiftpv-controller -- \
  sh -c 'cat > /tmp/provisioning-profile; chmod 700 /tmp/provisioning-profile' < .tmp/provisioning-profile
kubectl -n shiftpv-system exec deployment/shiftpv-controller -c shiftpv-controller -- \
  /tmp/provisioning-profile -case new -node shiftpv-profile-worker -count 10
```

| Case | Calls per iteration | Checks |
|---|---|---|
| `new` | One new name | Ready receipt, selected node and Pool group |
| `retry` | Same completed name | Response and complete durable state unchanged |
| `same-pool` | Two concurrent new names | Independent volume identities in the group |
| `different-pool` | Two concurrent new names | Same node, distinct groups (`-group`, `-other-group`) |

Concurrent cases default to ten iterations (twenty samples). Use `-count 5`
for ten samples. `same-pool` requires exactly one eligible Pool in the group.
`different-pool` requires registered independent disks or thick LVM volumes;
Kind directory Pools and unsupported loop devices do not qualify.

The `shiftpv_profile` build tag keeps this manual driver out of product coverage.
`make build` compiles it separately so CI still checks the executable.

JSONL start/end timestamps bound the timed window. Seed creation, state checks
and deletion are outside it. Correlate level-2 controller logs by volume ID and
timestamp; nested step durations must not be summed. Stage event counts are
not HTTP request counts. Confirm process exit success and empty error output,
then confirm all profile Volumes have been removed. Remove only the cluster
and mount directories created for this run.

## 2026-10-08 isolated comparison

Same three-node Kind cluster (Kubernetes 1.35.8), one eligible 10Gi directory
Pool per worker, sequential runs with ten samples per case. Baseline was
`b97ce45`; only the controller image was replaced with the CSI API budget change.
Nodes and Pool configuration stayed fixed during measurement.

| Direct CreateVolume case | Before median / max | After median / max |
|---|---|---|
| New volume | 8.803s / 9.008s | 0.733s / 0.877s |
| Completed-intent retry | 4.403s / 4.597s | 0.014s / 0.202s |
| Same Pool, two concurrent callers | 13.225s / 16.601s | 0.895s / 1.621s |

An additional thirty consecutive new volumes succeeded: median 0.784s,
max 1.285s; the last twenty had median 0.792s. This checks beyond the initial
burst, but is not a soak or production SLO. Both versions passed receipt checks,
retry state equality and deletion. The updated controller also passed the
focused authenticated Node RPC, Pod restart and fenced deletion E2E.

New-volume cleanup-fence median fell from 799ms to 2.36ms and Pool-list median
from 399ms to 1.01ms. These results support client throttling as the dominant
delay in this configuration. CSI typed and dynamic clients now share a dedicated
50 QPS / 100 burst limiter; observation, retry and probe budgets remain separate.
This increases possible API load. Fresh reads, locks, capacity holds and receipt
guards remain in place. Capacity-probe median grew from 29ms to 560ms under the
faster request stream; its remaining API budget is a follow-up measurement target.

Same-node independent Pool admission is covered by DI tests: one blocked probe,
node serialization and two separate Pool UID holds. Live independent-disk and
full CDI import timings remain separate qualification.

Container `imageID` evidence (imported manifest digests):

- Baseline controller and unchanged nodes: `sha256:9a2566385ab409ef6c396feea69f6da75107ffa4211ede3e2346430af3cde138`
- Updated controller: `sha256:97c83d9630825efb07a6768d960c29b9ee1f5dfaf2d932b21abc6728a365fdc5`
