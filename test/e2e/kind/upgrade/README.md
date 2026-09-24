# Upgrade E2E

Fresh-install suites only ever see one chart. They cannot observe an immutable field that changes between
releases, and they cannot observe a webhook rule that contradicts the upgrade documentation. This suite starts
from the last published release and upgrades in place.

```bash
./test/e2e/kind/upgrade/run.sh
make kind-upgrade-e2e
```

## Sequence

1. Install the previous release pinned in [`versions.env`](versions.env) on a Kind
   cluster named `shiftpv-upgrade-e2e`. The chart tarball is digest-verified before anything is installed from
   it. Register both Pools, then provision a PVC on the default ShiftPV StorageClass and write a marker file.
2. Build the combined image from this checkout, load it into the cluster, and `helm upgrade` to the local
   chart with that image. The controller Deployment and node DaemonSet must roll to the candidate image, the
   existing Pod must still read its marker, and a new PVC on the same class must provision and mount.
3. Exercise the StorageClass replacement documented in [`../../../../charts/shiftpv/README.md`](../../../../charts/shiftpv/README.md):
   deleting the class while PVCs are bound to it must be refused with `dependent storage exists`; once the
   workloads, their `PersistentVolume`s and their `ShiftPVVolume`s are gone the deletion must be accepted; the
   next chart sync must recreate the class with the chart's `reclaimPolicy` and provision on it again.

The class names and the expected `reclaimPolicy` come from `helm template -s templates/storage/storageclass.yaml`
against this checkout, so a chart that renames or repolicies a class — in values or in the template — moves this
suite with it. The run ends by printing every assertion it made, so the evidence is in the log rather than
implied by the exit code.

Both installs override `poolReadiness` to a two-second scan. The chart's one-minute default would leave every
cleanup wait in the replacement step one tick away from its own timeout.

## reclaimPolicy mismatch

`reclaimPolicy` is immutable. If the published class disagrees with the chart in this checkout, the plain
`helm upgrade` in step 2 would fail on that field rather than on anything the suite is about. The run detects
the mismatch first, prints it, and uses the step 3 replacement as the upgrade path instead. Either way the run
ends with the candidate images running and the chart-defined StorageClasses present.

`KEEP_CLUSTER=1` keeps the cluster, kubeconfig and host directories for bounded diagnosis. `E2E_KUBECONFIG`
places the kubeconfig outside the run directory.
