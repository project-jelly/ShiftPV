# Isolated Argo CD uninstall E2E

This suite creates a dedicated `shiftpv-argocd-e2e` kind cluster and uses a
cluster-local Helm repository so Argo CD renders the current checkout rather
than a remote branch. It installs pinned Argo CD 3.5.2 and validates the
fail-closed hook plus authoritative lifecycle-admission contract:

1. An Application with no dependent ShiftPV storage is deleted successfully.
2. An Application with a mounted ShiftPV volume remains in deletion while the
   `argocd` mode guard Job stays Running and lifecycle admission rejects
   protected resource deletion. The active Pool retains controller-owned
   deletion protection, including rejection of direct finalizer removal.
3. The Controller, Node Plugin, StorageClass, and mounted checksum remain intact
   after that denial.
4. Removing PVC/PV/Volume/Move journals and every capacity hold, then obtaining
   fresh complete empty Pool inventories, lets the running Job finish the same
   Application deletion.

Run from the repository root:

```bash
./test/e2e/kind/argocd/run.sh
```

The suite has its own cluster name, temporary kubeconfig, worker directory, and
image tag. It can therefore run alongside the base and mobility kind suites.
Set `KEEP_CLUSTER=1` only for failure diagnosis.

The bootstrap verifies the upstream Argo manifest checksum, pulls Redis from
Docker Hub by the same immutable digest as the upstream ECR mirror, and preloads
it into every kind node. Only Redis uses a local image alias and `IfNotPresent`;
host and node image IDs must match before Argo starts. This avoids depending on
the ECR mirror during Redis startup without changing Redis content. The original
manifest, rendered overlay, and image identity report are copied before teardown
to `.tmp/ci-argocd-diagnostics/redis-bootstrap/<run-directory>/`. The existing CI
failure artifact includes these three files; kubeconfig, credentials and workload
data are excluded. Missing files during an early failure are not fabricated, and
failure to preserve an existing evidence file fails the test while cleanup continues.
