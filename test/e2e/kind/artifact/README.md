# Public artifact smoke test

This isolated Kind test installs the published chart package and released images without
building product code from the checkout. By default, `resolve-latest.sh` selects the latest
stable chart in the public Helm repository, checks its package against the checksum attached
to the GitHub release, and resolves the chart's controller/node images to multi-platform
manifest digests. Each run then uses that fixed lock; it never falls back to an older release
when the latest publication is incomplete. This checks publication consistency, not signing
or provenance.

`versions.env` records a known published combination for explicit reproduction and supplies
the repository URL and pinned Kind node image. It does not select the scheduled smoke version.
The independent [`../upgrade/versions.env`](../upgrade/versions.env) preserves the older release
used by the upgrade suite. Updating the smoke reference must not advance that upgrade baseline.

Run from the repository root:

```bash
./test/e2e/kind/artifact/run.sh
```

The test downloads the requested chart through `helm repo add`, rejects a package hash
mismatch, installs the pinned images, registers the two mounted test pools and verifies a
real PVC/Pod mount, CSI identity, volume owner and data checksum. It deliberately remains a
small publication smoke test; source-level fault, recovery and mobility behavior belongs to
the other Kind suites.

GitHub runs this through the separate `Public Artifact Smoke` workflow on demand, daily,
and after a successful `Release Helm Chart` run. The resolved lock is uploaded as
`public-artifact-lock` so a failure can be reproduced with the exact same package and images.
It is intentionally not a pull-request merge gate because public availability can fail without
any relationship to the proposed source change. Lock validation remains part of `make verify`.

The cluster and temporary host data are removed on exit. Use `KEEP_CLUSTER=1` only for local
failure diagnosis. `ARTIFACT_LOCK_FILE` may select another validated lock for a candidate
artifact, but mutable image tags and malformed/unpinned values are rejected. For example:

```bash
ARTIFACT_LOCK_FILE="$PWD/test/e2e/kind/artifact/versions.env" ./test/e2e/kind/artifact/run.sh
```

The smoke explicitly leaves ShiftPV non-default and uses a PVC with `storageClassName: shiftpv`.
The upgrade suite opts in to the default class so it also checks preservation of that existing setup.
