# Full filesystem qualification

Opt-in Linux test using two disposable 64 MiB ext4 loopback images in a private
mount namespace. It exhausts blocks and inodes separately, checks the real
readiness probe and both cleanup gates return `NoSpace`, removes only its filler,
then verifies recovery, unchanged mount identity, and retained existing payload.
Inventory transport is injected; this is not a Kubernetes or complete cleanup
transaction test. It does not change production cleanup policy.

```sh
go test -c -o /tmp/full-filesystem.test ./test/integration/full-filesystem
sudo test/integration/full-filesystem/run.sh /tmp/full-filesystem.test
```

For an ARM64 Linux VM, build with `GOOS=linux GOARCH=arm64 CGO_ENABLED=0`, transfer
the binary and `run.sh`, then run there. Requires `mkfs.ext4`, `mount`, `unshare`
and loop devices. Default `go test` skips the test, even as root. The harness
unmounts and deletes its temporary images on exit; `TMPDIR` can select the
scratch parent directory.

Current recovery contract: allocation and cleanup both stop on `NoSpace`.
Recover filesystem headroom before retrying; this test frees only its own known
filler. It does not establish that arbitrary files are safe to remove or that
cleanup can bypass write-ahead metadata requirements on a full filesystem.
