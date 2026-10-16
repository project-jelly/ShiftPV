# Full filesystem qualification

Opt-in Linux test using two disposable 64 MiB ext4 loopback images in a private
mount namespace. It exhausts blocks and inodes separately and checks allocation
readiness returns `NoSpace`. `Writable` retains the real write-probe result;
existing cleanup/execution gates follow that observation independently of new
allocation. A real ownership cleanup must stop before retiring its target when
its write-ahead intent fails with `ENOSPC`, preserving the source inode, identity,
and payload. Removing only the test filler permits retrying the same cleanup
operation. The unrelated existing payload and mount identity remain unchanged.
Inventory transport and cleanup authority are injected; this is not a Kubernetes
or complete cleanup transaction/reservation-release test.

```sh
go test -c -o /tmp/full-filesystem.test ./test/integration/full-filesystem
sudo test/integration/full-filesystem/run.sh /tmp/full-filesystem.test
```

For an ARM64 Linux VM, build with `GOOS=linux GOARCH=arm64 CGO_ENABLED=0`, transfer
the binary and `run.sh`, then run there. Requires `mkfs.ext4`, `mount`, `unshare`
and loop devices. Default `go test` skips the test, even as root. The harness
unmounts and deletes its temporary images on exit; `TMPDIR` can select the
scratch parent directory.

Zero available bytes or inodes stops new allocation even if a small write probe
succeeds. A readable statfs result does not guarantee space for cleanup metadata.
Recover filesystem headroom when a required write fails; this test frees only its
own known filler. It does not establish that arbitrary files are safe to remove
or that cleanup can bypass write-ahead metadata requirements on a full filesystem.
