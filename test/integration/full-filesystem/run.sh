#!/usr/bin/env bash
set -euo pipefail

if [[ $(uname -s) != Linux || $(id -u) != 0 ]]; then
  echo 'SKIP: full-filesystem qualification requires Linux root.'
  exit 0
fi

if [[ ${1:-} == --private ]]; then
  binary=$2
  scratch=$3
  # shellcheck disable=SC2329 # Invoked by the private namespace's EXIT trap.
  cleanup() {
    for mode in inodes blocks; do
      if mountpoint -q "$scratch/$mode"; then
        umount "$scratch/$mode"
      fi
    done
  }
  trap cleanup EXIT
  for mode in blocks inodes; do
    mkdir "$scratch/$mode"
    truncate -s 64M "$scratch/$mode.img"
    inode_count=4096
    if [[ $mode == inodes ]]; then inode_count=128; fi
    mkfs.ext4 -q -F -m 0 -b 4096 -N "$inode_count" "$scratch/$mode.img"
    mount -o loop,nodev,nosuid,noexec "$scratch/$mode.img" "$scratch/$mode"
  done
  SHIFTPV_FULL_FILESYSTEM=1 SHIFTPV_ENOSPC_FIXTURE="$scratch" \
    "$binary" -test.v -test.timeout=2m -test.run '^TestFullFilesystemReadinessAndCleanupRecovery$'
  exit
fi

if [[ $# != 1 || ! -x $1 ]]; then
  echo "Usage: sudo $0 /absolute/path/to/full-filesystem.test" >&2
  exit 2
fi
binary=$(realpath "$1")
scratch=$(mktemp -d "${TMPDIR:-/tmp}/shiftpv-enospc.XXXXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
unshare --mount --propagation private -- "$0" --private "$binary" "$scratch"
