#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "linux mount integration requires Linux; skipping on $(uname -s)"
  exit 0
fi

for command in go sudo unshare readlink; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "required command not found: ${command}" >&2
    exit 1
  fi
done

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/shiftpv-linux-mount.XXXXXX")"
trap 'rm -rf "${temp_dir}"' EXIT
chmod 0755 "${temp_dir}"

test_binary="${temp_dir}/bind-mount.test"
parent_mount_namespace="$(readlink /proc/self/ns/mnt)"

go test -c -o "${test_binary}" ./src/node/mount
sudo unshare --mount --propagation private \
  env \
  SHIFTPV_LINUX_MOUNT_INTEGRATION=1 \
  SHIFTPV_PARENT_MOUNT_NAMESPACE="${parent_mount_namespace}" \
  "${test_binary}" \
  -test.v \
  -test.run '^TestLinuxMountIntegration'

measurement_binary="${temp_dir}/measurement.test"
go test -c -o "${measurement_binary}" ./src/cmd/volume-helper
sudo unshare --mount --propagation private \
  env SHIFTPV_LINUX_MOUNT_INTEGRATION=1 \
  "${measurement_binary}" \
  -test.v \
  -test.run '^TestLinuxMeasurementIntegration'

live_capacity_binary="${temp_dir}/live-capacity.test"
go test -c -o "${live_capacity_binary}" ./src/pool/measurement
sudo unshare --mount --propagation private \
  "${live_capacity_binary}" -test.v -test.run '^TestLinuxLiveCapacity'

node_effects_binary="${temp_dir}/node-effects.test"
go test -c -o "${node_effects_binary}" ./src/provisioning/nodeexecutor
sudo unshare --mount --propagation private \
  "${node_effects_binary}" -test.v -test.run '^TestNativeMountedPool'
