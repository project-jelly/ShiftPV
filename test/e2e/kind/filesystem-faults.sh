#!/usr/bin/env bash
set -euo pipefail

TEST_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${TEST_DIR}/node-path.sh"
: "${CLUSTER_NAME:?CLUSTER_NAME is required}"
: "${WORKER_B_POOL:?WORKER_B_POOL is required}"

FAULT_NODE="${CLUSTER_NAME}-worker2"
FAULT_POOL_PATH=/srv/shiftpv-b
MOUNT_STATE=normal

restore_pool_mount() {
  case "${MOUNT_STATE}" in
    enospc | tmpfs_rw)
      docker exec "${FAULT_NODE}" umount "${FAULT_POOL_PATH}" >/dev/null 2>&1 || true
      ;;
    tmpfs_readonly)
      docker exec "${FAULT_NODE}" mount -o remount,rw "${FAULT_POOL_PATH}" >/dev/null 2>&1 || true
      docker exec "${FAULT_NODE}" umount "${FAULT_POOL_PATH}" >/dev/null 2>&1 || true
      ;;
  esac
  MOUNT_STATE=normal
}
trap restore_pool_mount EXIT

wait_for_volume_hold() {
  local request_name=$1
  local attempt
  HELD_VOLUME_ID=""
  for ((attempt = 0; attempt < 120; attempt++)); do
    HELD_VOLUME_ID=$(kubectl get shiftpvvolumes \
      -o custom-columns=NAME:.metadata.name,REQUEST:.spec.requestName \
      --no-headers 2>/dev/null | awk -v request="${request_name}" '$2 == request { print $1 }')
    if [[ -n "${HELD_VOLUME_ID}" ]]; then
      return
    fi
    sleep 1
  done
  echo "ShiftPVVolume capacity hold for ${request_name} was not created" >&2
  exit 1
}

wait_for_unavailable_event() {
  local kind=$1
  local name=$2
  local attempt messages
  for ((attempt = 0; attempt < 120; attempt++)); do
    messages=$(kubectl get events \
      --field-selector "involvedObject.kind=${kind},involvedObject.name=${name}" \
      -o jsonpath='{range .items[*]}{.message}{"\n"}{end}' 2>/dev/null || true)
    if grep -Fq 'code = Unavailable' <<<"${messages}"; then
      return
    fi
    sleep 1
  done
  echo "${kind}/${name} did not report a retryable Unavailable error" >&2
  kubectl get events \
    --field-selector "involvedObject.kind=${kind},involvedObject.name=${name}" >&2 || true
  exit 1
}

wait_for_pool_reason() {
  local expected_status=$1
  local expected_reason=$2
  local attempt status reason
  for ((attempt = 0; attempt < 120; attempt++)); do
    status=$(kubectl get shiftpvpool worker-b -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
    reason=$(kubectl get shiftpvpool worker-b -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true)
    if [[ "${status}" == "${expected_status}" && "${reason}" == "${expected_reason}" ]]; then
      return
    fi
    sleep 1
  done
  echo "Pool did not reach Ready=${expected_status} reason=${expected_reason}" >&2
  kubectl get shiftpvpool worker-b -o yaml >&2 || true
  exit 1
}

wait_for_not_ready_event() {
  local name=$1
  local attempt messages
  for ((attempt = 0; attempt < 120; attempt++)); do
    messages=$(kubectl get events \
      --field-selector "involvedObject.kind=PersistentVolumeClaim,involvedObject.name=${name}" \
      -o jsonpath='{range .items[*]}{.message}{"\n"}{end}' 2>/dev/null || true)
    if grep -Eq 'Pool is not ready|has no Ready Pool in group' <<<"${messages}"; then
      return
    fi
    sleep 1
  done
  echo "PVC/${name} did not report a not-ready Pool" >&2
  exit 1
}

# Overlay the fault worker pool with a byte-sufficient tmpfs and exhaust only
# its inodes. The write probe must reject the Pool even though byte statfs alone
# would pass; the statfs-to-I/O race is covered separately by controller tests.
docker exec "${FAULT_NODE}" mount \
	-t tmpfs -o size=128m,nr_inodes=64 shiftpv-enospc "${FAULT_POOL_PATH}"
MOUNT_STATE=enospc
docker exec "${FAULT_NODE}" sh -ec '
  mkdir -p /srv/shiftpv-b/volumes
  index=0
  while [ "${index}" -lt 1000 ] && touch "/srv/shiftpv-b/fill-${index}" 2>/dev/null; do
    index=$((index + 1))
  done
  if [ "${index}" -eq 1000 ]; then
    echo "tmpfs inode limit was not enforced" >&2
    exit 1
  fi
  if mkdir /srv/shiftpv-b/volumes/probe 2>/dev/null; then
    echo "failed to exhaust tmpfs inodes" >&2
    exit 1
  fi
'
wait_for_pool_reason False NoSpace

kubectl apply -f "${TEST_DIR}/filesystem-fault-storageclass.yaml"
kubectl apply -f "${TEST_DIR}/filesystem-fault-pvc.yaml"
PVC_UID=$(kubectl get pvc shiftpv-filesystem-fault -o jsonpath='{.metadata.uid}')
kubectl apply -f "${TEST_DIR}/filesystem-fault-pod.yaml"
wait_for_not_ready_event shiftpv-filesystem-fault

PVC_PHASE=$(kubectl get pvc shiftpv-filesystem-fault -o jsonpath='{.status.phase}')
if [[ "${PVC_PHASE}" != "Pending" ]]; then
  echo "ENOSPC PVC unexpectedly left Pending: ${PVC_PHASE}" >&2
  exit 1
fi
if kubectl get shiftpvvolumes \
  -o custom-columns=REQUEST:.spec.requestName --no-headers | grep -Fxq "pvc-${PVC_UID}"; then
	echo "not-ready Pool provisioning created a ShiftPVVolume capacity hold" >&2
	exit 1
fi

# Free the fault files without replacing the mounted filesystem. The Pool must
# return to Ready and the same pending PVC may then create its Volume-owned hold.
docker exec "${FAULT_NODE}" sh -ec 'rm -f /srv/shiftpv-b/fill-*'
MOUNT_STATE=tmpfs_rw
wait_for_pool_reason True PoolReady
FAULT_NODE_POD=$(kubectl -n shiftpv-system get pod \
  -l app.kubernetes.io/instance=shiftpv,app.kubernetes.io/component=node \
  --field-selector "spec.nodeName=${FAULT_NODE}" \
  -o jsonpath='{.items[0].metadata.name}')
kubectl -n shiftpv-system delete "pod/${FAULT_NODE_POD}" \
  --grace-period=0 --force --wait=true
kubectl -n shiftpv-system rollout status daemonset/shiftpv-node --timeout=5m
wait_for_volume_hold "pvc-${PVC_UID}"
kubectl wait --for=condition=Ready pod/shiftpv-filesystem-fault --timeout=5m
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/shiftpv-filesystem-fault --timeout=2m

FAULT_PV=$(kubectl get pvc shiftpv-filesystem-fault -o jsonpath='{.spec.volumeName}')
VOLUME_ID=$(kubectl get "pv/${FAULT_PV}" -o jsonpath='{.spec.csi.volumeHandle}')
if [[ "${VOLUME_ID}" != "${HELD_VOLUME_ID}" ]]; then
  echo "retry changed the Volume-owned capacity identity" >&2
  exit 1
fi
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.requestName}')" = "pvc-${PVC_UID}"
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.initialNode}')" = "${FAULT_NODE}"
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.capacityBytes}')" = 67108864
kubectl exec shiftpv-filesystem-fault -- grep -Fx 'ShiftPV filesystem fault recovery' /data/payload
docker exec "${FAULT_NODE}" test -f "${FAULT_POOL_PATH}/volumes/${VOLUME_ID}/payload"

# Stop the writer, remount the tmpfs filesystem read-only, and request deletion.
# DeleteVolume must fail retryably without dropping metadata or data, then
# finish after the filesystem is restored read-write.
kubectl delete pod shiftpv-filesystem-fault --wait=true
docker exec "${FAULT_NODE}" mount -o remount,ro "${FAULT_POOL_PATH}"
MOUNT_STATE=tmpfs_readonly
if docker exec "${FAULT_NODE}" touch "${FAULT_POOL_PATH}/.shiftpv-readonly-probe" 2>/dev/null; then
  echo "pool remount did not become read-only" >&2
	exit 1
fi
wait_for_pool_reason False ReadOnly

kubectl delete pvc shiftpv-filesystem-fault --wait=false
kubectl wait --for=delete pvc/shiftpv-filesystem-fault --timeout=2m
wait_for_unavailable_event PersistentVolume "${FAULT_PV}"
kubectl get "pv/${FAULT_PV}" >/dev/null
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.requestName}')" = "pvc-${PVC_UID}"
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.reason}')" = VolumeDelete
if [[ "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.phase}')" == Completed ]]; then
	echo "read-only Pool released the Volume hold before cleanup could run" >&2
	exit 1
fi
docker exec "${FAULT_NODE}" test -f "${FAULT_POOL_PATH}/volumes/${VOLUME_ID}/payload"

docker exec "${FAULT_NODE}" mount -o remount,rw "${FAULT_POOL_PATH}"
MOUNT_STATE=tmpfs_rw
wait_for_pool_reason True PoolReady
kubectl wait --for=delete "pv/${FAULT_PV}" --timeout=5m
kubectl wait --for=delete "shiftpvvolume/${VOLUME_ID}" --timeout=2m
docker exec "${FAULT_NODE}" test ! -e "${FAULT_POOL_PATH}/volumes/${VOLUME_ID}"
docker exec "${FAULT_NODE}" umount "${FAULT_POOL_PATH}"
MOUNT_STATE=normal
assert_node_absent "${FAULT_NODE}" "${FAULT_POOL_PATH}/volumes/${VOLUME_ID}"
kubectl delete storageclass shiftpv-filesystem-fault --wait=true

echo "ShiftPV filesystem fault recovery passed: volume=${VOLUME_ID} node=${FAULT_NODE}"
