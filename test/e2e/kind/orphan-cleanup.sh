#!/usr/bin/env bash
set -euo pipefail

: "${ROOT_DIR:=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"

: "${CLUSTER_NAME:?CLUSTER_NAME is required}"
: "${WORKER_A_POOL:?WORKER_A_POOL is required}"

NAMESPACE=shiftpv-orphan-e2e
NODE="${CLUSTER_NAME}-worker"
POOL=worker-a
POOL_PATH=/mnt/shiftpv
MOUNT_TARGET=/var/lib/kubelet/pods/shiftpv-orphan-probe/volumes/kubernetes.io~csi/shiftpv/mount
PV_NAME=
VOLUME_ID=
COPY_ID=
MOUNTED=0
OLD_POOL_UID=

remove_test_copy() {
	[[ -n "${VOLUME_ID}" && -n "${COPY_ID}" ]] || return
	docker exec "${NODE}" rm -rf -- "${POOL_PATH}/volumes/${VOLUME_ID}" >/dev/null 2>&1 || true
	docker exec "${NODE}" rm -f -- \
		"${POOL_PATH}/.shiftpv/placements/placement-${COPY_ID}.json" \
		"${POOL_PATH}/.shiftpv/copy-${COPY_ID}.json" \
		"${POOL_PATH}/.shiftpv/lock-${VOLUME_ID}" >/dev/null 2>&1 || true
}

cleanup() {
	if [[ "${MOUNTED}" == "1" ]]; then
		docker exec "${NODE}" umount "${MOUNT_TARGET}" >/dev/null 2>&1 || true
	fi
	remove_test_copy
	kubectl delete namespace "${NAMESPACE}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_inventory_publication() {
	local expected=$1 deadline=$((SECONDS + 120))
	while ((SECONDS < deadline)); do
		if kubectl get "shiftpvpool/${POOL}" -o json | jq -e --arg volume "${VOLUME_ID}" --arg copy "${COPY_ID}" --argjson expected "${expected}" \
			'.status.inventory.valid == true and any(.status.inventory.copies[]?; .identity.volumeID == $volume and .identity.copyID == $copy and (.published // false) == $expected and .present == true and (.problem // "") == "")' >/dev/null; then
			return
		fi
		sleep 1
	done
	echo "Pool inventory did not preserve published=${expected} for ${VOLUME_ID}/${COPY_ID}" >&2
	kubectl get "shiftpvpool/${POOL}" -o yaml >&2 || true
	return 1
}

assert_no_orphan_executor() {
	if kubectl -n shiftpv-system get jobs -o json | jq -e --arg volume "${VOLUME_ID}" \
		'any(.items[]?; any(.spec.template.spec.containers[]?.args[]?; . == ("--authority-name=" + $volume)))' >/dev/null; then
		echo "unknown orphan unexpectedly received a cleanup executor: ${VOLUME_ID}" >&2
		return 1
	fi
}

for resource in shiftpvpools.shiftpv.io shiftpvvolumes.shiftpv.io shiftpvmoves.shiftpv.io; do
	kubectl get "customresourcedefinition/${resource}" >/dev/null
done
if kubectl get customresourcedefinition/shiftpvcleanups.shiftpv.io >/dev/null 2>&1; then
	echo 'standalone ShiftPVCleanup API is still installed' >&2
	exit 1
fi

kubectl create namespace "${NAMESPACE}"
kubectl -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
spec:
  storageClassName: shiftpv-retain
  accessModes: [ReadWriteOnce]
  volumeMode: Filesystem
  resources:
    requests:
      storage: 8Mi
---
apiVersion: v1
kind: Pod
metadata:
  name: writer
spec:
  nodeSelector:
    kubernetes.io/hostname: ${NODE}
  restartPolicy: Never
  terminationGracePeriodSeconds: 1
  containers:
    - name: writer
      image: busybox:1.37
      command: [sh, -ec]
      args:
        - printf 'ShiftPV unknown orphan preservation\n' > /data/payload; sleep 3600
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: data
EOF
kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod/writer --timeout=5m
PV_NAME=$(kubectl -n "${NAMESPACE}" get pvc/data -o jsonpath='{.spec.volumeName}')
PVC_UID=$(kubectl -n "${NAMESPACE}" get pvc/data -o jsonpath='{.metadata.uid}')
VOLUME_ID=$(kubectl get "pv/${PV_NAME}" -o jsonpath='{.spec.csi.volumeHandle}')
COPY_ID=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.currentCopy.copyID}')
CONTROLLER_SERVICE_ACCOUNT=$(kubectl -n shiftpv-system get deployment/shiftpv-controller -o jsonpath='{.spec.template.spec.serviceAccountName}')
CHECKSUM=$(pod_sha256 "${NAMESPACE}" writer /data/payload)
test "${CHECKSUM}" = "$(node_sha256 "${NODE}" "${POOL_PATH}/volumes/${VOLUME_ID}/payload")"
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.requestName}')" = "pvc-${PVC_UID}"
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.initialNode}')" = "${NODE}"
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.capacityBytes}')" = 8388608

# Preserve the copy before removing API parents. Delete reclaim policy would
# authorize CSI cleanup concurrently with this synthetic orphan scenario.
test "$(kubectl get "pv/${PV_NAME}" -o jsonpath='{.spec.persistentVolumeReclaimPolicy}')" = Retain
kubectl -n "${NAMESPACE}" delete pod/writer --wait=true
kubectl -n "${NAMESPACE}" delete pvc/data --wait=true
kubectl wait --for=jsonpath='{.status.phase}'=Released "pv/${PV_NAME}" --timeout=2m

for _ in {1..30}; do
	if docker exec "${NODE}" sh -ec 'mkdir -p "$2" && mount --bind "$1" "$2"' sh \
		"${POOL_PATH}/volumes/${VOLUME_ID}" "${MOUNT_TARGET}" >/dev/null 2>&1; then
		MOUNTED=1
		break
	fi
	sleep 1
done
if [[ "${MOUNTED}" != "1" ]]; then
	echo 'could not establish the synthetic kubelet publication mount' >&2
	exit 1
fi
wait_for_inventory_publication true

# Deliberately simulate loss of the API authority record. The isolated harness
# removes the finalizer as the trusted controller; normal users must never do
# this. Once both Volume and PV parents are gone, the copy is unknown.
kubectl --as="system:serviceaccount:shiftpv-system:${CONTROLLER_SERVICE_ACCOUNT}" \
	patch "shiftpvvolume/${VOLUME_ID}" --type=merge -p '{"metadata":{"finalizers":[]}}'
kubectl --as="system:serviceaccount:shiftpv-system:${CONTROLLER_SERVICE_ACCOUNT}" \
	delete "shiftpvvolume/${VOLUME_ID}" --wait=true
kubectl delete "pv/${PV_NAME}" --wait=true
test -z "$(kubectl get shiftpvmoves -o jsonpath="{.items[?(@.spec.volumeID=='${VOLUME_ID}')].metadata.name}" 2>/dev/null || true)"

# Unknown storage is report-only. A mounted or unmounted copy must remain
# byte-for-byte intact and must never gain an inferred cleanup intent or Job.
sleep 35
wait_for_inventory_publication true
assert_no_orphan_executor
assert_node_file "${NODE}" "${POOL_PATH}/volumes/${VOLUME_ID}/payload"
test "${CHECKSUM}" = "$(node_sha256 "${NODE}" "${POOL_PATH}/volumes/${VOLUME_ID}/payload")"

OLD_POOL_UID=$(kubectl get "shiftpvpool/${POOL}" -o jsonpath='{.metadata.uid}')
kubectl delete "shiftpvpool/${POOL}" --wait=false
kubectl wait --for=condition=Ready=false "shiftpvpool/${POOL}" --timeout=2m
test -n "$(kubectl get "shiftpvpool/${POOL}" -o jsonpath='{.metadata.deletionTimestamp}')"
test "$(kubectl get "shiftpvpool/${POOL}" -o jsonpath='{.metadata.finalizers[0]}')" = shiftpv.io/pool-protection
test "$(kubectl get "shiftpvpool/${POOL}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}')" = PoolDeregistering

docker exec "${NODE}" umount "${MOUNT_TARGET}"
# The probe lives under the kubelet pods directory so it looks like a real pod
# mount; once unmounted, kubelet housekeeping may remove the orphaned pod
# directory before this rmdir runs. Either outcome leaves no mount behind.
docker exec "${NODE}" sh -c 'rmdir "$1" 2>/dev/null || ! test -e "$1"' _ "${MOUNT_TARGET}"
MOUNTED=0
wait_for_inventory_publication false
sleep 35
assert_no_orphan_executor
assert_node_file "${NODE}" "${POOL_PATH}/volumes/${VOLUME_ID}/payload"
test -n "$(kubectl get "shiftpvpool/${POOL}" -o jsonpath='{.metadata.deletionTimestamp}')"

# The product intentionally provides no automatic destructive path for an
# unknown orphan. Remove this test-owned copy out of band so the shared Kind
# suite can continue, then prove Pool deregistration converges.
remove_test_copy
kubectl wait --for=delete "shiftpvpool/${POOL}" --timeout=2m

kubectl apply -f - <<EOF
apiVersion: shiftpv.io/v1alpha1
kind: ShiftPVPool
metadata:
  name: ${POOL}
spec:
  nodeName: ${NODE}
  mountPath: ${POOL_PATH}
  capacity:
    limit: 10Gi
EOF
kubectl wait --for=condition=Ready "shiftpvpool/${POOL}" --timeout=2m
kubectl wait --for=jsonpath='{.status.inventory.valid}'=true "shiftpvpool/${POOL}" --timeout=2m
kubectl wait --for=jsonpath='{.metadata.finalizers[0]}'=shiftpv.io/pool-protection "shiftpvpool/${POOL}" --timeout=2m
NEW_POOL_UID=$(kubectl get "shiftpvpool/${POOL}" -o jsonpath='{.metadata.uid}')
if [[ "${NEW_POOL_UID}" == "${OLD_POOL_UID}" ]]; then
	echo "re-registered Pool kept the deleted identity: ${NEW_POOL_UID}" >&2
	exit 1
fi

trap - EXIT
cleanup
echo "ShiftPV unknown orphan report-only E2E passed: volume=${VOLUME_ID} copy=${COPY_ID} checksum=${CHECKSUM}"
