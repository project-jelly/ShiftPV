#!/usr/bin/env bash
set -euo pipefail

: "${CLUSTER_NAME:?CLUSTER_NAME is required}"
: "${KUBECONFIG:?KUBECONFIG is required}"
: "${WORK_DIR:?WORK_DIR is required}"
: "${WORKER_A_POOL:?WORKER_A_POOL is required}"
: "${WORKER_B_POOL:?WORKER_B_POOL is required}"

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
# shellcheck source=test/e2e/kind/cleanup-journal.sh
source "${ROOT_DIR}/test/e2e/kind/cleanup-journal.sh"
# shellcheck source=test/e2e/kind/lib/wait.sh
source "${ROOT_DIR}/test/e2e/kind/lib/wait.sh"

test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"
test -d "${WORK_DIR}"
test -d "${WORKER_A_POOL}"
test -d "${WORKER_B_POOL}"

NAMESPACE=shiftpv-cleanup-retry
SOURCE_NODE="${CLUSTER_NAME}-worker"
DESTINATION_NODE="${CLUSTER_NAME}-worker2"
SOURCE_MOUNT=$(pool_mount_for_node "${SOURCE_NODE}")
DESTINATION_MOUNT=$(pool_mount_for_node "${DESTINATION_NODE}")
MOVE_CLEANUP_CAPABILITY=""

cleanup() {
	local result_code=$?
	if [[ -n "${MOVE_CLEANUP_CAPABILITY}" ]]; then
		restore_move_cleanup_capability "${MOVE_CLEANUP_CAPABILITY}" || result_code=1
	fi
	kubectl uncordon "${SOURCE_NODE}" >/dev/null 2>&1 || true
	kubectl uncordon "${DESTINATION_NODE}" >/dev/null 2>&1 || true
	kubectl delete namespace "${NAMESPACE}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
	return "${result_code}"
}
trap cleanup EXIT

cleanup_pod_name_for_uid() {
	local job_name=$1 pod_uid=$2
	kubectl -n shiftpv-system get pods -l "job-name=${job_name}" \
		-o jsonpath="{.items[?(@.metadata.uid=='${pod_uid}')].metadata.name}" 2>/dev/null || true
}

for command in docker kubectl awk jq; do
	command -v "${command}" >/dev/null || {
		echo "required command not found: ${command}" >&2
		exit 1
	}
done

for resource in shiftpvpools.shiftpv.io shiftpvvolumes.shiftpv.io shiftpvmoves.shiftpv.io; do
	kubectl get "customresourcedefinition/${resource}" >/dev/null
done
if kubectl get customresourcedefinition/shiftpvcleanups.shiftpv.io >/dev/null 2>&1; then
	echo 'standalone ShiftPVCleanup API is still installed' >&2
	exit 1
fi

kubectl create namespace "${NAMESPACE}"
kubectl label namespace "${NAMESPACE}" shiftpv.io/admission=enabled
kubectl uncordon "${SOURCE_NODE}" >/dev/null 2>&1 || true
kubectl cordon "${DESTINATION_NODE}"
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  namespace: ${NAMESPACE}
spec:
  storageClassName: shiftpv
  accessModes: [ReadWriteOnce]
  volumeMode: Filesystem
  resources:
    requests:
      storage: 16Mi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: writer
  namespace: ${NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${NAMESPACE}
  template:
    metadata:
      labels:
        app: ${NAMESPACE}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: writer
          image: busybox:1.37
          command: [sh, -ec]
          args:
            - printf 'ShiftPV cleanup Job retry Pod rebind\n' > /data/payload; sleep 3600
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: data
EOF

kubectl -n "${NAMESPACE}" rollout status deployment/writer --timeout=180s
kubectl -n "${NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Bound pvc/data --timeout=120s
PVC_UID=$(kubectl -n "${NAMESPACE}" get pvc/data -o jsonpath='{.metadata.uid}')
PV_NAME=$(kubectl -n "${NAMESPACE}" get pvc/data -o jsonpath='{.spec.volumeName}')
VOLUME_ID=$(kubectl get "pv/${PV_NAME}" -o jsonpath='{.spec.csi.volumeHandle}')
SOURCE_POD=$(kubectl -n "${NAMESPACE}" get pod -l "app=${NAMESPACE}" -o jsonpath='{.items[0].metadata.name}')
test "$(kubectl -n "${NAMESPACE}" get "pod/${SOURCE_POD}" -o jsonpath='{.spec.nodeName}')" = "${SOURCE_NODE}"

# Slow source cleanup enough to kill the first cleanup executor after it durably
# binds its Pod UID but before it writes a receipt.
# shellcheck disable=SC2016
kubectl -n "${NAMESPACE}" exec "${SOURCE_POD}" -- sh -ec '
	i=0
	while [ "${i}" -lt 20000 ]; do
		printf "cleanup-retry-%05d\n" "${i}" >"/data/retry-${i}"
		i=$((i + 1))
	done
	sync
'
SOURCE_CHECKSUM=$(pod_sha256 "${NAMESPACE}" "${SOURCE_POD}" /data/payload)

kubectl uncordon "${DESTINATION_NODE}"
# This case must exercise Job Pod rebinding even when resident cleanup is
# available. Restore the capability once this Move has bound its Job.
MOVE_CLEANUP_CAPABILITY=$(suspend_move_cleanup_capability "${SOURCE_NODE}")
kubectl cordon "${SOURCE_NODE}"
MOVE_NAME=$(wait_for_move "${VOLUME_ID}" 180)

killed_pod_uid=""
killed_pod_name=""
cleanup_job=""
cleanup_job_uid_before=""
for _ in {1..900}; do
	phase=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
	if [[ "${phase}" == "Blocked" || "${phase}" == "Succeeded" ]]; then
		echo "Move reached terminal phase before cleanup Pod retry injection: ${phase}" >&2
		kubectl get "shiftpvmove/${MOVE_NAME}" -o yaml >&2 || true
		exit 1
	fi
	cleanup_job=$(cleanup_job_name "shiftpvmove/${MOVE_NAME}" 2>/dev/null || true)
	killed_pod_uid=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.executor.podUID}' 2>/dev/null || true)
	receipt=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.receipt.operationID}' 2>/dev/null || true)
	if [[ -n "${cleanup_job}" && -n "${killed_pod_uid}" && -z "${receipt}" ]]; then
		cleanup_job_uid_before=$(kubectl -n shiftpv-system get "job/${cleanup_job}" -o jsonpath='{.metadata.uid}')
		killed_pod_name=$(cleanup_pod_name_for_uid "${cleanup_job}" "${killed_pod_uid}")
		if [[ -n "${killed_pod_name}" ]]; then
			kubectl -n shiftpv-system delete "pod/${killed_pod_name}" --grace-period=0 --force --wait=true
			break
		fi
	fi
	sleep 0.2
done
if [[ -z "${killed_pod_name}" ]]; then
	echo "cleanup executor did not expose a killable pre-receipt Pod" >&2
	kubectl get "shiftpvmove/${MOVE_NAME}" -o yaml >&2 || true
	exit 1
fi
restore_move_cleanup_capability "${MOVE_CLEANUP_CAPABILITY}"
MOVE_CLEANUP_CAPABILITY=""

retry_pod_uid=""
for _ in {1..300}; do
	retry_pod_uid=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.executor.podUID}' 2>/dev/null || true)
	receipt=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.receipt.operationID}' 2>/dev/null || true)
	if [[ -n "${retry_pod_uid}" && "${retry_pod_uid}" != "${killed_pod_uid}" && -z "${receipt}" ]]; then
		break
	fi
	sleep 1
done
if [[ -z "${retry_pod_uid}" || "${retry_pod_uid}" == "${killed_pod_uid}" ]]; then
	echo "cleanup executor did not rebind from failed Pod ${killed_pod_uid}" >&2
	kubectl get "shiftpvmove/${MOVE_NAME}" -o yaml >&2 || true
	kubectl -n shiftpv-system get pods -l "job-name=${cleanup_job}" -o wide >&2 || true
	exit 1
fi

kubectl wait "shiftpvmove/${MOVE_NAME}" --for=jsonpath='{.status.phase}'=Succeeded --timeout=900s
cleanup_job_uid_after=$(kubectl -n shiftpv-system get "job/${cleanup_job}" -o jsonpath='{.metadata.uid}')
final_pod_uid=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.executor.podUID}')
receipt_executor_uid=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.receipt.executorUID}')
test "$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.executor.kind}')" = ""
test "${cleanup_job_uid_after}" = "${cleanup_job_uid_before}"
test "${final_pod_uid}" = "${retry_pod_uid}"
test "${final_pod_uid}" != "${killed_pod_uid}"
test "${receipt_executor_uid}" = "${cleanup_job_uid_before}"

test "$(kubectl -n "${NAMESPACE}" get pvc/data -o jsonpath='{.metadata.uid}')" = "${PVC_UID}"
test "$(kubectl -n "${NAMESPACE}" get pvc/data -o jsonpath='{.spec.volumeName}')" = "${PV_NAME}"
kubectl -n "${NAMESPACE}" rollout status deployment/writer --timeout=180s
DESTINATION_POD=$(kubectl -n "${NAMESPACE}" get pod -l "app=${NAMESPACE}" \
	--field-selector "spec.nodeName=${DESTINATION_NODE}" -o jsonpath='{.items[0].metadata.name}')
test -n "${DESTINATION_POD}"
kubectl -n "${NAMESPACE}" wait --for=condition=Ready "pod/${DESTINATION_POD}" --timeout=120s
test "$(pod_sha256 "${NAMESPACE}" "${DESTINATION_POD}" /data/payload)" = "${SOURCE_CHECKSUM}"
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${DESTINATION_NODE}"
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.activeMove}')" = ""
assert_node_absent "${SOURCE_NODE}" "${SOURCE_MOUNT}/volumes/${VOLUME_ID}"
assert_node_file "${DESTINATION_NODE}" "${DESTINATION_MOUNT}/volumes/${VOLUME_ID}/payload"
source_copy=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.sourceCopy.copyID}')
assert_cleanup_journal "shiftpvmove/${MOVE_NAME}" MoveSource "${VOLUME_ID}" "${source_copy}" ShiftPVMove

trap - EXIT
cleanup
echo "ShiftPV cleanup Job retry Pod rebind passed: move=${MOVE_NAME} job=${cleanup_job} jobUID=${cleanup_job_uid_before} killedPodUID=${killed_pod_uid} retryPodUID=${retry_pod_uid}"
