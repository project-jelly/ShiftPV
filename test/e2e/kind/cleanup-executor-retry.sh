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
LOCK_RELEASE=""

cleanup() {
	local result_code=$?
	if [[ -n "${LOCK_RELEASE}" ]]; then
		docker exec "${SOURCE_NODE}" touch -- "${LOCK_RELEASE}" || result_code=1
	fi
	kubectl uncordon "${SOURCE_NODE}" >/dev/null 2>&1 || true
	kubectl uncordon "${DESTINATION_NODE}" >/dev/null 2>&1 || true
	kubectl delete namespace "${NAMESPACE}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
	return "${result_code}"
}
trap cleanup EXIT

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
            - if [ ! -f /data/payload ]; then printf 'initial cleanup Node retry payload\n' > /data/payload; fi; sleep 3600
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

# A blank destination must not regenerate the source's unique test content.
# shellcheck disable=SC2016
kubectl -n "${NAMESPACE}" exec "${SOURCE_POD}" -- sh -ec 'printf "%s\n" "$1" > /data/payload; sync' sh "cleanup-retry-${PVC_UID}-${VOLUME_ID}"
SOURCE_CHECKSUM=$(pod_sha256 "${NAMESPACE}" "${SOURCE_POD}" /data/payload)
POLICY=shiftpv-cleanup-retry-barrier
hold_cleanup_admission "${POLICY}" "${VOLUME_ID}"
kubectl uncordon "${DESTINATION_NODE}"
kubectl cordon "${SOURCE_NODE}"
MOVE_NAME=$(wait_for_move "${VOLUME_ID}" 180)
wait_for_cleanup_barrier "${MOVE_NAME}" "${VOLUME_ID}" "${SOURCE_NODE}"
MOVE_UID=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.metadata.uid}')
LOCK_PATH="${SOURCE_MOUNT}/.shiftpv/lock-${VOLUME_ID}"
LOCK_READY="/tmp/shiftpv-cleanup-${MOVE_UID}-ready"
LOCK_RELEASE="/tmp/shiftpv-cleanup-${MOVE_UID}-release"
docker exec "${SOURCE_NODE}" test -f "${LOCK_PATH}"
docker exec "${SOURCE_NODE}" test ! -e "${LOCK_READY}"
docker exec "${SOURCE_NODE}" test ! -e "${LOCK_RELEASE}"
docker exec "${SOURCE_NODE}" sh -ec 'command -v flock; command -v timeout' >/dev/null
# Hold the existing inode: no replacement lock or fake execution receipt.
# shellcheck disable=SC2016
docker exec -d "${SOURCE_NODE}" timeout 600 sh -ec '
 exec 9<"$1"
 flock -n 9
 umask 077
 set -C
 : >"$2"
 while [ ! -e "$3" ]; do sleep 0.1; done
' sh "${LOCK_PATH}" "${LOCK_READY}" "${LOCK_RELEASE}"
for _ in {1..100}; do
 if docker exec "${SOURCE_NODE}" test -f "${LOCK_READY}"; then break; fi
 sleep 0.1
done
docker exec "${SOURCE_NODE}" test -f "${LOCK_READY}"
release_cleanup_admission "${POLICY}"
kubectl wait "shiftpvmove/${MOVE_NAME}" --for=jsonpath='{.status.cleanup.status.phase}'=Running --timeout=120s
JOURNAL=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o json)
jq -e '.status.cleanup.status.executor.kind == "Node" and .status.cleanup.status.receipt == null' <<<"${JOURNAL}" >/dev/null
killed_pod_uid=$(jq -er '.status.cleanup.status.executor.podUID' <<<"${JOURNAL}")
killed_pod_name=$(jq -er '.status.cleanup.status.executor.podName' <<<"${JOURNAL}")
OPERATION=$(jq -er '.status.cleanup.spec.operationID' <<<"${JOURNAL}")
TARGET=$(jq -c '.status.cleanup.spec.target' <<<"${JOURNAL}")
RECEIPT_HASH=$(printf '%s' "${OPERATION}" | shasum -a 256 | cut -c1-32)
RECEIPT_PATH="${SOURCE_MOUNT}/.shiftpv/receipt-${RECEIPT_HASH}.json"
docker exec "${SOURCE_NODE}" test ! -e "${RECEIPT_PATH}"
assert_no_cleanup_job "${MOVE_NAME}"
test "$(kubectl -n shiftpv-system get "pod/${killed_pod_name}" -o jsonpath='{.metadata.uid}')" = "${killed_pod_uid}"
kubectl -n shiftpv-system delete "pod/${killed_pod_name}" --wait=true --timeout=120s
kubectl -n shiftpv-system rollout status daemonset/shiftpv-node --timeout=180s
retry_pod_uid=""
for _ in {1..120}; do
 JOURNAL=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o json)
 retry_pod_uid=$(jq -r '.status.cleanup.status.executor.podUID // ""' <<<"${JOURNAL}")
 if [[ -n "${retry_pod_uid}" && "${retry_pod_uid}" != "${killed_pod_uid}" ]]; then break; fi
 sleep 1
done
test -n "${retry_pod_uid}"
test "${retry_pod_uid}" != "${killed_pod_uid}"
jq -e --arg uid "${MOVE_UID}" --arg operation "${OPERATION}" --argjson target "${TARGET}" '
 .metadata.uid == $uid and .status.cleanup.spec.operationID == $operation and
 .status.cleanup.spec.target == $target and .status.cleanup.status.phase == "Running" and
 .status.cleanup.status.executor.kind == "Node" and .status.cleanup.status.receipt == null' <<<"${JOURNAL}" >/dev/null
docker exec "${SOURCE_NODE}" test ! -e "${RECEIPT_PATH}"
docker exec "${SOURCE_NODE}" touch -- "${LOCK_RELEASE}"
LOCK_RELEASE=""
kubectl wait "shiftpvmove/${MOVE_NAME}" --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
final_pod_uid=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.executor.podUID}')
receipt_executor_uid=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.receipt.executorUID}')
test "${final_pod_uid}" = "${retry_pod_uid}"
test "${receipt_executor_uid}" = "${retry_pod_uid}"
test "$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.metadata.uid}')" = "${MOVE_UID}"
assert_no_cleanup_job "${MOVE_NAME}"

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

# Exercise the real-node soak's policy against this genuinely completed Move.
# Only an unchanged server dry-run is submitted; no history is manufactured.
(
	# shellcheck source=test/e2e/real-node/cleanup-executor.sh
	source "${ROOT_DIR}/test/e2e/real-node/cleanup-executor.sh"
	k() { kubectl --request-timeout=30s "$@"; }
	MOVE_TIMEOUT_SECONDS=60
	ARTIFACT_DIR="${WORK_DIR}/soak-barrier"
	mkdir -p "${ARTIFACT_DIR}"
	deadline=$((SECONDS + 120))
	while ((SECONDS < deadline)); do
		k get "shiftpvmove/${MOVE_NAME}" -o json >"${ARTIFACT_DIR}/before.json"
		if jq -e '((.metadata.finalizers // []) | length) == 0' "${ARTIFACT_DIR}/before.json" >/dev/null; then break; fi
		sleep 1
	done
	trap release_soak_cleanup EXIT
	hold_soak_cleanup "shiftpv-soak-canary-${MOVE_UID}" "${VOLUME_ID}" "${MOVE_NAME}"
	k get "shiftpvmove/${MOVE_NAME}" -o json >"${ARTIFACT_DIR}/after.json"
	jq -e --slurpfile before "${ARTIFACT_DIR}/before.json" '
		.metadata.uid == $before[0].metadata.uid and .spec == $before[0].spec and .status == $before[0].status' \
		"${ARTIFACT_DIR}/after.json" >/dev/null
	release_soak_cleanup
	trap - EXIT
)

trap - EXIT
cleanup
echo "ShiftPV cleanup Node retry passed: move=${MOVE_NAME} operation=${OPERATION} killedPodUID=${killed_pod_uid} retryPodUID=${retry_pod_uid}"
