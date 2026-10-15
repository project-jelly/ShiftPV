#!/usr/bin/env bash
set -euo pipefail

# The caller owns a disposable cluster and its private kubeconfig.
: "${CLUSTER_NAME:?isolated Kind cluster name required}"
: "${KUBECONFIG:?isolated kubeconfig required}"
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
# shellcheck source=test/e2e/kind/cleanup-journal.sh
source "${ROOT_DIR}/test/e2e/kind/cleanup-journal.sh"
NAMESPACE=shiftpv-native-cleanup-test
POLICY=shiftpv-native-cleanup-test
for resource in namespace validatingadmissionpolicy validatingadmissionpolicybinding; do
	test -z "$(kubectl get "${resource}/${NAMESPACE}" --ignore-not-found -o name)"
done

node_pod() {
	kubectl -n shiftpv-system get pods -l app.kubernetes.io/component=node \
		--field-selector "spec.nodeName=${SOURCE}" -o json |
		jq -er '.items | map(select(.metadata.deletionTimestamp == null)) | if length == 1 then .[0].metadata.name else error("expected one Node Pod") end'
}

assert_pending_receipt() {
	kubectl get "shiftpvmove/${MOVE}" -o json | jq -e \
		--arg uid "${MOVE_UID}" --arg operation "${OPERATION}" --argjson target "${TARGET}" '
		.metadata.uid == $uid and
		(.status.phase == "WaitingForDestinationPublish" or .status.phase == "CleaningSource") and
		.status.capacityApproved == true and (.metadata.finalizers | length) > 0 and
		.status.cleanup.spec.operationID == $operation and .status.cleanup.spec.target == $target and
		.status.cleanup.status.executor.kind == "Node" and
		.status.cleanup.status.phase == "Running" and .status.cleanup.status.receipt == null' >/dev/null
	test "$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.activeMove}')" = "${MOVE}"
	assert_no_cleanup_job
}

assert_no_cleanup_job() {
	kubectl -n shiftpv-system get jobs -o json | jq -e --arg uid "${MOVE_UID}" '
		[.items[] | select(.metadata.labels["app.kubernetes.io/component"] == "cleanup-helper") |
		 select(any(.metadata.ownerReferences[]?; .uid == $uid))] | length == 0' >/dev/null
}

sed 's/shiftpv-mobility-test/shiftpv-native-cleanup-test/g' \
	"${ROOT_DIR}/test/e2e/kind/mobility/manifests/wffc-workload.yaml" | kubectl apply -f -
kubectl -n "${NAMESPACE}" rollout status deployment/wffc --timeout=180s
PVC_UID=$(kubectl -n "${NAMESPACE}" get pvc/wffc -o jsonpath='{.metadata.uid}')
PV=$(kubectl -n "${NAMESPACE}" get pvc/wffc -o jsonpath='{.spec.volumeName}')
PV_UID=$(kubectl get "pv/${PV}" -o jsonpath='{.metadata.uid}')
VOLUME=$(kubectl get "pv/${PV}" -o jsonpath='{.spec.csi.volumeHandle}')
SOURCE=$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.ownerNode}')
SOURCE_POOL=$(pool_mount_for_node "${SOURCE}")
CHECKSUM=$(pod_sha256 "${NAMESPACE}" deployment/wffc /data/payload)
kubectl patch "pv/${PV}" --type=merge -p '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
NODE_POD=$(node_pod)
kubectl -n shiftpv-system wait "pod/${NODE_POD}" \
	--for='jsonpath={.metadata.annotations.shiftpv\.io/node-move-cleanup}'=v1 --timeout=120s
NODE_UID=$(kubectl -n shiftpv-system get "pod/${NODE_POD}" -o jsonpath='{.metadata.uid}')

# Keep the real local receipt but reject its API write. The controller must
# retain the source hold until replay, then request a later absence scan.
kubectl apply -f - <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: ${POLICY}
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
    - apiGroups: [shiftpv.io]
      apiVersions: [v1alpha1]
      operations: [UPDATE]
      resources: [shiftpvmoves/status]
  validations:
  - expression: "object.spec.volumeID != '${VOLUME}' || !has(object.status.cleanup) || !has(object.status.cleanup.status) || !has(object.status.cleanup.status.receipt)"
    message: native cleanup receipt failure injection
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ${POLICY}
spec:
  policyName: ${POLICY}
  validationActions: [Deny]
EOF
kubectl cordon "${SOURCE}"
deadline=$((SECONDS + 180))
MOVE=""
while ((SECONDS < deadline)); do
	MOVE=$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.activeMove}')
	[[ -n "${MOVE}" ]] && break
	sleep 1
done
test -n "${MOVE}"
kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.cleanup.status.phase}'=Running --timeout=300s
MOVE_UID=$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.metadata.uid}')
OPERATION=$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.status.cleanup.spec.operationID}')
TARGET=$(kubectl get "shiftpvmove/${MOVE}" -o json | jq -c '.status.cleanup.spec.target')
SOURCE_COPY=$(jq -r '.copyID' <<<"${TARGET}")
test -n "${OPERATION}"
test "$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.status.cleanup.status.executor.podUID}')" = "${NODE_UID}"
RECEIPT_HASH=$(printf '%s' "${OPERATION}" | shasum -a 256 | cut -c1-32)
RECEIPT_PATH="${SOURCE_POOL}/.shiftpv/receipt-${RECEIPT_HASH}.json"
deadline=$((SECONDS + 120))
while ((SECONDS < deadline)); do
	if docker exec "${SOURCE}" test -f "${RECEIPT_PATH}"; then break; fi
	sleep 1
done
LOCAL_RECEIPT=$(docker exec "${SOURCE}" cat "${RECEIPT_PATH}")
jq -e --arg operation "${OPERATION}" --argjson target "${TARGET}" \
	'.version == 1 and .value.operationID == $operation and .value.target == $target and .value.retired and .value.purged' <<<"${LOCAL_RECEIPT}" >/dev/null
docker exec "${SOURCE}" test ! -e "${SOURCE_POOL}/volumes/${VOLUME}"
docker exec "${SOURCE}" test ! -e "${SOURCE_POOL}/.shiftpv/retired/${SOURCE_COPY}"
assert_pending_receipt
kubectl -n "${NAMESPACE}" rollout status deployment/wffc --timeout=120s
test "$(pod_sha256 "${NAMESPACE}" deployment/wffc /data/payload)" = "${CHECKSUM}"

# Drain only after owner commit and cleanup start. DaemonSet execution survives
# this drain; the explicit Node replacement then exercises durable replay.
kubectl drain "${SOURCE}" --ignore-daemonsets --delete-emptydir-data --force --timeout=120s
test "$(kubectl -n shiftpv-system get "pod/${NODE_POD}" -o jsonpath='{.metadata.uid}')" = "${NODE_UID}"
assert_pending_receipt
kubectl -n shiftpv-system delete "pod/${NODE_POD}" --wait=true --timeout=120s
kubectl -n shiftpv-system rollout restart deployment/shiftpv-controller
kubectl -n shiftpv-system rollout status daemonset/shiftpv-node --timeout=180s
kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=180s
NEW_POD=$(node_pod)
NEW_UID=$(kubectl -n shiftpv-system get "pod/${NEW_POD}" -o jsonpath='{.metadata.uid}')
test "${NEW_UID}" != "${NODE_UID}"
kubectl -n shiftpv-system wait "pod/${NEW_POD}" \
	--for='jsonpath={.metadata.annotations.shiftpv\.io/node-move-cleanup}'=v1 --timeout=120s
kubectl wait "shiftpvmove/${MOVE}" \
	--for="jsonpath={.status.cleanup.status.executor.podUID}=${NEW_UID}" --timeout=180s
assert_pending_receipt
test "$(docker exec "${SOURCE}" cat "${RECEIPT_PATH}")" = "${LOCAL_RECEIPT}"
kubectl delete validatingadmissionpolicybinding "${POLICY}"
kubectl delete validatingadmissionpolicy "${POLICY}"
kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.phase}'=Succeeded --timeout=180s
assert_cleanup_journal "shiftpvmove/${MOVE}" MoveSource "${VOLUME}" "${SOURCE_COPY}" ShiftPVMove
assert_no_cleanup_job
test "$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.metadata.uid}')" = "${MOVE_UID}"
test "$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.activeMove}')" = ""
test "$(kubectl -n "${NAMESPACE}" get pvc/wffc -o jsonpath='{.metadata.uid}')" = "${PVC_UID}"
test "$(kubectl get "pv/${PV}" -o jsonpath='{.metadata.uid}')" = "${PV_UID}"
test "$(kubectl get "pv/${PV}" -o jsonpath='{.spec.csi.volumeHandle}')" = "${VOLUME}"
test "$(pod_sha256 "${NAMESPACE}" deployment/wffc /data/payload)" = "${CHECKSUM}"
test "$(docker exec "${SOURCE}" cat "${RECEIPT_PATH}")" = "${LOCAL_RECEIPT}"
kubectl uncordon "${SOURCE}"
kubectl delete namespace "${NAMESPACE}" --wait=true --timeout=180s
kubectl wait "pv/${PV}" --for=delete --timeout=180s
kubectl wait "shiftpvvolume/${VOLUME}" --for=delete --timeout=180s
echo "native Move cleanup passed: move=${MOVE} operation=${OPERATION} executor=${NODE_UID}->${NEW_UID} checksum=${CHECKSUM}; receipt replay, postcommit drain, controller restart, later absence, no cleanup Job"
