#!/usr/bin/env bash
set -euo pipefail
set -o errtrace
trap 'printf "copy deadline failed at line %s: %s\n" "${LINENO}" "${BASH_COMMAND}" >&2' ERR

: "${CLUSTER_NAME:?owned Kind cluster required}"
: "${KUBECONFIG:?private kubeconfig required}"
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
# shellcheck source=test/e2e/kind/lib/wait.sh
source "${ROOT_DIR}/test/e2e/kind/lib/wait.sh"
# shellcheck source=test/e2e/kind/cleanup-journal.sh
source "${ROOT_DIR}/test/e2e/kind/cleanup-journal.sh"
EVIDENCE="${ROOT_DIR}/.tmp/copy-deadline"
mkdir -p "${EVIDENCE}"
NS=shiftpv-copy-deadline
SOURCE="${CLUSTER_NAME}-worker"
DESTINATION="${CLUSTER_NAME}-worker2"
SOURCE_MOUNT=$(pool_mount_for_node "${SOURCE}")
DESTINATION_MOUNT=$(pool_mount_for_node "${DESTINATION}")
SENTINEL="${DESTINATION_MOUNT}/.shiftpv-e2e-delay-copy-completion"

diagnostics() {
	local result=$?
	if [[ "$(kubectl config current-context)" != "kind-${CLUSTER_NAME}" ]]; then return 1; fi
	kubectl get shiftpvvolumes,shiftpvmoves,shiftpvpools -o json >"${EVIDENCE}/last-storage.json" 2>/dev/null || true
	kubectl -n shiftpv-system get jobs,pods -o json >"${EVIDENCE}/last-executors.json" 2>/dev/null || true
	kubectl -n shiftpv-system logs deployment/shiftpv-controller -c shiftpv-controller >"${EVIDENCE}/controller.log" 2>&1 || true
	# The caller captures diagnostics before tearing down this owned cluster.
	return "${result}"
}
trap diagnostics EXIT

# Keep the normal workload and real helper. Delay only the successful helper's
# exit, after it has copied and verified the recorded incoming filesystem copy.
# Do not shorten the Job deadline or fabricate status/receipts.
kubectl cordon "${DESTINATION}"
sed "s/shiftpv-mobility-test/${NS}/g" "${ROOT_DIR}/test/e2e/kind/mobility/manifests/wffc-workload.yaml" | kubectl apply -f -
kubectl -n "${NS}" rollout status deployment/wffc --timeout=180s
PVC_UID=$(kubectl -n "${NS}" get pvc/wffc -o jsonpath='{.metadata.uid}')
PV=$(kubectl -n "${NS}" get pvc/wffc -o jsonpath='{.spec.volumeName}')
kubectl get "pv/${PV}" -o json >"${EVIDENCE}/pv-before.json"
VOLUME=$(jq -er '.spec.csi.volumeHandle' "${EVIDENCE}/pv-before.json")
kubectl get "shiftpvvolume/${VOLUME}" -o json >"${EVIDENCE}/volume-before.json"
VOLUME_UID=$(jq -er '.metadata.uid' "${EVIDENCE}/volume-before.json")
test "$(jq -er '.status.ownerNode' "${EVIDENCE}/volume-before.json")" = "${SOURCE}"
# shellcheck disable=SC2016
kubectl -n "${NS}" exec deployment/wffc -- sh -ec 'printf "%s\n" "$1" > /data/payload; sync' sh "deadline-${PVC_UID}-${VOLUME_UID}"
CHECKSUM=$(node_sha256 "${SOURCE}" "${SOURCE_MOUNT}/volumes/${VOLUME}/payload")
assert_node_absent "${DESTINATION}" "${SENTINEL}"
docker exec "${DESTINATION}" touch -- "${SENTINEL}"
kubectl uncordon "${DESTINATION}"
kubectl cordon "${SOURCE}"
MOVE=$(wait_for_move "${VOLUME}" 120)
kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.phase}'=Copying --timeout=180s
created=0
for _ in {1..120}; do
	kubectl get "shiftpvmove/${MOVE}" -o json >"${EVIDENCE}/move-before.json"
	if COPY_JOB=$(jq -er '.status.copyJobName' "${EVIDENCE}/move-before.json") &&
		kubectl -n shiftpv-system get "job/${COPY_JOB}" -o json >"${EVIDENCE}/job-before.json"; then
		created=1
		break
	fi
	sleep 1
done
test "${created}" = 1
MOVE_UID=$(jq -er '.metadata.uid' "${EVIDENCE}/move-before.json")
INCOMING=$(jq -er '.status.incomingCopy.copyID' "${EVIDENCE}/move-before.json")
INCOMING_PATH="${DESTINATION_MOUNT}/.shiftpv/incoming/${INCOMING}"
test "$(jq -er '.spec.activeDeadlineSeconds' "${EVIDENCE}/job-before.json")" = 300
jq -e --arg uid "${MOVE_UID}" 'any(.metadata.ownerReferences[]; .uid == $uid and .kind == "ShiftPVMove")' "${EVIDENCE}/job-before.json" >/dev/null

copied=0
for _ in {1..90}; do
	if kubectl -n shiftpv-system logs "job/${COPY_JOB}" >"${EVIDENCE}/copy.log" 2>&1 &&
		grep -Fq 'injected completion delay after real verified copy' "${EVIDENCE}/copy.log"; then
		copied=1
		break
	fi
	sleep 1
done
test "${copied}" = 1
test "$(node_sha256 "${DESTINATION}" "${INCOMING_PATH}/payload")" = "${CHECKSUM}"
assert_node_absent "${DESTINATION}" "${DESTINATION_MOUNT}/volumes/${VOLUME}"

# Kubernetes must produce the real failure. The copied bytes are not permission
# to promote or commit when completion evidence was never observed.
kubectl -n shiftpv-system wait "job/${COPY_JOB}" --for=condition=Failed --timeout=360s
kubectl -n shiftpv-system get "job/${COPY_JOB}" -o json >"${EVIDENCE}/job-expired.json"
jq -e '.spec.activeDeadlineSeconds == 300 and any(.status.conditions[]; .type == "Failed" and .status == "True" and .reason == "DeadlineExceeded")' "${EVIDENCE}/job-expired.json" >/dev/null
kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.phase}'=Blocked --timeout=180s
kubectl get "shiftpvmove/${MOVE}" -o json >"${EVIDENCE}/move-blocked.json"
jq -e --arg uid "${MOVE_UID}" '.metadata.uid == $uid and .status.reason == "CopyFailed" and .status.capacityApproved and (.metadata.finalizers | length) > 0' "${EVIDENCE}/move-blocked.json" >/dev/null
kubectl get "shiftpvvolume/${VOLUME}" -o json >"${EVIDENCE}/volume-blocked.json"
jq -e --slurpfile before "${EVIDENCE}/volume-before.json" --arg move "${MOVE}" '
 .metadata.uid == $before[0].metadata.uid and .spec == $before[0].spec and
 .status.ownerNode == $before[0].status.ownerNode and .status.currentCopy == $before[0].status.currentCopy and
 .status.phase == "Blocked" and .status.activeMove == $move and (.status.publishedNodes // [] | length) == 0' "${EVIDENCE}/volume-blocked.json" >/dev/null
test "$(node_sha256 "${SOURCE}" "${SOURCE_MOUNT}/volumes/${VOLUME}/payload")" = "${CHECKSUM}"
test "$(node_sha256 "${DESTINATION}" "${INCOMING_PATH}/payload")" = "${CHECKSUM}"
assert_node_absent "${DESTINATION}" "${DESTINATION_MOUNT}/volumes/${VOLUME}"

docker exec "${DESTINATION}" rm -- "${SENTINEL}"
kubectl uncordon "${SOURCE}"
kubectl patch "shiftpvmove/${MOVE}" --type merge -p '{"spec":{"recovery":"ResumeOwner"}}'
kubectl patch "shiftpvmove/${MOVE}" --type merge -p '{"spec":{"recovery":"ResumeOwner"}}'
kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.recoveryPhase}'=Recovered --timeout=300s
settled=0
for _ in {1..120}; do
	kubectl get "shiftpvmove/${MOVE}" -o json >"${EVIDENCE}/move-settled.json"
	if jq -e '(.metadata.finalizers // [] | length) == 0' "${EVIDENCE}/move-settled.json" >/dev/null; then settled=1; break; fi
	sleep 1
done
test "${settled}" = 1
jq -e --arg uid "${MOVE_UID}" '
 .metadata.uid == $uid and .status.phase == "Blocked" and .status.recoveryPhase == "Recovered" and
 .status.capacityApproved == false and .status.capacityReason == "RecoverySettled" and .status.preparation == null' "${EVIDENCE}/move-settled.json" >/dev/null
assert_cleanup_journal "shiftpvmove/${MOVE}" MoveRollback "${VOLUME}" "${INCOMING}" ShiftPVMove
assert_no_cleanup_job "${MOVE}"
assert_node_absent "${DESTINATION}" "${INCOMING_PATH}"
assert_node_absent "${DESTINATION}" "${DESTINATION_MOUNT}/.shiftpv/retired/${INCOMING}"
assert_node_absent "${DESTINATION}" "${DESTINATION_MOUNT}/volumes/${VOLUME}"
kubectl -n shiftpv-system get jobs,pods,services,configmaps,secrets -o json | jq -e --arg uid "${MOVE_UID}" \
	'all(.items[]; all(.metadata.ownerReferences[]?; .uid != $uid))' >/dev/null

kubectl -n "${NS}" rollout status deployment/wffc --timeout=180s
POD=$(kubectl -n "${NS}" get pod -l app=shiftpv-mobility-wffc -o jsonpath='{.items[0].metadata.name}')
test "$(pod_sha256 "${NS}" "${POD}" /data/payload)" = "${CHECKSUM}"
test "$(kubectl -n "${NS}" get pvc/wffc -o jsonpath='{.metadata.uid}')" = "${PVC_UID}"
test "$(kubectl -n "${NS}" get pvc/wffc -o jsonpath='{.spec.volumeName}')" = "${PV}"
kubectl get "pv/${PV}" -o json | jq -e --slurpfile before "${EVIDENCE}/pv-before.json" '.metadata.uid == $before[0].metadata.uid and .spec == $before[0].spec' >/dev/null
kubectl get "shiftpvvolume/${VOLUME}" -o json >"${EVIDENCE}/volume-recovered.json"
jq -e --slurpfile before "${EVIDENCE}/volume-before.json" --arg source "${SOURCE}" '
 .metadata.uid == $before[0].metadata.uid and .spec == $before[0].spec and .status.currentCopy == $before[0].status.currentCopy and
 .status.ownerNode == $source and .status.phase == "Ready" and (.status.activeMove // "") == "" and
 .status.publishedNodes == [$source]' "${EVIDENCE}/volume-recovered.json" >/dev/null

# The unchanged original claim must still reclaim through the ordinary CSI path.
kubectl delete "namespace/${NS}" --wait=true --timeout=180s
kubectl wait "shiftpvvolume/${VOLUME}" --for=delete --timeout=300s
kubectl wait "pv/${PV}" --for=delete --timeout=120s
assert_node_absent "${SOURCE}" "${SOURCE_MOUNT}/volumes/${VOLUME}"
kubectl get shiftpvvolumes -o json | jq -e '.items | length == 0' >/dev/null
kubectl uncordon "${SOURCE}" "${DESTINATION}"
jq -n --arg move "${MOVE_UID}" --arg volume "${VOLUME_UID}" --arg claim "${PVC_UID}" --arg checksum "${CHECKSUM}" \
	'{result:"PASS",deadlineSeconds:300,moveUID:$move,volumeUID:$volume,claimUID:$claim,checksum:$checksum,actualDeadlineExceeded:true,verifiedIncomingCopy:true,originalOwnerRecovered:true,nativeRollbackSettled:true,ordinaryCSIReclaim:true}' >"${EVIDENCE}/result.json"
cat "${EVIDENCE}/result.json"
