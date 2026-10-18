#!/usr/bin/env bash
set -euo pipefail

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
EVIDENCE="${ROOT_DIR}/.tmp/workload-retirement"
mkdir -p "${EVIDENCE}"

diagnostics() {
	local result=$?
	if [[ "$(kubectl config current-context)" != "kind-${CLUSTER_NAME}" ]]; then return 1; fi
	kubectl get shiftpvvolumes,shiftpvmoves,shiftpvpools -o json >"${EVIDENCE}/last-storage.json" 2>/dev/null || true
	kubectl -n shiftpv-system logs deployment/shiftpv-controller -c shiftpv-controller >"${EVIDENCE}/controller.log" 2>&1 || true
	# Preserve failed journals and fixture resources for the suite diagnostics.
	# The caller owns teardown of this disposable cluster.
	return "${result}"
}
trap diagnostics EXIT

install_barriers() {
	local expression
	if [[ "${BOUNDARY}" == precommit ]]; then
		expression='object.status.ownerNode == oldObject.status.ownerNode'
	else
		expression="object.status.ownerNode == '${SOURCE}' || !has(object.status.publishedNodes) || size(object.status.publishedNodes) == 0"
	fi
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
      resources: [shiftpvvolumes/status, shiftpvmoves/status, shiftpvpools/status]
  validations:
  - expression: "request.resource.resource != 'shiftpvpools' || object.metadata.name != 'worker-a' || !has(request.dryRun) || !request.dryRun"
    message: retirement barrier activation
  - expression: "request.resource.resource != 'shiftpvvolumes' || object.metadata.name != '${VOLUME}' || object.metadata.uid != '${VOLUME_UID}' || (${expression})"
    message: retirement ${BOUNDARY} boundary held
  - expression: "request.resource.resource != 'shiftpvmoves' || object.spec.volumeID != '${VOLUME}' || !has(object.status.cleanup) || !has(object.status.cleanup.status.receipt)"
    message: retirement purge receipt held
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ${POLICY}
spec:
  policyName: ${POLICY}
  validationActions: [Deny]
EOF
	local active=0 object denial
	for _ in {1..60}; do
		object=$(kubectl get shiftpvpool/worker-a -o json)
		if ! denial=$(kubectl replace --subresource=status --dry-run=server --request-timeout=10s -f - <<<"${object}" 2>&1); then
			if grep -Fq 'retirement barrier activation' <<<"${denial}"; then active=1; break; fi
		fi
		sleep 1
	done
	test "${active}" = 1
}

wait_for_boundary() {
	local reached=0 object move destination destination_mount
	MOVE=$(wait_for_move "${VOLUME}" 180)
	for _ in {1..180}; do
		object=$(kubectl get "shiftpvvolume/${VOLUME}" -o json)
		move=$(kubectl get "shiftpvmove/${MOVE}" -o json)
		jq -e --arg uid "${VOLUME_UID}" '.metadata.uid == $uid' <<<"${object}" >/dev/null
		if ! jq -e '.status.capacityApproved and .status.destinationCopy != null and .spec.recovery == null' <<<"${move}" >/dev/null; then sleep 1; continue; fi
		destination=$(jq -er '.status.destinationNode' <<<"${move}")
		destination_mount=$(pool_mount_for_node "${destination}")
		if [[ "${BOUNDARY}" == precommit ]]; then
			jq -e --arg node "${SOURCE}" '.status.ownerNode == $node' <<<"${object}" >/dev/null
		else
			if ! jq -e --arg node "${destination}" --argjson copy "$(jq -c '.status.destinationCopy' <<<"${move}")" \
				'.status.ownerNode == $node and .status.currentCopy == $copy and (.status.publishedNodes // [] | length) == 0' <<<"${object}" >/dev/null; then sleep 1; continue; fi
		fi
		if node_path_exists "${destination}" "${destination_mount}/volumes/${VOLUME}/payload" &&
			[[ "$(node_sha256 "${destination}" "${destination_mount}/volumes/${VOLUME}/payload")" == "${CHECKSUM}" ]]; then
			reached=1
			break
		fi
		sleep 1
	done
	test "${reached}" = 1
	printf '%s\n' "${move}" >"${CASE_DIR}/move-before.json"
	printf '%s\n' "${object}" >"${CASE_DIR}/volume-before.json"
	MOVE_UID=$(jq -er '.metadata.uid' <<<"${move}")
	RETAINED=$(jq -c '.status.currentCopy' <<<"${object}")
	OWNER=$(jq -er '.status.ownerNode' <<<"${object}")
	if [[ "${BOUNDARY}" == precommit ]]; then
		TARGET=$(jq -c '.status.destinationCopy' <<<"${move}")
		REASON=MoveRollback
	else
		TARGET=$(jq -c '.status.sourceCopy' <<<"${move}")
		REASON=MoveSource
	fi
	TARGET_NODE=$(jq -er '.nodeName' <<<"${TARGET}")
	TARGET_MOUNT=$(pool_mount_for_node "${TARGET_NODE}")
	TARGET_COPY=$(jq -er '.copyID' <<<"${TARGET}")
	# Both real copies exist at the boundary, before any retired-workload proof.
	test "$(node_sha256 "${SOURCE}" "$(pool_mount_for_node "${SOURCE}")/volumes/${VOLUME}/payload")" = "${CHECKSUM}"
}

assert_retained_owner() {
	local object
	object=$(kubectl get "shiftpvvolume/${VOLUME}" -o json)
	jq -e --arg uid "${VOLUME_UID}" --arg owner "${OWNER}" --arg move "${MOVE}" --argjson copy "${RETAINED}" '
	 .metadata.uid == $uid and .status.ownerNode == $owner and .status.currentCopy == $copy and
	 .status.activeMove == $move and .status.phase == "Blocked" and
	 (.status.publishedNodes // [] | length) == 0 and .spec.capacityBytes == 16777216' <<<"${object}" >/dev/null
	test "$(node_sha256 "${OWNER}" "$(pool_mount_for_node "${OWNER}")/volumes/${VOLUME}/payload")" = "${CHECKSUM}"
}

assert_receipt_held() {
	local object
	object=$(kubectl get "shiftpvmove/${MOVE}" -o json)
	jq -e --arg uid "${MOVE_UID}" --arg owner "${OWNER}" --argjson target "${TARGET}" '
	 .metadata.uid == $uid and .spec.recovery == null and .status.settlementRequested and
	 .status.recoveryOwner == $owner and .status.capacityApproved and (.metadata.finalizers | length) > 0 and
	 .status.cleanup.spec.target == $target and .status.cleanup.status.phase == "Running" and
	 .status.cleanup.status.executor.kind == "Node" and .status.cleanup.status.receipt == null' <<<"${object}" >/dev/null
	assert_retained_owner
	assert_no_cleanup_job "${MOVE}"
}

settle_retired_workload() {
	kubectl -n "${NS}" delete deployment/wffc --cascade=foreground --wait=true --timeout=120s
	kubectl -n "${NS}" delete pvc/wffc --wait=true --timeout=120s
	kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.cleanup.status.phase}'=Running --timeout=300s
	local operation receipt_path receipt complete=0 object
	operation=$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.status.cleanup.spec.operationID}')
	receipt_path="${TARGET_MOUNT}/.shiftpv/receipt-$(printf '%s' "${operation}" | shasum -a 256 | cut -c1-32).json"
	for _ in {1..120}; do
		if node_path_exists "${TARGET_NODE}" "${receipt_path}"; then complete=1; break; fi
		sleep 1
	done
	test "${complete}" = 1
	receipt=$(docker exec "${TARGET_NODE}" cat "${receipt_path}")
	jq -e --arg operation "${operation}" --argjson target "${TARGET}" \
		'.version == 1 and .value.operationID == $operation and .value.target == $target and .value.retired and .value.purged' <<<"${receipt}" >/dev/null
	printf '%s\n' "${receipt}" >"${CASE_DIR}/local-receipt.json"
	assert_node_absent "${TARGET_NODE}" "${TARGET_MOUNT}/volumes/${VOLUME}"
	assert_node_absent "${TARGET_NODE}" "${TARGET_MOUNT}/.shiftpv/retired/${TARGET_COPY}"
	assert_receipt_held
	kubectl get "shiftpvmove/${MOVE}" -o json >"${CASE_DIR}/receipt-held.json"

	# Restart after the actual purge, while the API receipt still cannot persist.
	kubectl -n shiftpv-system rollout restart deployment/shiftpv-controller
	kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=180s
	assert_receipt_held
	kubectl patch "validatingadmissionpolicy/${POLICY}" --type=json \
		-p '[{"op":"replace","path":"/spec/validations/2/expression","value":"true"}]'
	# The owner-transfer/publication barrier remains active through settlement.
	kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.recoveryPhase}'=Recovered --timeout=300s
	complete=0
	for _ in {1..120}; do
		object=$(kubectl get "shiftpvmove/${MOVE}" -o json)
		if jq -e '(.metadata.finalizers // [] | length) == 0' <<<"${object}" >/dev/null; then complete=1; break; fi
		sleep 1
	done
	test "${complete}" = 1
	jq -e --arg uid "${MOVE_UID}" --arg owner "${OWNER}" --argjson target "${TARGET}" '
	 .metadata.uid == $uid and .spec.recovery == null and .status.settlementRequested and
	 .status.recoveryOwner == $owner and .status.capacityApproved == false and
	 .status.capacityReason == "RecoverySettled" and .status.rollbackRequiredGeneration > 0 and
	 .status.cleanup.spec.target == $target and .status.preparation == null' <<<"${object}" >/dev/null
	printf '%s\n' "${object}" >"${CASE_DIR}/settled-move.json"
	assert_cleanup_journal "shiftpvmove/${MOVE}" "${REASON}" "${VOLUME}" "${TARGET_COPY}" ShiftPVMove
	assert_no_cleanup_job "${MOVE}"
	kubectl -n shiftpv-system get jobs,pods,services,configmaps,secrets -o json | jq -e --arg uid "${MOVE_UID}" \
		'all(.items[]; all(.metadata.ownerReferences[]?; .uid != $uid))' >/dev/null
}

verify_policy_and_cleanup() {
	local object
	if [[ "${RECLAIM}" == Retain ]]; then
		object=$(kubectl get "shiftpvvolume/${VOLUME}" -o json)
		jq -e --arg uid "${VOLUME_UID}" --arg owner "${OWNER}" --argjson copy "${RETAINED}" '
		 .metadata.uid == $uid and .status.ownerNode == $owner and .status.currentCopy == $copy and
		 .status.phase == "Ready" and (.status.activeMove // "") == "" and
		 (.status.publishedNodes // [] | length) == 0 and .spec.capacityBytes == 16777216' <<<"${object}" >/dev/null
		printf '%s\n' "${object}" >"${CASE_DIR}/retained-volume.json"
		test "$(node_sha256 "${OWNER}" "$(pool_mount_for_node "${OWNER}")/volumes/${VOLUME}/payload")" = "${CHECKSUM}"
		kubectl get "pv/${PV}" -o json | jq -e --arg uid "${PV_UID}" --arg claim "${PVC_UID}" \
			'.metadata.uid == $uid and .spec.claimRef.uid == $claim and .spec.persistentVolumeReclaimPolicy == "Retain"' >/dev/null
		# After proving Retain, request ordinary CSI cleanup of this owned fixture.
		kubectl patch "pv/${PV}" --type=merge -p '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
	fi
	kubectl wait "shiftpvvolume/${VOLUME}" --for=delete --timeout=300s
	kubectl wait "pv/${PV}" --for=delete --timeout=120s
	assert_node_absent "${OWNER}" "$(pool_mount_for_node "${OWNER}")/volumes/${VOLUME}"
	assert_node_absent "${OWNER}" "$(pool_mount_for_node "${OWNER}")/.shiftpv/retired/$(jq -er '.copyID' <<<"${RETAINED}")"
	local incoming destination destination_mount
	incoming=$(jq -er '.status.incomingCopy.copyID' "${CASE_DIR}/move-before.json")
	destination=$(jq -er '.status.destinationNode' "${CASE_DIR}/move-before.json")
	destination_mount=$(pool_mount_for_node "${destination}")
	assert_node_absent "${destination}" "${destination_mount}/.shiftpv/incoming/${incoming}"
	kubectl delete "validatingadmissionpolicybinding/${POLICY}" "validatingadmissionpolicy/${POLICY}"
	kubectl delete "namespace/${NS}" --wait=true --timeout=120s
	kubectl uncordon "${SOURCE}"
	jq -n --arg boundary "${BOUNDARY}" --arg policy "${RECLAIM}" --arg volume "${VOLUME_UID}" --arg move "${MOVE_UID}" \
		'{result:"PASS",boundary:$boundary,reclaimPolicy:$policy,volumeUID:$volume,moveUID:$move,nativeReceiptReplay:true,manualRecoveryRequest:false}' >"${CASE_DIR}/result.json"
	cat "${CASE_DIR}/result.json"
}

for BOUNDARY in precommit postcommit; do
	for RECLAIM in Delete Retain; do
		policy_name=delete
		if [[ "${RECLAIM}" == Retain ]]; then policy_name=retain; fi
		NS="shiftpv-retirement-${BOUNDARY}-${policy_name}"
		POLICY="${NS}"
		CASE_DIR="${EVIDENCE}/${BOUNDARY}-${RECLAIM}"
		mkdir -p "${CASE_DIR}"
		for resource in namespace validatingadmissionpolicy validatingadmissionpolicybinding; do
			test -z "$(kubectl get "${resource}/${NS}" --ignore-not-found -o name)"
		done
		sed "s/shiftpv-mobility-test/${NS}/g" "${ROOT_DIR}/test/e2e/kind/mobility/manifests/wffc-workload.yaml" | kubectl apply -f -
		kubectl -n "${NS}" rollout status deployment/wffc --timeout=180s
		PVC_UID=$(kubectl -n "${NS}" get pvc/wffc -o jsonpath='{.metadata.uid}')
		PV=$(kubectl -n "${NS}" get pvc/wffc -o jsonpath='{.spec.volumeName}')
		PV_UID=$(kubectl get "pv/${PV}" -o jsonpath='{.metadata.uid}')
		VOLUME=$(kubectl get "pv/${PV}" -o jsonpath='{.spec.csi.volumeHandle}')
		VOLUME_UID=$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.metadata.uid}')
		SOURCE=$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.ownerNode}')
		# shellcheck disable=SC2016
		kubectl -n "${NS}" exec deployment/wffc -- sh -ec 'printf "%s\n" "$1" > /data/payload; sync' sh "retirement-${VOLUME_UID}"
		CHECKSUM=$(pod_sha256 "${NS}" deployment/wffc /data/payload)
		kubectl patch "pv/${PV}" --type=merge -p "{\"spec\":{\"persistentVolumeReclaimPolicy\":\"${RECLAIM}\"}}"
		install_barriers
		kubectl cordon "${SOURCE}"
		wait_for_boundary
		settle_retired_workload
		verify_policy_and_cleanup
	done
done
