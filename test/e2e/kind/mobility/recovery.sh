#!/usr/bin/env bash
# Sourced by run.sh: isolated kubeconfig and owned temporary pool directories only.

request_recovery() {
	local move=$1
	if kubectl patch "shiftpvmove/${move}" --type merge -p '{"spec":{"recovery":"ForceSource"}}' >"${WORK_DIR}/invalid-recovery.txt" 2>&1; then
		echo 'invalid recovery request was accepted' >&2
		return 1
	fi
	grep -q 'Unsupported value' "${WORK_DIR}/invalid-recovery.txt"
	kubectl patch "shiftpvmove/${move}" --type merge -p '{"spec":{"recovery":"ResumeOwner"}}'
	kubectl patch "shiftpvmove/${move}" --type merge -p '{"spec":{"recovery":"ResumeOwner"}}'
	if kubectl patch "shiftpvmove/${move}" --type merge -p '{"spec":{"recovery":null}}' >"${WORK_DIR}/removed-recovery.txt" 2>&1; then
		echo 'one-way recovery request was removed' >&2
		return 1
	fi
	grep -q 'recovery cannot be removed' "${WORK_DIR}/removed-recovery.txt"
}

restart_recovery_controller() {
	local move=$1 pod
	kubectl wait "shiftpvmove/${move}" --for=jsonpath='{.status.recoveryPhase}'=Verifying --timeout=300s
	pod=$(kubectl -n shiftpv-system get pod -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.name}')
	# Observe process termination; do not create overlapping controllers.
	kubectl -n shiftpv-system delete "pod/${pod}" --wait=true --timeout=120s
	kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=180s
}

restart_during_recovery() {
	local move=$1
	restart_recovery_controller "${move}"
	kubectl wait "shiftpvmove/${move}" --for=jsonpath='{.status.recoveryPhase}'=Recovered --timeout=300s
	kubectl get "shiftpvmove/${move}" -o jsonpath='{.status.message}' | grep -Fq 'no operator action is required'
	local recovery_event="" deadline=$((SECONDS + 60))
	while ((SECONDS < deadline)); do
		recovery_event=$(kubectl -n default get events \
			--field-selector "involvedObject.kind=ShiftPVMove,involvedObject.name=${move}" \
			-o jsonpath='{range .items[*]}{.reason}{"\n"}{end}' 2>/dev/null || true)
		grep -Fxq RecoveryRecovered <<<"${recovery_event}" && return
		sleep 1
	done
	echo "missing ShiftPVMove RecoveryRecovered event; observed: ${recovery_event}" >&2
	return 1
}

recover_source_only() {
	local source_claim_uid checksum pod source_mount rollback_generation destination_pool observed_generation
	source_claim_uid=$(kubectl -n shiftpv-mobility-blocked get pvc/source-only -o jsonpath='{.metadata.uid}')
	source_mount=$(pool_mount_for_node "${BLOCKED_SOURCE_NODE}")
	checksum=$(node_sha256 "${BLOCKED_SOURCE_NODE}" "${source_mount}/volumes/${BLOCKED_VOLUME}/payload")
	request_recovery "${BLOCKED_MOVE}"
	restart_during_recovery "${BLOCKED_MOVE}"
	rollback_generation=$(kubectl get "shiftpvmove/${BLOCKED_MOVE}" -o jsonpath='{.status.rollbackRequiredGeneration}')
	destination_pool=$(kubectl get "shiftpvmove/${BLOCKED_MOVE}" -o jsonpath='{.status.incomingCopy.poolName}')
	test -n "${rollback_generation}"
	test "${rollback_generation}" -gt 0
	observed_generation=$(kubectl get "shiftpvpool/${destination_pool}" -o jsonpath='{.status.observedGeneration}')
	test "${observed_generation}" -ge "${rollback_generation}"
	test "$(kubectl get "shiftpvmove/${BLOCKED_MOVE}" -o jsonpath='{.status.capacityApproved}')" = false
	kubectl -n shiftpv-mobility-blocked rollout status deployment/source-only --timeout=180s
	pod=$(kubectl -n shiftpv-mobility-blocked get pod -l app=shiftpv-mobility-source-only -o jsonpath='{.items[0].metadata.name}')
	test "$(pod_sha256 shiftpv-mobility-blocked "${pod}" /data/payload)" = "${checksum}"
	test "$(kubectl -n shiftpv-mobility-blocked get pvc/source-only -o jsonpath='{.metadata.uid}')" = "${source_claim_uid}"
	test "$(kubectl get "shiftpvvolume/${BLOCKED_VOLUME}" -o jsonpath='{.status.ownerNode}')" = "${BLOCKED_SOURCE_NODE}"
	test "$(kubectl get "shiftpvvolume/${BLOCKED_VOLUME}" -o jsonpath='{.status.activeMove}')" = ""
	test "$(kubectl get "shiftpvmove/${BLOCKED_MOVE}" -o jsonpath='{.status.phase}')" = "Blocked"
	echo 'source recovery passed: original PVC and payload, same owner, restart and duplicate request'
	kubectl delete namespace shiftpv-mobility-blocked --wait=true --timeout=180s
}

recover_after_commit_failure() {
	local return_move current_pod latest_checksum failed_job destination_mount source_copy
	# The current owner becomes the return Move source. The test helper fails the
	# approved cleanup before filesystem mutation without invalidating inventory.
	source_copy=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.currentCopy.copyID}')
	test -n "${source_copy}"
	kubectl uncordon "${SOURCE_NODE}"
	kubectl cordon "${DESTINATION_NODE}"
	return_move=""
	for _ in {1..120}; do
		return_move=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.activeMove}')
		[[ -n "${return_move}" ]] && break
		sleep 1
	done
	test -n "${return_move}"
	destination_mount=$(pool_mount_for_node "${DESTINATION_NODE}")
	local fault_path="${destination_mount}/.shiftpv-e2e-fail-cleanup"
	docker exec "${DESTINATION_NODE}" test ! -e "${fault_path}"
	docker exec "${DESTINATION_NODE}" touch -- "${fault_path}"
	kubectl wait "shiftpvmove/${return_move}" --for=jsonpath='{.status.phase}'=Blocked --timeout=480s
	test "$(kubectl get "shiftpvmove/${return_move}" -o jsonpath='{.spec.sourceNode}')" = "${DESTINATION_NODE}"
	test "$(kubectl get "shiftpvmove/${return_move}" -o jsonpath='{.status.reason}')" = "CleanupFailed"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${SOURCE_NODE}"
	test "$(kubectl get "shiftpvmove/${return_move}" -o jsonpath='{.status.cleanup.status.phase}')" = NeedsReview
	test "$(kubectl get "shiftpvmove/${return_move}" -o jsonpath='{.status.cleanup.spec.authority.name}')" = "${return_move}"
	test "$(kubectl get "shiftpvmove/${return_move}" -o jsonpath='{.status.cleanup.spec.target.copyID}')" = "${source_copy}"
	failed_job=$(cleanup_job_name "shiftpvmove/${return_move}")
	test -n "${failed_job}"
	kubectl -n shiftpv-system logs "job/${failed_job}" >"${WORK_DIR}/cleanup-failure.txt" 2>&1
	grep -Fq 'injected cleanup failure before filesystem mutation' "${WORK_DIR}/cleanup-failure.txt"
	kubectl -n shiftpv-mobility-test rollout status deployment/wffc --timeout=180s
	current_pod=$(kubectl -n shiftpv-mobility-test get pod -l app=shiftpv-mobility-wffc -o jsonpath='{.items[0].metadata.name}')
	# New writes on the committed destination must survive recovery.
	kubectl -n shiftpv-mobility-test exec "${current_pod}" -- sh -ec 'printf "after owner commit\n" >> /data/payload'
	latest_checksum=$(pod_sha256 shiftpv-mobility-test "${current_pod}" /data/payload)
	test "${latest_checksum}" != "${CHECKSUM_BEFORE}"
	docker exec "${DESTINATION_NODE}" test -f "${fault_path}"
	docker exec "${DESTINATION_NODE}" rm -- "${fault_path}"
	request_recovery "${return_move}"
	restart_recovery_controller "${return_move}"
	kubectl wait "shiftpvmove/${return_move}" --for=jsonpath='{.status.recoveryPhase}'=Retiring --timeout=300s
	kubectl wait "shiftpvmove/${return_move}" --for=jsonpath='{.status.recoveryReason}'=CleanupNeedsReview --timeout=300s
	test "$(kubectl get "shiftpvmove/${return_move}" -o jsonpath='{.status.phase}')" = Blocked
	test "$(pod_sha256 shiftpv-mobility-test "${current_pod}" /data/payload)" = "${latest_checksum}"
	test "$(kubectl -n shiftpv-mobility-test get pvc/wffc -o jsonpath='{.metadata.uid}')" = "${PVC_UID}"
	test "$(kubectl -n shiftpv-mobility-test get pvc/wffc -o jsonpath='{.spec.volumeName}')" = "${PV_NAME}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${SOURCE_NODE}"
	# The committed owner remains available, but NeedsReview is not cleanup
	# settlement. Preserve the non-owner copy, Move lock, and source capacity hold.
	docker exec "${DESTINATION_NODE}" test -f "${destination_mount}/volumes/${VOLUME_ID}/payload"
	test "$(node_sha256 "${DESTINATION_NODE}" "${destination_mount}/volumes/${VOLUME_ID}/payload")" = "${CHECKSUM_BEFORE}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.activeMove}')" = "${return_move}"
	test "$(kubectl get "shiftpvmove/${return_move}" -o jsonpath='{.status.capacityApproved}')" = true
	test "$(kubectl get "shiftpvmove/${return_move}" -o jsonpath='{.status.capacityReason}')" != RecoverySettled
	kubectl uncordon "${DESTINATION_NODE}"
	echo 'post-commit cleanup review passed: current owner writes preserved; non-owner copy, Move lock, and capacity hold retained'
	verify_cleanup_lifecycle "${return_move}" "${DESTINATION_NODE}" "${destination_mount}" "${latest_checksum}" "${current_pod}"
}
