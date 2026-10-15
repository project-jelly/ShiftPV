#!/usr/bin/env bash
# Shared assertions for the cleanup journal embedded in a ShiftPVVolume or
# ShiftPVMove parent. This file is sourced by the Kind suites.

wait_for_cleanup_phase() {
	local resource=$1 phase=$2 timeout=${3:-5m}
	kubectl wait "${resource}" \
		--for="jsonpath={.status.cleanup.status.phase}=${phase}" \
		--timeout="${timeout}"
}

cleanup_job_name() {
	local resource=$1
	kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.executor.jobName}'
}

# Select the legacy Job backend for one test Move without disabling Volume
# effects. Return the original Pod plus scope; restore only after Job binding.
suspend_move_cleanup_capability() {
	local node=$1 pod patch context
	: "${CLUSTER_NAME:?isolated Kind cluster name required}"
	: "${KUBECONFIG:?isolated kubeconfig required}"
	context=$(kubectl config current-context) || return
	test "${context}" = "kind-${CLUSTER_NAME}" || return
	[[ "${node}" == "${CLUSTER_NAME}-"* ]] || return 1
	pod=$(kubectl -n shiftpv-system get pods -l app.kubernetes.io/component=node \
		--field-selector "spec.nodeName=${node}" -o json) || return
	pod=$(jq -ce --arg node "${node}" --arg context "${context}" --arg kubeconfig "${KUBECONFIG}" '
		select(.items | length == 1) | .items[0] |
		select(.spec.nodeName == $node and .metadata.deletionTimestamp == null and
			.metadata.uid != null and .metadata.resourceVersion != null and
			.metadata.annotations["shiftpv.io/node-move-cleanup"] == "v1" and
			any(.metadata.ownerReferences[]?; .kind == "DaemonSet" and .name == "shiftpv-node" and .controller == true)) |
		. + {_testContext: $context, _testKubeconfig: $kubeconfig}' <<<"${pod}") || return
	patch=$(jq -c '[
		{op:"test", path:"/metadata/uid", value:.metadata.uid},
		{op:"test", path:"/metadata/resourceVersion", value:.metadata.resourceVersion},
		{op:"test", path:"/metadata/annotations/shiftpv.io~1node-move-cleanup", value:"v1"},
		{op:"remove", path:"/metadata/annotations/shiftpv.io~1node-move-cleanup"}]' <<<"${pod}") || return
	kubectl -n shiftpv-system patch "pod/$(jq -r '.metadata.name' <<<"${pod}")" \
		--type=json -p "${patch}" >/dev/null || return
	printf '%s\n' "${pod}"
}

restore_move_cleanup_capability() {
	local original=$1 pod name uid node patch context
	: "${CLUSTER_NAME:?isolated Kind cluster name required}"
	: "${KUBECONFIG:?isolated kubeconfig required}"
	context=$(kubectl config current-context) || return
	test "${context}" = "kind-${CLUSTER_NAME}" || return
	jq -e --arg context "${context}" --arg kubeconfig "${KUBECONFIG}" \
		'._testContext == $context and ._testKubeconfig == $kubeconfig and .metadata.namespace == "shiftpv-system"' \
		<<<"${original}" >/dev/null || return
	name=$(jq -er '.metadata.name' <<<"${original}") || return
	uid=$(jq -er '.metadata.uid' <<<"${original}") || return
	node=$(jq -er '.spec.nodeName' <<<"${original}") || return
	pod=$(kubectl -n shiftpv-system get "pod/${name}" -o json) || return
	jq -e --arg uid "${uid}" --arg node "${node}" \
		'.metadata.uid == $uid and .spec.nodeName == $node and .metadata.deletionTimestamp == null' \
		<<<"${pod}" >/dev/null || return
	if jq -e '.metadata.annotations["shiftpv.io/node-move-cleanup"] == "v1"' <<<"${pod}" >/dev/null; then
		return
	fi
	jq -e '.metadata.annotations["shiftpv.io/node-move-cleanup"] == null' <<<"${pod}" >/dev/null || return
	patch=$(jq -c --arg uid "${uid}" '[
		{op:"test", path:"/metadata/uid", value:$uid},
		{op:"test", path:"/metadata/resourceVersion", value:.metadata.resourceVersion},
		{op:"add", path:"/metadata/annotations/shiftpv.io~1node-move-cleanup", value:"v1"}]' <<<"${pod}") || return
	kubectl -n shiftpv-system patch "pod/${name}" --type=json -p "${patch}" >/dev/null || return
}

wait_for_cleanup_job_binding() {
	local resource=$1 deadline=$((SECONDS + 480)) parent
	while ((SECONDS < deadline)); do
		parent=$(kubectl get "${resource}" -o json) || return
		if jq -e '.status.cleanup.status.executor.kind == "Node"' <<<"${parent}" >/dev/null; then
			echo "expected legacy Job cleanup binding in ${resource}, got Node" >&2
			return 1
		fi
		if jq -e '(.status.cleanup.status.executor.jobUID // "") != "" and
			(.status.cleanup.status.executor.jobName // "") != "" and
			(.status.cleanup.status.executor.kind // "") == ""' <<<"${parent}" >/dev/null; then
			return
		fi
		sleep 1
	done
	echo "timed out waiting for legacy Job cleanup binding in ${resource}" >&2
	return 1
}

assert_cleanup_journal() {
	local resource=$1 expected_reason=$2 expected_volume=$3 expected_copy=$4 expected_kind=$5
	local parent_name parent_uid operation_id executor_uid executor_pod_uid receipt_executor_uid
	local required_generation observed_generation

	parent_name=${resource#*/}
	parent_uid=$(kubectl get "${resource}" -o jsonpath='{.metadata.uid}')
	operation_id=$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.spec.operationID}')
	executor_uid=$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.executor.jobUID}')
	executor_pod_uid=$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.executor.podUID}')
	receipt_executor_uid=$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.receipt.executorUID}')
	case "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.executor.kind}')" in
	Node)
		case "${expected_kind}:${expected_reason}" in
		ShiftPVVolume:VolumeDelete | ShiftPVMove:MoveSource | ShiftPVMove:MoveRollback) ;;
		*) echo "unsupported Node cleanup authority/reason: ${expected_kind}/${expected_reason}" >&2; return 1 ;;
		esac
		test -z "$(cleanup_job_name "${resource}")"
		test -z "${executor_uid}"
		test -n "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.executor.namespace}')"
		test -n "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.executor.podName}')"
		executor_uid=${executor_pod_uid}
		;;
	"")
		test -n "$(cleanup_job_name "${resource}")"
		;;
	*)
		echo "unsupported cleanup executor in ${resource}" >&2
		return 1
		;;
	esac

	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.phase}')" = Completed
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.spec.reason}')" = "${expected_reason}"
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.spec.authority.kind}')" = "${expected_kind}"
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.spec.authority.name}')" = "${parent_name}"
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.spec.authority.uid}')" = "${parent_uid}"
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.spec.target.volumeID}')" = "${expected_volume}"
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.spec.target.copyID}')" = "${expected_copy}"
	test -n "${operation_id}"
	test -n "${executor_uid}"
	test -n "${executor_pod_uid}"
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.executor.nodeName}')" = \
		"$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.spec.target.nodeName}')"
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.receipt.operationID}')" = "${operation_id}"
	test "${receipt_executor_uid}" = "${executor_uid}"
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.receipt.retired}')" = true
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.receipt.purged}')" = true
	test -n "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.receipt.localReceiptDigest}')"

	test -n "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.absenceProof.requestID}')"
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.absenceProof.poolName}')" = \
		"$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.spec.target.poolName}')"
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.absenceProof.poolUID}')" = \
		"$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.spec.target.poolUID}')"
	required_generation=$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.absenceProof.requiredGeneration}')
	observed_generation=$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.absenceProof.observedGeneration}')
	[[ "${required_generation}" =~ ^[1-9][0-9]*$ ]]
	[[ "${observed_generation}" =~ ^[1-9][0-9]*$ ]]
	((observed_generation >= required_generation))
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.absenceProof.valid}')" = true
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.absenceProof.complete}')" = true
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.absenceProof.absent}')" = true
	test -n "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.absenceProof.confirmedAt}')"
	test -n "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.settledAt}')"
}
