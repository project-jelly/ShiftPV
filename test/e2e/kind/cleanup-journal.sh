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
		test "${expected_kind}" = ShiftPVVolume
		test "${expected_reason}" = VolumeDelete
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
