#!/usr/bin/env bash
# Shared assertions for the cleanup journal embedded in a ShiftPVVolume or
# ShiftPVMove parent. This file is sourced by the Kind suites.

# Pause one Move before a cleanup journal can authorize any Node effect.
# The policy only denies the first journal write; it cannot create evidence.
hold_cleanup_admission() {
	local policy=$1 volume=$2
	test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}" || return
	test -z "$(kubectl get "validatingadmissionpolicy/${policy}" --ignore-not-found -o name)" || return
	test -z "$(kubectl get "validatingadmissionpolicybinding/${policy}" --ignore-not-found -o name)" || return
	kubectl apply -f - <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: ${policy}
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
    - apiGroups: [shiftpv.io]
      apiVersions: [v1alpha1]
      operations: [UPDATE]
      resources: [shiftpvmoves/status]
  validations:
  - expression: "object.spec.volumeID != '${volume}' || !has(object.status) || !has(object.status.cleanup) || (has(oldObject.status) && has(oldObject.status.cleanup))"
    message: cleanup admission test barrier
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ${policy}
spec:
  policyName: ${policy}
  validationActions: [Deny]
EOF
}

release_cleanup_admission() {
	local policy=$1
	test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}" || return
	kubectl delete "validatingadmissionpolicybinding/${policy}" || return
	kubectl delete "validatingadmissionpolicy/${policy}"
}

wait_for_cleanup_barrier() {
	local move=$1 volume=$2 source=$3 parent attempt denial
	kubectl wait "shiftpvmove/${move}" --for=jsonpath='{.status.phase}'=WaitingForDestinationPublish --timeout=300s || return
	test "$(kubectl get "shiftpvvolume/${volume}" -o jsonpath='{.status.ownerNode}')" != "${source}" || return
	test "$(kubectl get "shiftpvvolume/${volume}" -o jsonpath='{.status.activeMove}')" = "${move}" || return
	parent=$(kubectl get "shiftpvmove/${move}" -o json) || return
	jq -e --arg source "${source}" '
		.status.sourceCopy.nodeName == $source and .status.capacityApproved == true and
		.status.cleanup == null and .status.destinationCopy != null' <<<"${parent}" >/dev/null || return
	# A server dry-run verifies this exact intent is blocked without storing it.
	attempt=$(jq -c '.status.cleanup = {spec:{operationID:("cleanup-" + .metadata.uid),
		target:.status.sourceCopy, reason:"MoveSource", authority:{kind:"ShiftPVMove", name:.metadata.name, uid:.metadata.uid}},
		status:{phase:"Pending"}}' <<<"${parent}") || return
	if denial=$(kubectl replace --subresource=status --dry-run=server -f - <<<"${attempt}" 2>&1); then
		echo 'cleanup admission barrier was not enforced' >&2
		return 1
	fi
	grep -Fq 'cleanup admission test barrier' <<<"${denial}"
}

assert_no_cleanup_job() {
	local move=$1 uid
	uid=$(kubectl get "shiftpvmove/${move}" -o jsonpath='{.metadata.uid}') || return
	test -n "${uid}" || return
	kubectl -n shiftpv-system get jobs -o json | jq -e --arg uid "${uid}" '
		[.items[] | select(.metadata.labels["app.kubernetes.io/component"] == "cleanup-helper") |
		 select(any(.metadata.ownerReferences[]?; .uid == $uid))] | length == 0' >/dev/null
}

wait_for_cleanup_phase() {
	local resource=$1 phase=$2 timeout=${3:-5m}
	kubectl wait "${resource}" \
		--for="jsonpath={.status.cleanup.status.phase}=${phase}" \
		--timeout="${timeout}"
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
	test "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.executor.kind}')" = Node
	case "${expected_kind}:${expected_reason}" in
	ShiftPVVolume:VolumeDelete | ShiftPVMove:MoveSource | ShiftPVMove:MoveRollback) ;;
	*) echo "unsupported Node cleanup authority/reason: ${expected_kind}/${expected_reason}" >&2; return 1 ;;
	esac
	test -z "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.executor.jobName}')"
	test -z "${executor_uid}"
	test -n "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.executor.namespace}')"
	test -n "$(kubectl get "${resource}" -o jsonpath='{.status.cleanup.status.executor.podName}')"
	executor_uid=${executor_pod_uid}

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
