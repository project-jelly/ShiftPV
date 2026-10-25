#!/usr/bin/env bash
# Sourced by soak.sh and its local command-stub tests. All API calls use k().

cleanup_source_role() {
	case "$1" in
	"${SOURCE_NODE}") printf 'source\t%s\n' "${SOURCE_POOL}" ;;
	"${DESTINATION_NODE}") printf 'destination\t%s\n' "${DESTINATION_POOL}" ;;
	*) echo "unreviewed cleanup source: $1" >&2; return 1 ;;
	esac
}

hold_soak_cleanup() {
	local policy=$1 volume=$2 previous=$3 object canary denial deadline=$((SECONDS + MOVE_TIMEOUT_SECONDS))
	# A settled prior Move is a read-only canary for this exact Volume. The same
	# policy/binding must reject it before a new Move is triggered.
	test -n "${previous}" || return 1
	canary=$(k get "shiftpvmove/${previous}" -o json) || return
	jq -e --arg volume "${volume}" --arg previous "${previous}" '
		.metadata.name == $previous and .spec.volumeID == $volume and
		.status.phase == "Succeeded" and .status.cleanup.status.phase == "Completed" and
		((.metadata.finalizers // []) | length) == 0' <<<"${canary}" >/dev/null || return
	# create (never apply) refuses an existing policy/binding, including a race.
	CLEANUP_POLICY=${policy}
	object=$(k create -f - -o json <<EOF
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
  - expression: "object.spec.volumeID != '${volume}' || (request.dryRun != true && (!has(object.status) || !has(object.status.cleanup) || (has(oldObject.status) && has(oldObject.status.cleanup))))"
    message: real-node cleanup admission barrier
EOF
	) || return
	CLEANUP_POLICY_UID=$(jq -er '.metadata.uid | select(length > 0)' <<<"${object}") || return
	object=$(k create -f - -o json <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ${policy}
spec:
  policyName: ${policy}
  validationActions: [Deny]
EOF
	) || return
	CLEANUP_BINDING_UID=$(jq -er '.metadata.uid | select(length > 0)' <<<"${object}") || return
	while ((SECONDS < deadline)); do
		if denial=$(k replace --subresource=status --dry-run=server -f - <<<"${canary}" 2>&1); then
			sleep 0.2
		elif grep -Fq 'real-node cleanup admission barrier' <<<"${denial}"; then
			printf '%s\n' "${denial}" >"${ARTIFACT_DIR}/${policy}-activation.txt"
			return
		else
			printf '%s\n' "${denial}" >&2
			return 1
		fi
	done
	echo "cleanup admission policy did not activate: ${policy}" >&2
	return 1
}

delete_cleanup_uid() {
	local path=$1 uid=$2
	test -n "${uid}" || return 1
	jq -nc --arg uid "${uid}" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' |
		k delete --raw "${path}" -f -
}

release_soak_cleanup() {
	if [[ -n ${CLEANUP_BINDING_UID:-} ]]; then
		delete_cleanup_uid "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/${CLEANUP_POLICY}" "${CLEANUP_BINDING_UID}" >/dev/null || return
		CLEANUP_BINDING_UID=''
	fi
	if [[ -n ${CLEANUP_POLICY_UID:-} ]]; then
		delete_cleanup_uid "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/${CLEANUP_POLICY}" "${CLEANUP_POLICY_UID}" >/dev/null || return
		CLEANUP_POLICY_UID=''
	fi
}

# Return only the immutable transaction/hold identity, not phase or timestamps.
cleanup_intent() {
	jq -cS '{uid:.metadata.uid, volume:.spec.volumeID, source:.spec.sourceNode,
		sourceCopy:.status.sourceCopy, destinationCopy:.status.destinationCopy,
		capacityApproved:.status.capacityApproved, preparation:.status.preparation,
		cleanup:.status.cleanup.spec}' "$1"
}

validate_cleanup_move() {
	local file=$1 source=$2 destination=${DESTINATION_NODE}
	cleanup_source_role "${source}" >/dev/null || return
	if [[ "${source}" == "${DESTINATION_NODE}" ]]; then destination=${SOURCE_NODE}; fi
	jq -e --arg volume "${VOLUME_ID}" --arg volume_uid "${VOLUME_UID}" --arg source "${source}" --arg destination "${destination}" --arg pv "${PV_UID}" --arg claim "${PVC_UID}" '
		.spec.protocolVersion == "prepared-v1" and .spec.volumeID == $volume and .spec.sourceNode == $source and
		(.metadata.uid | type == "string" and length > 0) and
		.status.persistentVolumeUID == $pv and .status.persistentVolumeClaimUID == $claim and
		.status.capacityApproved == true and .status.preparation == null and
		.status.sourceCopy.nodeName == $source and .status.sourceCopy.volumeID == $volume and .status.sourceCopy.volumeUID == $volume_uid and
		(.status.sourceCopy.copyID | type == "string" and length > 0) and
		.status.destinationCopy.volumeID == $volume and .status.destinationCopy.volumeUID == $volume_uid and .status.destinationCopy.nodeName == $destination and
		(.status.destinationCopy.copyID | type == "string" and length > 0)' "${file}" >/dev/null
}

validate_cleanup_pod() {
	local file=$1 source=$2 namespace name uid pod ds
	validate_cleanup_move "${file}" "${source}" || return
	jq -e --arg namespace "${SYSTEM_NAMESPACE}" --arg source "${source}" '
		.status.cleanup.spec as $s | .status.cleanup.status as $c |
		(.status.phase == "WaitingForDestinationPublish" or .status.phase == "CleaningSource") and
		$s.reason == "MoveSource" and $s.target == .status.sourceCopy and
		$s.authority == {kind:"ShiftPVMove", name:.metadata.name, uid:.metadata.uid} and
		$s.operationID == ("cleanup-" + .metadata.uid) and
		$c.phase == "Running" and $c.receipt == null and
		$c.executor.kind == "Node" and $c.executor.namespace == $namespace and $c.executor.nodeName == $source and
		($c.executor.podName | type == "string" and length > 0) and
		($c.executor.podUID | type == "string" and length > 0) and
		(($c.executor.jobName // "") == "") and (($c.executor.jobUID // "") == "")' "${file}" >/dev/null || return
	read -r namespace name uid < <(jq -r '.status.cleanup.status.executor | [.namespace,.podName,.podUID] | @tsv' "${file}") || return
	pod=$(k -n "${namespace}" get "pod/${name}" -o json) || return
	ds=$(k -n "${SYSTEM_NAMESPACE}" get "daemonset/${NODE_DAEMONSET}" -o json) || return
	jq -e --arg image "${EXPECTED_NODE_IMAGE}" '.metadata.deletionTimestamp == null and
		(.metadata.uid | type == "string" and length > 0) and
		any(.spec.template.spec.containers[]?; .name == "shiftpv-node" and .image == $image)' <<<"${ds}" >/dev/null || return
	jq -e --arg uid "${uid}" --arg node "${source}" --arg image "${EXPECTED_NODE_IMAGE}" --argjson ds "${ds}" '
		.metadata.uid == $uid and .metadata.namespace == $ds.metadata.namespace and .metadata.deletionTimestamp == null and
		.spec.nodeName == $node and .status.phase == "Running" and
		.spec.serviceAccountName == $ds.spec.template.spec.serviceAccountName and
		.metadata.annotations["shiftpv.io/node-effects"] == "v1" and
		.metadata.annotations["shiftpv.io/node-move-cleanup"] == "v1" and
		([.metadata.ownerReferences[]? | select(.controller == true)] | length) == 1 and
		any(.metadata.ownerReferences[]?; .controller == true and .apiVersion == "apps/v1" and
		    .kind == "DaemonSet" and .name == $ds.metadata.name and .uid == $ds.metadata.uid) and
		any(.spec.containers[]?; .name == "shiftpv-node" and .image == $image) and
		any(.status.conditions[]?; .type == "Ready" and .status == "True")' <<<"${pod}" >/dev/null || return
	printf '%s\n' "${pod}" >"${ARTIFACT_DIR}/${uid}-node-pod.json"
	printf '%s\n' "${ds}" >"${ARTIFACT_DIR}/${uid}-node-daemonset.json"
}

assert_no_soak_cleanup_job() {
	local file=$1 uid
	uid=$(jq -er '.metadata.uid' "${file}") || return
	k -n "${SYSTEM_NAMESPACE}" get jobs -o json | jq -e --arg uid "${uid}" '
		[.items[] | select(.metadata.labels["app.kubernetes.io/component"] == "cleanup-helper") |
		 select(any(.metadata.ownerReferences[]?; .uid == $uid))] | length == 0' >/dev/null
}

# Send DELETE once, even after a lost response. Re-observe the old UID; a new
# Pod with the same name is evidence of retirement, never another delete target.
delete_bound_cleanup_pod() {
	local file=$1 source=$2 namespace name uid
	validate_cleanup_pod "${file}" "${source}" || return
	read -r namespace name uid < <(jq -r '.status.cleanup.status.executor | [.namespace,.podName,.podUID] | @tsv' "${file}") || return
	delete_soak_pod "${namespace}" "${name}" "${uid}"
}

delete_soak_pod() {
	local namespace=$1 name=$2 uid=$3 observed deadline=$((SECONDS + MOVE_TIMEOUT_SECONDS))
	delete_cleanup_uid "/api/v1/namespaces/${namespace}/pods/${name}" "${uid}" \
		>"${ARTIFACT_DIR}/cleanup-delete-${uid}.json" 2>"${ARTIFACT_DIR}/cleanup-delete-${uid}.log" || true
	while ((SECONDS < deadline)); do
		observed=$(k -n "${namespace}" get "pod/${name}" --ignore-not-found -o json) || return
		if [[ -z "${observed}" ]] || [[ $(jq -er '.metadata.uid' <<<"${observed}") != "${uid}" ]]; then
			return
		fi
		sleep 1
	done
	echo "old fault Pod UID did not disappear: ${uid}" >&2
	return 1
}

validate_cleanup_rebind() {
	local before=$1 after=$2 source=$3
	test "$(cleanup_intent "${before}")" = "$(cleanup_intent "${after}")" || return
	test "$(jq -er '.status.cleanup.status.executor.podUID' "${before}")" != \
		"$(jq -er '.status.cleanup.status.executor.podUID' "${after}")" || return
	validate_cleanup_pod "${after}" "${source}"
}

validate_native_cleanup_proof() {
	local file=$1 reference=${2:-}
	validate_cleanup_move "${file}" "$(jq -er '.spec.sourceNode' "${file}")" || return
	if [[ -n "${reference}" ]]; then
		test "$(cleanup_intent "${reference}")" = "$(cleanup_intent "${file}")" || return
		test "$(jq -cS '.status.cleanup.status.executor' "${reference}")" = \
			"$(jq -cS '.status.cleanup.status.executor' "${file}")" || return
	fi
	jq -e --arg namespace "${SYSTEM_NAMESPACE}" '.status.cleanup.spec as $s | .status.cleanup.status as $c |
		.status.phase == "Succeeded" and .status.preparation == null and
		((.metadata.finalizers // []) | length) == 0 and
		$s.operationID == ("cleanup-" + .metadata.uid) and $s.reason == "MoveSource" and $s.target == .status.sourceCopy and
		$s.authority == {kind:"ShiftPVMove",name:.metadata.name,uid:.metadata.uid} and
		$c.phase == "Completed" and $c.executor.kind == "Node" and $c.executor.namespace == $namespace and
		(($c.executor.jobName // "") == "") and (($c.executor.jobUID // "") == "") and
		$c.executor.nodeName == $s.target.nodeName and ($c.executor.podUID | length > 0) and
		$c.receipt.operationID == $s.operationID and $c.receipt.executorUID == $c.executor.podUID and
		$c.receipt.retired == true and $c.receipt.purged == true and ($c.receipt.observedAt | length > 0) and
		($c.receipt.localReceiptDigest | test("^[0-9a-f]{64}$")) and
		$c.absenceProof.poolName == $s.target.poolName and $c.absenceProof.poolUID == $s.target.poolUID and
		($c.absenceProof.requestID | length > 0) and $c.absenceProof.requiredGeneration > 0 and
		$c.absenceProof.observedGeneration >= $c.absenceProof.requiredGeneration and
		$c.absenceProof.valid == true and $c.absenceProof.complete == true and $c.absenceProof.absent == true and
		($c.absenceProof.confirmedAt | length > 0) and ($c.settledAt | length > 0)' "${file}" >/dev/null
}

wait_cleanup_process() {
	local pid=$1 budget=$2 deadline=$((SECONDS + $2))
	while kill -0 "${pid}" 2>/dev/null && ((SECONDS < deadline)); do sleep 0.1; done
	if kill -0 "${pid}" 2>/dev/null; then
		kill -KILL "${pid}" 2>/dev/null || true
		wait "${pid}" 2>/dev/null || true
		echo "cleanup process exceeded local ${budget}s budget: ${pid}" >&2
		return 1
	fi
	wait "${pid}"
}

release_cleanup_lock() {
	local release_pid
	if [[ -n ${CLEANUP_LOCK_ROLE:-} ]]; then
		exec_ssh_node "${CLEANUP_LOCK_ROLE}" "sudo -n timeout -k 5 10 sh -c $(jq -nr --arg command 'if test -e "$1" && test ! -e "$2"; then set -C; : >"$2"; fi' '$command|@sh') sh $(jq -nr --arg p "${CLEANUP_LOCK_READY}" '$p|@sh') $(jq -nr --arg p "${CLEANUP_LOCK_RELEASE}" '$p|@sh')" >"${ARTIFACT_DIR}/cleanup-lock-release.log" 2>&1 &
		release_pid=$!
		if ! wait_cleanup_process "${release_pid}" 15; then
			kill -KILL "${CLEANUP_LOCK_PID}" 2>/dev/null || true
			wait "${CLEANUP_LOCK_PID}" 2>/dev/null || true
			return 1
		fi
		wait_cleanup_process "${CLEANUP_LOCK_PID}" 15 || return
		CLEANUP_LOCK_ROLE=''
	fi
}

fault_native_cleanup() {
	local move=$1 source=$2 role pool volume pool_json attempt denial uid command deadline=$((SECONDS + MOVE_TIMEOUT_SECONDS))
	local before="${ARTIFACT_DIR}/${move}-cleanup-before.json" after="${ARTIFACT_DIR}/${move}-cleanup-rebound.json"
	IFS=$'\t' read -r role pool < <(cleanup_source_role "${source}") || return
	k wait "shiftpvmove/${move}" --for=jsonpath='{.status.phase}'=WaitingForDestinationPublish --timeout="${MOVE_TIMEOUT_SECONDS}s" || return
	k get "shiftpvmove/${move}" -o json >"${before}" || return
	test "$(jq -er '.metadata.name' "${before}")" = "${move}" || return
	validate_cleanup_move "${before}" "${source}" || return
	jq -e '.status.cleanup == null' "${before}" >/dev/null || return
	while ((SECONDS < deadline)); do
		volume=$(k get "shiftpvvolume/${VOLUME_ID}" -o json) || return
		jq -e --arg uid "${VOLUME_UID}" --arg move "${move}" --slurpfile parent "${before}" '
			.metadata.uid == $uid and .status.phase == "Ready" and .status.activeMove == $move and
			.status.ownerNode == $parent[0].status.destinationCopy.nodeName and
			.status.currentCopy == $parent[0].status.destinationCopy' <<<"${volume}" >/dev/null || return
		# Publication of the exact destination and source unpublish must both be observed.
		if jq -e --arg source "${source}" '.status as $s | ($s.publishedNodes // []) as $nodes |
			($nodes | index($source)) == null and ($nodes | index($s.ownerNode)) != null' <<<"${volume}" >/dev/null; then
			break
		fi
		sleep 1
	done
	((SECONDS < deadline)) || return 1
	pool_json=$(k get "shiftpvpool/$(jq -er '.status.sourceCopy.poolName' "${before}")" -o json) || return
	jq -e --arg node "${source}" --arg path "${pool}" --slurpfile parent "${before}" '
		.metadata.uid == $parent[0].status.sourceCopy.poolUID and .spec.nodeName == $node and .spec.mountPath == $path' <<<"${pool_json}" >/dev/null || return
	# A denied dry-run proves this exact first intent is held; no synthetic write.
	attempt=$(jq -c '.status.cleanup = {spec:{operationID:("cleanup-" + .metadata.uid), target:.status.sourceCopy,
		reason:"MoveSource",authority:{kind:"ShiftPVMove",name:.metadata.name,uid:.metadata.uid}},status:{phase:"Pending"}}' "${before}") || return
	if denial=$(k replace --subresource=status --dry-run=server -f - <<<"${attempt}" 2>&1); then
		echo 'cleanup admission barrier was not enforced' >&2; return 1
	fi
	grep -Fq 'real-node cleanup admission barrier' <<<"${denial}" || return
	uid=$(jq -er '.metadata.uid' "${before}") || return
	CLEANUP_LOCK_READY="/tmp/shiftpv-soak-${uid}-ready"
	CLEANUP_LOCK_RELEASE="/tmp/shiftpv-soak-${uid}-release"
	command="sudo -n timeout -k 5 ${MOVE_TIMEOUT_SECONDS} sh -s -- $(jq -nr --arg p "${pool}/.shiftpv/lock-${VOLUME_ID}" '$p|@sh') $(jq -nr --arg p "${CLEANUP_LOCK_READY}" '$p|@sh') $(jq -nr --arg p "${CLEANUP_LOCK_RELEASE}" '$p|@sh')"
	CLEANUP_LOCK_ROLE=${role}
	# Existing inode only. The ready marker is removed whenever the holder exits.
	exec_ssh_node "${role}" "${command}" >"${ARTIFACT_DIR}/${move}-lock.log" 2>&1 <<'REMOTE' &
set -eu
test -f "$1"; test ! -L "$1"; test ! -e "$2"; test ! -e "$3"
exec 9<"$1"
flock -n 9
umask 077; set -C; : >"$2"
trap 'rm -f -- "$2" "$3"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
while [ ! -e "$3" ]; do sleep 0.1; done
REMOTE
	CLEANUP_LOCK_PID=$!
	while ! ssh_node "${role}" sudo -n test -f "${CLEANUP_LOCK_READY}"; do
		kill -0 "${CLEANUP_LOCK_PID}" 2>/dev/null && ((SECONDS < deadline)) || return 1
		sleep 1
	done
	release_soak_cleanup || return
	k wait "shiftpvmove/${move}" --for=jsonpath='{.status.cleanup.status.phase}'=Running --timeout="${MOVE_TIMEOUT_SECONDS}s" || return
	k get "shiftpvmove/${move}" -o json >"${after}" || return
	# The cleanup spec is new; compare the already committed disk plan first.
	jq -e --slurpfile old "${before}" '.metadata.uid == $old[0].metadata.uid and
		.status.sourceCopy == $old[0].status.sourceCopy and .status.destinationCopy == $old[0].status.destinationCopy' "${after}" >/dev/null || return
	cp "${after}" "${before}"
	validate_cleanup_pod "${before}" "${source}" || return
	assert_no_soak_cleanup_job "${before}" || return
	ssh_node "${role}" sudo -n test -f "${CLEANUP_LOCK_READY}" || return
	delete_bound_cleanup_pod "${before}" "${source}" || return
	while ((SECONDS < deadline)); do
		k get "shiftpvmove/${move}" -o json >"${after}" || return
		if [[ $(jq -r '.status.cleanup.status.executor.podUID // ""' "${after}") != "$(jq -r '.status.cleanup.status.executor.podUID' "${before}")" ]]; then
			validate_cleanup_rebind "${before}" "${after}" "${source}" || return
			ssh_node "${role}" sudo -n test -f "${CLEANUP_LOCK_READY}" || return
			release_cleanup_lock || return
			return
		fi
		sleep 1
	done
	echo "cleanup executor did not rebind: ${move}" >&2
	return 1
}
