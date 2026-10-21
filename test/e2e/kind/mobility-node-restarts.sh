#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
# shellcheck source=test/e2e/kind/cleanup-journal.sh
source "${ROOT_DIR}/test/e2e/kind/cleanup-journal.sh"
# shellcheck source=test/e2e/kind/mobility/controller.sh
source "${ROOT_DIR}/test/e2e/kind/mobility/controller.sh"
# shellcheck source=test/e2e/kind/lib/wait.sh
source "${ROOT_DIR}/test/e2e/kind/lib/wait.sh"
: "${CLUSTER_NAME:?CLUSTER_NAME is required}"
: "${WORK_DIR:?WORK_DIR is required}"
: "${WORKER_A_POOL:?WORKER_A_POOL is required}"
: "${WORKER_B_POOL:?WORKER_B_POOL is required}"

SOURCE_NODE="${CLUSTER_NAME}-worker"
DESTINATION_NODE="${CLUSTER_NAME}-worker2"
SOURCE_MOUNT=$(pool_mount_for_node "${SOURCE_NODE}")
DESTINATION_MOUNT=$(pool_mount_for_node "${DESTINATION_NODE}")
CONTROLLER_TOLERATIONS=""
DESTINATION_TAINT_KEY=e2e.shiftpv.io/mobility-destination
DESTINATION_TAINT_VALUE=""

allow_controller_on_cordoned_source() {
	CONTROLLER_TOLERATIONS=$(kubectl -n shiftpv-system get deployment/shiftpv-controller -o json | jq -c '.spec.template.spec.tolerations // null')
	kubectl -n shiftpv-system patch deployment/shiftpv-controller --type merge -p "$(jq -nc \
		--argjson tolerations "${CONTROLLER_TOLERATIONS}" \
		'{spec:{template:{spec:{tolerations:(($tolerations // []) + [{key:"node.kubernetes.io/unschedulable",operator:"Exists",effect:"NoSchedule"}])}}}}')"
}

release_destination_test_taint() {
	[[ -n "${DESTINATION_TAINT_VALUE}" ]] || return 0
	local node patch
	for _ in {1..5}; do
		node=$(kubectl get "node/${DESTINATION_NODE}" -o json) || return 1
		patch=$(jq -c --arg key "${DESTINATION_TAINT_KEY}" --arg value "${DESTINATION_TAINT_VALUE}" '
			(.spec.taints // []) as $taints |
			if any($taints[]; .key == $key and .value == $value and .effect == "NoSchedule") then
			  [{op:"test",path:"/metadata/uid",value:.metadata.uid},
			   {op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},
			   {op:"replace",path:"/spec/taints",value:
			    ($taints | map(select(.key != $key or .value != $value or .effect != "NoSchedule")))}]
			else [] end' <<<"${node}") || return 1
		if [[ "${patch}" == '[]' ]] || kubectl patch "node/${DESTINATION_NODE}" --type json -p "${patch}"; then
			DESTINATION_TAINT_VALUE=""
			return
		fi
	done
	echo 'could not remove exact destination test taint' >&2
	return 1
}

restore_controller_tolerations() {
	if [[ -n "${CONTROLLER_TOLERATIONS}" ]]; then
		kubectl -n shiftpv-system patch deployment/shiftpv-controller --type merge \
			-p "$(jq -nc --argjson tolerations "${CONTROLLER_TOLERATIONS}" '{spec:{template:{spec:{tolerations:$tolerations}}}}')"
		CONTROLLER_TOLERATIONS=""
	fi
}
restore_cluster() {
	for node in "${SOURCE_NODE}" "${DESTINATION_NODE}"; do
		docker start "${node}" >/dev/null 2>&1 || true
	done
	release_destination_test_taint >/dev/null 2>&1 || true
	kubectl uncordon "${SOURCE_NODE}" >/dev/null 2>&1 || true
	kubectl uncordon "${DESTINATION_NODE}" >/dev/null 2>&1 || true
	restore_controller_tolerations >/dev/null 2>&1 || true
	kubectl -n shiftpv-system scale deployment/shiftpv-controller --replicas=1 >/dev/null 2>&1 || true
}
trap restore_cluster EXIT

wait_for_node_condition() {
	local node=$1 expected=$2 deadline=$((SECONDS + 180)) condition=""
	while ((SECONDS < deadline)); do
		condition=$(kubectl get "node/${node}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
		case "${expected}:${condition}" in
		ready:True | unavailable:False | unavailable:Unknown)
			return
			;;
		esac
		sleep 1
	done
	echo "node ${node} did not become ${expected}; Ready=${condition}" >&2
	kubectl get "node/${node}" -o wide >&2 || true
	return 1
}

render_workload() {
	local namespace=$1 payload=$2
	sed \
		-e "s|__NAMESPACE__|${namespace}|g" \
		-e "s|__PAYLOAD__|${payload}|g" \
		"${ROOT_DIR}/test/e2e/kind/mobility/manifests/filesystem-fault-workload.yaml.tpl" \
		>"${WORK_DIR}/${namespace}.yaml"
}

create_source_workload() {
	local namespace=$1 payload=$2
	kubectl uncordon "${SOURCE_NODE}" >/dev/null 2>&1 || true
	kubectl cordon "${DESTINATION_NODE}"
	render_workload "${namespace}" "${payload}"
	kubectl apply -f "${WORK_DIR}/${namespace}.yaml"
	kubectl -n "${namespace}" rollout status deployment/writer --timeout=180s
	kubectl -n "${namespace}" wait --for=jsonpath='{.status.phase}'=Bound pvc/data --timeout=120s

	PVC_UID=$(kubectl -n "${namespace}" get pvc/data -o jsonpath='{.metadata.uid}')
	PV_NAME=$(kubectl -n "${namespace}" get pvc/data -o jsonpath='{.spec.volumeName}')
	VOLUME_ID=$(kubectl get "pv/${PV_NAME}" -o jsonpath='{.spec.csi.volumeHandle}')
	SOURCE_POD=$(kubectl -n "${namespace}" get pod -l "app=${namespace}" -o jsonpath='{.items[0].metadata.name}')
	test "$(kubectl -n "${namespace}" get "pod/${SOURCE_POD}" -o jsonpath='{.spec.nodeName}')" = "${SOURCE_NODE}"
	# The replacement template cannot recreate this per-PVC data after data loss.
	printf 'ShiftPV durable payload pvc=%s volume=%s\n' "${PVC_UID}" "${VOLUME_ID}" \
		| kubectl -n "${namespace}" exec -i "${SOURCE_POD}" -- sh -ec 'cat > /data/payload; sync'
	SOURCE_CHECKSUM=$(pod_sha256 "${namespace}" "${SOURCE_POD}" /data/payload)
	SOURCE_VOLUME_UID=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.metadata.uid}')
	SOURCE_COPY=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o json | jq -c '.status.currentCopy')
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${SOURCE_NODE}"
	kubectl uncordon "${DESTINATION_NODE}"
}

placement_pod_for_move() {
	local pod owner
	while IFS= read -r pod; do
		[[ -n "${pod}" ]] || continue
		owner=$(kubectl -n shiftpv-system get "${pod}" \
			-o jsonpath='{.metadata.ownerReferences[?(@.kind=="ShiftPVMove")].name}' 2>/dev/null || true)
		if [[ "${owner}" == "${MOVE_NAME}" ]]; then
			printf '%s\n' "${pod#pod/}"
			return
		fi
	done < <(kubectl -n shiftpv-system get pod -l shiftpv.io/role=placement -o name 2>/dev/null || true)
	return 1
}

pause_before_copy() {
	local deadline=$((SECONDS + 300)) phase="" replacement="" replacement_node="" replacement_hold="" placement="" placement_node=""
	while ((SECONDS < deadline)); do
		phase=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
		if [[ "${phase}" == WaitingForDestination ]]; then
			controller_down
			phase=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.phase}')
			break
		fi
		if [[ "${phase}" == Blocked || "${phase}" == Succeeded || "${phase}" == Cancelled ]]; then
			echo "Move reached terminal phase before node fault injection: ${phase}" >&2
			return 1
		fi
		sleep 0.2
	done
	if [[ "${phase}" != WaitingForDestination ]]; then
		echo "Move did not reach WaitingForDestination before deadline; phase=${phase}" >&2
		return 1
	fi

	deadline=$((SECONDS + 120))
	while ((SECONDS < deadline)); do
		replacement=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.replacementName}' 2>/dev/null || true)
		if [[ -z "${replacement}" ]]; then
			replacement=$(kubectl -n "${TEST_NAMESPACE}" get pod \
				-l "app=${TEST_NAMESPACE},shiftpv.io/managed=true" \
				-o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
		fi
		if [[ -n "${replacement}" ]]; then
			replacement_node=$(kubectl -n "${TEST_NAMESPACE}" get "pod/${replacement}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)
			replacement_hold=$(kubectl -n "${TEST_NAMESPACE}" get "pod/${replacement}" \
				-o jsonpath='{.spec.schedulingGates[?(@.name=="shiftpv.io/placement-hold")].name}' 2>/dev/null || true)
			placement=$(placement_pod_for_move || true)
			if [[ -n "${placement}" ]]; then
				placement_node=$(kubectl -n shiftpv-system get "pod/${placement}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)
				if [[ -z "${replacement_node}" && "${replacement_hold}" == "shiftpv.io/placement-hold" && "${placement_node}" == "${DESTINATION_NODE}" ]]; then
					REPLACEMENT_POD=${replacement}
					REPLACEMENT_UID=$(kubectl -n "${TEST_NAMESPACE}" get "pod/${replacement}" -o jsonpath='{.metadata.uid}')
					PLACEMENT_POD=${placement}
					PLACEMENT_UID=$(kubectl -n shiftpv-system get "pod/${placement}" -o jsonpath='{.metadata.uid}')
					MOVE_UID=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.metadata.uid}')
					PREPARATION=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o json | jq -c '.status.preparation')
					test "${PREPARATION}" != null
					assert_no_execution_approval
					kubectl -n shiftpv-system wait "pod/${placement}" --for=condition=Ready --timeout=60s
					return
				fi
			fi
		fi
		sleep 0.2
	done
	echo "placement hold was not safely selected on ${DESTINATION_NODE}: replacement=${replacement} replacementNode=${replacement_node} hold=${replacement_hold} placement=${placement} placementNode=${placement_node}" >&2
	return 1
}

pause_at_phase() {
	local expected_phase=$1 deadline=$((SECONDS + 300)) phase=""
	while ((SECONDS < deadline)); do
		phase=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
		if [[ "${phase}" == "${expected_phase}" ]]; then
			controller_down
			phase=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.phase}')
			test "${phase}" = "${expected_phase}"
			test "$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.capacityApproved}')" = true
			return
		fi
		if [[ "${phase}" == Blocked || "${phase}" == Succeeded || "${phase}" == Cancelled ]]; then
			echo "Move reached terminal phase before ${expected_phase} node fault injection: ${phase}" >&2
			return 1
		fi
		sleep 0.2
	done
	echo "Move did not reach ${expected_phase} before deadline; phase=${phase}" >&2
	return 1
}

pause_before_source_cleanup() {
	local namespace=$1 pod destination_pool destination_identity deadline published_nodes observed_copy
	pause_at_phase WaitingForDestinationPublish
	test -z "$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.phase}')"

	# With the controller stopped, let kubelet and the node scanner establish the
	# destination publication fence. No cleanup Job can exist before the fault.
	kubectl -n "${namespace}" rollout status deployment/writer --timeout=180s
	pod=$(kubectl -n "${namespace}" get pod -l "app=${namespace}" -o jsonpath='{.items[0].metadata.name}')
	test "$(kubectl -n "${namespace}" get "pod/${pod}" -o jsonpath='{.spec.nodeName}')" = "${DESTINATION_NODE}"
	destination_identity=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o json | jq -c '.status.destinationCopy')
	destination_pool=$(jq -r '.poolName' <<<"${destination_identity}")
	test -n "${destination_pool}"
	test "${destination_identity}" != null
	deadline=$((SECONDS + 180))
	while ((SECONDS < deadline)); do
		published_nodes=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o json 2>/dev/null || true)
		observed_copy=$(kubectl get "shiftpvpool/${destination_pool}" -o json 2>/dev/null || true)
		if jq -e --arg node "${DESTINATION_NODE}" '.status.publishedNodes | index($node) != null' <<<"${published_nodes}" >/dev/null 2>&1 &&
			jq -e --argjson identity "${destination_identity}" '
				.metadata.name == $identity.poolName and
				.metadata.uid == $identity.poolUID and
				.spec.nodeName == $identity.nodeName and
				.status.inventory.valid == true and
				(.status.inventory.truncated // false) == false and
				(.status.inventory.message // "") == "" and
				any(.status.inventory.copies[]?;
					.identity == $identity and .present == true and .published == true and (.problem // "") == "")
			' <<<"${observed_copy}" >/dev/null 2>&1; then
			assert_destination_publish_metadata "${namespace}" "${pod}"
			assert_node_file "${SOURCE_NODE}" "${SOURCE_MOUNT}/volumes/${VOLUME_ID}/payload"
			assert_node_file "${DESTINATION_NODE}" "${DESTINATION_MOUNT}/volumes/${VOLUME_ID}/payload"
			return
		fi
		sleep 1
	done
	echo "destination publication was not observed before source cleanup: volume=${VOLUME_ID} move=${MOVE_NAME}" >&2
	return 1
}

stop_node() {
	local node=$1
	docker stop -t 1 "${node}" >/dev/null
	wait_for_node_condition "${node}" unavailable
}

start_node() {
	local node=$1
	docker start "${node}" >/dev/null
	wait_for_node_condition "${node}" ready
	kubectl -n shiftpv-system rollout status daemonset/shiftpv-node --timeout=180s
}

assert_blocked_source_authority() {
	local reason=$1
	kubectl wait "shiftpvmove/${MOVE_NAME}" --for=jsonpath='{.status.phase}'=Blocked --timeout=180s
	test "$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.reason}')" = "${reason}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.phase}')" = Blocked
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${SOURCE_NODE}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.activeMove}')" = "${MOVE_NAME}"
	assert_node_file "${SOURCE_NODE}" "${SOURCE_MOUNT}/volumes/${VOLUME_ID}/payload"
	assert_node_absent "${DESTINATION_NODE}" "${DESTINATION_MOUNT}/volumes/${VOLUME_ID}"
}

recover_source() {
	local namespace=$1 pod checksum
	kubectl uncordon "${SOURCE_NODE}"
	kubectl patch "shiftpvmove/${MOVE_NAME}" --type merge -p '{"spec":{"recovery":"ResumeOwner"}}'
	kubectl wait "shiftpvmove/${MOVE_NAME}" --for=jsonpath='{.status.recoveryPhase}'=Recovered --timeout=300s
	kubectl -n "${namespace}" rollout status deployment/writer --timeout=180s
	pod=$(kubectl -n "${namespace}" get pod -l "app=${namespace}" -o jsonpath='{.items[0].metadata.name}')
	test "$(kubectl -n "${namespace}" get "pod/${pod}" -o jsonpath='{.spec.nodeName}')" = "${SOURCE_NODE}"
	checksum=$(pod_sha256 "${namespace}" "${pod}" /data/payload)
	test "${checksum}" = "${SOURCE_CHECKSUM}"
	test "$(kubectl -n "${namespace}" get pvc/data -o jsonpath='{.metadata.uid}')" = "${PVC_UID}"
	test "$(kubectl -n "${namespace}" get pvc/data -o jsonpath='{.spec.volumeName}')" = "${PV_NAME}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.phase}')" = Ready
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${SOURCE_NODE}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.activeMove}')" = ""
}

assert_no_execution_approval() {
	kubectl get "shiftpvmove/${MOVE_NAME}" -o json | jq -e --arg uid "${MOVE_UID}" '
		.metadata.uid == $uid and .spec.protocolVersion == "prepared-v1" and
		.status.capacityApproved != true and .status.incomingCopy == null and .status.destinationCopy == null and
		(.status.copyOperationID // "") == "" and (.status.promotionOperationID // "") == "" and
		(.status.copyJobName // "") == "" and (.status.promotionJobName // "") == "" and
		.status.cleanup == null and (.status.rollbackRequiredGeneration // 0) == 0' >/dev/null
	kubectl get jobs -A -o json | jq -e --arg uid "${MOVE_UID}" \
		'all(.items[]; all(.metadata.ownerReferences[]?; .uid != $uid))' >/dev/null
	assert_node_absent "${DESTINATION_NODE}" "${DESTINATION_MOUNT}/volumes/${VOLUME_ID}"
}

assert_preparation_source() {
	kubectl get "shiftpvvolume/${VOLUME_ID}" -o json | jq -e \
		--arg uid "${SOURCE_VOLUME_UID}" --arg node "${SOURCE_NODE}" \
		--arg move "${MOVE_NAME}" --argjson copy "${SOURCE_COPY}" '
		.metadata.uid == $uid and .status.phase == "Moving" and .status.ownerNode == $node and
		.status.currentCopy == $copy and .status.activeMove == $move' >/dev/null
	assert_node_file "${SOURCE_NODE}" "${SOURCE_MOUNT}/volumes/${VOLUME_ID}/payload"
}

assert_preparation_wait() {
	local reason=$1 deadline
	kubectl wait "shiftpvmove/${MOVE_NAME}" --for=jsonpath='{.status.reason}'="${reason}" --timeout=180s
	# Observe more than two reconciliation intervals with the fault still active.
	# No cancellation, new Move, or replacement recreation may hide the gap.
	deadline=$((SECONDS + 25))
	while ((SECONDS < deadline)); do
		kubectl get "shiftpvmove/${MOVE_NAME}" -o json | jq -e \
			--arg uid "${MOVE_UID}" --arg reason "${reason}" --argjson preparation "${PREPARATION}" '
			.metadata.uid == $uid and .status.phase == "WaitingForDestination" and
			.status.reason == $reason and .status.preparation == $preparation' >/dev/null
		assert_no_execution_approval
		assert_preparation_source
		test "$(kubectl get "node/${SOURCE_NODE}" -o jsonpath='{.spec.unschedulable}')" = true
		kubectl -n "${TEST_NAMESPACE}" get "pod/${REPLACEMENT_POD}" -o json | jq -e --arg uid "${REPLACEMENT_UID}" '
			.metadata.uid == $uid and .metadata.deletionTimestamp == null and (.spec.nodeName // "") == "" and
			any(.spec.schedulingGates[]?; .name == "shiftpv.io/placement-hold")' >/dev/null
		kubectl -n shiftpv-system get "pod/${PLACEMENT_POD}" -o json | jq -e \
			--arg uid "${PLACEMENT_UID}" --arg move "${MOVE_UID}" --arg node "${DESTINATION_NODE}" '
			.metadata.uid == $uid and .metadata.deletionTimestamp == null and .spec.nodeName == $node and
			any(.metadata.ownerReferences[]?; .kind == "ShiftPVMove" and .uid == $move)' >/dev/null
		sleep 2
	done
}

finish_pre_copy_move() {
	local namespace=$1 restarted_node=${2:-}
	# Freeze the writer while the node returns so the original bytes and exact
	# held identities can be checked before any resumed copy or source cleanup.
	controller_down
	if [[ -n "${restarted_node}" ]]; then start_node "${restarted_node}"; fi
	release_destination_test_taint
	test "$(node_sha256 "${SOURCE_NODE}" "${SOURCE_MOUNT}/volumes/${VOLUME_ID}/payload")" = "${SOURCE_CHECKSUM}"
	test "$(kubectl get "node/${SOURCE_NODE}" -o jsonpath='{.spec.unschedulable}')" = true
	assert_preparation_source
	kubectl get "shiftpvmove/${MOVE_NAME}" -o json | jq -e --arg uid "${MOVE_UID}" --argjson preparation "${PREPARATION}" '
		.metadata.uid == $uid and .status.phase == "WaitingForDestination" and .status.preparation == $preparation' >/dev/null
	controller_up
	finish_destination_move "${namespace}"
	kubectl -n "${namespace}" wait "pod/${REPLACEMENT_POD}" --for=condition=Ready --timeout=180s
	kubectl -n "${namespace}" get "pod/${REPLACEMENT_POD}" -o json | jq -e \
		--arg uid "${REPLACEMENT_UID}" --arg node "${DESTINATION_NODE}" '
		.metadata.uid == $uid and .metadata.deletionTimestamp == null and .spec.nodeName == $node and
		all(.spec.schedulingGates[]?; .name != "shiftpv.io/placement-hold")' >/dev/null
	test "$(pod_sha256 "${namespace}" "${REPLACEMENT_POD}" /data/payload)" = "${SOURCE_CHECKSUM}"
	test "$(kubectl get "node/${SOURCE_NODE}" -o jsonpath='{.spec.unschedulable}')" = true
	kubectl get shiftpvmoves -o json | jq -e --arg volume "${VOLUME_ID}" --arg uid "${MOVE_UID}" '
		[.items[] | select(.spec.volumeID == $volume)] | length == 1 and
		all(.[]; .metadata.uid == $uid and .status.phase == "Succeeded")' >/dev/null
}

assert_destination_unavailable_wait() {
	local expected_phase=$1 expected_owner=$2
	kubectl wait "shiftpvmove/${MOVE_NAME}" --for=jsonpath='{.status.reason}'=DestinationUnavailable --timeout=180s
	test "$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.phase}')" = "${expected_phase}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${expected_owner}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.activeMove}')" = "${MOVE_NAME}"
	assert_node_file "${SOURCE_NODE}" "${SOURCE_MOUNT}/volumes/${VOLUME_ID}/payload"
	if [[ "${expected_owner}" == "${SOURCE_NODE}" ]]; then
		test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.phase}')" = Moving
	else
		test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.phase}')" = Ready
	fi
}

assert_destination_publish_metadata() {
	local namespace=$1 pod=$2 pod_uid volume_data_path placement
	pod_uid=$(kubectl -n "${namespace}" get "pod/${pod}" -o jsonpath='{.metadata.uid}')
	volume_data_path="/var/lib/kubelet/pods/${pod_uid}/volumes/kubernetes.io~csi/${PV_NAME}/vol_data.json"
	if ! docker exec "${DESTINATION_NODE}" test -f "${volume_data_path}"; then
		echo "destination kubelet CSI metadata is missing: ${volume_data_path}" >&2
		docker exec "${DESTINATION_NODE}" find "/var/lib/kubelet/pods/${pod_uid}/volumes" -maxdepth 4 -print >&2 2>/dev/null || true
		return 1
	fi
	if docker exec "${DESTINATION_NODE}" journalctl -u kubelet --no-pager 2>&1 \
		| grep -E 'failed to open volume data file.*vol_data\.json|vol_data\.json.*no such file or directory'; then
		echo 'destination kubelet reported incomplete CSI volume metadata' >&2
		return 1
	fi
	placement=$(placement_pod_for_move || true)
	if [[ -n "${placement}" ]]; then
		echo "placement hold remained after successful move: ${placement}" >&2
		return 1
	fi
}

finish_destination_move() {
	local namespace=$1 pod checksum source_copy
	kubectl wait "shiftpvmove/${MOVE_NAME}" --for=jsonpath='{.status.phase}'=Succeeded --timeout=600s
	kubectl -n "${namespace}" rollout status deployment/writer --timeout=180s
	pod=$(kubectl -n "${namespace}" get pod -l "app=${namespace}" -o jsonpath='{.items[0].metadata.name}')
	test "$(kubectl -n "${namespace}" get "pod/${pod}" -o jsonpath='{.spec.nodeName}')" = "${DESTINATION_NODE}"
	test "$(kubectl -n "${namespace}" get "pod/${pod}" -o jsonpath='{.metadata.annotations.shiftpv\.io/placement}')" = owner
	test -z "$(kubectl -n "${namespace}" get "pod/${pod}" -o jsonpath='{.spec.schedulingGates[?(@.name=="shiftpv.io/placement-hold")].name}')"
	checksum=$(pod_sha256 "${namespace}" "${pod}" /data/payload)
	test "${checksum}" = "${SOURCE_CHECKSUM}"
	test "$(kubectl -n "${namespace}" get pvc/data -o jsonpath='{.metadata.uid}')" = "${PVC_UID}"
	test "$(kubectl -n "${namespace}" get pvc/data -o jsonpath='{.spec.volumeName}')" = "${PV_NAME}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.phase}')" = Ready
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${DESTINATION_NODE}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.activeMove}')" = ""
	assert_node_absent "${SOURCE_NODE}" "${SOURCE_MOUNT}/volumes/${VOLUME_ID}"
	assert_node_file "${DESTINATION_NODE}" "${DESTINATION_MOUNT}/volumes/${VOLUME_ID}/payload"
	source_copy=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.sourceCopy.copyID}')
	test -n "${source_copy}"
	assert_node_absent "${SOURCE_NODE}" "${SOURCE_MOUNT}/.shiftpv/retired/${source_copy}"
	assert_cleanup_journal "shiftpvmove/${MOVE_NAME}" MoveSource "${VOLUME_ID}" "${source_copy}" ShiftPVMove
	assert_destination_publish_metadata "${namespace}" "${pod}"
}

assert_source_cleanup_interruption() {
	local stopped_node=$1 expected_reason=$2 phase cleanup_phase deadline
	if [[ "${stopped_node}" == "${SOURCE_NODE}" ]]; then
		# The journal did not exist before the fault. Seeing it Pending or Running
		# proves the restarted controller attempted exact source cleanup while the
		# source node was unavailable, without completing or abandoning the hold.
		deadline=$((SECONDS + 180))
		while ((SECONDS < deadline)); do
			phase=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.phase}')
			cleanup_phase=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.phase}' 2>/dev/null || true)
			if [[ "${phase}" == WaitingForDestinationPublish || "${phase}" == CleaningSource ]] &&
				[[ "${cleanup_phase}" == Pending || "${cleanup_phase}" == Running ]]; then
				break
			fi
			if [[ "${phase}" == Blocked || "${phase}" == Succeeded || "${cleanup_phase}" == Completed || "${cleanup_phase}" == NeedsReview ]]; then
				echo "source cleanup crossed an unsafe terminal boundary while the source node was unavailable: phase=${phase} cleanup=${cleanup_phase}" >&2
				return 1
			fi
			sleep 1
		done
		if [[ "${cleanup_phase}" != Pending && "${cleanup_phase}" != Running ]]; then
			echo "controller did not attempt source cleanup while the source node was unavailable: phase=${phase} cleanup=${cleanup_phase}" >&2
			return 1
		fi
	else
		kubectl wait "shiftpvmove/${MOVE_NAME}" --for=jsonpath='{.status.reason}'="${expected_reason}" --timeout=180s
	fi
	for _ in {1..5}; do
		phase=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.phase}')
		cleanup_phase=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.cleanup.status.phase}' 2>/dev/null || true)
		if [[ "${phase}" != WaitingForDestinationPublish && "${phase}" != CleaningSource ]]; then
			echo "Move left the source-cleanup boundary while node was unavailable: phase=${phase}" >&2
			return 1
		fi
		if [[ "${stopped_node}" == "${SOURCE_NODE}" ]]; then
			if [[ "${cleanup_phase}" != Pending && "${cleanup_phase}" != Running ]]; then
				echo "source cleanup left its retryable journal while the source node was unavailable: cleanup=${cleanup_phase}" >&2
				return 1
			fi
		elif [[ -n "${cleanup_phase}" ]]; then
			echo "source cleanup started while destination node ${stopped_node} was unavailable: cleanup=${cleanup_phase}" >&2
			return 1
		fi
		test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.phase}')" = Ready
		test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${DESTINATION_NODE}"
		test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.activeMove}')" = "${MOVE_NAME}"
		test "$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.capacityApproved}')" = true
		sleep 1
	done
	assert_node_file "${SOURCE_NODE}" "${SOURCE_MOUNT}/volumes/${VOLUME_ID}/payload"
	assert_node_file "${DESTINATION_NODE}" "${DESTINATION_MOUNT}/volumes/${VOLUME_ID}/payload"
}

cleanup_case() {
	local namespace=$1
	kubectl delete "namespace/${namespace}" --wait=true --timeout=180s
	kubectl uncordon "${SOURCE_NODE}" >/dev/null 2>&1 || true
	kubectl uncordon "${DESTINATION_NODE}" >/dev/null 2>&1 || true
}

run_pre_copy_source_restart_case() {
	local namespace=$1 payload=$2
	TEST_NAMESPACE="${namespace}"
	create_source_workload "${namespace}" "${payload}"
	kubectl cordon "${SOURCE_NODE}"
	MOVE_NAME=$(wait_for_move "${VOLUME_ID}" 120 0.2)
	pause_before_copy
	stop_node "${SOURCE_NODE}"
	controller_up
	assert_preparation_wait SourceUnavailable
	finish_pre_copy_move "${namespace}" "${SOURCE_NODE}"
	echo "mobility pre-copy source restart continuation passed: volume=${VOLUME_ID} move=${MOVE_NAME} replacementUID=${REPLACEMENT_UID} checksum=${SOURCE_CHECKSUM}"
	cleanup_case "${namespace}"
}

run_copying_source_restart_case() {
	local namespace=$1 payload=$2
	TEST_NAMESPACE="${namespace}"
	create_source_workload "${namespace}" "${payload}"
	kubectl cordon "${SOURCE_NODE}"
	MOVE_NAME=$(wait_for_move "${VOLUME_ID}" 120 0.2)
	pause_at_phase Copying
	stop_node "${SOURCE_NODE}"
	controller_up
	assert_blocked_source_authority SourceUnavailable
	start_node "${SOURCE_NODE}"
	recover_source "${namespace}"
	echo "mobility Copying source restart recovery passed: volume=${VOLUME_ID} move=${MOVE_NAME} checksum=${SOURCE_CHECKSUM}"
	cleanup_case "${namespace}"
}

run_pre_copy_destination_restart_case() {
	local namespace=$1 payload=$2
	TEST_NAMESPACE="${namespace}"
	create_source_workload "${namespace}" "${payload}"
	kubectl cordon "${SOURCE_NODE}"
	MOVE_NAME=$(wait_for_move "${VOLUME_ID}" 120 0.2)
	pause_before_copy
	stop_node "${DESTINATION_NODE}"
	# Keep the source cordoned throughout recovery. Only the controller may
	# schedule onto it while the selected destination is unavailable.
	allow_controller_on_cordoned_source
	controller_up
	test "$(kubectl get "node/${SOURCE_NODE}" -o jsonpath='{.spec.unschedulable}')" = true
	assert_preparation_wait PreparedDestinationUnavailable
	finish_pre_copy_move "${namespace}" "${DESTINATION_NODE}"
	restore_controller_tolerations
	kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=180s
	echo "mobility pre-copy destination restart continuation passed: volume=${VOLUME_ID} move=${MOVE_NAME} replacementUID=${REPLACEMENT_UID} checksum=${SOURCE_CHECKSUM}"
	cleanup_case "${namespace}"
}

run_pre_copy_destination_taint_case() {
	local namespace=$1 payload=$2
	TEST_NAMESPACE="${namespace}"
	create_source_workload "${namespace}" "${payload}"
	kubectl cordon "${SOURCE_NODE}"
	MOVE_NAME=$(wait_for_move "${VOLUME_ID}" 120 0.2)
	pause_before_copy
	kubectl get "node/${DESTINATION_NODE}" -o json | jq -e --arg key "${DESTINATION_TAINT_KEY}" \
		'all(.spec.taints[]?; .key != $key or .effect != "NoSchedule")' >/dev/null
	# Arm exact cleanup before sending in case the successful write loses its response.
	DESTINATION_TAINT_VALUE=${MOVE_UID}
	kubectl taint node "${DESTINATION_NODE}" "${DESTINATION_TAINT_KEY}=${DESTINATION_TAINT_VALUE}:NoSchedule"
	allow_controller_on_cordoned_source
	controller_up
	assert_preparation_wait PreparedDestinationUnavailable
	finish_pre_copy_move "${namespace}"
	restore_controller_tolerations
	kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=180s
	echo "mobility pre-copy destination taint continuation passed: volume=${VOLUME_ID} move=${MOVE_NAME} replacementUID=${REPLACEMENT_UID} checksum=${SOURCE_CHECKSUM}"
	cleanup_case "${namespace}"
}

run_destination_restart_case() {
	local namespace=$1 payload=$2 fault_phase=$3 expected_owner=$4
	TEST_NAMESPACE="${namespace}"
	create_source_workload "${namespace}" "${payload}"
	kubectl cordon "${SOURCE_NODE}"
	MOVE_NAME=$(wait_for_move "${VOLUME_ID}" 120 0.2)
	pause_at_phase "${fault_phase}"
	if [[ "${expected_owner}" == "${DESTINATION_NODE}" ]]; then
		test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${DESTINATION_NODE}"
	else
		test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${SOURCE_NODE}"
	fi
	stop_node "${DESTINATION_NODE}"
	# The transaction is already past preflight. Uncordoning the source lets the
	# controller run while the selected destination is unavailable; it does not
	# change the persisted destination or volume authority.
	kubectl uncordon "${SOURCE_NODE}"
	controller_up
	assert_destination_unavailable_wait "${fault_phase}" "${expected_owner}"
	start_node "${DESTINATION_NODE}"
	finish_destination_move "${namespace}"
	echo "mobility ${fault_phase} destination restart continuation passed: volume=${VOLUME_ID} move=${MOVE_NAME} checksum=${SOURCE_CHECKSUM}"
	cleanup_case "${namespace}"
}

run_cleaning_source_restart_case() {
	local namespace=$1 payload=$2 stopped_node=$3 expected_reason=$4
	TEST_NAMESPACE="${namespace}"
	create_source_workload "${namespace}" "${payload}"
	kubectl cordon "${SOURCE_NODE}"
	MOVE_NAME=$(wait_for_move "${VOLUME_ID}" 120 0.2)
	pause_before_source_cleanup "${namespace}"
	stop_node "${stopped_node}"
	if [[ "${stopped_node}" == "${DESTINATION_NODE}" ]]; then
		kubectl uncordon "${SOURCE_NODE}"
	fi
	controller_up
	assert_source_cleanup_interruption "${stopped_node}" "${expected_reason}"
	start_node "${stopped_node}"
	finish_destination_move "${namespace}"
	echo "mobility source-cleanup boundary ${stopped_node} restart continuation passed: volume=${VOLUME_ID} move=${MOVE_NAME} checksum=${SOURCE_CHECKSUM}"
	cleanup_case "${namespace}"
}

# Restart cases use real Kind container stop/start; destination-taint changes
# only an owned NoSchedule taint. Cleanup cases stop the controller before it
# can create a cleanup executor.
helm upgrade shiftpv "${ROOT_DIR}/charts/shiftpv" \
	--namespace shiftpv-system \
	--reuse-values \
	--set mobility.interval=10s \
	--wait \
	--timeout 5m

ALL_CASES='source destination destination-taint copying-source copying-destination promoting-destination committed-destination cleaning-source cleaning-destination'

run_named_case() {
	case $1 in
	source)
		run_pre_copy_source_restart_case shiftpv-node-restart-source 'ShiftPV pre-copy source node restart continuation'
		;;
	destination)
		run_pre_copy_destination_restart_case shiftpv-node-restart-destination 'ShiftPV pre-copy destination node restart continuation'
		;;
	destination-taint)
		run_pre_copy_destination_taint_case shiftpv-destination-taint 'ShiftPV pre-copy destination NoSchedule continuation'
		;;
	copying-source)
		run_copying_source_restart_case shiftpv-node-restart-copying-source 'ShiftPV Copying source restart recovery'
		;;
	copying-destination)
		run_destination_restart_case shiftpv-node-restart-copying-destination 'ShiftPV Copying destination restart continuation' Copying "${SOURCE_NODE}"
		;;
	promoting-destination)
		run_destination_restart_case shiftpv-node-restart-promoting-destination 'ShiftPV Promoting destination restart continuation' Promoting "${SOURCE_NODE}"
		;;
	committed-destination)
		run_destination_restart_case shiftpv-node-restart-committed-destination 'ShiftPV committed destination restart continuation' WaitingForDestinationPublish "${DESTINATION_NODE}"
		;;
	cleaning-source)
		run_cleaning_source_restart_case shiftpv-node-restart-cleaning-source 'ShiftPV CleaningSource source restart continuation' "${SOURCE_NODE}" ''
		;;
	cleaning-destination)
		run_cleaning_source_restart_case shiftpv-node-restart-cleaning-destination 'ShiftPV CleaningSource destination restart continuation' "${DESTINATION_NODE}" DestinationUnavailable
		;;
	*)
		echo "unsupported MOBILITY_NODE_RESTART_CASE: $1" >&2
		exit 1
		;;
	esac
}

# Each case is independent, so CI splits them across jobs. The selector takes a
# comma- or space-separated list; `all` keeps the full serialized sweep in the
# order the cases were written.
SELECTED_CASES=${MOBILITY_NODE_RESTART_CASE:-all}
if [[ "${SELECTED_CASES}" == all ]]; then
	SELECTED_CASES=${ALL_CASES}
fi
for selected_case in ${SELECTED_CASES//,/ }; do
	run_named_case "${selected_case}"
done

echo 'ShiftPV mobility node-container restart recovery passed'
