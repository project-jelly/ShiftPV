#!/usr/bin/env bash
set -uo pipefail

usage() {
	cat <<'EOF'
Read-only safety gate for ShiftPV real-node qualification.

Required environment:
  KUBECTL_CONTEXT
  SOURCE_NODE
  DESTINATION_NODE
  FAULT_NODE
  FAULT_SSH_TARGET
  SOURCE_SSH_TARGET           required by the fault stages that run this preflight
  DESTINATION_SSH_TARGET      required by the fault stages that run this preflight
  EXPECTED_CONTROLLER_IMAGE   immutable image reference containing @sha256:
  EXPECTED_NODE_IMAGE         immutable image reference containing @sha256:

Optional environment:
  SYSTEM_NAMESPACE=shiftpv-system
  CONTROLLER_DEPLOYMENT=shiftpv-controller
  NODE_DAEMONSET=shiftpv-node
  STORAGE_CLASS=shiftpv
  EXPECTED_NODE_COUNT=2
  MAX_INVENTORY_AGE_SECONDS=120
  SSH_CONNECT_TIMEOUT=5
  EXPECTED_NON_DAEMONSET_PODS_SHA256=
  NODE_RUNTIME_STOP_CMD                 used by the fault stages that run this
  NODE_RUNTIME_START_CMD                preflight; default to the MicroK8s
  NODE_RUNTIME_JOURNAL_UNIT             runtime, kubelite, and journal unit

The script does not cordon, drain, restart, reboot, power off, create, patch, or
delete anything. By default it requires a fault node without non-DaemonSet
workloads. A reviewed shared-node maintenance window can instead provide the
SHA-256 of the exact sorted workload inventory. Any inventory change blocks the
run.
EOF
}

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
	usage
	exit 0
fi

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
# shellcheck source=test/e2e/real-node/lib.sh
source "${ROOT_DIR}/test/e2e/real-node/lib.sh"

SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE:-shiftpv-system}
CONTROLLER_DEPLOYMENT=${CONTROLLER_DEPLOYMENT:-shiftpv-controller}
NODE_DAEMONSET=${NODE_DAEMONSET:-shiftpv-node}
STORAGE_CLASS=${STORAGE_CLASS:-shiftpv}
EXPECTED_NODE_COUNT=${EXPECTED_NODE_COUNT:-2}
MAX_INVENTORY_AGE_SECONDS=${MAX_INVENTORY_AGE_SECONDS:-120}
SSH_CONNECT_TIMEOUT=${SSH_CONNECT_TIMEOUT:-5}
EXPECTED_NON_DAEMONSET_PODS_SHA256=${EXPECTED_NON_DAEMONSET_PODS_SHA256:-}

blocked=0
block() {
	printf 'BLOCK %s\n' "$*" >&2
	blocked=$((blocked + 1))
}

pass() {
	printf 'PASS  %s\n' "$*"
}

for command in kubectl jq ssh; do
	if ! command -v "${command}" >/dev/null 2>&1; then
		block "required command is unavailable: ${command}"
	fi
done

for variable in \
	KUBECTL_CONTEXT SOURCE_NODE DESTINATION_NODE FAULT_NODE FAULT_SSH_TARGET \
	SOURCE_SSH_TARGET DESTINATION_SSH_TARGET \
	EXPECTED_CONTROLLER_IMAGE EXPECTED_NODE_IMAGE; do
	if [[ -z ${!variable:-} ]]; then
		block "required environment is empty: ${variable}"
	fi
done

if ((blocked > 0)); then
	printf 'PREFLIGHT_BLOCKED count=%d\n' "${blocked}" >&2
	exit 1
fi

if [[ "${SOURCE_NODE}" == "${DESTINATION_NODE}" ]]; then
	block 'SOURCE_NODE and DESTINATION_NODE must differ'
fi
if [[ "${FAULT_NODE}" != "${SOURCE_NODE}" && "${FAULT_NODE}" != "${DESTINATION_NODE}" ]]; then
	block 'FAULT_NODE must be SOURCE_NODE or DESTINATION_NODE'
fi
if [[ ( "${FAULT_NODE}" == "${SOURCE_NODE}" && "${FAULT_SSH_TARGET}" != "${SOURCE_SSH_TARGET}" ) ||
	( "${FAULT_NODE}" == "${DESTINATION_NODE}" && "${FAULT_SSH_TARGET}" != "${DESTINATION_SSH_TARGET}" ) ]]; then
	block 'FAULT_SSH_TARGET must match the reviewed SSH role for FAULT_NODE'
fi
case ${EXPECTED_CONTROLLER_IMAGE} in
*@sha256:*) pass 'controller image expectation is immutable' ;;
*) block 'EXPECTED_CONTROLLER_IMAGE must contain @sha256:' ;;
esac
case ${EXPECTED_NODE_IMAGE} in
*@sha256:*) pass 'node image expectation is immutable' ;;
*) block 'EXPECTED_NODE_IMAGE must contain @sha256:' ;;
esac

if ! cluster_json=$(k get nodes -o json 2>/dev/null); then
	block "cannot read nodes through context ${KUBECTL_CONTEXT}"
	printf 'PREFLIGHT_BLOCKED count=%d\n' "${blocked}" >&2
	exit 1
fi
pass "context is reachable: ${KUBECTL_CONTEXT}"

node_count=$(jq '.items | length' <<<"${cluster_json}")
if [[ "${node_count}" == "${EXPECTED_NODE_COUNT}" ]]; then
	pass "node count is exactly ${EXPECTED_NODE_COUNT}"
else
	block "node count is ${node_count}; expected ${EXPECTED_NODE_COUNT}"
fi

for node in "${SOURCE_NODE}" "${DESTINATION_NODE}"; do
	matches=$(jq --arg node "${node}" '[.items[] | select(.metadata.name == $node)] | length' <<<"${cluster_json}")
	if [[ "${matches}" != 1 ]]; then
		block "node identity is not exact: ${node} matches=${matches}"
		continue
	fi
	ready=$(jq -r --arg node "${node}" '.items[] | select(.metadata.name == $node) | [.status.conditions[] | select(.type == "Ready")][0].status // ""' <<<"${cluster_json}")
	unschedulable=$(jq -r --arg node "${node}" '.items[] | select(.metadata.name == $node) | (.spec.unschedulable // false)' <<<"${cluster_json}")
	pressure=$(jq -r --arg node "${node}" '[.items[] | select(.metadata.name == $node) | .status.conditions[] | select((.type == "DiskPressure" or .type == "MemoryPressure" or .type == "PIDPressure") and .status == "True")] | length' <<<"${cluster_json}")
	if [[ "${ready}" == True && "${unschedulable}" == false && "${pressure}" == 0 ]]; then
		pass "node is Ready, schedulable, and pressure-free: ${node}"
	else
		block "node is not healthy: ${node} ready=${ready} unschedulable=${unschedulable} pressure=${pressure}"
	fi
done

fault_role=$(jq -r --arg node "${FAULT_NODE}" '.items[] | select(.metadata.name == $node) | (.metadata.labels // {}) |
	has("node.kubernetes.io/microk8s-controlplane") or has("node-role.kubernetes.io/control-plane") or has("node-role.kubernetes.io/master")' <<<"${cluster_json}")
if [[ "${fault_role}" == false ]]; then
	pass "fault node is not labelled as a control-plane node: ${FAULT_NODE}"
else
	block "fault node carries a control-plane role: ${FAULT_NODE}"
fi

controller_json=$(k -n "${SYSTEM_NAMESPACE}" get "deployment/${CONTROLLER_DEPLOYMENT}" -o json 2>/dev/null || true)
controller_image=$(jq -r '.spec.template.spec.containers[]? | select(.name == "shiftpv-controller") | .image' <<<"${controller_json}" 2>/dev/null || true)
controller_available=$(jq -r '.status.availableReplicas // 0' <<<"${controller_json}" 2>/dev/null || true)
controller_replicas=$(jq -r '.spec.replicas // 0' <<<"${controller_json}" 2>/dev/null || true)
if [[ "${controller_image}" == "${EXPECTED_CONTROLLER_IMAGE}" && "${controller_available}" == "${controller_replicas}" && "${controller_replicas}" -gt 0 ]]; then
	pass "controller is available with the exact candidate image"
else
	block "controller candidate mismatch or unavailable: image=${controller_image:-missing} available=${controller_available:-0}/${controller_replicas:-0}"
fi

node_json=$(k -n "${SYSTEM_NAMESPACE}" get "daemonset/${NODE_DAEMONSET}" -o json 2>/dev/null || true)
node_image=$(jq -r '.spec.template.spec.containers[]? | select(.name == "shiftpv-node") | .image' <<<"${node_json}" 2>/dev/null || true)
node_ready=$(jq -r '.status.numberReady // 0' <<<"${node_json}" 2>/dev/null || true)
node_desired=$(jq -r '.status.desiredNumberScheduled // 0' <<<"${node_json}" 2>/dev/null || true)
if [[ "${node_image}" == "${EXPECTED_NODE_IMAGE}" && "${node_ready}" == "${node_desired}" && "${node_desired}" == "${EXPECTED_NODE_COUNT}" ]]; then
	pass "node DaemonSet is ready with the exact candidate image"
else
	block "node candidate mismatch or unavailable: image=${node_image:-missing} ready=${node_ready:-0}/${node_desired:-0}"
fi

for argument in helper-image mobility-helper-image; do
	if jq -e --arg expected "--${argument}=${EXPECTED_CONTROLLER_IMAGE}" '.spec.template.spec.containers[]? | select(.name == "shiftpv-controller") | .args | index($expected) != null' <<<"${controller_json}" >/dev/null 2>&1; then
		pass "${argument} uses the exact controller candidate image"
	else
		block "${argument} does not use the exact controller candidate image"
	fi
done

crd_names=$(k get crd -o json 2>/dev/null | jq -r '[.items[].metadata.name | select(endswith(".shiftpv.io"))] | sort | join(",")')
expected_crds='shiftpvmoves.shiftpv.io,shiftpvpools.shiftpv.io,shiftpvvolumes.shiftpv.io'
if [[ "${crd_names}" == "${expected_crds}" ]]; then
	pass 'durable ShiftPV API surface contains exactly Pool, Volume, and Move'
else
	block "unexpected ShiftPV CRD surface: ${crd_names:-none}"
fi

storage_class_json=$(k get "storageclass/${STORAGE_CLASS}" -o json 2>/dev/null || true)
provisioner=$(jq -r '.provisioner // ""' <<<"${storage_class_json}" 2>/dev/null || true)
default_class=$(jq -r '.metadata.annotations["storageclass.kubernetes.io/is-default-class"] // "false"' <<<"${storage_class_json}" 2>/dev/null || true)
if [[ "${provisioner}" == csi.shiftpv.io && "${default_class}" != true ]]; then
	pass "storage class is non-default and owned by ShiftPV: ${STORAGE_CLASS}"
else
	block "storage class is unsafe: provisioner=${provisioner:-missing} default=${default_class:-missing}"
fi

volume_count=$(k get shiftpvvolumes -o json 2>/dev/null | jq '.items | length' 2>/dev/null || printf 'unknown')
move_json=$(k get shiftpvmoves -o json 2>/dev/null || true)
move_count=$(jq '.items | length' <<<"${move_json}" 2>/dev/null || printf 'unknown')
if ! jq -e '.items | type == "array"' <<<"${move_json}" >/dev/null 2>&1; then
	block 'cannot completely observe existing Move journals'
fi
unsettled_moves=$(jq -r '
	.items[]?
	| select(
		((.status.phase == "Succeeded" and .status.cleanup.status.phase == "Completed") or
		 (.status.phase == "Blocked" and .status.recoveryPhase == "Recovered" and
		  (.status.capacityApproved // false) == false and .status.capacityReason == "RecoverySettled")) and
		((.metadata.finalizers // []) | length) == 0
	  | not)
	| "\(.metadata.name):phase=\(.status.phase // "missing"),recovery=\(.status.recoveryPhase // "missing"),cleanup=\(.status.cleanup.status.phase // "missing"),capacityApproved=\(.status.capacityApproved // false),capacityReason=\(.status.capacityReason // "missing"),finalizers=\((.metadata.finalizers // []) | join(","))"' \
	<<<"${move_json}" 2>/dev/null || true)
if [[ "${volume_count}" == 0 && -z "${unsettled_moves}" ]]; then
	pass "no pre-existing Volume or unsettled Move can be confused with the qualification workload: settledMoves=${move_count}"
else
	block "ShiftPV state is unsafe: volumes=${volume_count} moves=${move_count}"
	while IFS= read -r move; do
		[[ -z "${move}" ]] || printf '      %s\n' "${move}" >&2
	done <<<"${unsettled_moves}"
fi

pool_json=$(k get shiftpvpools -o json 2>/dev/null || true)
now_epoch=$(date -u +%s)
for node in "${SOURCE_NODE}" "${DESTINATION_NODE}"; do
	pool_count=$(jq --arg node "${node}" '[.items[] | select(.spec.nodeName == $node)] | length' <<<"${pool_json}" 2>/dev/null || printf 'unknown')
	if [[ "${pool_count}" != 1 ]]; then
		block "pool identity is not exact for node ${node}: matches=${pool_count}"
		continue
	fi
	pool_name=$(jq -r --arg node "${node}" '.items[] | select(.spec.nodeName == $node) | .metadata.name' <<<"${pool_json}")
	pool_ready=$(jq -r --arg node "${node}" '.items[] | select(.spec.nodeName == $node) | [.status.conditions[]? | select(.type == "Ready")][0].status // ""' <<<"${pool_json}")
	inventory_valid=$(jq -r --arg node "${node}" '.items[] | select(.spec.nodeName == $node) | (.status.inventory.valid // false)' <<<"${pool_json}")
	inventory_truncated=$(jq -r --arg node "${node}" '.items[] | select(.spec.nodeName == $node) | (.status.inventory.truncated // false)' <<<"${pool_json}")
	inventory_age=$(jq -r --arg node "${node}" --argjson now "${now_epoch}" '.items[] | select(.spec.nodeName == $node) | if .status.inventory.observedAt then ($now - (.status.inventory.observedAt | fromdateiso8601)) else 999999 end' <<<"${pool_json}" 2>/dev/null || printf '999999')
	if [[ "${pool_ready}" == True && "${inventory_valid}" == true && "${inventory_truncated}" == false && "${inventory_age}" -ge 0 && "${inventory_age}" -le "${MAX_INVENTORY_AGE_SECONDS}" ]]; then
		pass "pool is Ready with fresh, valid inventory: ${pool_name} age=${inventory_age}s"
	else
		block "pool is unsafe: ${pool_name} ready=${pool_ready} valid=${inventory_valid} truncated=${inventory_truncated} inventoryAge=${inventory_age}s"
	fi
done

# Soak alternates its source. Validate both reviewed SSH roles, not only the
# initial FAULT_NODE, before either resident executor can be interrupted.
for role in source destination; do
	if [[ "${role}" == source ]]; then expected_node=${SOURCE_NODE}; else expected_node=${DESTINATION_NODE}; fi
	if remote_hostname=$(ssh_node "${role}" 'set -e; hostname; sudo -n true; command -v flock >/dev/null; command -v timeout >/dev/null' 2>/dev/null); then
		if [[ "${remote_hostname}" == "${expected_node}" ]]; then
			pass "SSH, sudo, flock and timeout reach the exact ${role} node: ${expected_node}"
		else
			block "SSH ${role} identity mismatch: expected=${expected_node} actual=${remote_hostname:-missing}"
		fi
	else
		block "SSH, sudo, flock or timeout is unavailable for ${role}: ${expected_node}"
	fi
done

if ! pods_json=$(k get pods -A --field-selector "spec.nodeName=${FAULT_NODE}" -o json 2>/dev/null) ||
	! jq -e '.items | type == "array"' <<<"${pods_json}" >/dev/null 2>&1; then
	block "cannot completely observe workloads on ${FAULT_NODE}"
	pods_json='{"items":[]}'
fi
unsafe_pods=$(jq -r '
	.items[]
	| select(.status.phase != "Succeeded" and .status.phase != "Failed")
	| select((.metadata.ownerReferences[0].kind // "") != "DaemonSet")
	| "\(.metadata.namespace)/\(.metadata.name) owner=\(.metadata.ownerReferences[0].kind // "none")/\(.metadata.ownerReferences[0].name // "none")"' <<<"${pods_json}" | sort)
if [[ -z "${unsafe_pods}" ]]; then
	pass "fault node has no non-DaemonSet workload: ${FAULT_NODE}"
elif [[ -n "${EXPECTED_NON_DAEMONSET_PODS_SHA256}" ]]; then
	if command -v sha256sum >/dev/null 2>&1; then
		actual_pods_sha256=$(printf '%s\n' "${unsafe_pods}" | sha256sum | awk '{print $1}')
	else
		actual_pods_sha256=$(printf '%s\n' "${unsafe_pods}" | shasum -a 256 | awk '{print $1}')
	fi
	if [[ "${EXPECTED_NON_DAEMONSET_PODS_SHA256}" =~ ^[0-9a-f]{64}$ && "${actual_pods_sha256}" == "${EXPECTED_NON_DAEMONSET_PODS_SHA256}" ]]; then
		workload_count=$(wc -l <<<"${unsafe_pods}" | tr -d ' ')
		pass "fault node matches the reviewed shared-workload baseline: ${FAULT_NODE} count=${workload_count} sha256=${actual_pods_sha256}"
		while IFS= read -r pod; do
			[[ -z "${pod}" ]] || printf '      %s\n' "${pod}"
		done <<<"${unsafe_pods}"
	else
		block "fault node shared-workload baseline changed: ${FAULT_NODE} expected=${EXPECTED_NON_DAEMONSET_PODS_SHA256} actual=${actual_pods_sha256}"
	fi
else
	block "fault node still hosts non-DaemonSet workloads: ${FAULT_NODE}"
	while IFS= read -r pod; do
		[[ -z "${pod}" ]] || printf '      %s\n' "${pod}" >&2
	done <<<"${unsafe_pods}"
fi

if ((blocked > 0)); then
	printf 'PREFLIGHT_BLOCKED count=%d context=%s source=%s destination=%s fault=%s\n' \
		"${blocked}" "${KUBECTL_CONTEXT}" "${SOURCE_NODE}" "${DESTINATION_NODE}" "${FAULT_NODE}" >&2
	exit 1
fi

printf 'PREFLIGHT_OK context=%s source=%s destination=%s fault=%s\n' \
	"${KUBECTL_CONTEXT}" "${SOURCE_NODE}" "${DESTINATION_NODE}" "${FAULT_NODE}"
