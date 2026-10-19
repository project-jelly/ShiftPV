#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
# shellcheck source=test/e2e/kind/lib/cluster.sh
source "${ROOT_DIR}/test/e2e/kind/lib/cluster.sh"
# shellcheck source=test/e2e/kind/lib/wait.sh
source "${ROOT_DIR}/test/e2e/kind/lib/wait.sh"
CLUSTER_NAME=${CLUSTER_NAME:-shiftpv-mobility-e2e}
NODE_IMAGE=${NODE_IMAGE:-$(kind_node_image)}
KEEP_CLUSTER=${KEEP_CLUSTER:-0}
PHASE_TIMEOUT_SECONDS=${PHASE_TIMEOUT_SECONDS:-180}
# shellcheck source=test/e2e/kind/cleanup-journal.sh
source "${ROOT_DIR}/test/e2e/kind/cleanup-journal.sh"
# shellcheck source=test/e2e/kind/mobility/controller.sh
source "${ROOT_DIR}/test/e2e/kind/mobility/controller.sh"
# shellcheck source=test/e2e/kind/mobility/recovery.sh
source "${ROOT_DIR}/test/e2e/kind/mobility/recovery.sh"
# shellcheck source=test/e2e/kind/mobility/preflight.sh
source "${ROOT_DIR}/test/e2e/kind/mobility/preflight.sh"
# shellcheck source=test/e2e/kind/mobility/cleanup-lifecycle.sh
source "${ROOT_DIR}/test/e2e/kind/mobility/cleanup-lifecycle.sh"
# shellcheck source=test/e2e/kind/mobility/terminal-journal-gc.sh
source "${ROOT_DIR}/test/e2e/kind/mobility/terminal-journal-gc.sh"

for command in docker kind kubectl helm sed jq shasum; do
	command -v "${command}" >/dev/null || {
		echo "required command not found: ${command}" >&2
		exit 1
	}
done

mkdir -p "${ROOT_DIR}/.tmp"
WORK_DIR=$(mktemp -d "${ROOT_DIR}/.tmp/shiftpv-mobility.XXXXXX")
WORKER_A_POOL="${WORK_DIR}/worker-a"
WORKER_B_POOL="${WORK_DIR}/worker-b"
mkdir -p "${WORKER_A_POOL}" "${WORKER_B_POOL}"
export KUBECONFIG="${E2E_KUBECONFIG:-${WORK_DIR}/kubeconfig}"

trap delete_cluster_unless_kept EXIT

assert_move_diagnostics() {
	local move=$1 expected_phase=$2 expected_reason=$3 expected_event=$4
	local phase reason message transition_time progress_time event_reasons="" deadline
	phase=$(kubectl get "shiftpvmove/${move}" -o jsonpath='{.status.phase}')
	reason=$(kubectl get "shiftpvmove/${move}" -o jsonpath='{.status.reason}')
	message=$(kubectl get "shiftpvmove/${move}" -o jsonpath='{.status.message}')
	transition_time=$(kubectl get "shiftpvmove/${move}" -o jsonpath='{.status.lastTransitionTime}')
	progress_time=$(kubectl get "shiftpvmove/${move}" -o jsonpath='{.status.lastProgressTime}')
	test "${phase}" = "${expected_phase}"
	test "${reason}" = "${expected_reason}"
	test -n "${message}"
	[[ "${transition_time}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T ]]
	[[ "${progress_time}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T ]]
	if [[ "${expected_phase}" == Blocked ]]; then
		grep -Fq 'operator action required' <<<"${message}"
	else
		grep -Eq 'automatic retry|no operator action is required' <<<"${message}"
	fi
	kubectl get "shiftpvmove/${move}" | sed -n '1p' | grep -q 'REASON.*LASTPROGRESS'
	deadline=$((SECONDS + 60))
	while ((SECONDS < deadline)); do
		event_reasons=$(kubectl -n default get events \
			--field-selector "involvedObject.kind=ShiftPVMove,involvedObject.name=${move}" \
			-o jsonpath='{range .items[*]}{.reason}{"\n"}{end}' 2>/dev/null || true)
		grep -Fxq "${expected_event}" <<<"${event_reasons}" && return
		sleep 1
	done
	echo "missing ShiftPVMove event ${expected_event}; observed: ${event_reasons}" >&2
	return 1
}

sed \
	-e "s|__WORKER_A_POOL__|${WORKER_A_POOL}|g" \
	-e "s|__WORKER_B_POOL__|${WORKER_B_POOL}|g" \
	"${ROOT_DIR}/test/e2e/kind/cluster.yaml.tpl" >"${WORK_DIR}/cluster.yaml"
sed \
	-e "s|__WORKER_A_NODE__|${CLUSTER_NAME}-worker|g" \
	-e "s|__WORKER_B_NODE__|${CLUSTER_NAME}-worker2|g" \
	"${ROOT_DIR}/test/e2e/kind/pools.yaml.tpl" >"${WORK_DIR}/pools.yaml"

kind create cluster --name "${CLUSTER_NAME}" --image "${NODE_IMAGE}" --config "${WORK_DIR}/cluster.yaml"
docker build \
	--target combined \
	--build-arg CONTROLLER_VERSION=dev \
	--build-arg NODE_VERSION=dev \
	-f "${ROOT_DIR}/build/package/Dockerfile" \
	-t shiftpv:dev \
	"${ROOT_DIR}"
docker build \
	-f "${ROOT_DIR}/test/e2e/kind/mobility/fault-helper.Dockerfile" \
	-t shiftpv-mobility-fault:dev \
	"${ROOT_DIR}/test/e2e/kind/mobility"
kind load docker-image shiftpv:dev --name "${CLUSTER_NAME}"
kind load docker-image shiftpv-mobility-fault:dev --name "${CLUSTER_NAME}"

helm upgrade --install shiftpv "${ROOT_DIR}/charts/shiftpv" \
	--namespace shiftpv-system \
	--create-namespace \
	--values "${ROOT_DIR}/test/e2e/kind/values.yaml" \
	--set helperPod.image=shiftpv-mobility-fault:dev \
	--set mobility.helperImage=shiftpv-mobility-fault:dev \
	--set mobility.interval=10s \
	--wait \
	--timeout 5m
WEBHOOK_SECRET=shiftpv-webhook-tls
WEBHOOK_SERVICE=shiftpv-webhook
WEBHOOK_CONFIGURATION=shiftpv-mobility
for key in 'ca\.crt' 'ca\.key' 'tls\.crt' 'tls\.key'; do
	test -n "$(kubectl -n shiftpv-system get "secret/${WEBHOOK_SECRET}" -o "jsonpath={.data.${key}}")"
done
SERVICE_UID=$(kubectl -n shiftpv-system get service/shiftpv-webhook -o jsonpath='{.metadata.uid}')
DRIVER_UID=$(kubectl get csidriver/csi.shiftpv.io -o jsonpath='{.metadata.uid}')
test "$(kubectl -n shiftpv-system get "secret/${WEBHOOK_SECRET}" -o jsonpath='{.metadata.ownerReferences[0].kind}')" = "Service"
test "$(kubectl -n shiftpv-system get "secret/${WEBHOOK_SECRET}" -o jsonpath='{.metadata.ownerReferences[0].uid}')" = "${SERVICE_UID}"
test "$(kubectl get "mutatingwebhookconfiguration/${WEBHOOK_CONFIGURATION}" -o jsonpath='{.metadata.ownerReferences[0].kind}')" = "CSIDriver"
test "$(kubectl get "mutatingwebhookconfiguration/${WEBHOOK_CONFIGURATION}" -o jsonpath='{.metadata.ownerReferences[0].uid}')" = "${DRIVER_UID}"
WEBHOOK_CA=$(kubectl -n shiftpv-system get "secret/${WEBHOOK_SECRET}" -o jsonpath='{.data.ca\.crt}')
test "${WEBHOOK_CA}" = "$(kubectl get "mutatingwebhookconfiguration/${WEBHOOK_CONFIGURATION}" -o jsonpath='{.webhooks[0].clientConfig.caBundle}')"
WEBHOOK_CERT_BEFORE=$(kubectl -n shiftpv-system get "secret/${WEBHOOK_SECRET}" -o jsonpath='{.data.tls\.crt}')
kubectl apply -f "${WORK_DIR}/pools.yaml"
kubectl -n shiftpv-system wait --for=condition=Ready pod \
	-l app.kubernetes.io/instance=shiftpv --timeout=5m

BLOCKED_SOURCE_NODE="${CLUSTER_NAME}-worker"

# Scenario groups. CI runs each group on a separate cluster.
# `all` (the default) runs preparation, then the original g1/g2/g3 sweep. The split targets equal wall clock once the ~200 s cluster and install
# setup that every group pays for is included:
#
#   preparation  legacy in-flight upgrade, then durable admission/restart/cancel
#   g1  preflight (478 s)
#       The four deferral scenarios share the retained-volume bookkeeping that
#       test_preflight itself re-asserts after every scenario, so they stay
#       together; it is also the single longest block, hence a group of its own.
#   g2  blocked pre-commit copy failure and source recovery (240 s) +
#       completion (190 s)
#       recover_source_only reads BLOCKED_MOVE/BLOCKED_VOLUME from the blocked
#       scenario, so the two are inseparable. completion.sh owns its own
#       namespace and admission policy and refuses to run over leftovers, so it
#       is free to follow anything.
#   g3  WFFC migration with controller restarts at Copying/Promoting/Committing
#       plus terminal journal GC (216 s) + post-commit cleanup review, rollback
#       and cleanup lifecycle (222 s) + the mobility-disabled upgrade (~30 s)
#       recover_after_commit_failure consumes VOLUME_ID, PV_NAME, PVC_UID,
#       SOURCE_NODE, DESTINATION_NODE and CHECKSUM_BEFORE from the WFFC
#       migration, and the cleanup lifecycle it ends with uninstalls and rolls
#       back the release, so it must stay last before the mobility-disabled
#       upgrade that makes admission inert.
#
# assert_retained_volumes_unchanged after the commit-failure recovery only has
# preflight leftovers to inspect in `all`; in g3 the array is empty and the call
# is a no-op. The assertion itself keeps its coverage inside g1, where
# test_preflight runs it after every scenario.
KIND_MOBILITY_GROUP=${KIND_MOBILITY_GROUP:-all}
case "${KIND_MOBILITY_GROUP}" in
all | g1 | g2 | g3 | preparation | node-cleanup | retirement | copy-deadline) ;;
*)
	echo "unsupported KIND_MOBILITY_GROUP: ${KIND_MOBILITY_GROUP}" >&2
	exit 1
	;;
esac

group_selected() {
	[[ "${KIND_MOBILITY_GROUP}" == all || "${KIND_MOBILITY_GROUP}" == "$1" ]]
}

if group_selected preparation; then
	CLUSTER_NAME="${CLUSTER_NAME}" bash "${ROOT_DIR}/test/e2e/kind/mobility/legacy-upgrade.sh"
	CLUSTER_NAME="${CLUSTER_NAME}" bash "${ROOT_DIR}/test/e2e/kind/mobility/preparation.sh"
fi

if group_selected node-cleanup; then
	CLUSTER_NAME="${CLUSTER_NAME}" bash "${ROOT_DIR}/test/e2e/kind/mobility/native-cleanup.sh"
fi

if group_selected retirement; then
	CLUSTER_NAME="${CLUSTER_NAME}" bash "${ROOT_DIR}/test/e2e/kind/mobility/workload-retirement.sh"
fi

if group_selected copy-deadline; then
	CLUSTER_NAME="${CLUSTER_NAME}" bash "${ROOT_DIR}/test/e2e/kind/mobility/copy-deadline.sh"
fi

if group_selected g1; then
	test_preflight
fi

if group_selected g2; then
	# Exercise source recovery after a real pre-commit copy failure.
	kubectl cordon "${CLUSTER_NAME}-worker2"
	sed \
		-e "s|__SOURCE_NODE__|${BLOCKED_SOURCE_NODE}|g" \
		-e '/      nodeSelector:/,+1d' \
		"${ROOT_DIR}/test/e2e/kind/mobility/manifests/blocked-workload.yaml.tpl" >"${WORK_DIR}/blocked-workload.yaml"
	kubectl apply -f "${WORK_DIR}/blocked-workload.yaml"
	kubectl -n shiftpv-mobility-blocked rollout status deployment/source-only --timeout=5m
	kubectl -n shiftpv-mobility-blocked wait --for=jsonpath='{.status.phase}'=Bound pvc/source-only --timeout=2m
	BLOCKED_PV=$(kubectl -n shiftpv-mobility-blocked get pvc source-only -o jsonpath='{.spec.volumeName}')
	BLOCKED_VOLUME=$(kubectl get "pv/${BLOCKED_PV}" -o jsonpath='{.spec.csi.volumeHandle}')
	COPY_FAULT_NODE="${CLUSTER_NAME}-worker2"
	UNRECORDED_PATH="$(pool_mount_for_node "${COPY_FAULT_NODE}")/.shiftpv/incoming/e2e-unrecorded-path"
	docker exec "${COPY_FAULT_NODE}" mkdir -p -- "$(dirname "${UNRECORDED_PATH}")"
	docker exec "${COPY_FAULT_NODE}" test ! -e "${UNRECORDED_PATH}"
	docker exec "${COPY_FAULT_NODE}" touch -- "${UNRECORDED_PATH}"
	kubectl wait shiftpvpool/worker-b --for=jsonpath='{.status.inventory.valid}'=false --timeout=120s
	COPY_FAULT_SENTINEL="$(pool_mount_for_node "${COPY_FAULT_NODE}")/.shiftpv-e2e-fail-copy"
	docker exec "${COPY_FAULT_NODE}" touch -- "${COPY_FAULT_SENTINEL}"

	# Unknown data must fail closed before discovery can lock the volume or start a
	# helper. The sentinel is outside managed directories and remains invisible to
	# Pool inventory while the second half exercises the failed-Job contract.
	kubectl uncordon "${COPY_FAULT_NODE}"
	kubectl cordon "${BLOCKED_SOURCE_NODE}"
	FAIL_CLOSED_DEADLINE=$((SECONDS + 25))
	while ((SECONDS < FAIL_CLOSED_DEADLINE)); do
		test -z "$(kubectl get shiftpvmoves -o jsonpath="{.items[?(@.spec.volumeID=='${BLOCKED_VOLUME}')].metadata.name}" 2>/dev/null || true)"
		test "$(kubectl get "shiftpvvolume/${BLOCKED_VOLUME}" -o jsonpath='{.status.phase}')" = Ready
		test -z "$(kubectl get "shiftpvvolume/${BLOCKED_VOLUME}" -o jsonpath='{.status.activeMove}')"
		sleep 2
	done
	echo "ShiftPV unknown destination data fail-closed E2E passed: volume=${BLOCKED_VOLUME}"

	docker exec "${COPY_FAULT_NODE}" rm -- "${UNRECORDED_PATH}"
	kubectl wait shiftpvpool/worker-b --for=jsonpath='{.status.inventory.valid}'=true --timeout=120s
	BLOCKED_MOVE=$(wait_for_move "${BLOCKED_VOLUME}" 120)
	kubectl wait "shiftpvmove/${BLOCKED_MOVE}" --for=jsonpath='{.status.phase}'=Blocked --timeout=300s
	BLOCKED_PHASE=$(kubectl get "shiftpvmove/${BLOCKED_MOVE}" -o jsonpath='{.status.phase}')
	test "${BLOCKED_PHASE:-}" = "Blocked"
	test "$(kubectl get "shiftpvmove/${BLOCKED_MOVE}" -o jsonpath='{.status.reason}')" = "CopyFailed"
	assert_move_diagnostics "${BLOCKED_MOVE}" Blocked CopyFailed CopyFailed
	BLOCKED_COPY_JOB=$(kubectl get "shiftpvmove/${BLOCKED_MOVE}" -o jsonpath='{.status.copyJobName}')
	test -n "${BLOCKED_COPY_JOB}"
	kubectl -n shiftpv-system logs "job/${BLOCKED_COPY_JOB}" | grep -Fq 'injected copy failure before filesystem mutation'
	test "$(kubectl get "shiftpvvolume/${BLOCKED_VOLUME}" -o jsonpath='{.status.phase}')" = "Blocked"
	test "$(kubectl get "shiftpvvolume/${BLOCKED_VOLUME}" -o jsonpath='{.status.ownerNode}')" = "${BLOCKED_SOURCE_NODE}"
	assert_node_file "${BLOCKED_SOURCE_NODE}" "/mnt/shiftpv/volumes/${BLOCKED_VOLUME}/payload"
	assert_node_absent "${CLUSTER_NAME}-worker2" "/srv/shiftpv-b/volumes/${BLOCKED_VOLUME}"
	kubectl uncordon "${BLOCKED_SOURCE_NODE}"
	docker exec "${COPY_FAULT_NODE}" rm -- "${COPY_FAULT_SENTINEL}"

	echo "ShiftPV blocked mobility E2E passed: volume=${BLOCKED_VOLUME} move=${BLOCKED_MOVE} reason=CopyFailed"
	recover_source_only

	CLUSTER_NAME="${CLUSTER_NAME}" bash "${ROOT_DIR}/test/e2e/kind/mobility/completion.sh"
fi

if group_selected g3; then
	kubectl apply -f "${ROOT_DIR}/test/e2e/kind/mobility/manifests/wffc-workload.yaml"
	kubectl -n shiftpv-mobility-test rollout status deployment/wffc --timeout=5m
	kubectl -n shiftpv-mobility-test wait --for=jsonpath='{.status.phase}'=Bound pvc/wffc --timeout=2m
	# Recreate on the owner and prove the injected hostname pin still permits migration.
	kubectl -n shiftpv-mobility-test delete pod -l app=shiftpv-mobility-wffc --wait=true --timeout=120s
	kubectl -n shiftpv-mobility-test rollout status deployment/wffc --timeout=180s
	test "$(kubectl -n shiftpv-mobility-test get pod -l app=shiftpv-mobility-wffc -o jsonpath='{.items[0].metadata.annotations.shiftpv\.io/placement}')" = owner

	PVC_UID=$(kubectl -n shiftpv-mobility-test get pvc wffc -o jsonpath='{.metadata.uid}')
	PV_NAME=$(kubectl -n shiftpv-mobility-test get pvc wffc -o jsonpath='{.spec.volumeName}')
	VOLUME_ID=$(kubectl get "pv/${PV_NAME}" -o jsonpath='{.spec.csi.volumeHandle}')
	OLD_POD=$(kubectl -n shiftpv-mobility-test get pod -l app=shiftpv-mobility-wffc -o jsonpath='{.items[0].metadata.name}')
	OLD_POD_UID=$(kubectl -n shiftpv-mobility-test get pod "${OLD_POD}" -o jsonpath='{.metadata.uid}')
	SOURCE_NODE=$(kubectl -n shiftpv-mobility-test get pod "${OLD_POD}" -o jsonpath='{.spec.nodeName}')
	CHECKSUM_BEFORE=$(pod_sha256 shiftpv-mobility-test "${OLD_POD}" /data/payload)
	hold_owner_commit "${VOLUME_ID}"

	if [[ "${SOURCE_NODE}" == "${CLUSTER_NAME}-worker" ]]; then
		DESTINATION_NODE="${CLUSTER_NAME}-worker2"
	else
		DESTINATION_NODE="${CLUSTER_NAME}-worker"
	fi

	kubectl cordon "${SOURCE_NODE}"
	MOVE_NAME=$(wait_for_move "${VOLUME_ID}" 120)

	if kubectl patch "shiftpvmove/${MOVE_NAME}" --type merge -p '{"spec":{"recovery":"ResumeOwner"}}' >"${WORK_DIR}/early-recovery.txt" 2>&1; then
		echo 'recovery was accepted before the move was Blocked' >&2
		exit 1
	fi
	grep -q 'recovery may only be requested on a Blocked move' "${WORK_DIR}/early-recovery.txt"

	restart_at_phase() {
		local phase=$1
		local deadline=$((SECONDS + PHASE_TIMEOUT_SECONDS))
		local current=""
		while ((SECONDS < deadline)); do
			current=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
			if [[ "${current}" == "${phase}" ]]; then
				if [[ "${phase}" == "Committing" ]]; then
					local committed_owner
					committed_owner=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}' 2>/dev/null || true)
					if [[ "${committed_owner}" != "${DESTINATION_NODE}" ]]; then
						sleep 0.1
						continue
					fi
				fi
				local pod uid_before uid_after
				pod=$(kubectl -n shiftpv-system get pod -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.name}')
				uid_before=$(kubectl -n shiftpv-system get "pod/${pod}" -o jsonpath='{.metadata.uid}')
				if [[ "${phase}" == Copying ]]; then
					# Lose an approved scheduler reservation while the controller is
					# stopped. A new API Pod UID must remain valid for this same Move.
					controller_down
					local move_json placement placement_uid recreated_uid="" recreation_deadline
					move_json=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o json)
					jq -e '.status.capacityApproved == true and
					 (.status.phase == "Copying" or .status.phase == "Promoting" or .status.phase == "Committing")' <<<"${move_json}" >/dev/null
					test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${SOURCE_NODE}"
					placement_uid=$(jq -r '.status.placementPodUID' <<<"${move_json}")
					placement=$(kubectl -n shiftpv-system get pods -l shiftpv.io/role=placement -o json |
						jq -er --arg uid "${placement_uid}" '.items[] | select(.metadata.uid == $uid) | .metadata.name')
					kubectl -n shiftpv-system delete "pod/${placement}" --wait=true --timeout=120s
					controller_up
					recreation_deadline=$((SECONDS + 120))
					while ((SECONDS < recreation_deadline)); do
						current=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.phase}')
						if [[ "${current}" == Blocked ]]; then
							kubectl get "shiftpvmove/${MOVE_NAME}" -o yaml >&2
							return 1
						fi
						recreated_uid=$(kubectl -n shiftpv-system get "pod/${placement}" --ignore-not-found -o jsonpath='{.metadata.uid}')
						if [[ -n "${recreated_uid}" && "${recreated_uid}" != "${placement_uid}" ]]; then break; fi
						sleep 0.2
					done
					test -n "${recreated_uid}"
					test "${recreated_uid}" != "${placement_uid}"
					echo "approved placement recreated: ${placement_uid} -> ${recreated_uid}"
				elif [[ "${phase}" == Promoting ]]; then
					controller_down
					local old_replacement old_replacement_uid replacement_uid="" replacement_deadline
					old_replacement=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.replacementName}')
					old_replacement_uid=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.replacementUID}')
					test -n "${old_replacement_uid}"
					kubectl -n shiftpv-mobility-test delete "pod/${old_replacement}" --wait=true --timeout=120s
					controller_up
					replacement_deadline=$((SECONDS + 120))
					while ((SECONDS < replacement_deadline)); do
						test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${SOURCE_NODE}"
						replacement_uid=$(kubectl -n shiftpv-mobility-test get pods -l app=shiftpv-mobility-wffc -o json |
							jq -r --arg old "${old_replacement_uid}" '[.items[] | select(.metadata.uid != $old and .metadata.deletionTimestamp == null) |
							 select(any(.spec.schedulingGates[]?; .name == "shiftpv.io/placement-hold")) | .metadata.uid][0] // empty')
						[[ -n "${replacement_uid}" ]] && break
						sleep 0.2
						done
					test -n "${replacement_uid}"
					test "${replacement_uid}" != "${old_replacement_uid}"
					test "$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.replacementUID}')" = "${old_replacement_uid}"
					release_owner_commit
					echo "consumer recreated before owner commit: ${old_replacement_uid} -> ${replacement_uid}"
				else
					kubectl -n shiftpv-system delete "pod/${pod}" --grace-period=0 --force --wait=true
					kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=5m
				fi
				uid_after=$(kubectl -n shiftpv-system get pod -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.uid}')
				test "${uid_before}" != "${uid_after}"
				echo "controller restart injected at ${phase}: ${uid_before} -> ${uid_after}"
				return
			fi
			[[ "${current}" == "Blocked" || "${current}" == "Succeeded" ]] && return 1
			sleep 0.2
		done
		echo "phase ${phase} was not observed within ${PHASE_TIMEOUT_SECONDS}s; current=${current}" >&2
		kubectl get "shiftpvmove/${MOVE_NAME}" -o yaml >&2 || true
		return 1
	}

	restart_at_phase Copying
	restart_at_phase Promoting
	restart_at_phase Committing

	for _ in {1..600}; do
		MOVE_PHASE=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
		[[ "${MOVE_PHASE}" == "Succeeded" || "${MOVE_PHASE}" == "Blocked" ]] && break
		sleep 1
	done
	if [[ "${MOVE_PHASE}" != "Succeeded" ]]; then
		kubectl get "shiftpvmove/${MOVE_NAME}" -o yaml >&2
		exit 1
	fi
	assert_move_diagnostics "${MOVE_NAME}" Succeeded '' MobilitySucceeded

	kubectl -n shiftpv-mobility-test rollout status deployment/wffc --timeout=5m
	NEW_POD=$(kubectl -n shiftpv-mobility-test get pod -l app=shiftpv-mobility-wffc -o jsonpath='{.items[0].metadata.name}')
	NEW_POD_UID=$(kubectl -n shiftpv-mobility-test get pod "${NEW_POD}" -o jsonpath='{.metadata.uid}')
	test "${NEW_POD_UID}" != "${OLD_POD_UID}"
	test "$(kubectl -n shiftpv-mobility-test get pod "${NEW_POD}" -o jsonpath='{.spec.nodeName}')" = "${DESTINATION_NODE}"

	CHECKSUM_AFTER=$(pod_sha256 shiftpv-mobility-test "${NEW_POD}" /data/payload)
	test "${CHECKSUM_BEFORE}" = "${CHECKSUM_AFTER}"
	test "$(kubectl -n shiftpv-mobility-test get pvc wffc -o jsonpath='{.metadata.uid}')" = "${PVC_UID}"
	test "$(kubectl -n shiftpv-mobility-test get pvc wffc -o jsonpath='{.spec.volumeName}')" = "${PV_NAME}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.phase}')" = "Ready"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.ownerNode}')" = "${DESTINATION_NODE}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.activeMove}')" = ""
	DESTINATION_POOL=$(pool_mount_for_node "${DESTINATION_NODE}")
	SOURCE_POOL=$(pool_mount_for_node "${SOURCE_NODE}")
	assert_node_file "${DESTINATION_NODE}" "${DESTINATION_POOL}/volumes/${VOLUME_ID}/payload"
	assert_node_absent "${SOURCE_NODE}" "${SOURCE_POOL}/volumes/${VOLUME_ID}"
	SOURCE_COPY_ID=$(kubectl get "shiftpvmove/${MOVE_NAME}" -o jsonpath='{.status.sourceCopy.copyID}')
	test -n "${SOURCE_COPY_ID}"
	assert_node_absent "${SOURCE_NODE}" "${SOURCE_POOL}/.shiftpv/retired/${SOURCE_COPY_ID}"
	assert_cleanup_journal "shiftpvmove/${MOVE_NAME}" MoveSource "${VOLUME_ID}" "${SOURCE_COPY_ID}" ShiftPVMove
	test "${WEBHOOK_CERT_BEFORE}" = "$(kubectl -n shiftpv-system get "secret/${WEBHOOK_SECRET}" -o jsonpath='{.data.tls\.crt}')"
	verify_terminal_move_journal_gc "${MOVE_NAME}" "${VOLUME_ID}" "${NEW_POD}" "${DESTINATION_NODE}" "${DESTINATION_POOL}" "${CHECKSUM_AFTER}"

	recover_after_commit_failure
	assert_retained_volumes_unchanged

	helm upgrade shiftpv "${ROOT_DIR}/charts/shiftpv" \
		--namespace shiftpv-system \
		--reuse-values \
		--set mobility.enabled=false \
		--wait \
		--timeout 5m
	kubectl -n shiftpv-system get "service/${WEBHOOK_SERVICE}" >/dev/null
	kubectl -n shiftpv-system get "secret/${WEBHOOK_SECRET}" >/dev/null
	test "$(kubectl get "mutatingwebhookconfiguration/${WEBHOOK_CONFIGURATION}" -o jsonpath='{.webhooks[0].failurePolicy}')" = "Ignore"
	test "$(kubectl get "mutatingwebhookconfiguration/${WEBHOOK_CONFIGURATION}" -o jsonpath='{.webhooks[0].matchConditions[0].expression}')" = "false"
	test "${WEBHOOK_CERT_BEFORE}" = "$(kubectl -n shiftpv-system get "secret/${WEBHOOK_SECRET}" -o jsonpath='{.data.tls\.crt}')"
fi

echo "ShiftPV closed-loop mobility E2E passed (group=${KIND_MOBILITY_GROUP})"
if group_selected g3; then
	echo "volume=${VOLUME_ID} pv=${PV_NAME} move=${MOVE_NAME} source=${SOURCE_NODE} destination=${DESTINATION_NODE} checksum=${CHECKSUM_AFTER}; webhook certificates reconciled and disabled admission is inert"
fi
