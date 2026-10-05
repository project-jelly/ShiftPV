#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
# shellcheck source=test/e2e/kind/lib/cluster.sh
source "${ROOT_DIR}/test/e2e/kind/lib/cluster.sh"
CLUSTER_NAME=${CLUSTER_NAME:-shiftpv-e2e}
NODE_IMAGE=${NODE_IMAGE:-$(kind_node_image)}
KEEP_CLUSTER=${KEEP_CLUSTER:-0}

for command in docker kind kubectl helm sed jq; do
  command -v "${command}" >/dev/null || {
    echo "required command not found: ${command}" >&2
    exit 1
  }
done

KIND_VERSION=$(kind version | awk '{print $2}' | sed 's/^v//')
IFS=. read -r KIND_MAJOR KIND_MINOR _ <<<"${KIND_VERSION}"
if ((10#${KIND_MAJOR} == 0 && 10#${KIND_MINOR} < 33)); then
  echo "kind >= 0.33.0 is required for the pinned Kubernetes 1.35.8 node image; found ${KIND_VERSION}" >&2
  exit 1
fi

ACTIVE_DOCKER_CONTEXT=$(docker context show)
ACTIVE_DOCKER_HOST=$(docker context inspect "${ACTIVE_DOCKER_CONTEXT}" --format '{{.Endpoints.docker.Host}}')

mkdir -p "${ROOT_DIR}/.tmp"
WORK_DIR=$(mktemp -d "${ROOT_DIR}/.tmp/shiftpv-e2e.XXXXXX")
WORKER_A_POOL="${WORK_DIR}/worker-a"
WORKER_B_POOL="${WORK_DIR}/worker-b"
DOCKER_CONFIG_DIR="${WORK_DIR}/docker-config"
mkdir -p "${WORKER_A_POOL}" "${WORKER_B_POOL}" "${DOCKER_CONFIG_DIR}"
export KUBECONFIG="${E2E_KUBECONFIG:-${WORK_DIR}/kubeconfig}"

# Avoid inheriting a desktop-specific credential helper when using an isolated
# Docker engine such as Colima. DOCKER_HOST preserves the engine selected before
# DOCKER_CONFIG is switched to the empty per-run directory.
export DOCKER_HOST=${DOCKER_HOST:-${ACTIVE_DOCKER_HOST}}
export DOCKER_CONFIG=${DOCKER_CONFIG_DIR}
unset DOCKER_CONTEXT
docker info >/dev/null

trap delete_cluster_unless_kept EXIT

sed \
  -e "s|__WORKER_A_POOL__|${WORKER_A_POOL}|g" \
  -e "s|__WORKER_B_POOL__|${WORKER_B_POOL}|g" \
  "${ROOT_DIR}/test/e2e/kind/cluster.yaml.tpl" > "${WORK_DIR}/cluster.yaml"
sed \
  -e "s|__WORKER_A_NODE__|${CLUSTER_NAME}-worker|g" \
  -e "s|__WORKER_B_NODE__|${CLUSTER_NAME}-worker2|g" \
  "${ROOT_DIR}/test/e2e/kind/pools.yaml.tpl" > "${WORK_DIR}/pools.yaml"

kind create cluster \
  --name "${CLUSTER_NAME}" \
  --image "${NODE_IMAGE}" \
  --config "${WORK_DIR}/cluster.yaml"

docker build \
  --target combined \
  --build-arg CONTROLLER_VERSION=dev \
  --build-arg NODE_VERSION=dev \
  -f "${ROOT_DIR}/build/package/Dockerfile" \
  -t shiftpv:dev \
  "${ROOT_DIR}"
kind load docker-image shiftpv:dev --name "${CLUSTER_NAME}"

install_shiftpv() {
	local default_class=${1:-true}
	helm upgrade --install shiftpv "${ROOT_DIR}/charts/shiftpv" \
		--namespace shiftpv-system \
		--create-namespace \
		--values "${ROOT_DIR}/test/e2e/kind/values.yaml" \
		--set "storageClass.defaultClass=${default_class}" \
		--wait \
		--timeout 5m
	kubectl -n shiftpv-system wait \
    --for=condition=Ready pod \
    -l app.kubernetes.io/instance=shiftpv \
		--timeout=5m
	kubectl apply -f "${WORK_DIR}/pools.yaml"
	kubectl wait --for=condition=Ready shiftpvpool --all --timeout=2m
}

assert_shiftpv_release_removed() {
	local namespaced cluster_scoped deadline
	deadline=$((SECONDS + 60))
	while true; do
		namespaced=$(kubectl -n shiftpv-system get \
			deployments,daemonsets,replicasets,pods,services,jobs,serviceaccounts,roles,rolebindings,configmaps,secrets \
			-l app.kubernetes.io/instance=shiftpv \
			-o name)
		cluster_scoped=$(kubectl get \
			clusterroles,clusterrolebindings,storageclasses,csidrivers \
			-l app.kubernetes.io/instance=shiftpv \
			-o name)
		if [[ -z "${namespaced}" && -z "${cluster_scoped}" ]]; then
			break
		fi
		if ((SECONDS >= deadline)); then
			break
		fi
		sleep 1
	done

	if [[ -n "${namespaced}" || -n "${cluster_scoped}" ]]; then
		echo "Helm uninstall left ShiftPV release resources:" >&2
		printf '%s\n%s\n' "${namespaced}" "${cluster_scoped}" | sed '/^$/d' >&2
		exit 1
	fi
	for resource in \
		job/shiftpv-uninstall-guard \
		configmap/shiftpv-uninstall-permit \
		secret/shiftpv-webhook-tls; do
		if kubectl -n shiftpv-system get "${resource}" >/dev/null 2>&1; then
			echo "Helm uninstall left ${resource}" >&2
			exit 1
		fi
	done

	if kubectl get mutatingwebhookconfiguration shiftpv-mobility >/dev/null 2>&1; then
		echo "Helm uninstall left the ShiftPV mobility webhook" >&2
		exit 1
	fi
	if kubectl get validatingwebhookconfiguration shiftpv-lifecycle >/dev/null 2>&1; then
		echo "Helm uninstall left the ShiftPV lifecycle webhook" >&2
		exit 1
	fi
	if helm status shiftpv --namespace shiftpv-system >/dev/null 2>&1; then
		echo "Helm uninstall left the ShiftPV release installed" >&2
		exit 1
	fi
}

run_mobility_filesystem_faults() {
	CLUSTER_NAME="${CLUSTER_NAME}" WORK_DIR="${WORK_DIR}" \
		WORKER_A_POOL="${WORKER_A_POOL}" WORKER_B_POOL="${WORKER_B_POOL}" \
		"${ROOT_DIR}/test/e2e/kind/mobility-filesystem-faults.sh"
}

run_mobility_node_restarts() {
	CLUSTER_NAME="${CLUSTER_NAME}" WORK_DIR="${WORK_DIR}" \
		WORKER_A_POOL="${WORKER_A_POOL}" WORKER_B_POOL="${WORKER_B_POOL}" \
		"${ROOT_DIR}/test/e2e/kind/mobility-node-restarts.sh"
}

run_pool_capacity() {
	# Pod readiness can precede the API server's webhook endpoint refresh.
	# Exercise the read-only admission path before starting this focused case.
	local attempt ready=0
	for ((attempt = 0; attempt < 30; attempt++)); do
		if kubectl delete shiftpvpool worker-b --dry-run=server >/dev/null 2>&1; then
			ready=1
			break
		fi
		sleep 1
	done
	if [[ "${ready}" != 1 ]]; then
		echo "Pool admission webhook did not become reachable" >&2
		return 1
	fi
	CLUSTER_NAME="${CLUSTER_NAME}" "${ROOT_DIR}/test/e2e/kind/pool-capacity.sh"
}

run_directory_pool() {
	CLUSTER_NAME="${CLUSTER_NAME}" WORK_DIR="${WORK_DIR}" \
		"${ROOT_DIR}/test/e2e/kind/directory-pool.sh"
}

run_orphan_preservation() {
	CLUSTER_NAME="${CLUSTER_NAME}" WORKER_A_POOL="${WORKER_A_POOL}" \
		"${ROOT_DIR}/test/e2e/kind/orphan-cleanup.sh"
}

run_retain_reclaim() {
	CLUSTER_NAME="${CLUSTER_NAME}" "${ROOT_DIR}/test/e2e/kind/retain-reclaim.sh"
}

run_volume_delete_cleanup() {
	CLUSTER_NAME="${CLUSTER_NAME}" "${ROOT_DIR}/test/e2e/kind/volume-delete-cleanup.sh"
}

run_cleanup_job_retry() {
	CLUSTER_NAME="${CLUSTER_NAME}" WORK_DIR="${WORK_DIR}" \
		WORKER_A_POOL="${WORKER_A_POOL}" WORKER_B_POOL="${WORKER_B_POOL}" \
		"${ROOT_DIR}/test/e2e/kind/cleanup-job-retry.sh"
}

install_shiftpv true

if [[ "${POOL_CAPACITY_ONLY:-0}" == "1" ]]; then
	run_pool_capacity
	echo "ShiftPV focused Pool capacity E2E passed"
	exit 0
fi

if [[ "${ORPHAN_CLEANUP_ONLY:-0}" == "1" ]]; then
	run_orphan_preservation
	echo "ShiftPV focused unknown-orphan preservation during Pool deregistration E2E passed"
	exit 0
fi

if [[ "${VOLUME_DELETE_CLEANUP_ONLY:-0}" == "1" ]]; then
	run_volume_delete_cleanup
	echo "ShiftPV focused DeleteVolume cleanup ordering E2E passed"
	exit 0
fi

if [[ "${CLEANUP_JOB_RETRY_ONLY:-0}" == "1" ]]; then
	run_cleanup_job_retry
	echo "ShiftPV focused cleanup Job retry E2E passed"
	exit 0
fi

if [[ "${MOBILITY_NODE_RESTARTS_ONLY:-0}" == "1" ]]; then
	run_mobility_node_restarts
	if [[ "${MOBILITY_NODE_RESTART_CASE:-all}" == all ]]; then
		run_cleanup_job_retry
	fi
	echo "ShiftPV focused mobility node-container restart E2E passed"
	exit 0
fi

# Scenario groups. CI runs g1/g2/g3 as parallel shards on separate clusters;
# `all` (the default, and what every focused entry point above keeps using) runs
# the original sweep on one cluster in the original order. The split targets
# equal wall clock once the ~170 s cluster setup that every group pays for is
# included, and only reuses orderings the focused entry points already prove:
#
#   g1  directory-pool (262 s) + metrics (10 s) + retain-reclaim (~70 s)
#       + pool-capacity (138 s)
#       The metrics assertions count the CSI calls, the observed Copying move
#       and the settled reserved-byte gauge that directory-pool produces, and
#       require exactly the two base Pools, so those two stay adjacent and in
#       order. retain-reclaim runs behind the metrics check, which leaves no
#       capacity hold, and reuses the live Prometheus to prove
#       shiftpv_copy_observations{state="Missing"} stays zero across the
#       reclaim. pool-capacity also has a standalone focused entry point.
#   g2  volume-delete-cleanup (138 s) + the release lifecycle (257 s:
#       uninstall guard, break-glass reinstall, forced controller/node restarts,
#       fsGroup, StorageClass coexistence) + filesystem-faults (77 s)
#       The release lifecycle needs a cluster with no retained PVC/PV/Volume for
#       its first successful uninstall, and it rewrites the release afterwards,
#       so nothing that depends on the original install may follow it in the
#       same group. filesystem-faults keeps its current position behind it.
#   g3  orphan-cleanup (139 s) + mobility-filesystem-faults (369 s)
#       Both are standalone entry points today (ORPHAN_CLEANUP_ONLY runs
#       orphan-cleanup straight after install, MOBILITY_FILESYSTEM_FAULTS_ONLY
#       runs the fault sweep behind orphan-cleanup), so pairing the longest
#       scenario with a short one balances the three shards.
KIND_E2E_GROUP=${KIND_E2E_GROUP:-all}
case "${KIND_E2E_GROUP}" in
all | g1 | g2 | g3) ;;
*)
	echo "unsupported KIND_E2E_GROUP: ${KIND_E2E_GROUP}" >&2
	exit 1
	;;
esac

group_selected() {
	[[ "${KIND_E2E_GROUP}" == all || "${KIND_E2E_GROUP}" == "$1" ]]
}

if group_selected g1; then
	kubectl apply -f "${ROOT_DIR}/test/e2e/kind/metrics/prometheus.yaml"
	kubectl -n shiftpv-system rollout status deployment/metrics-test --timeout=3m

	run_directory_pool

	bash "${ROOT_DIR}/test/e2e/kind/metrics/check.sh"
fi

if [[ "${DIRECTORY_POOL_ONLY:-0}" == "1" ]]; then
	echo "ShiftPV focused ordinary directory Pool E2E passed"
	exit 0
fi

if group_selected g1; then
	run_retain_reclaim
	run_pool_capacity
fi
if group_selected g3; then
	run_orphan_preservation
fi

if [[ "${MOBILITY_FILESYSTEM_FAULTS_ONLY:-0}" == "1" ]]; then
	run_mobility_filesystem_faults
	echo "ShiftPV focused mobility filesystem fault E2E passed"
	exit 0
fi

if group_selected g2; then
	run_volume_delete_cleanup

	# Lifecycle admission is read-only. A direct dry-run DELETE must not mint an
	# uninstall permit even when no storage dependency exists.
	if kubectl -n shiftpv-system delete deployment shiftpv-controller --dry-run=server; then
		echo "direct dry-run DELETE unexpectedly bypassed the uninstall guard" >&2
		exit 1
	fi
	kubectl -n shiftpv-system get deployment/shiftpv-controller >/dev/null
	if kubectl -n shiftpv-system get configmap/shiftpv-uninstall-permit >/dev/null 2>&1; then
		echo "direct dry-run DELETE created uninstall state" >&2
		exit 1
	fi

	# Pool registration alone is safe to retain. With no PVC/PV/Volume/Move, the
	# hook must allow a normal uninstall. CRDs and Pool registration remain by
	# design, while every release-owned workload, RBAC object, storage object,
	# webhook and successful hook Job must be gone before reinstall.
	helm uninstall shiftpv --namespace shiftpv-system --wait --timeout 2m
	assert_shiftpv_release_removed
	kubectl get customresourcedefinition/shiftpvpools.shiftpv.io >/dev/null
	kubectl get shiftpvpool/worker-a shiftpvpool/worker-b >/dev/null
	install_shiftpv true

	DEFAULT_CLASS=$(kubectl get storageclass shiftpv \
	  -o jsonpath='{.metadata.annotations.storageclass\.kubernetes\.io/is-default-class}')
	DEFAULT_POLICY=$(kubectl get storageclass shiftpv -o jsonpath='{.reclaimPolicy}')
	RETAIN_CLASS=$(kubectl get storageclass shiftpv-retain \
	  -o jsonpath='{.metadata.annotations.storageclass\.kubernetes\.io/is-default-class}')
	RETAIN_POLICY=$(kubectl get storageclass shiftpv-retain -o jsonpath='{.reclaimPolicy}')
	if [[ "${DEFAULT_CLASS}" != "true" || "${DEFAULT_POLICY}" != "Delete" || \
	  "${RETAIN_CLASS}" != "false" || "${RETAIN_POLICY}" != "Retain" ]]; then
	  echo "unexpected StorageClass contract: shiftpv=${DEFAULT_CLASS}/${DEFAULT_POLICY} shiftpv-retain=${RETAIN_CLASS}/${RETAIN_POLICY}" >&2
	  exit 1
	fi

	kubectl apply -f "${ROOT_DIR}/test/e2e/kind/pvc.yaml"
	kubectl wait \
	  --for=jsonpath='{.spec.storageClassName}'=shiftpv \
	  pvc/shiftpv-e2e \
	  --timeout=2m
	kubectl apply -f "${ROOT_DIR}/test/e2e/kind/pod.yaml"
	kubectl wait --for=condition=Ready pod/shiftpv-e2e --timeout=5m
	kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/shiftpv-e2e --timeout=2m

	PV_NAME=$(kubectl get pvc shiftpv-e2e -o jsonpath='{.spec.volumeName}')
	PVC_UID=$(kubectl get pvc shiftpv-e2e -o jsonpath='{.metadata.uid}')
	OWNER_NODE=$(kubectl get pod shiftpv-e2e -o jsonpath='{.spec.nodeName}')
	CHECKSUM_BEFORE=$(pod_sha256 default shiftpv-e2e /data/payload)
	VOLUME_ID=$(kubectl get "pv/${PV_NAME}" -o jsonpath='{.spec.csi.volumeHandle}')
	PV_DRIVER=$(kubectl get "pv/${PV_NAME}" -o jsonpath='{.spec.csi.driver}')
	if [[ "${PV_DRIVER}" != "csi.shiftpv.io" ]]; then
	  echo "PVC was not provisioned by ShiftPV: ${PV_DRIVER}" >&2
	  exit 1
	fi
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.requestName}')" = "pvc-${PVC_UID}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.capacityBytes}')" = 67108864
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.initialNode}')" = "${OWNER_NODE}"

	# Force replacement of both controller and owner-node plugin Pods while the
	# workload keeps the volume mounted. Their Kubernetes UIDs must change.
	CONTROLLER_POD_BEFORE=$(kubectl -n shiftpv-system get pod \
	  -l app.kubernetes.io/instance=shiftpv,app.kubernetes.io/component=controller \
	  -o jsonpath='{.items[0].metadata.name}')
	CONTROLLER_UID_BEFORE=$(kubectl -n shiftpv-system get "pod/${CONTROLLER_POD_BEFORE}" \
	  -o jsonpath='{.metadata.uid}')
	kubectl -n shiftpv-system delete "pod/${CONTROLLER_POD_BEFORE}" \
	  --grace-period=0 --force --wait=true
	kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=5m
	CONTROLLER_UID_AFTER=$(kubectl -n shiftpv-system get pod \
	  -l app.kubernetes.io/instance=shiftpv,app.kubernetes.io/component=controller \
	  -o jsonpath='{.items[0].metadata.uid}')
	if [[ "${CONTROLLER_UID_BEFORE}" == "${CONTROLLER_UID_AFTER}" ]]; then
	  echo "controller Pod UID did not change after forced replacement" >&2
	  exit 1
	fi

	NODE_POD_BEFORE=$(kubectl -n shiftpv-system get pod \
	  -l app.kubernetes.io/instance=shiftpv,app.kubernetes.io/component=node \
	  --field-selector "spec.nodeName=${OWNER_NODE}" \
	  -o jsonpath='{.items[0].metadata.name}')
	NODE_UID_BEFORE=$(kubectl -n shiftpv-system get "pod/${NODE_POD_BEFORE}" \
	  -o jsonpath='{.metadata.uid}')
	kubectl -n shiftpv-system delete "pod/${NODE_POD_BEFORE}" \
	  --grace-period=0 --force --wait=true
	kubectl -n shiftpv-system rollout status daemonset/shiftpv-node --timeout=5m
	NODE_UID_AFTER=$(kubectl -n shiftpv-system get pod \
	  -l app.kubernetes.io/instance=shiftpv,app.kubernetes.io/component=node \
	  --field-selector "spec.nodeName=${OWNER_NODE}" \
	  -o jsonpath='{.items[0].metadata.uid}')
	if [[ "${NODE_UID_BEFORE}" == "${NODE_UID_AFTER}" ]]; then
	  echo "owner-node plugin Pod UID did not change after forced replacement" >&2
	  exit 1
	fi

	kubectl wait --for=condition=Ready pod/shiftpv-e2e --timeout=2m
	CHECKSUM_AFTER_RESTARTS=$(pod_sha256 default shiftpv-e2e /data/payload)
	if [[ "${CHECKSUM_BEFORE}" != "${CHECKSUM_AFTER_RESTARTS}" ]]; then
	  echo "checksum mismatch after controller and node plugin replacement" >&2
	  exit 1
	fi

	# Verify kubelet applies Pod fsGroup ownership on a previously root-owned
	# ShiftPV volume before testing an ordinary unpublish/publish.
	kubectl delete pod shiftpv-e2e --wait=true
	kubectl apply -f "${ROOT_DIR}/test/e2e/kind/fs-group-pod.yaml"
	kubectl wait --for=condition=Ready pod/shiftpv-fsgroup-e2e --timeout=5m
	test "$(kubectl exec shiftpv-fsgroup-e2e -- stat -c %g /data)" = 10001
	kubectl exec shiftpv-fsgroup-e2e -- grep -Fx 'ShiftPV non-root fsGroup write' /data/non-root
	kubectl delete pod shiftpv-fsgroup-e2e --wait=true

	# Verify ordinary kubelet unpublish/publish before testing the Helm boundary.
	kubectl apply -f "${ROOT_DIR}/test/e2e/kind/pod.yaml"
	kubectl wait --for=condition=Ready pod/shiftpv-e2e --timeout=5m
	CHECKSUM_RECREATED=$(pod_sha256 default shiftpv-e2e /data/payload)
	if [[ "${CHECKSUM_BEFORE}" != "${CHECKSUM_RECREATED}" ]]; then
	  echo "checksum mismatch after Pod recreation" >&2
	  exit 1
	fi
	kubectl wait --for=jsonpath="{.status.publishedNodes[0]}=${OWNER_NODE}" \
	  "shiftpvvolume/${VOLUME_ID}" --timeout=2m

	# A failed pre-delete hook must leave the release and the running workload intact.
	if helm uninstall shiftpv --namespace shiftpv-system --timeout 2m; then
	  echo "Helm uninstall unexpectedly succeeded while a ShiftPV workload was running" >&2
	  exit 1
	fi
	helm status shiftpv --namespace shiftpv-system >/dev/null
	kubectl -n shiftpv-system get deployment/shiftpv-controller >/dev/null
	kubectl -n shiftpv-system get daemonset/shiftpv-node >/dev/null
	kubectl get validatingwebhookconfiguration shiftpv-lifecycle >/dev/null
	kubectl -n shiftpv-system wait --for=delete configmap/shiftpv-uninstall-permit --timeout=30s
	kubectl -n shiftpv-system logs job/shiftpv-uninstall-guard | grep -F 'ShiftPV uninstall denied'
	CHECKSUM_AFTER_DENIAL=$(pod_sha256 default shiftpv-e2e /data/payload)
	if [[ "${CHECKSUM_BEFORE}" != "${CHECKSUM_AFTER_DENIAL}" ]]; then
	  echo "checksum mismatch after denied Helm uninstall" >&2
	  exit 1
	fi

	# Stopping the Pod is not enough: retained PVC/PV/Volume state still requires an
	# explicit recovery decision. A second denial also exercises hook replacement.
	kubectl delete pod shiftpv-e2e --wait=true
	if helm uninstall shiftpv --namespace shiftpv-system --timeout 2m; then
	  echo "Helm uninstall unexpectedly succeeded while retained ShiftPV resources existed" >&2
	  exit 1
	fi
	helm status shiftpv --namespace shiftpv-system >/dev/null
	kubectl -n shiftpv-system logs job/shiftpv-uninstall-guard | grep -F 'PersistentVolume'

	# The break-glass path deliberately bypasses hooks. Remove the failed hook Job,
	# which Helm does not own, so the subsequent reinstall starts without leftovers.
	kubectl -n shiftpv-system delete job shiftpv-uninstall-guard --ignore-not-found --wait=true
	kubectl delete validatingwebhookconfiguration shiftpv-lifecycle --ignore-not-found --wait=true
	helm uninstall shiftpv --namespace shiftpv-system --no-hooks

	kubectl get pvc shiftpv-e2e >/dev/null
	kubectl get "pv/${PV_NAME}" >/dev/null
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.requestName}')" = "pvc-${PVC_UID}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.capacityBytes}')" = 67108864

	DATA_MOUNT=$(pool_mount_for_node "${OWNER_NODE}")
	assert_node_file "${OWNER_NODE}" "${DATA_MOUNT}/volumes/${VOLUME_ID}/payload"

	install_shiftpv true
	kubectl apply -f "${ROOT_DIR}/test/e2e/kind/pod.yaml"
	kubectl wait --for=condition=Ready pod/shiftpv-e2e --timeout=5m
	CHECKSUM_AFTER=$(pod_sha256 default shiftpv-e2e /data/payload)

	if [[ "${CHECKSUM_BEFORE}" != "${CHECKSUM_AFTER}" ]]; then
		echo "checksum mismatch after Helm reinstall" >&2
		exit 1
	fi

	# Reinstall with ShiftPV opt-in while an unrelated default class already exists.
	kubectl delete pod shiftpv-e2e --wait=true
	kubectl delete validatingwebhookconfiguration shiftpv-lifecycle --ignore-not-found --wait=true
	helm uninstall shiftpv --namespace shiftpv-system --no-hooks
	kubectl apply -f "${ROOT_DIR}/test/e2e/kind/existing-default-storageclass.yaml"
	install_shiftpv false

	EXISTING_DEFAULT=$(kubectl get storageclass existing-default \
		-o jsonpath='{.metadata.annotations.storageclass\.kubernetes\.io/is-default-class}')
	SHIFTPV_DEFAULT=$(kubectl get storageclass shiftpv \
		-o jsonpath='{.metadata.annotations.storageclass\.kubernetes\.io/is-default-class}')
	SHIFTPV_RETAIN_DEFAULT=$(kubectl get storageclass shiftpv-retain \
		-o jsonpath='{.metadata.annotations.storageclass\.kubernetes\.io/is-default-class}')
	if [[ "${EXISTING_DEFAULT}" != "true" || "${SHIFTPV_DEFAULT}" != "false" || \
		"${SHIFTPV_RETAIN_DEFAULT}" != "false" ]]; then
		echo "StorageClass default annotations changed unexpectedly: existing=${EXISTING_DEFAULT} shiftpv=${SHIFTPV_DEFAULT} shiftpv-retain=${SHIFTPV_RETAIN_DEFAULT}" >&2
		exit 1
	fi

	kubectl apply -f "${ROOT_DIR}/test/e2e/kind/implicit-pvc.yaml"
	kubectl wait \
		--for=jsonpath='{.spec.storageClassName}'=existing-default \
		pvc/existing-default-e2e \
		--timeout=2m

	kubectl apply -f "${ROOT_DIR}/test/e2e/kind/coexistence-pvc.yaml"
	kubectl apply -f "${ROOT_DIR}/test/e2e/kind/coexistence-pod.yaml"
	kubectl wait --for=condition=Ready pod/shiftpv-coexistence --timeout=5m
	kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/shiftpv-coexistence --timeout=2m
	COEXISTENCE_PV=$(kubectl get pvc shiftpv-coexistence -o jsonpath='{.spec.volumeName}')
	COEXISTENCE_DRIVER=$(kubectl get "pv/${COEXISTENCE_PV}" -o jsonpath='{.spec.csi.driver}')
	if [[ "${COEXISTENCE_DRIVER}" != "csi.shiftpv.io" ]]; then
		echo "explicit ShiftPV PVC used unexpected driver: ${COEXISTENCE_DRIVER}" >&2
		exit 1
	fi
	kubectl exec shiftpv-coexistence -- grep -Fx 'ShiftPV StorageClass coexistence' /data/payload

	kubectl delete pod shiftpv-coexistence --wait=true
	kubectl delete pvc shiftpv-coexistence --wait=true
	CLUSTER_NAME="${CLUSTER_NAME}" WORKER_B_POOL="${WORKER_B_POOL}" \
	  "${ROOT_DIR}/test/e2e/kind/filesystem-faults.sh"
fi

if group_selected g3; then
	run_mobility_filesystem_faults
fi

echo "ShiftPV kind e2e passed (group=${KIND_E2E_GROUP})"
if group_selected g2; then
	echo "PV=${PV_NAME} volume=${VOLUME_ID} node=${OWNER_NODE} checksum=${CHECKSUM_AFTER} controller_restart=${CONTROLLER_UID_AFTER} node_restart=${NODE_UID_AFTER}"
fi
