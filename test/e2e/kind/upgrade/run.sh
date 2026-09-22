#!/usr/bin/env bash
set -euo pipefail

TEST_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${TEST_DIR}/../../../.." && pwd)
# shellcheck source=test/e2e/kind/lib/artifact.sh
source "${ROOT_DIR}/test/e2e/kind/lib/artifact.sh"
# shellcheck source=test/e2e/kind/lib/cluster.sh
source "${ROOT_DIR}/test/e2e/kind/lib/cluster.sh"

LOCK_FILE=${ARTIFACT_LOCK_FILE:-"${ROOT_DIR}/test/e2e/kind/artifact/versions.env"}
CLUSTER_NAME=${CLUSTER_NAME:-shiftpv-upgrade-e2e}
KEEP_CLUSTER=${KEEP_CLUSTER:-0}
CANDIDATE_IMAGE=${CANDIDATE_IMAGE:-shiftpv:upgrade-dev}
NAMESPACE=shiftpv-system
RELEASE=shiftpv

# The suite provokes and then waits out volume cleanup, which the controller
# only confirms against a fresh Pool inventory. The chart default scans once a
# minute, so every wait here would sit one tick from its own timeout.
READINESS_ARGS=(--set poolReadiness.interval=2s --set poolReadiness.staleAfter=10s)

require_commands awk docker helm kind kubectl sed
load_artifact_lock "${LOCK_FILE}"
require_kind_version

mkdir -p "${ROOT_DIR}/.tmp"
WORK_DIR=$(mktemp -d "${ROOT_DIR}/.tmp/shiftpv-upgrade.XXXXXX")
WORKER_A_POOL="${WORK_DIR}/worker-a"
WORKER_B_POOL="${WORK_DIR}/worker-b"
mkdir -p "${WORKER_A_POOL}" "${WORKER_B_POOL}"
export KUBECONFIG="${E2E_KUBECONFIG:-${WORK_DIR}/kubeconfig}"
isolate_tool_environment "${WORK_DIR}"
docker info >/dev/null

trap delete_cluster_unless_kept EXIT

# The upgrade target is whatever this checkout declares. Render the chart's own
# StorageClasses rather than re-reading values.yaml by hand, so a template that
# derives a name or policy still moves the suite with it.
CANDIDATE_CLASSES="${WORK_DIR}/candidate-storageclasses.txt"
helm template "${RELEASE}" "${ROOT_DIR}/charts/shiftpv" \
	--kube-version "$(awk -F: '{ print $2 }' <<<"${KIND_NODE_IMAGE%%@*}" | sed 's/^v//')" \
	--set storageClass.defaultClass=true \
	-s templates/storage/storageclass.yaml |
	awk '
		/^---/ { name = ""; default_class = ""; next }
		$1 == "name:" && name == "" { name = $2 }
		$1 == "storageclass.kubernetes.io/is-default-class:" { gsub(/"/, "", $2); default_class = $2 }
		$1 == "reclaimPolicy:" { print name, $2, default_class }
	' >"${CANDIDATE_CLASSES}"

DEFAULT_CLASS=$(awk '$3 == "true" { print $1; exit }' "${CANDIDATE_CLASSES}")
CANDIDATE_RECLAIM=$(awk '$3 == "true" { print $2; exit }' "${CANDIDATE_CLASSES}")
RETAIN_CLASS=$(awk '$3 != "true" { print $1; exit }' "${CANDIDATE_CLASSES}")
[[ -n "${DEFAULT_CLASS}" && -n "${CANDIDATE_RECLAIM}" && -n "${RETAIN_CLASS}" ]] || {
	echo "could not read the chart's StorageClasses from the rendered template" >&2
	cat "${CANDIDATE_CLASSES}" >&2
	exit 1
}

sed \
	-e "s|__WORKER_A_POOL__|${WORKER_A_POOL}|g" \
	-e "s|__WORKER_B_POOL__|${WORKER_B_POOL}|g" \
	"${ROOT_DIR}/test/e2e/kind/cluster.yaml.tpl" >"${WORK_DIR}/cluster.yaml"
sed \
	-e "s|__WORKER_A_NODE__|${CLUSTER_NAME}-worker|g" \
	-e "s|__WORKER_B_NODE__|${CLUSTER_NAME}-worker2|g" \
	"${ROOT_DIR}/test/e2e/kind/pools.yaml.tpl" >"${WORK_DIR}/pools.yaml"

# A workload pair per stage: the marker proves the published release's data
# survives the upgrade, and each later pair proves provisioning still works.
create_workload() {
	local name=$1 storage_class=$2 marker=$3
	kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${name}
spec:
  accessModes:
    - ReadWriteOnce
  volumeMode: Filesystem
  storageClassName: ${storage_class}
  resources:
    requests:
      storage: 64Mi
---
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 1
  containers:
    - name: writer
      image: busybox:1.37
      command:
        - sh
        - -ec
        - |
          if [ ! -f /data/marker ]; then
            printf '%s\n' '${marker}' > /data/marker
          fi
          sleep 3600
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${name}
EOF
	# The class binds on first consumer, so the claim binds while the Pod is
	# still starting; assert the binding first and the running Pod after.
	kubectl wait --for=jsonpath='{.status.phase}'=Bound "pvc/${name}" --timeout=5m
	kubectl wait --for=condition=Ready "pod/${name}" --timeout=5m
	assert_equal "storage class of pvc/${name}" "${storage_class}" \
		"$(kubectl get "pvc/${name}" -o jsonpath='{.spec.storageClassName}')"
	assert_marker "${name}" "${marker}"
}

# The Pod is Ready as soon as its container starts, which can be observed
# before the writer's first line reaches the volume. Poll rather than race it.
assert_marker() {
	local name=$1 expected=$2 timeout=${3:-60}
	local deadline=$((SECONDS + timeout)) actual=""
	while ((SECONDS < deadline)); do
		actual=$(kubectl exec "pod/${name}" -- cat /data/marker 2>/dev/null || true)
		if [[ "${actual}" == "${expected}" ]]; then
			return
		fi
		sleep 2
	done
	echo "marker in ${name} never matched within ${timeout}s: expected '${expected}', last read '${actual}'" >&2
	return 1
}

# The documented procedure requires that no ShiftPV storage depends on the class
# before it is removed. The webhook's dependency report counts the driver's
# PersistentVolumes as well as the ShiftPVVolumes, and the PV is removed
# asynchronously after the claim, so both have to be gone before the next step.
delete_workload() {
	local name=$1 persistent_volume volume_id
	persistent_volume=$(kubectl get "pvc/${name}" -o jsonpath='{.spec.volumeName}')
	volume_id=$(kubectl get "pv/${persistent_volume}" -o jsonpath='{.spec.csi.volumeHandle}')
	kubectl delete "pod/${name}" --wait=true
	kubectl delete "pvc/${name}" --wait=true
	kubectl wait --for=delete "pv/${persistent_volume}" --timeout=5m
	kubectl wait --for=delete "shiftpvvolume/${volume_id}" --timeout=5m
}

# The candidate upgrade changes exactly two things against the published
# install: the chart (including CRD schemas) and images built from it.
upgrade_candidate() {
	# Helm does not update schemas from crds/ on upgrade. Apply them before
	# rolling a controller that persists the new rollback generation fence.
	kubectl apply --field-manager=shiftpv-crd-upgrade -f "${ROOT_DIR}/charts/shiftpv/crds/"
	kubectl wait --for=condition=Established crd/shiftpvmoves.shiftpv.io --timeout=60s
	assert_equal "installed rollback fence schema" integer \
		"$(kubectl get crd/shiftpvmoves.shiftpv.io -o jsonpath='{.spec.versions[0].schema.openAPIV3Schema.properties.status.properties.rollbackRequiredGeneration.type}')"
	helm upgrade --install "${RELEASE}" "${ROOT_DIR}/charts/shiftpv" \
		--namespace "${NAMESPACE}" \
		--set storageClass.defaultClass=true \
		--set-string controller.image.repository="${CANDIDATE_IMAGE%%:*}" \
		--set-string controller.image.tag="${CANDIDATE_IMAGE#*:}" \
		--set-string controller.image.pullPolicy=IfNotPresent \
		--set-string node.image.repository="${CANDIDATE_IMAGE%%:*}" \
		--set-string node.image.tag="${CANDIDATE_IMAGE#*:}" \
		--set-string node.image.pullPolicy=IfNotPresent \
		--set-string mobility.helperImage="${CANDIDATE_IMAGE}" \
		"${READINESS_ARGS[@]}" \
		--wait --timeout 8m
}

# Rollout status follows the current workload generation. A label-selected
# kubectl wait can capture an old Pod that is deleted during this upgrade.
assert_candidate_images() {
	kubectl -n "${NAMESPACE}" rollout status deployment/shiftpv-controller --timeout=5m
	kubectl -n "${NAMESPACE}" rollout status daemonset/shiftpv-node --timeout=5m
	assert_equal "controller image" "${CANDIDATE_IMAGE}" \
		"$(kubectl -n "${NAMESPACE}" get deployment/shiftpv-controller \
			-o jsonpath='{.spec.template.spec.containers[?(@.name=="shiftpv-controller")].image}')"
	assert_equal "node image" "${CANDIDATE_IMAGE}" \
		"$(kubectl -n "${NAMESPACE}" get daemonset/shiftpv-node \
			-o jsonpath='{.spec.template.spec.containers[?(@.name=="shiftpv-node")].image}')"
}

assert_chart_storage_classes() {
	assert_equal "reclaimPolicy of ${DEFAULT_CLASS}" "${CANDIDATE_RECLAIM}" \
		"$(kubectl get "storageclass/${DEFAULT_CLASS}" -o jsonpath='{.reclaimPolicy}')"
	assert_equal "default-class annotation of ${DEFAULT_CLASS}" true \
		"$(kubectl get "storageclass/${DEFAULT_CLASS}" \
			-o jsonpath='{.metadata.annotations.storageclass\.kubernetes\.io/is-default-class}')"
	kubectl get "storageclass/${RETAIN_CLASS}" >/dev/null
}

assert_storage_class_delete_refused() {
	local output
	if output=$(kubectl delete "storageclass/${DEFAULT_CLASS}" 2>&1); then
		echo "StorageClass ${DEFAULT_CLASS} was deleted while ShiftPV storage still depends on it" >&2
		echo "${output}" >&2
		return 1
	fi
	if ! grep -Fq "dependent storage exists" <<<"${output}"; then
		echo "StorageClass deletion was refused without naming the dependent storage:" >&2
		echo "${output}" >&2
		return 1
	fi
	kubectl get "storageclass/${DEFAULT_CLASS}" >/dev/null
}

# Pool inventory and the driver's own objects settle asynchronously after the
# last claim is gone, so the admitted-deletion step is a bounded poll rather
# than a single attempt. A server-side dry run asks the webhook without
# removing the class.
wait_for_storage_class_delete_allowed() {
	local name=$1 timeout=$2 interval=${3:-5}
	local deadline=$((SECONDS + timeout)) output=""
	while ((SECONDS < deadline)); do
		if output=$(kubectl delete "storageclass/${name}" --dry-run=server 2>&1); then
			return
		fi
		sleep "${interval}"
	done
	echo "StorageClass ${name} deletion was still refused after ${timeout}s; last denial:" >&2
	echo "${output}" >&2
	return 1
}

chart_package=$(pull_published_chart "${WORK_DIR}")

kind create cluster \
	--name "${CLUSTER_NAME}" \
	--image "${KIND_NODE_IMAGE}" \
	--config "${WORK_DIR}/cluster.yaml"

install_published_release "${chart_package}" "${NAMESPACE}" "${RELEASE}" "${READINESS_ARGS[@]}"
kubectl apply -f "${WORK_DIR}/pools.yaml"
kubectl wait --for=condition=Ready shiftpvpool --all --timeout=2m

assert_equal "published controller image" "${CONTROLLER_IMAGE}" \
	"$(kubectl -n "${NAMESPACE}" get deployment/shiftpv-controller \
		-o jsonpath='{.spec.template.spec.containers[?(@.name=="shiftpv-controller")].image}')"
assert_equal "published node image" "${NODE_IMAGE}" \
	"$(kubectl -n "${NAMESPACE}" get daemonset/shiftpv-node \
		-o jsonpath='{.spec.template.spec.containers[?(@.name=="shiftpv-node")].image}')"
# The replacement procedure below acts on one class name. A release that renamed
# it needs a different procedure, so fail loudly instead of testing a stale name.
kubectl get "storageclass/${DEFAULT_CLASS}" >/dev/null

MARKER="shiftpv upgrade marker from chart ${CHART_VERSION}"
create_workload shiftpv-upgrade-existing "${DEFAULT_CLASS}" "${MARKER}"

docker build \
	--target combined \
	--build-arg CONTROLLER_VERSION=dev \
	--build-arg NODE_VERSION=dev \
	-f "${ROOT_DIR}/build/package/Dockerfile" \
	-t "${CANDIDATE_IMAGE}" \
	"${ROOT_DIR}"
kind load docker-image "${CANDIDATE_IMAGE}" --name "${CLUSTER_NAME}"

# reclaimPolicy is immutable. When the published class disagrees with the chart,
# a plain helm upgrade fails on the field instead of on anything the suite is
# about, so the documented replacement becomes the upgrade path.
INSTALLED_RECLAIM=$(kubectl get "storageclass/${DEFAULT_CLASS}" -o jsonpath='{.reclaimPolicy}')
RECLAIM_MISMATCH=0
if [[ "${INSTALLED_RECLAIM}" != "${CANDIDATE_RECLAIM}" ]]; then
	RECLAIM_MISMATCH=1
	echo "StorageClass ${DEFAULT_CLASS} reclaimPolicy changed between the published chart ${CHART_VERSION} (${INSTALLED_RECLAIM}) and this checkout (${CANDIDATE_RECLAIM}); upgrading through the documented replacement"
fi

ASSERTIONS=()
if [[ "${RECLAIM_MISMATCH}" == 0 ]]; then
	upgrade_candidate
	assert_candidate_images
	assert_chart_storage_classes
	assert_marker shiftpv-upgrade-existing "${MARKER}"
	create_workload shiftpv-upgrade-new "${DEFAULT_CLASS}" "shiftpv upgrade marker after the candidate upgrade"
	ASSERTIONS+=(
		"CRD schema update and helm upgrade rolled the controller and node to the candidate image"
		"the pre-upgrade Pod still read its marker across the upgrade"
		"a new PVC provisioned and mounted on the unchanged class"
	)
else
	ASSERTIONS+=(
		"the immutable reclaimPolicy change made the plain helm upgrade inapplicable, so the replacement below was the upgrade path"
		"the pre-upgrade marker could not survive: the procedure requires removing the storage that depends on the class"
	)
fi

# The documented StorageClass replacement, exercised either way: refused while
# ShiftPV storage depends on the class, allowed once it is gone, recreated by
# the next chart sync.
assert_storage_class_delete_refused

delete_workload shiftpv-upgrade-existing
if [[ "${RECLAIM_MISMATCH}" == 0 ]]; then
	delete_workload shiftpv-upgrade-new
fi

wait_for_storage_class_delete_allowed "${DEFAULT_CLASS}" 300
kubectl delete "storageclass/${DEFAULT_CLASS}"
if kubectl get "storageclass/${DEFAULT_CLASS}" >/dev/null 2>&1; then
	echo "StorageClass ${DEFAULT_CLASS} still exists after an accepted deletion" >&2
	exit 1
fi

# The resync is the candidate chart in both paths: a no-op re-apply after the
# plain upgrade, and the upgrade itself after an immutable-field replacement.
upgrade_candidate
assert_candidate_images
assert_chart_storage_classes
create_workload shiftpv-upgrade-resynced "${DEFAULT_CLASS}" "shiftpv upgrade marker after the StorageClass resync"
delete_workload shiftpv-upgrade-resynced
ASSERTIONS+=(
	"deleting the class was refused with 'dependent storage exists' while PVCs were bound to it"
	"deleting the class was accepted once no PersistentVolume or ShiftPVVolume depended on it"
	"the chart sync recreated the class with reclaimPolicy ${CANDIDATE_RECLAIM} alongside ${RETAIN_CLASS}"
	"the resynced class provisioned a PVC whose Pod wrote and read its marker"
	"the candidate controller and node images are running after the resync"
)

echo "ShiftPV upgrade E2E passed: published chart=${CHART_VERSION} controller=${CONTROLLER_IMAGE} node=${NODE_IMAGE} -> candidate=${CANDIDATE_IMAGE}; StorageClass ${DEFAULT_CLASS} reclaimPolicy ${INSTALLED_RECLAIM} -> ${CANDIDATE_RECLAIM}"
printf 'asserted: %s\n' "${ASSERTIONS[@]}"
