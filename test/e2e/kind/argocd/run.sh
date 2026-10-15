#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
# shellcheck source=test/e2e/kind/cleanup-journal.sh
source "${ROOT_DIR}/test/e2e/kind/cleanup-journal.sh"
# shellcheck source=test/e2e/kind/lib/cluster.sh
source "${ROOT_DIR}/test/e2e/kind/lib/cluster.sh"
CLUSTER_NAME=${CLUSTER_NAME:-shiftpv-argocd-e2e}
NODE_IMAGE=${NODE_IMAGE:-$(kind_node_image)}
ARGOCD_VERSION=${ARGOCD_VERSION:-v3.5.2}
ARGOCD_MANIFEST_SHA256=${ARGOCD_MANIFEST_SHA256:-9a87f2b3e14c278f12501eb0ef5c3955b27cf05370ca425381c6a908cf85a5c5}
# The ECR mirror and Docker Hub have this exact Redis 8.2.3-alpine index.
ARGOCD_REDIS_DIGEST=sha256:08ad0b1d280850169a790dba1393ff7a90aef951fc19632cf4d3ce4f78e679ba
ARGOCD_REDIS_SOURCE="docker.io/library/redis@${ARGOCD_REDIS_DIGEST}"
ARGOCD_REDIS_LOCAL="shiftpv-argocd-redis:sha256-${ARGOCD_REDIS_DIGEST#sha256:}"
IMAGE_REPOSITORY=${IMAGE_REPOSITORY:-shiftpv-argocd-e2e}
IMAGE_TAG=${IMAGE_TAG:-dev}
KEEP_CLUSTER=${KEEP_CLUSTER:-0}
NODE="${CLUSTER_NAME}-worker"
POOL_PATH=/mnt/shiftpv
VOLUME_FINALIZER_POLICY=shiftpv-argocd-volume-finalizer

for command in awk curl docker helm kind kubectl sed; do
	command -v "${command}" >/dev/null || {
		echo "required command not found: ${command}" >&2
		exit 1
	}
done

sha256_file() {
	if command -v sha256sum >/dev/null; then
		sha256sum "$1" | awk '{print $1}'
	elif command -v shasum >/dev/null; then
		shasum -a 256 "$1" | awk '{print $1}'
	else
		echo "neither sha256sum nor shasum is available" >&2
		return 1
	fi
}

prepare_argocd_redis() {
	local image_id platform expected_id nodes node node_image_id
	docker pull "${ARGOCD_REDIS_SOURCE}"
	image_id=$(docker image inspect --format '{{.Id}}' "${ARGOCD_REDIS_SOURCE}")
	platform=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "${ARGOCD_REDIS_SOURCE}")
	case "${platform}" in
	linux/amd64) expected_id=sha256:f508abec2c47b3b5099daf9e7b11ead2a9396c17ca46654f2f9723c14892e3da ;;
	linux/arm64) expected_id=sha256:7dc4da3a2de931d1c82d12b569038c90df13fe90be2b87d807201b133b4b4d21 ;;
	*) echo "unsupported Argo Redis platform: ${platform}" >&2; return 1 ;;
	esac
	if [[ "${image_id}" != "${expected_id}" ]]; then
		echo "Argo Redis image ID mismatch: ${image_id}, expected ${expected_id}" >&2
		return 1
	fi
	# A local alias survives Docker archive import without assuming preservation
	# of the original OCI index reference. Verify the loaded content on every node.
	docker image tag "${image_id}" "${ARGOCD_REDIS_LOCAL}"
	kind load docker-image "${ARGOCD_REDIS_LOCAL}" --name "${CLUSTER_NAME}"
	nodes=$(kind get nodes --name "${CLUSTER_NAME}")
	test -n "${nodes}"
	printf 'source=%s\nplatform=%s\nimageID=%s\n' \
		"${ARGOCD_REDIS_SOURCE}" "${platform}" "${image_id}" >"${WORK_DIR}/argocd-redis-image.txt"
	while IFS= read -r node; do
		node_image_id=$(docker exec "${node}" crictl inspecti \
			--output go-template --template '{{.status.id}}' "${ARGOCD_REDIS_LOCAL}")
		printf 'node=%s imageID=%s\n' "${node}" "${node_image_id}" >>"${WORK_DIR}/argocd-redis-image.txt"
		if [[ "${node_image_id}" != "${image_id}" ]]; then
			echo "Argo Redis preload mismatch on ${node}: ${node_image_id}, expected ${image_id}" >&2
			return 1
		fi
	done <<<"${nodes}"
}

preserve_argocd_bootstrap() {
	local output="${ROOT_DIR}/.tmp/ci-argocd-diagnostics/redis-bootstrap/${WORK_DIR##*/}" filename
	mkdir -p "${output}" || return 1
	# Public pinned inputs and image IDs only; never copy kubeconfig, secrets or data.
	for filename in argocd-install.yaml argocd-rendered.yaml argocd-redis-image.txt; do
		test ! -L "${WORK_DIR}/${filename}" || return 1
		if [[ -f "${WORK_DIR}/${filename}" ]]; then
			cp -- "${WORK_DIR}/${filename}" "${output}/${filename}" || return 1
		fi
	done
}

# shellcheck disable=SC2317,SC2329 # Invoked by the EXIT trap.
finish_argocd_test() {
	local result=$?
	trap - EXIT
	preserve_argocd_bootstrap || {
		echo 'failed to preserve Argo bootstrap input/image evidence' >&2
		result=1
	}
	delete_cluster_unless_kept || result=1
	exit "${result}"
}

mkdir -p "${ROOT_DIR}/.tmp"
WORK_DIR=$(mktemp -d "${ROOT_DIR}/.tmp/shiftpv-argocd.XXXXXX")
WORKER_POOL="${WORK_DIR}/worker"
CHART_REPOSITORY="${WORK_DIR}/chart-repository"
DOCKER_CONFIG_DIR="${WORK_DIR}/docker-config"
mkdir -p "${WORKER_POOL}" "${CHART_REPOSITORY}" "${DOCKER_CONFIG_DIR}"
export KUBECONFIG="${E2E_KUBECONFIG:-${WORK_DIR}/kubeconfig}"

ACTIVE_DOCKER_CONTEXT=$(docker context show)
ACTIVE_DOCKER_HOST=$(docker context inspect "${ACTIVE_DOCKER_CONTEXT}" --format '{{.Endpoints.docker.Host}}')
export DOCKER_HOST=${DOCKER_HOST:-${ACTIVE_DOCKER_HOST}}
export DOCKER_CONFIG=${DOCKER_CONFIG_DIR}
unset DOCKER_CONTEXT

trap finish_argocd_test EXIT

sed "s|__WORKER_POOL__|${WORKER_POOL}|g" \
	"${ROOT_DIR}/test/e2e/kind/argocd/cluster.yaml.tpl" >"${WORK_DIR}/cluster.yaml"
sed "s|__WORKER_NODE__|${CLUSTER_NAME}-worker|g" \
	"${ROOT_DIR}/test/e2e/kind/argocd/pool.yaml.tpl" >"${WORK_DIR}/pool.yaml"

kind create cluster --name "${CLUSTER_NAME}" --image "${NODE_IMAGE}" --config "${WORK_DIR}/cluster.yaml"

docker build \
	--target combined \
	--build-arg CONTROLLER_VERSION=dev \
	--build-arg NODE_VERSION=dev \
	-f "${ROOT_DIR}/build/package/Dockerfile" \
	-t "${IMAGE_REPOSITORY}:${IMAGE_TAG}" \
	"${ROOT_DIR}"
kind load docker-image "${IMAGE_REPOSITORY}:${IMAGE_TAG}" --name "${CLUSTER_NAME}"

CHART_VERSION=$(helm show chart "${ROOT_DIR}/charts/shiftpv" | awk '$1 == "version:" {print $2}')
test -n "${CHART_VERSION}"
helm package "${ROOT_DIR}/charts/shiftpv" --destination "${CHART_REPOSITORY}" >/dev/null
helm repo index "${CHART_REPOSITORY}"

kubectl create namespace shiftpv-chart-repository
kubectl -n shiftpv-chart-repository create configmap shiftpv-chart-repository \
	--from-file="${CHART_REPOSITORY}/index.yaml" \
	--from-file="${CHART_REPOSITORY}/shiftpv-${CHART_VERSION}.tgz"
kubectl apply -f "${ROOT_DIR}/test/e2e/kind/argocd/chart-repository.yaml"
kubectl -n shiftpv-chart-repository rollout status deployment/shiftpv-chart-repository --timeout=3m

ARGOCD_MANIFEST="${WORK_DIR}/argocd-install.yaml"
curl -fsSL "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml" \
	-o "${ARGOCD_MANIFEST}"
test "$(sha256_file "${ARGOCD_MANIFEST}")" = "${ARGOCD_MANIFEST_SHA256}"
prepare_argocd_redis
cat >"${WORK_DIR}/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - argocd-install.yaml
patches:
  - target:
      group: apps
      version: v1
      kind: Deployment
      name: argocd-redis
    patch: |-
      - op: test
        path: /spec/template/spec/containers/0/name
        value: redis
      - op: test
        path: /spec/template/spec/containers/0/image
        value: public.ecr.aws/docker/library/redis:8.2.3-alpine
      - op: test
        path: /spec/template/spec/containers/0/imagePullPolicy
        value: Always
      - op: replace
        path: /spec/template/spec/containers/0/image
        value: ${ARGOCD_REDIS_LOCAL}
      - op: replace
        path: /spec/template/spec/containers/0/imagePullPolicy
        value: IfNotPresent
EOF
ARGOCD_RENDERED="${WORK_DIR}/argocd-rendered.yaml"
kubectl kustomize "${WORK_DIR}" >"${ARGOCD_RENDERED}"
awk -v image="${ARGOCD_REDIS_LOCAL}" \
	'$1 == "image:" && $2 == image { count++ } END { exit count != 1 }' "${ARGOCD_RENDERED}"
kubectl create namespace argocd
kubectl apply --server-side --force-conflicts -n argocd -f "${ARGOCD_RENDERED}" >/dev/null

for deployment in \
	argocd-applicationset-controller \
	argocd-dex-server \
	argocd-notifications-controller \
	argocd-redis \
	argocd-repo-server \
	argocd-server; do
	kubectl -n argocd rollout status "deployment/${deployment}" --timeout=10m
done
kubectl -n argocd rollout status statefulset/argocd-application-controller --timeout=10m

sed \
	-e "s|__CHART_VERSION__|${CHART_VERSION}|g" \
	-e "s|__IMAGE_REPOSITORY__|${IMAGE_REPOSITORY}|g" \
	-e "s|__IMAGE_TAG__|${IMAGE_TAG}|g" \
	"${ROOT_DIR}/test/e2e/kind/argocd/application.yaml.tpl" >"${WORK_DIR}/application.yaml"

apply_and_wait_for_application() {
	kubectl apply -f "${WORK_DIR}/application.yaml"
	kubectl -n argocd wait --for=jsonpath='{.status.sync.status}'=Synced application/shiftpv --timeout=10m
	kubectl -n argocd wait --for=jsonpath='{.status.health.status}'=Healthy application/shiftpv --timeout=10m
	kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=5m
	kubectl -n shiftpv-system rollout status daemonset/shiftpv-node --timeout=5m
}

# No dependent storage exists, so Argo CD must execute the PreDelete hook and
# finish cascading Application deletion.
apply_and_wait_for_application
kubectl -n argocd delete application shiftpv --wait=false
kubectl -n argocd wait --for=delete application/shiftpv --timeout=5m
if kubectl get storageclass shiftpv >/dev/null 2>&1; then
	echo "Argo CD left the ShiftPV StorageClass after an allowed Application deletion" >&2
	exit 1
fi
if kubectl -n shiftpv-system get job shiftpv-uninstall-guard >/dev/null 2>&1; then
	echo "successful Argo CD uninstall guard Job was not deleted" >&2
	exit 1
fi

# Reinstall in Argo CD mode and create a real mounted volume. The long-running
# PreDelete hook must keep the Application and every driver component alive
# until the dependency is removed.
apply_and_wait_for_application
kubectl apply -f "${WORK_DIR}/pool.yaml"
kubectl apply -f "${ROOT_DIR}/test/e2e/kind/argocd/workload.yaml"
kubectl wait --for=condition=Ready pod/shiftpv-argocd-e2e --timeout=5m
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/shiftpv-argocd-e2e --timeout=2m

PV_NAME=$(kubectl get pvc shiftpv-argocd-e2e -o jsonpath='{.spec.volumeName}')
VOLUME_ID=$(kubectl get "pv/${PV_NAME}" -o jsonpath='{.spec.csi.volumeHandle}')
PVC_UID=$(kubectl get pvc shiftpv-argocd-e2e -o jsonpath='{.metadata.uid}')
COPY_ID=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.currentCopy.copyID}')
CHECKSUM_BEFORE=$(pod_sha256 default shiftpv-argocd-e2e /data/payload)
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.requestName}')" = "pvc-${PVC_UID}"
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.capacityBytes}')" = 67108864
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.initialNode}')" = "${NODE}"

# Pool deletion is fenced by a controller-owned finalizer. This Application
# test keeps the active Pool registered; directory-pool.sh proves terminating
# Pools remain until exact storage dependencies converge.
kubectl wait --for=jsonpath='{.metadata.finalizers[0]}'=shiftpv.io/pool-protection \
	shiftpvpool/worker --timeout=2m
if kubectl patch shiftpvpool/worker --type=json \
	-p='[{"op":"remove","path":"/metadata/finalizers/0"}]'; then
	echo "direct ShiftPVPool finalizer removal bypassed lifecycle admission" >&2
	exit 1
fi
if kubectl delete customresourcedefinition/shiftpvpools.shiftpv.io --wait=false; then
	echo "direct ShiftPVPool CRD deletion bypassed lifecycle admission" >&2
	exit 1
fi
if kubectl delete "shiftpvvolume/${VOLUME_ID}" --wait=false; then
	echo "direct ShiftPVVolume deletion bypassed lifecycle admission" >&2
	exit 1
fi
kubectl get shiftpvpool/worker >/dev/null
kubectl get customresourcedefinition/shiftpvpools.shiftpv.io >/dev/null
kubectl get "shiftpvvolume/${VOLUME_ID}" >/dev/null

kubectl -n argocd delete application shiftpv --wait=false
kubectl -n shiftpv-system wait --for=create job/shiftpv-uninstall-guard --timeout=2m
kubectl -n shiftpv-system wait --for=jsonpath='{.status.active}'=1 job/shiftpv-uninstall-guard --timeout=2m
test -n "$(kubectl -n argocd get application shiftpv -o jsonpath='{.metadata.deletionTimestamp}')"

kubectl -n shiftpv-system get deployment/shiftpv-controller >/dev/null
kubectl -n shiftpv-system get daemonset/shiftpv-node >/dev/null
kubectl get storageclass shiftpv >/dev/null
kubectl get validatingwebhookconfiguration shiftpv-lifecycle >/dev/null

# The guard may be between bounded attempts. It must never grant deletion while
# the mounted volume still exists.
UNINSTALL_STATE=$(kubectl -n shiftpv-system get configmap/shiftpv-uninstall-permit \
	-o jsonpath='{.data.state}' 2>/dev/null || true)
if [[ "${UNINSTALL_STATE}" == "granted" ]]; then
	echo "failed Argo CD deletion left an uninstall grant active" >&2
	exit 1
fi

CHECKSUM_AFTER_DENIAL=$(pod_sha256 default shiftpv-argocd-e2e /data/payload)
if [[ "${CHECKSUM_BEFORE}" != "${CHECKSUM_AFTER_DENIAL}" ]]; then
	echo "checksum mismatch after denied Argo CD Application deletion" >&2
	exit 1
fi

# Hold the Volume finalizer for one reconciliation after its embedded cleanup
# journal settles. This makes the complete receipt and later absence proof
# observable before the Volume-owned capacity hold is released.
kubectl apply -f - <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: ${VOLUME_FINALIZER_POLICY}
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
      - apiGroups: [shiftpv.io]
        apiVersions: [v1alpha1]
        operations: [UPDATE]
        resources: [shiftpvvolumes]
  validations:
    - expression: "object.metadata.name != '${VOLUME_ID}' || !has(oldObject.status.cleanup) || oldObject.status.cleanup.status.phase != 'Completed' || object.metadata.finalizers.exists(finalizer, finalizer == 'shiftpv.io/volume-protection')"
      message: volume cleanup settlement observation hold
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ${VOLUME_FINALIZER_POLICY}
spec:
  policyName: ${VOLUME_FINALIZER_POLICY}
  validationActions: [Deny]
EOF
kubectl patch "pv/${PV_NAME}" --type=merge \
	-p '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
kubectl delete pod shiftpv-argocd-e2e --wait=true
kubectl delete pvc shiftpv-argocd-e2e --wait=true
wait_for_cleanup_phase "shiftpvvolume/${VOLUME_ID}" Completed 5m
assert_cleanup_journal "shiftpvvolume/${VOLUME_ID}" VolumeDelete "${VOLUME_ID}" "${COPY_ID}" ShiftPVVolume
test -n "$(kubectl -n argocd get application shiftpv -o jsonpath='{.metadata.deletionTimestamp}')"
GUARD_LOG=$(kubectl -n shiftpv-system logs job/shiftpv-uninstall-guard)
grep -Fq ShiftPVVolume <<<"${GUARD_LOG}"
grep -Fq "${VOLUME_ID}" <<<"${GUARD_LOG}"
assert_node_absent "${NODE}" "${POOL_PATH}/volumes/${VOLUME_ID}"

kubectl delete validatingadmissionpolicybinding "${VOLUME_FINALIZER_POLICY}"
kubectl delete validatingadmissionpolicy "${VOLUME_FINALIZER_POLICY}"
kubectl wait --for=delete "pv/${PV_NAME}" --timeout=5m
kubectl wait --for=delete "shiftpvvolume/${VOLUME_ID}" --timeout=2m

# The same pending Application deletion now completes without restarting its
# PreDelete hook because the Volume hold and exact copy obligation are gone.
kubectl -n argocd wait --for=delete application/shiftpv --timeout=5m

if kubectl get storageclass shiftpv >/dev/null 2>&1; then
	echo "Argo CD did not finish deletion after uninstall blockers were removed" >&2
	exit 1
fi
if kubectl get validatingwebhookconfiguration shiftpv-lifecycle >/dev/null 2>&1; then
	echo "Argo CD left lifecycle validation after a completed Application deletion" >&2
	exit 1
fi
assert_node_absent "${NODE}" "${POOL_PATH}/volumes/${VOLUME_ID}"
assert_node_absent "${NODE}" "${POOL_PATH}/.shiftpv/placements/placement-${COPY_ID}.json"
assert_node_absent "${NODE}" "${POOL_PATH}/.shiftpv/copy-${COPY_ID}.json"

echo "ShiftPV Argo CD uninstall guard E2E passed"
echo "ArgoCD=${ARGOCD_VERSION} PV=${PV_NAME} volume=${VOLUME_ID} checksum=${CHECKSUM_AFTER_DENIAL}; embedded cleanup receipt and absence proof settled before deletion"
