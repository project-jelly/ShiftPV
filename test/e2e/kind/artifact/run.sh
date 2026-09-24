#!/usr/bin/env bash
set -euo pipefail

TEST_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${TEST_DIR}/../../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
# shellcheck source=test/e2e/kind/lib/artifact.sh
source "${ROOT_DIR}/test/e2e/kind/lib/artifact.sh"
LOCK_FILE=${ARTIFACT_LOCK_FILE:-}
CLUSTER_NAME=${CLUSTER_NAME:-shiftpv-artifact-e2e}
KEEP_CLUSTER=${KEEP_CLUSTER:-0}

require_commands awk docker helm kind kubectl sed
require_kind_version

mkdir -p "${ROOT_DIR}/.tmp"
work_dir=$(mktemp -d "${ROOT_DIR}/.tmp/shiftpv-artifact.XXXXXX")
worker_a_pool="${work_dir}/worker-a"
worker_b_pool="${work_dir}/worker-b"
mkdir -p "${worker_a_pool}" "${worker_b_pool}"
export KUBECONFIG="${E2E_KUBECONFIG:-${work_dir}/kubeconfig}"

cleanup() {
  if [[ "${KEEP_CLUSTER}" == 1 ]]; then
    echo "keeping cluster ${CLUSTER_NAME}, kubeconfig ${KUBECONFIG}, and data under ${work_dir}"
    return
  fi
  kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
  rm -rf -- "${work_dir}"
}
trap cleanup EXIT

if [[ -z "${LOCK_FILE}" ]]; then
  LOCK_FILE="${work_dir}/resolved.env"
  "${TEST_DIR}/resolve-latest.sh" >"${LOCK_FILE}"
fi
load_artifact_lock "${LOCK_FILE}"
# Resolve before isolating Docker's config: desktop installations may discover
# the buildx CLI plugin through that config. The resolver isolates Helm itself.
isolate_tool_environment "${work_dir}"

sed -e "s|__WORKER_A_POOL__|${worker_a_pool}|g" \
  -e "s|__WORKER_B_POOL__|${worker_b_pool}|g" \
  "${ROOT_DIR}/test/e2e/kind/cluster.yaml.tpl" >"${work_dir}/cluster.yaml"
sed -e "s|__WORKER_A_NODE__|${CLUSTER_NAME}-worker|g" \
  -e "s|__WORKER_B_NODE__|${CLUSTER_NAME}-worker2|g" \
  "${ROOT_DIR}/test/e2e/kind/pools.yaml.tpl" >"${work_dir}/pools.yaml"

chart_package=$(pull_published_chart "${work_dir}")

kind create cluster --name "${CLUSTER_NAME}" --image "${KIND_NODE_IMAGE}" \
  --config "${work_dir}/cluster.yaml"

install_published_release "${chart_package}" shiftpv-system shiftpv --set storageClass.defaultClass=false
kubectl apply -f "${work_dir}/pools.yaml"

assert_equal "installed chart" "shiftpv-${CHART_VERSION}" \
  "$(helm -n shiftpv-system list -o json | awk -v version="shiftpv-${CHART_VERSION}" '
  index($0, "\"chart\":\"" version "\"") { print version }')"
assert_equal "controller image" "${CONTROLLER_IMAGE}" \
  "$(kubectl -n shiftpv-system get deployment/shiftpv-controller \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="shiftpv-controller")].image}')"
assert_equal "node image" "${NODE_IMAGE}" \
  "$(kubectl -n shiftpv-system get daemonset/shiftpv-node \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="shiftpv-node")].image}')"

assert_equal "ShiftPV is not the cluster default" false \
  "$(kubectl get storageclass/shiftpv -o jsonpath='{.metadata.annotations.storageclass\.kubernetes\.io/is-default-class}')"
kubectl apply -f "${TEST_DIR}/pvc.yaml"
kubectl wait --for=jsonpath='{.spec.storageClassName}'=shiftpv pvc/shiftpv-e2e --timeout=2m
kubectl apply -f "${ROOT_DIR}/test/e2e/kind/pod.yaml"
kubectl wait --for=condition=Ready pod/shiftpv-e2e --timeout=5m
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/shiftpv-e2e --timeout=2m
pv_name=$(kubectl get pvc/shiftpv-e2e -o jsonpath='{.spec.volumeName}')
volume_id=$(kubectl get "pv/${pv_name}" -o jsonpath='{.spec.csi.volumeHandle}')
test "$(kubectl get "pv/${pv_name}" -o jsonpath='{.spec.csi.driver}')" = csi.shiftpv.io
test "$(kubectl get "shiftpvvolume/${volume_id}" -o jsonpath='{.status.phase}')" = Ready
owner_node=$(kubectl get "shiftpvvolume/${volume_id}" -o jsonpath='{.status.ownerNode}')
test "$(kubectl get pod/shiftpv-e2e -o jsonpath='{.spec.nodeName}')" = "${owner_node}"
checksum=$(pod_sha256 default shiftpv-e2e /data/payload)
test -n "${checksum}"

echo "ShiftPV public artifact smoke passed: chart=${CHART_VERSION} chartSHA256=${CHART_SHA256} controller=${CONTROLLER_IMAGE} node=${NODE_IMAGE} volume=${volume_id} owner=${owner_node} checksum=${checksum}"
