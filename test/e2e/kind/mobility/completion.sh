#!/usr/bin/env bash
set -euo pipefail

# Run against the caller's isolated Kind cluster after mobility installation.
: "${CLUSTER_NAME:?isolated Kind cluster name required}"
: "${KUBECONFIG:?isolated kubeconfig required}"
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
# shellcheck source=test/e2e/kind/cleanup-journal.sh
source "${ROOT_DIR}/test/e2e/kind/cleanup-journal.sh"
NAMESPACE=shiftpv-completion-test
POLICY=shiftpv-completion-test
EXISTING_NAMESPACE=$(kubectl get namespace "${NAMESPACE}" --ignore-not-found -o name)
EXISTING_POLICY=$(kubectl get validatingadmissionpolicy "${POLICY}" --ignore-not-found -o name)
EXISTING_BINDING=$(kubectl get validatingadmissionpolicybinding "${POLICY}" --ignore-not-found -o name)
if [[ -n "${EXISTING_NAMESPACE}${EXISTING_POLICY}${EXISTING_BINDING}" ]]; then
	echo 'completion test resources already exist; refusing to overwrite them' >&2
	exit 1
fi

sed 's/shiftpv-mobility-test/shiftpv-completion-test/g' \
	"${ROOT_DIR}/test/e2e/kind/mobility/manifests/wffc-workload.yaml" | kubectl apply -f -
kubectl -n "${NAMESPACE}" rollout status deployment/wffc --timeout=180s
PV=$(kubectl -n "${NAMESPACE}" get pvc/wffc -o jsonpath='{.spec.volumeName}')
VOLUME=$(kubectl get "pv/${PV}" -o jsonpath='{.spec.csi.volumeHandle}')
SOURCE=$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.ownerNode}')
# Write unique source content after startup so an empty destination cannot pass.
# shellcheck disable=SC2016
kubectl -n "${NAMESPACE}" exec deployment/wffc -- sh -ec 'printf "%s\n" "$1" > /data/payload; sync' sh "completion-${VOLUME}"
CHECKSUM=$(pod_sha256 "${NAMESPACE}" deployment/wffc /data/payload)
kubectl patch "pv/${PV}" --type=merge -p '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'

kubectl apply -f - <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: ${POLICY}
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
    - apiGroups: [shiftpv.io]
      apiVersions: [v1alpha1]
      operations: [UPDATE]
      resources: [shiftpvmoves/status]
  validations:
  - expression: "object.spec.volumeID != '${VOLUME}' || !has(object.status.phase) || object.status.phase != 'Succeeded'"
    message: completion journal failure injection
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ${POLICY}
spec:
  policyName: ${POLICY}
  validationActions: [Deny]
EOF
kubectl cordon "${SOURCE}"
deadline=$((SECONDS + 180))
MOVE=""
while ((SECONDS < deadline)); do
	MOVE=$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.activeMove}')
	[[ -n "${MOVE}" ]] && break
	sleep 1
done
test -n "${MOVE}"
kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.phase}'=Completing --timeout=300s
deadline=$((SECONDS + 120))
while ((SECONDS < deadline)); do
	ACTIVE=$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.activeMove}')
	[[ -z "${ACTIVE}" ]] && break
	sleep 1
done
test -z "${ACTIVE}"
test "$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.status.phase}')" = Completing
if DENIAL=$(kubectl patch "shiftpvmove/${MOVE}" --subresource=status --type=merge \
	-p '{"status":{"phase":"Succeeded"}}' --dry-run=server 2>&1); then
	echo 'completion journal fault was not enforced' >&2
	exit 1
fi
grep -Fq 'completion journal failure injection' <<<"${DENIAL}"
test "$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.ownerNode}')" != "${SOURCE}"
kubectl -n "${NAMESPACE}" rollout status deployment/wffc --timeout=180s
test "$(pod_sha256 "${NAMESPACE}" deployment/wffc /data/payload)" = "${CHECKSUM}"
SOURCE_POOL=$(kubectl get shiftpvpools -o jsonpath="{.items[?(@.spec.nodeName=='${SOURCE}')].spec.mountPath}")
test -n "${SOURCE_POOL}"
docker exec "${SOURCE}" test ! -e "${SOURCE_POOL}/volumes/${VOLUME}"
SOURCE_COPY=$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.status.sourceCopy.copyID}')
test -n "${SOURCE_COPY}"
docker exec "${SOURCE}" test ! -e "${SOURCE_POOL}/.shiftpv/retired/${SOURCE_COPY}"
JOURNAL=$(kubectl get "shiftpvmove/${MOVE}" -o json)
jq -e '.status.cleanup.status.executor.kind == "Node"' <<<"${JOURNAL}" >/dev/null
NODE_POD=$(jq -er '.status.cleanup.status.executor.podName' <<<"${JOURNAL}")
NODE_UID=$(jq -er '.status.cleanup.status.executor.podUID' <<<"${JOURNAL}")
assert_cleanup_journal "shiftpvmove/${MOVE}" MoveSource "${VOLUME}" "${SOURCE_COPY}" ShiftPVMove
assert_no_cleanup_job "${MOVE}"
test "$(kubectl -n shiftpv-system get "pod/${NODE_POD}" -o jsonpath='{.metadata.uid}')" = "${NODE_UID}"
kubectl -n shiftpv-system delete "pod/${NODE_POD}" --wait=true --timeout=120s
kubectl -n shiftpv-system rollout status daemonset/shiftpv-node --timeout=180s

# Normal CSI deletion races with the still-rejected terminal journal.
kubectl delete namespace "${NAMESPACE}" --wait=true --timeout=180s
kubectl wait "pv/${PV}" --for=delete --timeout=180s
kubectl wait "shiftpvvolume/${VOLUME}" --for=delete --timeout=180s
kubectl uncordon "${SOURCE}"
kubectl -n shiftpv-system rollout restart deployment/shiftpv-controller
kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=180s
test "$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.status.phase}')" = Completing
kubectl delete validatingadmissionpolicybinding "${POLICY}"
kubectl delete validatingadmissionpolicy "${POLICY}"
kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.phase}'=Succeeded --timeout=120s
assert_no_cleanup_job "${MOVE}"
assert_cleanup_journal "shiftpvmove/${MOVE}" MoveSource "${VOLUME}" "${SOURCE_COPY}" ShiftPVMove
echo "completion journal recovery E2E passed: move=${MOVE} volume=${VOLUME} checksum=${CHECKSUM}; CSI deletion, Node Pod replacement and controller restart"
