#!/usr/bin/env bash
set -euo pipefail

# The chart/CRDs have already rolled forward. Prove that the published controller's
# in-flight legacy journal completes after the candidate controller takes over.
: "${CLUSTER_NAME:?isolated Kind cluster name required}"
: "${KUBECONFIG:?isolated kubeconfig required}"
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
NAMESPACE=shiftpv-legacy-upgrade-test
POLICY=shiftpv-legacy-upgrade-test
LEGACY_CONTROLLER=ghcr.io/project-jelly/shiftpv-controller:0.4.22@sha256:933d06dca8a0b1a04e70ead704489af629b3ff3f70e2ee728d0ada882fd1ed83
CANDIDATE_CONTROLLER=$(kubectl -n shiftpv-system get deployment/shiftpv-controller -o jsonpath='{.spec.template.spec.containers[?(@.name=="shiftpv-controller")].image}')
CANDIDATE_ARGS=$(kubectl -n shiftpv-system get deployment/shiftpv-controller -o json | jq -c '.spec.template.spec.containers[] | select(.name == "shiftpv-controller") | .args')
# The older binary predates this flag; preserve and restore the exact candidate
# arguments while changing the image and supported arguments in one update.
LEGACY_ARGS=$(jq -c 'map(select(startswith("--mobility-placement-priority-class=") | not))' <<<"${CANDIDATE_ARGS}")
SOURCE="${CLUSTER_NAME}-worker"
DESTINATION="${CLUSTER_NAME}-worker2"
if [[ -n "$(kubectl get namespace "${NAMESPACE}" --ignore-not-found -o name)$(kubectl get validatingadmissionpolicy "${POLICY}" --ignore-not-found -o name)$(kubectl get validatingadmissionpolicybinding "${POLICY}" --ignore-not-found -o name)" ]]; then
	echo 'legacy upgrade test resources already exist; refusing to overwrite' >&2
	exit 1
fi
# Running the old writer over prepared journals is an unsupported downgrade,
# including terminal history. The CI shard runs this before prepared scenarios.
if ! kubectl get shiftpvmoves -o json | jq -e 'all(.items[]; (.spec.protocolVersion // "") == "")' >/dev/null; then
	echo 'legacy upgrade test requires no prepared journals before starting the published controller' >&2
	exit 1
fi
kubectl -n shiftpv-system patch deployment/shiftpv-controller --type=strategic -p "$(jq -nc \
	--arg image "${LEGACY_CONTROLLER}" --argjson args "${LEGACY_ARGS}" \
	'{spec:{template:{spec:{containers:[{name:"shiftpv-controller",image:$image,args:$args}]}}}}')"
kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=180s
kubectl uncordon "${SOURCE}"
kubectl cordon "${DESTINATION}"
sed "s/shiftpv-mobility-test/${NAMESPACE}/g" \
	"${ROOT_DIR}/test/e2e/kind/mobility/manifests/wffc-workload.yaml" | kubectl apply -f -
kubectl -n "${NAMESPACE}" rollout status deployment/wffc --timeout=180s
PV=$(kubectl -n "${NAMESPACE}" get pvc/wffc -o jsonpath='{.spec.volumeName}')
CLAIM_UID=$(kubectl -n "${NAMESPACE}" get pvc/wffc -o jsonpath='{.metadata.uid}')
VOLUME=$(kubectl get "pv/${PV}" -o jsonpath='{.spec.csi.volumeHandle}')
# The replacement template cannot recreate this per-PVC data after data loss.
printf 'ShiftPV durable payload pvc=%s volume=%s\n' "${CLAIM_UID}" "${VOLUME}" \
	| kubectl -n "${NAMESPACE}" exec -i deployment/wffc -- sh -ec 'cat > /data/payload; sync'
CHECKSUM=$(pod_sha256 "${NAMESPACE}" deployment/wffc /data/payload)
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
  - expression: "object.spec.volumeID != '${VOLUME}' || !has(object.status.phase) || object.status.phase != 'Copying'"
    message: legacy upgrade execution boundary failure injection
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ${POLICY}
spec:
  policyName: ${POLICY}
  validationActions: [Deny]
EOF
kubectl uncordon "${DESTINATION}"
kubectl cordon "${SOURCE}"
deadline=$((SECONDS + 240))
MOVE=""
while ((SECONDS < deadline)); do
	MOVE=$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.activeMove}')
	if [[ -n "${MOVE}" ]] && [[ "$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.status.capacityApproved}')" = true ]]; then break; fi
	sleep 2
done
test -n "${MOVE}"
kubectl get "shiftpvmove/${MOVE}" -o json | jq -e '.spec.protocolVersion == null and .status.preparation == null and .status.capacityApproved == true and .status.phase == "WaitingForCapacity"' >/dev/null
MOVE_UID=$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.metadata.uid}')
if DENIAL=$(kubectl patch "shiftpvmove/${MOVE}" --subresource=status --type=merge \
	-p '{"status":{"phase":"Copying"}}' --dry-run=server 2>&1); then
	echo 'legacy upgrade boundary policy was not enforced' >&2
	exit 1
fi
grep -Fq 'legacy upgrade execution boundary failure injection' <<<"${DENIAL}"
kubectl -n shiftpv-system patch deployment/shiftpv-controller --type=strategic -p "$(jq -nc \
	--arg image "${CANDIDATE_CONTROLLER}" --argjson args "${CANDIDATE_ARGS}" \
	'{spec:{template:{spec:{containers:[{name:"shiftpv-controller",image:$image,args:$args}]}}}}')"
kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=180s
test "$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.metadata.uid}')" = "${MOVE_UID}"
test -z "$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.spec.protocolVersion}')"
kubectl delete validatingadmissionpolicybinding "${POLICY}"
kubectl delete validatingadmissionpolicy "${POLICY}"
kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.phase}'=Succeeded --timeout=600s
kubectl -n "${NAMESPACE}" rollout status deployment/wffc --timeout=180s
test "$(pod_sha256 "${NAMESPACE}" deployment/wffc /data/payload)" = "${CHECKSUM}"
test "$(kubectl -n "${NAMESPACE}" get pvc/wffc -o jsonpath='{.metadata.uid}')" = "${CLAIM_UID}"
test "$(kubectl get "pv/${PV}" -o jsonpath='{.spec.csi.volumeHandle}')" = "${VOLUME}"
test "$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.ownerNode}')" = "${DESTINATION}"
test -z "$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.activeMove}')"
test "$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.metadata.uid}')" = "${MOVE_UID}"
test -z "$(kubectl get "shiftpvmove/${MOVE}" -o jsonpath='{.spec.protocolVersion}')"
kubectl uncordon "${SOURCE}"
kubectl delete namespace "${NAMESPACE}" --wait=true --timeout=180s
kubectl wait "pv/${PV}" --for=delete --timeout=180s
kubectl wait "shiftpvvolume/${VOLUME}" --for=delete --timeout=180s
echo 'published legacy controller journal completed under candidate without protocol rewrite'
