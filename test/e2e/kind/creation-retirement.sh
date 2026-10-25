#!/usr/bin/env bash
set -euo pipefail

: "${CLUSTER_NAME:?owned Kind cluster is required}"
: "${KUBECONFIG:?isolated kubeconfig is required}"
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"

NS=shiftpv-creation-retirement-e2e
POLICY=shiftpv-creation-retirement-receipt
POOL=worker-a
NODE="${CLUSTER_NAME}-worker"
FINALIZER=tests.shiftpv.io/receipt-observer
VOLUME=
EVIDENCE="${ROOT_DIR}/.tmp/creation-retirement"
mkdir -p "${EVIDENCE}"

cleanup() {
	local result=$?
	if [[ "$(kubectl config current-context)" != "kind-${CLUSTER_NAME}" ]]; then return 1; fi
	if [[ -n "${VOLUME}" ]]; then
		kubectl get "shiftpvvolume/${VOLUME}" -o json >"${EVIDENCE}/last-volume.json" 2>/dev/null || true
	fi
	kubectl get "shiftpvpool/${POOL}" -o json >"${EVIDENCE}/last-pool.json" 2>/dev/null || true
	kubectl -n shiftpv-system logs deployment/shiftpv-controller -c shiftpv-controller >"${EVIDENCE}/controller.log" 2>&1 || true
	kubectl delete "validatingadmissionpolicybinding/${POLICY}" "validatingadmissionpolicy/${POLICY}" --ignore-not-found --wait=false >/dev/null || true
	kubectl delete "namespace/${NS}" --ignore-not-found --wait=false >/dev/null || true
	# An unsettled fixture keeps its driver protection and evidence on failure;
	# the suite owns and removes this isolated cluster afterwards.
	return "${result}"
}
trap cleanup EXIT
test -z "$(kubectl get "namespace/${NS}" --ignore-not-found -o name)"
test -z "$(kubectl get "validatingadmissionpolicy/${POLICY}" --ignore-not-found -o name)"
kubectl create namespace "${NS}"
kubectl -n "${NS}" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
spec:
  storageClassName: shiftpv
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 8Mi
EOF
PVC_UID=$(kubectl -n "${NS}" get pvc/data -o jsonpath='{.metadata.uid}')
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
      resources: [shiftpvvolumes/status, shiftpvpools/status]
  validations:
  - expression: "request.resource.resource != 'shiftpvpools' || object.metadata.name != '${POOL}' || !has(request.dryRun) || !request.dryRun"
    message: creation receipt barrier activation
  - expression: "request.resource.resource != 'shiftpvvolumes' || !has(object.spec.creationClaim) || object.spec.creationClaim.uid != '${PVC_UID}' || !has(object.status.creationReceipt)"
    message: original creation receipt held by test
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ${POLICY}
spec:
  policyName: ${POLICY}
  validationActions: [Deny]
EOF
ACTIVE=0
for _ in {1..60}; do
	POOL_OBJECT=$(kubectl get "shiftpvpool/${POOL}" -o json)
	if ! DENIAL=$(kubectl replace --subresource=status --dry-run=server --request-timeout=10s -f - <<<"${POOL_OBJECT}" 2>&1); then
		if grep -Fq 'creation receipt barrier activation' <<<"${DENIAL}"; then ACTIVE=1; break; fi
	fi
	sleep 1
done
test "${ACTIVE}" = 1
kubectl -n "${NS}" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: consumer
spec:
  nodeSelector:
    kubernetes.io/hostname: ${NODE}
  terminationGracePeriodSeconds: 1
  containers:
  - name: reader
    image: busybox:1.37
    command: [sh, -ec, "sleep 3600"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: data
EOF
for _ in {1..120}; do
	OBJECT=$(kubectl get shiftpvvolumes -o json | jq -c --arg uid "${PVC_UID}" '.items[] | select(.spec.creationClaim.uid == $uid)')
	if [[ -n "${OBJECT}" ]] && jq -e '.status.phase == "NodeCreating" and .status.creationReceipt == null and .status.currentCopy != null' <<<"${OBJECT}" >/dev/null; then
		VOLUME=$(jq -er '.metadata.name' <<<"${OBJECT}")
		if node_path_exists "${NODE}" "/mnt/shiftpv/volumes/${VOLUME}"; then break; fi
	fi
	sleep 1
done
test -n "${VOLUME}"
assert_node_file "${NODE}" "/mnt/shiftpv/volumes/${VOLUME}"
VOLUME_UID=$(jq -er '.metadata.uid' <<<"${OBJECT}")
COPY=$(jq -c '.status.currentCopy' <<<"${OBJECT}")
COPY_ID=$(jq -er '.status.currentCopy.copyID' <<<"${OBJECT}")
jq -e --arg ns "${NS}" --arg uid "${PVC_UID}" '.spec.creationClaim == {namespace:$ns,name:"data",uid:$uid} and .status.creationReceipt == null' <<<"${OBJECT}" >/dev/null
printf '%s\n' "${OBJECT}" >"${EVIDENCE}/original.json"
docker exec "${NODE}" sh -ec 'printf "%s\n" "$2" > "$1/payload"' sh "/mnt/shiftpv/volumes/${VOLUME}" "${VOLUME_UID}"
PATCH=$(jq -c --arg finalizer "${FINALIZER}" '[{op:"test",path:"/metadata/uid",value:.metadata.uid},{op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},{op:"add",path:"/metadata/finalizers/-",value:$finalizer}]' <<<"${OBJECT}")
kubectl patch "shiftpvvolume/${VOLUME}" --type=json -p "${PATCH}"
kubectl -n "${NS}" delete pod/consumer --wait=true --timeout=60s
kubectl -n "${NS}" delete pvc/data --wait=true --timeout=60s
kubectl get pv -o json | jq -e --arg id "${VOLUME}" 'all(.items[]; .spec.csi.volumeHandle != $id)' >/dev/null

# Keep the receipt-denial barrier active throughout cancellation: no retry may
# manufacture a successful creation to unlock normal deletion.
COMPLETE=0
for _ in {1..180}; do
	OBJECT=$(kubectl get "shiftpvvolume/${VOLUME}" -o json)
	jq -e --arg uid "${VOLUME_UID}" --argjson copy "${COPY}" '.metadata.uid == $uid and .status.currentCopy == $copy and .status.creationReceipt == null' <<<"${OBJECT}" >/dev/null
	if jq -e --arg finalizer "${FINALIZER}" '
	 .status.cleanup.status as $s |
	 .metadata.deletionTimestamp != null and .metadata.finalizers == [$finalizer] and
	 .status.phase == "Deleting" and .status.cleanup.spec.reason == "CreationCancelled" and
	 $s.phase == "Completed" and $s.executor.kind == "Node" and
	 $s.receipt.executorUID == $s.executor.podUID and $s.receipt.retired and $s.receipt.purged and
	 $s.absenceProof.valid and $s.absenceProof.complete and $s.absenceProof.absent and
	 $s.absenceProof.observedGeneration >= $s.absenceProof.requiredGeneration' <<<"${OBJECT}" >/dev/null; then COMPLETE=1; break; fi
	sleep 1
done
test "${COMPLETE}" = 1
printf '%s\n' "${OBJECT}" >"${EVIDENCE}/settled.json"
assert_node_absent "${NODE}" "/mnt/shiftpv/volumes/${VOLUME}"
assert_node_absent "${NODE}" "/mnt/shiftpv/.shiftpv/incoming/create-${COPY_ID}"
assert_node_absent "${NODE}" "/mnt/shiftpv/.shiftpv/retired/${COPY_ID}"
# This fixture's observer releases only its own protection after checking the
# actual native receipt and generation-fenced absence; never the driver's.
PATCH=$(jq -c '[{op:"test",path:"/metadata/uid",value:.metadata.uid},{op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},{op:"replace",path:"/metadata/finalizers",value:[]}]' <<<"${OBJECT}")
kubectl patch "shiftpvvolume/${VOLUME}" --type=json -p "${PATCH}"
kubectl wait --for=delete "shiftpvvolume/${VOLUME}" --timeout=60s
echo "ShiftPV automatic original-claim retirement passed: native partial copy, no PV/creation receipt, exact cleanup and absence"
