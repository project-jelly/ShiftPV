#!/usr/bin/env bash
set -euo pipefail
set -o errtrace
trap 'printf "delayed PV handoff failed at line %s: %s\n" "${LINENO}" "${BASH_COMMAND}" >&2' ERR

: "${CLUSTER_NAME:?owned Kind cluster required}"
: "${KUBECONFIG:?private kubeconfig required}"
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
NS=shiftpv-delayed-pv-e2e
POLICY=shiftpv-delayed-pv-e2e
NODE="${CLUSTER_NAME}-worker"
EVIDENCE="${ROOT_DIR}/.tmp/delayed-pv-handoff"
VOLUME=
mkdir -p "${EVIDENCE}"

diagnostics() {
	local result=$?
	test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}" || return 1
	kubectl -n shiftpv-system logs deployment/shiftpv-controller -c csi-provisioner >"${EVIDENCE}/provisioner.log" 2>&1 || true
	if [[ -n "${VOLUME}" ]]; then
		kubectl get "shiftpvvolume/${VOLUME}" -o json >"${EVIDENCE}/last-volume.json" 2>/dev/null || true
	fi
	# Preserve the barrier and failed fixture for suite diagnostics. The parent
	# removes only its own disposable Kind cluster after uploading the evidence.
	return "${result}"
}
trap diagnostics EXIT

controller_fingerprint() {
	kubectl -n shiftpv-system get "pod/${CONTROLLER}" -o json | jq -ce '
	 {uid:.metadata.uid,provisioner:(.status.containerStatuses[]|select(.name == "csi-provisioner")|
	 {containerID,restartCount,startedAt:.state.running.startedAt})}'
}

create_claim() {
	kubectl -n "${NS}" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
spec:
  storageClassName: shiftpv-retain
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 8Mi
EOF
}

assert_ready_preserved() {
	local object
	object=$(kubectl get "shiftpvvolume/${VOLUME}" -o json)
	jq -e --argjson before "${BEFORE}" '
	 .metadata.uid == $before.metadata.uid and .metadata.deletionTimestamp == null and
	 .metadata.finalizers == $before.metadata.finalizers and .spec == $before.spec and
	 .status.phase == "Ready" and .status.currentCopy == $before.status.currentCopy and
	 .status.ownerNode == $before.status.ownerNode and .status.creationReceipt == $before.status.creationReceipt and
	 (.status.publishedNodes // [] | length) == 0 and (.status.activeMove // "") == "" and
	 (.status.deletionOperationID // "") == "" and .status.cleanup == null' <<<"${object}" >/dev/null
	test "$(node_sha256 "${NODE}" "/mnt/shiftpv/volumes/${VOLUME}/payload")" = "${CHECKSUM}"
	test "$(controller_fingerprint)" = "${FINGERPRINT}"
}

for resource in namespace validatingadmissionpolicy validatingadmissionpolicybinding; do
	test -z "$(kubectl get "${resource}/${NS}" --ignore-not-found -o name)"
done
kubectl create namespace "${NS}"
create_claim
PVC_UID=$(kubectl -n "${NS}" get pvc/data -o jsonpath='{.metadata.uid}')
PV="pvc-${PVC_UID}"
CONTROLLER=$(kubectl -n shiftpv-system get pods -l app.kubernetes.io/component=controller -o json | jq -er '.items | if length == 1 then .[0].metadata.name else error("expected sole Controller") end')
FINGERPRINT=$(controller_fingerprint)
kubectl apply -f - <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: ${POLICY}
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: [v1]
      operations: [CREATE]
      resources: [persistentvolumes]
  validations:
  - expression: "!has(object.spec.claimRef) || !has(object.spec.claimRef.uid) || object.spec.claimRef.uid != '${PVC_UID}'"
    message: original PV handoff held by test
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ${POLICY}
spec:
  policyName: ${POLICY}
  validationActions: [Deny]
EOF
# A dry-run probe warms the exact PVC-UID policy before native creation starts.
PROBE=$(jq -n --arg ns "${NS}" --arg uid "${PVC_UID}" --arg name "${POLICY}-probe" '{
 apiVersion:"v1",kind:"PersistentVolume",metadata:{name:$name},spec:{capacity:{storage:"8Mi"},
 accessModes:["ReadWriteOnce"],persistentVolumeReclaimPolicy:"Retain",storageClassName:"shiftpv-retain",
 csi:{driver:"csi.shiftpv.io",volumeHandle:"barrier-probe"},claimRef:{namespace:$ns,name:"data",uid:$uid}}}')
ACTIVE=0
for _ in {1..60}; do
	if ! DENIAL=$(kubectl create --dry-run=server -f - <<<"${PROBE}" 2>&1); then
		if grep -Fq 'original PV handoff held by test' <<<"${DENIAL}"; then ACTIVE=1; break; fi
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
READY=0
for _ in {1..180}; do
	BEFORE=$(kubectl get shiftpvvolumes -o json | jq -c --arg uid "${PVC_UID}" '.items[]|select(.spec.creationClaim.uid == $uid)')
	if [[ -n "${BEFORE}" ]] && jq -e '.status.phase == "Ready" and .status.creationReceipt != null and .spec.capacityBytes == 8388608' <<<"${BEFORE}" >/dev/null; then
		kubectl -n shiftpv-system logs "pod/${CONTROLLER}" -c csi-provisioner >"${EVIDENCE}/provisioner-held.log"
		if grep -F "${PV}" "${EVIDENCE}/provisioner-held.log" | grep -F 'original PV handoff held by test' >/dev/null; then READY=1; break; fi
	fi
	sleep 1
done
test "${READY}" = 1
VOLUME=$(jq -er '.metadata.name' <<<"${BEFORE}")
COPY_ID=$(jq -er '.status.currentCopy.copyID' <<<"${BEFORE}")
printf '%s\n' "${BEFORE}" >"${EVIDENCE}/ready-before.json"
test -z "$(kubectl get "pv/${PV}" --ignore-not-found -o name)"
docker exec "${NODE}" sh -ec 'printf "%s\n" "$2" > "$1/payload"; sync' sh "/mnt/shiftpv/volumes/${VOLUME}" "${PVC_UID}"
CHECKSUM=$(node_sha256 "${NODE}" "/mnt/shiftpv/volumes/${VOLUME}/payload")
kubectl -n "${NS}" delete pod/consumer --wait=true --timeout=60s
kubectl -n "${NS}" delete pvc/data --wait=true --timeout=60s
# Name reuse cannot bind the deferred original PV or approve another request.
create_claim
REPLACEMENT_UID=$(kubectl -n "${NS}" get pvc/data -o jsonpath='{.metadata.uid}')
test "${REPLACEMENT_UID}" != "${PVC_UID}"
for _ in {1..4}; do
	assert_ready_preserved
	test -z "$(kubectl get "pv/${PV}" --ignore-not-found -o name)"
	sleep 10
done
kubectl get "shiftpvvolume/${VOLUME}" -o json >"${EVIDENCE}/ready-after-claim-end.json"
kubectl delete "validatingadmissionpolicybinding/${POLICY}"
kubectl wait "pv/${PV}" --for=create --timeout=180s
kubectl wait "pv/${PV}" --for=jsonpath='{.status.phase}'=Released --timeout=120s
PV_OBJECT=$(kubectl get "pv/${PV}" -o json)
jq -e --arg id "${VOLUME}" --arg uid "${PVC_UID}" --arg ns "${NS}" '
 .spec.csi.driver == "csi.shiftpv.io" and .spec.csi.volumeHandle == $id and
 .spec.claimRef.namespace == $ns and .spec.claimRef.name == "data" and .spec.claimRef.uid == $uid and
 .spec.persistentVolumeReclaimPolicy == "Retain"' <<<"${PV_OBJECT}" >/dev/null
printf '%s\n' "${PV_OBJECT}" >"${EVIDENCE}/deferred-pv.json"
kubectl -n "${NS}" get pvc/data -o json | jq -e --arg uid "${REPLACEMENT_UID}" '.metadata.uid == $uid and .status.phase == "Pending" and (.spec.volumeName // "") == ""' >/dev/null
assert_ready_preserved
# Reclaim the owned Retain fixture only through its normal CSI Delete path.
kubectl patch "pv/${PV}" --type=merge -p '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
kubectl wait "pv/${PV}" --for=delete --timeout=180s
kubectl wait "shiftpvvolume/${VOLUME}" --for=delete --timeout=180s
assert_node_absent "${NODE}" "/mnt/shiftpv/volumes/${VOLUME}"
assert_node_absent "${NODE}" "/mnt/shiftpv/.shiftpv/copy-${COPY_ID}.json"
kubectl delete "validatingadmissionpolicy/${POLICY}"
kubectl delete "namespace/${NS}" --wait=true --timeout=60s
jq -n --arg uid "${PVC_UID}" --arg replacement "${REPLACEMENT_UID}" --arg checksum "${CHECKSUM}" \
 '{result:"PASS",originalClaimUID:$uid,replacementClaimUID:$replacement,checksum:$checksum,
 readyPreservedWithoutPV:true,deferredOriginalHandoff:true,sameProvisionerProcess:true,normalCSIReclaim:true}' >"${EVIDENCE}/result.json"
cat "${EVIDENCE}/result.json"
