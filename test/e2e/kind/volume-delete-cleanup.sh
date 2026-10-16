#!/usr/bin/env bash
set -euo pipefail

: "${ROOT_DIR:=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"

: "${CLUSTER_NAME:?CLUSTER_NAME is required}"
: "${KUBECONFIG:?isolated kubeconfig is required}"
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"

NAMESPACE=shiftpv-delete-cleanup-e2e
NODE="${CLUSTER_NAME}-worker"
POOL=worker-a
POOL_PATH=/mnt/shiftpv
STORAGE_CLASS=shiftpv-delete-cleanup
FIRST_PVC=data
SECOND_PVC=reuse
VOLUME_ID=
VOLUME_UID=
COPY_ID=
PV_NAME=
SECOND_PV_NAME=
POOL_UID=
BASELINE_SCAN_EPOCH=
BASELINE_GENERATION=
NODE_POD=
NODE_POD_UID=
OBSERVATION_POLICY=shiftpv-delete-cleanup-observation
OBSERVATION_HELD=0
WATCH_PID=
WATCH_FILE=

cleanup() {
	local result_code=$?
	if [[ -n "${WATCH_PID}" ]]; then
		kill "${WATCH_PID}" >/dev/null 2>&1 || true
		wait "${WATCH_PID}" >/dev/null 2>&1 || true
	fi
	if [[ -n "${WATCH_FILE}" ]]; then
		rm -f -- "${WATCH_FILE}"
	fi
	if [[ "$(kubectl config current-context)" != "kind-${CLUSTER_NAME}" ]]; then
		echo "refusing cleanup outside owned Kind context" >&2
		return 1
	fi
	if [[ "${OBSERVATION_HELD}" == "1" ]]; then
		release_pool_observation >/dev/null 2>&1 || true
	fi
	kubectl -n "${NAMESPACE}" delete pod "${FIRST_PVC}" "${SECOND_PVC}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
	kubectl -n "${NAMESPACE}" delete pvc "${FIRST_PVC}" "${SECOND_PVC}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
	if [[ -n "${PV_NAME}" ]]; then
		kubectl delete "pv/${PV_NAME}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
	fi
	if [[ -n "${SECOND_PV_NAME}" ]]; then
		kubectl delete "pv/${SECOND_PV_NAME}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
	fi
	kubectl delete namespace "${NAMESPACE}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
	kubectl delete storageclass "${STORAGE_CLASS}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
	kubectl patch shiftpvpool "${POOL}" --type=merge -p '{"spec":{"capacity":{"limit":"10Gi"}}}' >/dev/null 2>&1 || true
	return "${result_code}"
}
trap cleanup EXIT

create_workload() {
	local name=$1 size=$2 payload=$3
	kubectl -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${name}
spec:
  storageClassName: ${STORAGE_CLASS}
  accessModes: [ReadWriteOnce]
  volumeMode: Filesystem
  resources:
    requests:
      storage: ${size}
---
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
spec:
  nodeSelector:
    kubernetes.io/hostname: ${NODE}
  restartPolicy: Never
  terminationGracePeriodSeconds: 1
  containers:
    - name: writer
      image: busybox:1.37
      command: [sh, -ec]
      args:
        - printf '${payload}\n' > /data/payload; sleep 3600
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${name}
EOF
}

wait_for_volume_absent_from_inventory() {
	local deadline=$((SECONDS + 120))
	while ((SECONDS < deadline)); do
		if kubectl get "shiftpvpool/${POOL}" -o json | jq -e --arg volume "${VOLUME_ID}" \
			'.status.inventory.valid == true and all(.status.inventory.copies[]?; .identity.volumeID != $volume)' >/dev/null; then
			return
		fi
		sleep 1
	done
	echo "Pool inventory still reports volume ${VOLUME_ID}" >&2
	kubectl get "shiftpvpool/${POOL}" -o yaml >&2 || true
	return 1
}

wait_for_unpublished() {
	local deadline=$((SECONDS + 120))
	while ((SECONDS < deadline)); do
		if [[ -z "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.publishedNodes[*]}' 2>/dev/null || true)" ]]; then
			return
		fi
		sleep 1
	done
	echo "ShiftPVVolume ${VOLUME_ID} did not clear publishedNodes" >&2
	kubectl get "shiftpvvolume/${VOLUME_ID}" -o yaml >&2 || true
	return 1
}

# Capture a coherent, unpublished inventory before installing the barrier.
# Unpublish reconciles the Volume under its local lock; the later Pool probe
# proves the same exact copy is no longer mounted. The Node remains available
# to execute cleanup while scanEpoch still equals this observed baseline.
wait_for_unpublished_pool_observation() {
	local deadline=$((SECONDS + 120)) pool
	while ((SECONDS < deadline)); do
		pool=$(kubectl get "shiftpvpool/${POOL}" -o json)
		if jq -e --arg uid "${POOL_UID}" --arg volume "${VOLUME_ID}" --arg volume_uid "${VOLUME_UID}" --arg copy "${COPY_ID}" '
			.metadata.uid == $uid and .status.observedGeneration == .metadata.generation and
			.status.inventory.valid == true and .status.inventory.truncated != true and
			any(.status.conditions[]?; .type == "Ready" and .status == "True" and .observedGeneration == $generation) and
			any(.status.inventory.copies[]?; .identity.volumeID == $volume and .identity.volumeUID == $volume_uid and
				.identity.copyID == $copy and .present == true and .published != true) and
			((.status.lastProbeTime | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601) >= now - 30)
		' --argjson generation "$(jq '.metadata.generation' <<<"${pool}")" <<<"${pool}" >/dev/null; then
			BASELINE_SCAN_EPOCH=$(jq -er '.spec.scanEpoch // 0' <<<"${pool}")
			BASELINE_GENERATION=$(jq -er '.metadata.generation' <<<"${pool}")
			return
		fi
		sleep 1
	done
	echo "Pool did not observe exact unpublished copy before cleanup" >&2
	kubectl get "shiftpvpool/${POOL}" -o yaml >&2 || true
	return 1
}

hold_pool_observation() {
	local deadline pool denial
	test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"
	test -z "$(kubectl get "validatingadmissionpolicy/${OBSERVATION_POLICY}" --ignore-not-found -o name)"
	test -z "$(kubectl get "validatingadmissionpolicybinding/${OBSERVATION_POLICY}" --ignore-not-found -o name)"
	OBSERVATION_HELD=1
	kubectl apply -f - <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: ${OBSERVATION_POLICY}
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
    - apiGroups: [shiftpv.io]
      apiVersions: [v1alpha1]
      operations: [UPDATE]
      resources: [shiftpvpools/status]
  validations:
  - expression: "object.metadata.name != '${POOL}' || object.metadata.uid != '${POOL_UID}' || !has(request.dryRun) || !request.dryRun || (has(object.spec.scanEpoch) && object.spec.scanEpoch > ${BASELINE_SCAN_EPOCH})"
    message: volume delete observation barrier activation
  - expression: "object.metadata.name != '${POOL}' || object.metadata.uid != '${POOL_UID}' || !has(object.spec.scanEpoch) || object.spec.scanEpoch <= ${BASELINE_SCAN_EPOCH}"
    message: volume delete post-receipt observation barrier
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ${OBSERVATION_POLICY}
spec:
  policyName: ${OBSERVATION_POLICY}
  validationActions: [Deny]
EOF
	# Policy creation is asynchronous. Prove the exact target's dry-run rule is
	# active before deletion; real baseline writes still pass both rules.
	deadline=$((SECONDS + 60))
	while ((SECONDS < deadline)); do
		pool=$(kubectl get "shiftpvpool/${POOL}" --request-timeout=10s -o json)
		jq -e --arg uid "${POOL_UID}" --argjson baseline "${BASELINE_SCAN_EPOCH}" \
			'.metadata.uid == $uid and (.spec.scanEpoch // 0) == $baseline' <<<"${pool}" >/dev/null
		if ! denial=$(kubectl replace --subresource=status --dry-run=server --request-timeout=10s -f - <<<"${pool}" 2>&1); then
			if grep -Fq 'volume delete observation barrier activation' <<<"${denial}"; then
				return
			fi
		fi
		sleep 1
	done
	echo "Pool observation barrier did not become active before deletion: ${denial}" >&2
	kubectl get "validatingadmissionpolicy/${OBSERVATION_POLICY}" -o yaml >&2 || true
	return 1
}

assert_pool_observation_barrier() {
	local pool denial required_generation
	pool=$(kubectl get "shiftpvpool/${POOL}" -o json)
	required_generation=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.absenceProof.requiredGeneration}')
	jq -e --arg uid "${POOL_UID}" --argjson baseline "${BASELINE_SCAN_EPOCH}" --argjson required "${required_generation}" '
		.metadata.uid == $uid and .spec.scanEpoch > $baseline and
		.metadata.generation >= $required and .status.observedGeneration < $required
	' <<<"${pool}" >/dev/null
	# Use the real post-fence spec. A fabricated newer status is never stored.
	if denial=$(kubectl replace --subresource=status --dry-run=server -f - <<<"${pool}" 2>&1); then
		echo "post-receipt Pool observation barrier was not enforced" >&2
		return 1
	fi
	grep -Fq 'volume delete post-receipt observation barrier' <<<"${denial}"
}

release_pool_observation() {
	test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}" || return
	kubectl delete "validatingadmissionpolicybinding/${OBSERVATION_POLICY}" --ignore-not-found || return
	kubectl delete "validatingadmissionpolicy/${OBSERVATION_POLICY}" --ignore-not-found || return
	OBSERVATION_HELD=0
}

assert_second_claim_blocked_by_capacity_hold() {
	local second_pvc_uid deadline messages
	second_pvc_uid=$(kubectl -n "${NAMESPACE}" get "pvc/${SECOND_PVC}" -o jsonpath='{.metadata.uid}')
	deadline=$((SECONDS + 120))
	while ((SECONDS < deadline)); do
		test -z "$(kubectl -n "${NAMESPACE}" get "pvc/${SECOND_PVC}" -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)"
		kubectl get shiftpvvolumes.shiftpv.io -o json | jq -e --arg request "pvc-${second_pvc_uid}" \
			'all(.items[]?; .spec.requestName != $request)' >/dev/null
		messages=$(kubectl -n "${NAMESPACE}" get events \
			--field-selector "involvedObject.kind=PersistentVolumeClaim,involvedObject.name=${SECOND_PVC}" \
			-o jsonpath='{range .items[*]}{.message}{"\n"}{end}' 2>/dev/null || true)
		if grep -Fq 'code = ResourceExhausted' <<<"${messages}"; then
			return
		fi
		sleep 1
	done
	echo "second PVC did not prove ResourceExhausted while the first delete retained capacity" >&2
	kubectl -n "${NAMESPACE}" get "pvc/${SECOND_PVC}" -o yaml >&2 || true
	kubectl -n "${NAMESPACE}" get events --field-selector "involvedObject.name=${SECOND_PVC}" >&2 || true
	kubectl get shiftpvvolumes.shiftpv.io -o yaml >&2 || true
	return 1
}

assert_second_claim_unbound_without_volume() {
	local second_pvc_uid
	second_pvc_uid=$(kubectl -n "${NAMESPACE}" get "pvc/${SECOND_PVC}" -o jsonpath='{.metadata.uid}')
	test -z "$(kubectl -n "${NAMESPACE}" get "pvc/${SECOND_PVC}" -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)"
	kubectl get shiftpvvolumes.shiftpv.io -o json | jq -e --arg request "pvc-${second_pvc_uid}" \
		'all(.items[]?; .spec.requestName != $request)' >/dev/null
}

assert_confirming_volume_delete_journal() {
	local parent_uid operation_id executor_uid receipt_executor_uid required_generation
	parent_uid=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.metadata.uid}')
	operation_id=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.operationID}')
	executor_uid=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.executor.podUID}')
	receipt_executor_uid=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.receipt.executorUID}')
	required_generation=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.absenceProof.requiredGeneration}')

	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.metadata.finalizers[0]}')" = shiftpv.io/volume-protection
	test "${parent_uid}" = "${VOLUME_UID}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.capacityBytes}')" = 8388608
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.phase}')" = Deleting
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.phase}')" = ConfirmingAbsence
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.reason}')" = VolumeDelete
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.authority.kind}')" = ShiftPVVolume
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.authority.name}')" = "${VOLUME_ID}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.authority.uid}')" = "${parent_uid}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.target.volumeID}')" = "${VOLUME_ID}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.target.volumeUID}')" = "${VOLUME_UID}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.target.copyID}')" = "${COPY_ID}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.target.poolUID}')" = "${POOL_UID}"
	test -n "${operation_id}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.executor.kind}')" = Node
	test -z "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.executor.jobName}')"
	test -z "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.executor.jobUID}')"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.executor.namespace}')" = shiftpv-system
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.executor.podName}')" = "${NODE_POD}"
	test "${executor_uid}" = "${NODE_POD_UID}"
	test "$(kubectl -n shiftpv-system get "pod/${NODE_POD}" -o jsonpath='{.metadata.uid}')" = "${NODE_POD_UID}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.receipt.operationID}')" = "${operation_id}"
	test "${receipt_executor_uid}" = "${executor_uid}"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.receipt.retired}')" = true
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.receipt.purged}')" = true
	[[ "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.receipt.localReceiptDigest}')" =~ ^[0-9a-f]{64}$ ]]
	test -n "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.absenceProof.requestID}')"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.absenceProof.poolName}')" = \
		"$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.target.poolName}')"
	test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.status.absenceProof.poolUID}')" = \
		"$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.cleanup.spec.target.poolUID}')"
	[[ "${required_generation}" =~ ^[1-9][0-9]*$ ]]
	((required_generation > BASELINE_GENERATION))
}

start_volume_watch() {
	local deadline=$((SECONDS + 30))
	WATCH_FILE=$(mktemp)
	kubectl get "shiftpvvolume/${VOLUME_ID}" -o json --watch --output-watch-events >"${WATCH_FILE}" &
	WATCH_PID=$!
	# Establish the initial resourceVersion before releasing the barrier, so a
	# fast Completed->parent deletion cannot overtake watch startup.
	while ((SECONDS < deadline)); do
		kill -0 "${WATCH_PID}"
		if jq -e --arg uid "${VOLUME_UID}" '
			select(.type == "ADDED" and .object.metadata.uid == $uid and
				.object.status.cleanup.status.phase == "ConfirmingAbsence")
		' "${WATCH_FILE}" >/dev/null 2>&1; then
			return
		fi
		sleep 1
	done
	echo "volume watch did not establish the held cleanup observation" >&2
	return 1
}

stop_volume_watch() {
	if [[ -n "${WATCH_PID}" ]]; then
		kill "${WATCH_PID}" >/dev/null 2>&1 || true
		wait "${WATCH_PID}" >/dev/null 2>&1 || true
		WATCH_PID=
	fi
}

assert_watch_saw_completed_before_finalizer_release() {
	if ! jq -e --arg uid "${VOLUME_UID}" --arg executor "${NODE_POD_UID}" '
		select(.type == "MODIFIED")
		| .object
		| select(.metadata.uid == $uid)
		| select((.metadata.finalizers // []) | index("shiftpv.io/volume-protection"))
		| select(.status.cleanup.spec.reason == "VolumeDelete")
		| select(.status.cleanup.status.phase == "Completed")
		| select(.status.cleanup.status.receipt.retired == true and .status.cleanup.status.receipt.purged == true)
		| select(.status.cleanup.status.executor.kind == "Node" and .status.cleanup.status.executor.podUID == $executor)
		| select(.status.cleanup.status.receipt.executorUID == $executor)
		| select(.status.cleanup.status.absenceProof.valid == true)
		| select(.status.cleanup.status.absenceProof.complete == true)
		| select(.status.cleanup.status.absenceProof.absent == true)
	' "${WATCH_FILE}" >/dev/null; then
		echo "watch did not observe completed VolumeDelete cleanup before finalizer release" >&2
		cat "${WATCH_FILE}" >&2
		return 1
	fi
}

for command in docker kind kubectl jq; do
	command -v "${command}" >/dev/null || {
		echo "required command not found: ${command}" >&2
		exit 1
	}
done

for resource in shiftpvpools.shiftpv.io shiftpvvolumes.shiftpv.io shiftpvmoves.shiftpv.io; do
	kubectl get "customresourcedefinition/${resource}" >/dev/null
done
if kubectl get customresourcedefinition/shiftpvcleanups.shiftpv.io >/dev/null 2>&1; then
	echo 'standalone ShiftPVCleanup API is still installed' >&2
	exit 1
fi

kubectl create namespace "${NAMESPACE}"
kubectl patch shiftpvpool "${POOL}" --type=merge -p '{"spec":{"capacity":{"limit":"16Mi"}}}' >/dev/null
kubectl wait --for=condition=Ready "shiftpvpool/${POOL}" --timeout=3m >/dev/null
kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ${STORAGE_CLASS}
provisioner: csi.shiftpv.io
reclaimPolicy: Delete
allowVolumeExpansion: false
volumeBindingMode: WaitForFirstConsumer
EOF

create_workload "${FIRST_PVC}" 8Mi "ShiftPV DeleteVolume cleanup ordering"
kubectl -n "${NAMESPACE}" wait --for=condition=Ready "pod/${FIRST_PVC}" --timeout=5m
kubectl -n "${NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Bound "pvc/${FIRST_PVC}" --timeout=2m
PV_NAME=$(kubectl -n "${NAMESPACE}" get "pvc/${FIRST_PVC}" -o jsonpath='{.spec.volumeName}')
PVC_UID=$(kubectl -n "${NAMESPACE}" get "pvc/${FIRST_PVC}" -o jsonpath='{.metadata.uid}')
VOLUME_ID=$(kubectl get "pv/${PV_NAME}" -o jsonpath='{.spec.csi.volumeHandle}')
VOLUME_UID=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.metadata.uid}')
COPY_ID=$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.status.currentCopy.copyID}')
POOL_UID=$(kubectl get "shiftpvpool/${POOL}" -o jsonpath='{.metadata.uid}')
CHECKSUM=$(pod_sha256 "${NAMESPACE}" "${FIRST_PVC}" /data/payload)
test "${CHECKSUM}" = "$(node_sha256 "${NODE}" "${POOL_PATH}/volumes/${VOLUME_ID}/payload")"
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.requestName}')" = "pvc-${PVC_UID}"
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.capacityBytes}')" = 8388608
test "$(kubectl get "shiftpvvolume/${VOLUME_ID}" -o jsonpath='{.spec.initialNode}')" = "${NODE}"

create_workload "${SECOND_PVC}" 16Mi "ShiftPV capacity hold reuse after delete"
assert_second_claim_blocked_by_capacity_hold
kubectl -n "${NAMESPACE}" delete "pod/${FIRST_PVC}" --wait=true
wait_for_unpublished
wait_for_unpublished_pool_observation
NODE_POD=$(kubectl -n shiftpv-system get pods -l app.kubernetes.io/instance=shiftpv,app.kubernetes.io/component=node \
	--field-selector "spec.nodeName=${NODE}" -o json |
	jq -er '.items | map(select(.metadata.deletionTimestamp == null)) | if length == 1 then .[0].metadata.name else error("expected one Node executor") end')
kubectl -n shiftpv-system wait --for=condition=Ready "pod/${NODE_POD}" --timeout=120s >/dev/null
NODE_POD_UID=$(kubectl -n shiftpv-system get "pod/${NODE_POD}" -o jsonpath='{.metadata.uid}')
hold_pool_observation
kubectl -n "${NAMESPACE}" delete "pvc/${FIRST_PVC}" --wait=true
kubectl wait "shiftpvvolume/${VOLUME_ID}" \
	--for=jsonpath='{.status.cleanup.status.phase}'=ConfirmingAbsence \
	--timeout=3m >/dev/null
assert_pool_observation_barrier
assert_confirming_volume_delete_journal
assert_node_absent "${NODE}" "${POOL_PATH}/volumes/${VOLUME_ID}"
assert_second_claim_unbound_without_volume
start_volume_watch
release_pool_observation
kubectl wait --for=delete "pv/${PV_NAME}" --timeout=5m
kubectl wait --for=delete "shiftpvvolume/${VOLUME_ID}" --timeout=2m
stop_volume_watch
assert_watch_saw_completed_before_finalizer_release
PV_NAME=
wait_for_volume_absent_from_inventory

kubectl -n "${NAMESPACE}" wait --for=condition=Ready "pod/${SECOND_PVC}" --timeout=5m
kubectl -n "${NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Bound "pvc/${SECOND_PVC}" --timeout=2m
SECOND_PV_NAME=$(kubectl -n "${NAMESPACE}" get "pvc/${SECOND_PVC}" -o jsonpath='{.spec.volumeName}')
kubectl -n "${NAMESPACE}" delete "pod/${SECOND_PVC}" --wait=true
kubectl -n "${NAMESPACE}" delete "pvc/${SECOND_PVC}" --wait=true
kubectl wait --for=delete "pv/${SECOND_PV_NAME}" --timeout=5m
SECOND_PV_NAME=

trap - EXIT
cleanup
echo "ShiftPV DeleteVolume cleanup ordering E2E passed: volume=${VOLUME_ID} copy=${COPY_ID} checksum=${CHECKSUM}"
