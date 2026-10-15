#!/usr/bin/env bash
set -euo pipefail

# Only the caller's disposable Kind cluster. This tests requested capacity,
# using tiny payloads; it never fills a filesystem or drains a node.
: "${CLUSTER_NAME:?isolated Kind cluster name required}"
: "${KUBECONFIG:?isolated kubeconfig required}"
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"
command -v jq >/dev/null
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
# shellcheck source=test/e2e/kind/mobility/controller.sh
source "${ROOT_DIR}/test/e2e/kind/mobility/controller.sh"
NAMESPACE=shiftpv-preparation-test
POLICY=shiftpv-preparation-test
SOURCE="${CLUSTER_NAME}-worker"
DESTINATION="${CLUSTER_NAME}-worker2"
kubectl get priorityclass/shiftpv-placement -o json | jq -e \
	'.value == 0 and .preemptionPolicy == "Never" and (.globalDefault // false) == false' >/dev/null
if [[ -n "$(kubectl get namespace "${NAMESPACE}" --ignore-not-found -o name)$(kubectl get validatingadmissionpolicy "${POLICY}" --ignore-not-found -o name)$(kubectl get validatingadmissionpolicybinding "${POLICY}" --ignore-not-found -o name)" ]]; then
	echo 'preparation test resources already exist; refusing to overwrite' >&2
	exit 1
fi
test "$(kubectl get shiftpvpools -o json | jq '[.items[] | select(.spec.nodeName == "'"${DESTINATION}"'")] | length')" = 1
ORIGINAL_LIMIT=$(kubectl get shiftpvpool/worker-b -o jsonpath='{.spec.capacity.limit}')
kubectl patch shiftpvpool/worker-b --type=merge -p '{"spec":{"capacity":{"limit":"5Gi"}}}'
GENERATION=$(kubectl get shiftpvpool/worker-b -o jsonpath='{.metadata.generation}')
kubectl wait shiftpvpool/worker-b --for=jsonpath='{.status.observedGeneration}'="${GENERATION}" --timeout=120s
kubectl wait shiftpvpool/worker-b --for=condition=Ready --timeout=120s
DESTINATION_POOL_UID=$(kubectl get shiftpvpool/worker-b -o jsonpath='{.metadata.uid}')
kubectl uncordon "${SOURCE}"
kubectl cordon "${DESTINATION}"

PVS=() VOLUMES=() CLAIM_UIDS=() ORIGINAL_PODS=() ORIGINAL_UIDS=() SOURCE_COPIES=() CHECKSUMS=()
for index in 0 1 2 3; do
	# Four controllers and four PVCs, not four replicas sharing a PVC.
	sed -e "s/shiftpv-mobility-test/${NAMESPACE}/g" \
		-e "s/wffc/prepared-${index}/g" -e 's/storage: 16Mi/storage: 2Gi/' \
		"${ROOT_DIR}/test/e2e/kind/mobility/manifests/wffc-workload.yaml" | kubectl apply -f -
	kubectl -n "${NAMESPACE}" rollout status "deployment/prepared-${index}" --timeout=180s
	PVS[index]=$(kubectl -n "${NAMESPACE}" get "pvc/prepared-${index}" -o jsonpath='{.spec.volumeName}')
	VOLUMES[index]=$(kubectl get "pv/${PVS[index]}" -o jsonpath='{.spec.csi.volumeHandle}')
	CLAIM_UIDS[index]=$(kubectl -n "${NAMESPACE}" get "pvc/prepared-${index}" -o jsonpath='{.metadata.uid}')
	ORIGINAL_PODS[index]=$(kubectl -n "${NAMESPACE}" get pods -l "app=shiftpv-mobility-prepared-${index}" -o jsonpath='{.items[0].metadata.name}')
	ORIGINAL_UIDS[index]=$(kubectl -n "${NAMESPACE}" get "pod/${ORIGINAL_PODS[index]}" -o jsonpath='{.metadata.uid}')
	test "$(kubectl -n "${NAMESPACE}" get "pod/${ORIGINAL_PODS[index]}" -o jsonpath='{.spec.nodeName}')" = "${SOURCE}"
	test "$(kubectl get "shiftpvvolume/${VOLUMES[index]}" -o jsonpath='{.spec.capacityBytes}')" = 2147483648
	SOURCE_COPIES[index]=$(kubectl get "shiftpvvolume/${VOLUMES[index]}" -o json | jq -c '.status.currentCopy')
	# The replacement template cannot recreate this per-PVC data after data loss.
	printf 'ShiftPV durable payload pvc=%s volume=%s\n' "${CLAIM_UIDS[index]}" "${VOLUMES[index]}" \
		| kubectl -n "${NAMESPACE}" exec -i "${ORIGINAL_PODS[index]}" -- sh -ec 'cat > /data/payload; sync'
	CHECKSUMS[index]=$(pod_sha256 "${NAMESPACE}" "${ORIGINAL_PODS[index]}" /data/payload)
done
VOLUME_JSON=$(printf '%s\n' "${VOLUMES[@]}" | jq -R . | jq -cs .)

test_moves() {
	kubectl get shiftpvmoves -o json | jq --argjson volumes "${VOLUME_JSON}" \
		'[.items[] | select(.spec.volumeID as $id | $volumes | index($id))]'
}

assert_originals() {
	local index pod state
	for index in 0 1 2 3; do
		pod=$(kubectl -n "${NAMESPACE}" get "pod/${ORIGINAL_PODS[index]}" -o json)
		jq -e --arg uid "${ORIGINAL_UIDS[index]}" --arg node "${SOURCE}" \
			'.metadata.uid == $uid and .metadata.deletionTimestamp == null and .status.phase == "Running" and .spec.nodeName == $node' <<<"${pod}" >/dev/null
		state=$(kubectl get "shiftpvvolume/${VOLUMES[index]}" -o json)
		jq -e --arg node "${SOURCE}" --argjson copy "${SOURCE_COPIES[index]}" \
			'.status.phase == "Ready" and .status.ownerNode == $node and .status.currentCopy == $copy' <<<"${state}" >/dev/null
		test "$(pod_sha256 "${NAMESPACE}" "${ORIGINAL_PODS[index]}" /data/payload)" = "${CHECKSUMS[index]}"
		kubectl -n "${NAMESPACE}" exec "${ORIGINAL_PODS[index]}" -- sh -ec 'printf "still writable\n" > /data/preparation-probe; sync; test -s /data/preparation-probe'
	done
}

assert_reserved() {
	local expected=$1 deadline pod metrics
	deadline=$((SECONDS + 60))
	while ((SECONDS < deadline)); do
		pod=$(kubectl -n shiftpv-system get pods -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.name}')
		metrics=$(kubectl get --raw "/api/v1/namespaces/shiftpv-system/pods/${pod}:8080/proxy/metrics")
		if awk -v value="${expected}" -v node="${DESTINATION}" '
			/^shiftpv_pool_reserved_bytes\{/ && index($0,"pool=\"worker-b\"") && index($0,"node=\"" node "\"") { found=1; if ($NF+0 == value+0) ok=1 }
			END { exit !(found && ok) }' <<<"${metrics}"; then return; fi
		sleep 2
	done
	echo "destination reservation did not converge to ${expected}" >&2
	return 1
}

wait_two_preparations() {
	local deadline moves
	deadline=$((SECONDS + 180))
	while ((SECONDS < deadline)); do
		moves=$(test_moves)
		if jq -e --arg pool "${DESTINATION_POOL_UID}" \
			'([.[] | select(.status.preparation != null)] | length) == 2 and
			 ([.[] | select(.status.preparation != null) | .status.preparation.reservedBytes] | add) == 4294967296 and
			 all(.[] | select(.status.preparation != null); .spec.protocolVersion == "prepared-v1" and .status.preparation.pool.uid == $pool) and
			 all(.[]; .status.capacityApproved != true and .status.incomingCopy == null and .status.destinationCopy == null and (.status.sourceBytes // 0) == 0)' <<<"${moves}" >/dev/null; then
			# Real priority admission must accept the non-preempting reservation.
			kubectl -n shiftpv-system get pods -l shiftpv.io/role=placement -o json | jq -e \
				'(.items | length) > 0 and all(.items[]; .spec.priorityClassName == "shiftpv-placement" and .spec.priority == 0 and .spec.preemptionPolicy == "Never")' >/dev/null
			assert_originals
			assert_reserved 4294967296
			return
		fi
		sleep 2
	done
	test_moves >&2
	echo 'did not observe exactly two preparations' >&2
	return 1
}

wait_cancelled() {
	local deadline index active
	deadline=$((SECONDS + 180))
	while ((SECONDS < deadline)); do
		active=0
		for index in 0 1 2 3; do
			[[ -z "$(kubectl get "shiftpvvolume/${VOLUMES[index]}" -o jsonpath='{.status.activeMove}')" ]] || active=$((active + 1))
		done
		if [[ "${active}" = 0 ]] && test_moves | jq -e 'all(.[]; .status.preparation == null)' >/dev/null; then return; fi
		sleep 2
	done
	echo 'preparation cancellation did not release holds and ActiveMove' >&2
	return 1
}

assert_reserved 0
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
  - expression: >-
      !(object.spec.volumeID in ${VOLUME_JSON}) || !has(object.status.phase) || object.status.phase != 'StartingDisruption'
    message: preparation disruption boundary failure injection
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
wait_two_preparations
PREPARED_MOVE=$(test_moves | jq -r '[.[] | select(.status.preparation != null)][0].metadata.name')
if DENIAL=$(kubectl patch "shiftpvmove/${PREPARED_MOVE}" --subresource=status --type=merge \
	-p '{"status":{"phase":"StartingDisruption"}}' --dry-run=server 2>&1); then
	echo 'preparation boundary policy was not enforced' >&2
	exit 1
fi
grep -Fq 'preparation disruption boundary failure injection' <<<"${DENIAL}"

# Cancel while every original still serves; the denial policy permits cancellation.
kubectl uncordon "${SOURCE}"
wait_cancelled
assert_originals
assert_reserved 0
test "$(kubectl -n shiftpv-system get pods -l shiftpv.io/role=placement -o json | jq '.items | length')" = 0

# Reprepare, then restart the real controller while both durable holds are frozen.
kubectl cordon "${SOURCE}"
wait_two_preparations
BEFORE=$(test_moves | jq -c '[.[] | select(.status.preparation != null) | {uid:.metadata.uid, preparation:.status.preparation}] | sort_by(.uid)')
controller_down
assert_originals
controller_up
wait_two_preparations
AFTER=$(test_moves | jq -c '[.[] | select(.status.preparation != null) | {uid:.metadata.uid, preparation:.status.preparation}] | sort_by(.uid)')
test "${BEFORE}" = "${AFTER}"
GRANTED_VOLUMES=$(test_moves | jq -c '[.[] | select(.status.preparation != null) | .spec.volumeID]')
GRANTED_MOVES=$(test_moves | jq -r '.[] | select(.status.preparation != null) | .metadata.name')
kubectl delete validatingadmissionpolicybinding "${POLICY}"
kubectl delete validatingadmissionpolicy "${POLICY}"
while IFS= read -r move; do
	kubectl wait "shiftpvmove/${move}" --for=jsonpath='{.status.phase}'=Succeeded --timeout=600s
done <<<"${GRANTED_MOVES}"

# Observe beyond two discovery intervals: the unreserved originals must not
# enter disruption after the first pair's hold becomes destination ownership.
deadline=$((SECONDS + 25))
while ((SECONDS < deadline)); do
	for index in 0 1 2 3; do
		if jq -e --arg id "${VOLUMES[index]}" 'index($id) != null' <<<"${GRANTED_VOLUMES}" >/dev/null; then continue; fi
		test "$(kubectl -n "${NAMESPACE}" get "pod/${ORIGINAL_PODS[index]}" -o jsonpath='{.metadata.uid}')" = "${ORIGINAL_UIDS[index]}"
		test "$(kubectl -n "${NAMESPACE}" get "pod/${ORIGINAL_PODS[index]}" -o jsonpath='{.status.phase}')" = Running
		test -z "$(kubectl -n "${NAMESPACE}" get "pod/${ORIGINAL_PODS[index]}" -o jsonpath='{.metadata.deletionTimestamp}')"
	done
	sleep 2
done
for index in 0 1 2 3; do
	kubectl -n "${NAMESPACE}" rollout status "deployment/prepared-${index}" --timeout=180s
	test "$(pod_sha256 "${NAMESPACE}" "deployment/prepared-${index}" /data/payload)" = "${CHECKSUMS[index]}"
	test "$(kubectl -n "${NAMESPACE}" get "pvc/prepared-${index}" -o jsonpath='{.metadata.uid}')" = "${CLAIM_UIDS[index]}"
	test "$(kubectl get "pv/${PVS[index]}" -o jsonpath='{.spec.csi.volumeHandle}')" = "${VOLUMES[index]}"
	if jq -e --arg id "${VOLUMES[index]}" 'index($id) != null' <<<"${GRANTED_VOLUMES}" >/dev/null; then
		test "$(kubectl get "shiftpvvolume/${VOLUMES[index]}" -o jsonpath='{.status.ownerNode}')" = "${DESTINATION}"
	else
		test "$(kubectl get "shiftpvvolume/${VOLUMES[index]}" -o jsonpath='{.status.ownerNode}')" = "${SOURCE}"
		kubectl -n "${NAMESPACE}" exec "${ORIGINAL_PODS[index]}" -- sh -ec 'printf "still serving\n" >> /data/preparation-probe; sync'
	fi
done
assert_reserved 4294967296
test "$(kubectl get shiftpvpool/worker-b -o jsonpath='{.spec.capacity.limit}')" = 5Gi
echo 'preparation admission passed: 5Gi limit, 4Gi reserved, 1Gi headroom, two moved and two original consumers preserved'

# Cancel the remaining preparations before deleting only this test's workloads.
kubectl uncordon "${SOURCE}"
wait_cancelled
kubectl delete namespace "${NAMESPACE}" --wait=true --timeout=180s
for index in 0 1 2 3; do
	kubectl wait "pv/${PVS[index]}" --for=delete --timeout=180s
	kubectl wait "shiftpvvolume/${VOLUMES[index]}" --for=delete --timeout=180s
done
assert_reserved 0
kubectl patch shiftpvpool/worker-b --type=merge -p "{\"spec\":{\"capacity\":{\"limit\":\"${ORIGINAL_LIMIT}\"}}}"
echo 'ShiftPV prepared mobility E2E passed'
