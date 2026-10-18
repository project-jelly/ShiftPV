#!/usr/bin/env bash
# Sourced by upgrade/run.sh after installing the verified published baseline.
: "${NAMESPACE:?upgrade/run.sh namespace required}"

# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"

recovery_command() {
	local name=$1 expected=$2 command=$3
	shift 3
	local args pod
	args=$(jq -cn --args '$ARGS.positional' -- "$@")
	pod=$(jq -n --arg name "${name}" --arg ns "${NAMESPACE}" --arg image "${CANDIDATE_IMAGE}" --arg command "${command}" --argjson args "${args}" '{
	 apiVersion:"v1",kind:"Pod",metadata:{name:$name,namespace:$ns},
	 spec:{serviceAccountName:"shiftpv-controller",restartPolicy:"Never",containers:[{
	 name:"command",image:$image,imagePullPolicy:"IfNotPresent",command:["/shiftpv-controller",$command],args:$args}]}}')
	test -z "$(kubectl -n "${NAMESPACE}" get "pod/${name}" --ignore-not-found -o name)"
	kubectl apply -f - <<<"${pod}"
	kubectl -n "${NAMESPACE}" wait "pod/${name}" --for="jsonpath={.status.phase}=${expected}" --timeout=180s
	kubectl -n "${NAMESPACE}" logs "pod/${name}" >"${RECOVERY_EVIDENCE}/${name}.log" 2>&1
	kubectl -n "${NAMESPACE}" get "pod/${name}" -o json >"${RECOVERY_EVIDENCE}/${name}.json"
	kubectl -n "${NAMESPACE}" delete "pod/${name}" --wait=true --timeout=60s
}

test_creation_recovery_transition() {
	test "${CHART_VERSION}" = 0.6.13
	test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"
	RECOVERY_EVIDENCE="${ROOT_DIR}/.tmp/creation-recovery"
	mkdir -p "${RECOVERY_EVIDENCE}"
	local ns=shiftpv-upgrade-creation policy=shiftpv-upgrade-creation
	local node="${CLUSTER_NAME}-worker" mount=/mnt/shiftpv
	local ready_pv ready_volume ready_uid original_ready partial object uid copy claim_uid complete=0
	ready_pv=$(kubectl get pvc/shiftpv-upgrade-existing -o jsonpath='{.spec.volumeName}')
	ready_volume=$(kubectl get "pv/${ready_pv}" -o jsonpath='{.spec.csi.volumeHandle}')
	original_ready=$(kubectl get "shiftpvvolume/${ready_volume}" -o json)
	ready_uid=$(jq -er '.metadata.uid' <<<"${original_ready}")
	printf '%s\n' "${original_ready}" >"${RECOVERY_EVIDENCE}/ready-before.json"
	kubectl get pvc/shiftpv-upgrade-existing -o json >"${RECOVERY_EVIDENCE}/ready-pvc-before.json"
	kubectl get "pv/${ready_pv}" -o json >"${RECOVERY_EVIDENCE}/ready-pv-before.json"
	jq -e '.status.phase == "Ready" and .spec.creationClaim == null' <<<"${original_ready}" >/dev/null
	for resource in namespace validatingadmissionpolicy validatingadmissionpolicybinding; do
		test -z "$(kubectl get "${resource}/${ns}" --ignore-not-found -o name)"
	done
	kubectl create namespace "${ns}"
	kubectl -n "${ns}" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: partial
spec:
  storageClassName: ${DEFAULT_CLASS}
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 8Mi
EOF
	claim_uid=$(kubectl -n "${ns}" get pvc/partial -o jsonpath='{.metadata.uid}')
	kubectl apply -f - <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: ${policy}
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
    - apiGroups: [shiftpv.io]
      apiVersions: [v1alpha1]
      operations: [UPDATE]
      resources: [shiftpvvolumes/status, shiftpvpools/status]
  validations:
  - expression: "request.resource.resource != 'shiftpvpools' || object.metadata.name != 'worker-a' || !has(request.dryRun) || !request.dryRun"
    message: native recovery barrier activation
  - expression: "request.resource.resource != 'shiftpvvolumes' || object.spec.requestName != 'pvc-${claim_uid}' || !has(object.status.creationReceipt)"
    message: original creation receipt held during recovery
  - expression: "request.resource.resource != 'shiftpvvolumes' || object.spec.requestName != 'pvc-${claim_uid}' || !has(object.status.cleanup) || !has(object.status.cleanup.status.receipt)"
    message: native recovery purge receipt held
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ${policy}
spec:
  policyName: ${policy}
  validationActions: [Deny]
EOF
	local active=0 denial
	for _ in {1..60}; do
		object=$(kubectl get shiftpvpool/worker-a -o json)
		if ! denial=$(kubectl replace --subresource=status --dry-run=server -f - <<<"${object}" 2>&1); then
			if grep -Fq 'native recovery barrier activation' <<<"${denial}"; then active=1; break; fi
		fi
		sleep 1
	done
	test "${active}" = 1
	kubectl -n "${ns}" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: partial
spec:
  nodeSelector:
    kubernetes.io/hostname: ${node}
  terminationGracePeriodSeconds: 1
  containers:
  - name: consumer
    image: busybox:1.37
    command: [sh, -ec, "sleep 3600"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: partial
EOF
	for _ in {1..180}; do
		object=$(kubectl get shiftpvvolumes -o json | jq -c --arg name "pvc-${claim_uid}" '.items[]|select(.spec.requestName == $name)')
		if [[ -n "${object}" ]] && jq -e '.status.phase == "NodeCreating" and .status.creationReceipt == null' <<<"${object}" >/dev/null; then
			partial=$(jq -er '.metadata.name' <<<"${object}")
			if node_path_exists "${node}" "${mount}/volumes/${partial}"; then complete=1; break; fi
		fi
		sleep 1
	done
	test "${complete}" = 1
	uid=$(jq -er '.metadata.uid' <<<"${object}")
	copy=$(jq -c '.status.currentCopy' <<<"${object}")
	printf '%s\n' "${object}" >"${RECOVERY_EVIDENCE}/partial-before.json"
	jq -e '.spec.creationClaim == null and (.status.publishedNodes // [] | length) == 0' <<<"${object}" >/dev/null
	docker exec "${node}" sh -ec 'printf "%s\n" "$2" > "$1/recovery-marker"; sync' sh "${mount}/volumes/${partial}" "${uid}"
	kubectl -n "${ns}" delete pod/partial --wait=true --timeout=60s
	kubectl -n "${ns}" delete pvc/partial --wait=true --timeout=60s
	kubectl get pv -o json | jq -e --arg id "${partial}" 'all(.items[]; .spec.csi.volumeHandle != $id)' >/dev/null

	# Stage only schema, permissions and Node execution. Keep the old Controller
	# and its webhook available; no second background Controller is started.
	kubectl apply --field-manager=shiftpv-crd-upgrade -f "${ROOT_DIR}/charts/shiftpv/crds/"
	helm template "${RELEASE}" "${ROOT_DIR}/charts/shiftpv" --namespace "${NAMESPACE}" \
		--set storageClass.defaultClass=true \
		--set-string node.image.repository="${CANDIDATE_IMAGE%%:*}" --set-string node.image.tag="${CANDIDATE_IMAGE#*:}" \
		--set node.image.pullPolicy=IfNotPresent "${READINESS_ARGS[@]}" >"${RECOVERY_EVIDENCE}/node-stage.yaml"
	awk 'BEGIN {RS="---"; ORS="---\n"} /\nkind: (ClusterRole|ClusterRoleBinding|Role|RoleBinding|DaemonSet|PriorityClass)\n/ {print}' \
		"${RECOVERY_EVIDENCE}/node-stage.yaml" | kubectl apply -f -
	kubectl -n "${NAMESPACE}" rollout status daemonset/shiftpv-node --timeout=240s
	assert_equal "old Controller remains" "${CONTROLLER_IMAGE}" \
		"$(kubectl -n "${NAMESPACE}" get deployment/shiftpv-controller -o jsonpath='{.spec.template.spec.containers[?(@.name=="shiftpv-controller")].image}')"
	assert_marker shiftpv-upgrade-existing "${MARKER}"

	recovery_command native-check-before Failed upgrade-check --namespace="${NAMESPACE}" --node-daemonset-name=shiftpv-node
	head -n 1 "${RECOVERY_EVIDENCE}/native-check-before.log" | jq -e --arg id "${partial}" \
		'.complete and any(.blockers[]; .kind == "ShiftPVVolume" and .name == $id)' >/dev/null
	recovery_command native-ready-preserved Failed cancel-creation --namespace="${NAMESPACE}" --volume-id="${ready_volume}" --volume-uid="${ready_uid}"
	assert_marker shiftpv-upgrade-existing "${MARKER}"
	# This call records cancellation but cannot finish while the API receipt is
	# denied. A new command Pod must continue the same intent, never make Ready.
	recovery_command native-cancel-interrupted Failed cancel-creation --namespace="${NAMESPACE}" --volume-id="${partial}" --volume-uid="${uid}" --timeout=10s
	object=$(kubectl get "shiftpvvolume/${partial}" -o json)
	jq -e --arg uid "${uid}" --argjson copy "${copy}" '
	 .metadata.uid == $uid and .status.currentCopy == $copy and .status.phase == "Deleting" and
	 .status.deletionOperationID == ("cancel-create-"+$uid) and .status.creationReceipt == null and
	 .spec.creationClaim == null and .spec.capacityBytes == 8388608' <<<"${object}" >/dev/null
	printf '%s\n' "${object}" >"${RECOVERY_EVIDENCE}/cancel-interrupted.json"
	kubectl patch "validatingadmissionpolicy/${policy}" --type=json -p '[{"op":"replace","path":"/spec/validations/2/expression","value":"true"}]'
	recovery_command native-cancel-complete Succeeded cancel-creation --namespace="${NAMESPACE}" --volume-id="${partial}" --volume-uid="${uid}" --timeout=120s
	jq -e --arg id "${partial}" --arg uid "${uid}" '.complete and .volumeID == $id and .volumeUID == $uid' "${RECOVERY_EVIDENCE}/native-cancel-complete.log" >/dev/null
	kubectl wait "shiftpvvolume/${partial}" --for=delete --timeout=60s
	assert_node_absent "${node}" "${mount}/volumes/${partial}"
	assert_node_absent "${node}" "${mount}/.shiftpv/incoming/create-$(jq -er '.copyID' <<<"${copy}")"
	assert_node_absent "${node}" "${mount}/.shiftpv/retired/$(jq -er '.copyID' <<<"${copy}")"
	recovery_command native-check-settled Succeeded upgrade-check --namespace="${NAMESPACE}" --node-daemonset-name=shiftpv-node
	jq -e '.complete and (.blockers|length) == 0' "${RECOVERY_EVIDENCE}/native-check-settled.log" >/dev/null

	# The strict transition check is now SAFE, before replacing the sole writer.
	upgrade_candidate
	assert_candidate_images
	assert_marker shiftpv-upgrade-existing "${MARKER}"
	object=$(kubectl get "shiftpvvolume/${ready_volume}" -o json)
	jq -e --argjson before "${original_ready}" '.metadata.uid == $before.metadata.uid and .status.currentCopy == $before.status.currentCopy and .spec.capacityBytes == $before.spec.capacityBytes and .spec.creationClaim == null' <<<"${object}" >/dev/null
	printf '%s\n' "${object}" >"${RECOVERY_EVIDENCE}/ready-after.json"
	assert_equal "existing PVC UID" "$(jq -er '.metadata.uid' "${RECOVERY_EVIDENCE}/ready-pvc-before.json")" "$(kubectl get pvc/shiftpv-upgrade-existing -o jsonpath='{.metadata.uid}')"
	assert_equal "existing PV UID" "$(jq -er '.metadata.uid' "${RECOVERY_EVIDENCE}/ready-pv-before.json")" "$(kubectl get "pv/${ready_pv}" -o jsonpath='{.metadata.uid}')"
	create_workload shiftpv-recovery-new "${DEFAULT_CLASS}" 'new creation after native recovery transition'
	delete_workload shiftpv-recovery-new
	delete_workload shiftpv-upgrade-existing
	kubectl delete "validatingadmissionpolicybinding/${policy}" "validatingadmissionpolicy/${policy}"
	kubectl delete "namespace/${ns}" --wait=true --timeout=60s
	jq -n --arg partial "${uid}" --arg ready "${ready_uid}" '{result:"PASS",publishedChart:"0.6.13",partialUID:$partial,readyUID:$ready,exactCancellationRestart:true,readyPreserved:true,strictCheckBeforeWriterReplacement:true}' >"${RECOVERY_EVIDENCE}/result.json"
	cat "${RECOVERY_EVIDENCE}/result.json"
}
