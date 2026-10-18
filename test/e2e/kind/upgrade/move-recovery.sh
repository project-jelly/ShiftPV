#!/usr/bin/env bash
# Sourced after the pinned chart0.6.13 install and candidate image load.
: "${NAMESPACE:?upgrade/run.sh namespace required}"
# shellcheck source=test/e2e/kind/node-path.sh
source "${ROOT_DIR}/test/e2e/kind/node-path.sh"
# shellcheck source=test/e2e/kind/cleanup-journal.sh
source "${ROOT_DIR}/test/e2e/kind/cleanup-journal.sh"

load_retirement_case() {
	CASE_DIR=$1
	local original
	original=$(cat "${CASE_DIR}/original.json")
	NS=$(jq -er '.namespace' <<<"${original}")
	BOUNDARY=$(jq -er '.boundary' <<<"${original}")
	RECLAIM=$(jq -er '.reclaim' <<<"${original}")
	VOLUME=$(jq -er '.volume' <<<"${original}")
	VOLUME_UID=$(jq -er '.volumeUID' <<<"${original}")
	PV=$(jq -er '.pv' <<<"${original}")
	PVC_UID=$(jq -er '.pvcUID' <<<"${original}")
	CHECKSUM=$(jq -er '.checksum' <<<"${original}")
}

old_writer_replicas() {
	local replicas=$1 stopped=0
	assert_equal 'old writer image before scaling' "${CONTROLLER_IMAGE}" \
		"$(kubectl -n "${NAMESPACE}" get deployment/shiftpv-controller -o jsonpath='{.spec.template.spec.containers[?(@.name=="shiftpv-controller")].image}')"
	kubectl -n "${NAMESPACE}" scale deployment/shiftpv-controller --replicas="${replicas}"
	if [[ "${replicas}" == 1 ]]; then
		kubectl -n "${NAMESPACE}" rollout status deployment/shiftpv-controller --timeout=180s
		return
	fi
	for _ in {1..90}; do
		if kubectl -n "${NAMESPACE}" get pods -l app.kubernetes.io/component=controller -o json | jq -e '(.items|length) == 0' >/dev/null; then stopped=1; break; fi
		sleep 1
	done
	test "${stopped}" = 1
}

assert_case_owner() {
	local object before owner copy
	object=$(kubectl get "shiftpvvolume/${VOLUME}" -o json)
	before=$(cat "${CASE_DIR}/volume-boundary.json")
	owner=$(jq -er '.status.ownerNode' <<<"${before}")
	copy=$(jq -c '.status.currentCopy' <<<"${before}")
	jq -e --arg uid "${VOLUME_UID}" --arg owner "${owner}" --argjson copy "${copy}" '
	 .metadata.uid == $uid and .status.ownerNode == $owner and .status.currentCopy == $copy and
	 .spec.capacityBytes == 16777216 and (.status.publishedNodes // [] | length) == 0' <<<"${object}" >/dev/null
	test "$(node_sha256 "${owner}" "$(pool_mount_for_node "${owner}")/volumes/${VOLUME}/payload")" = "${CHECKSUM}"
}

wait_old_boundary() {
	local object move complete=0 destination
	for _ in {1..300}; do
		object=$(kubectl get "shiftpvvolume/${VOLUME}" -o json)
		MOVE=$(jq -r '.status.activeMove // ""' <<<"${object}")
		if [[ -z "${MOVE}" ]]; then sleep 1; continue; fi
		move=$(kubectl get "shiftpvmove/${MOVE}" -o json)
		if ! jq -e '.spec.protocolVersion == "prepared-v1" and .spec.recovery == null and .status.capacityApproved and .status.destinationCopy != null' <<<"${move}" >/dev/null; then sleep 1; continue; fi
		jq -e --arg claim "${PVC_UID}" --arg pv "${PV}" --arg uid "$(jq -er '.pvUID' "${CASE_DIR}/original.json")" \
			--argjson copy "$(jq -c '.sourceCopy' "${CASE_DIR}/original.json")" '
		 .status.persistentVolumeClaimUID == $claim and .status.persistentVolumeName == $pv and
		 .status.persistentVolumeUID == $uid and .status.sourceCopy == $copy' <<<"${move}" >/dev/null
		destination=$(jq -er '.status.destinationNode' <<<"${move}")
		if [[ "${BOUNDARY}" == precommit ]]; then
			jq -e --arg node "${SOURCE}" '.status.ownerNode == $node' <<<"${object}" >/dev/null
		else
			if ! jq -e --arg node "${destination}" --argjson copy "$(jq -c '.status.destinationCopy' <<<"${move}")" \
				'.status.ownerNode == $node and .status.currentCopy == $copy and (.status.publishedNodes // [] | length) == 0' <<<"${object}" >/dev/null; then sleep 1; continue; fi
		fi
		if node_path_exists "${destination}" "$(pool_mount_for_node "${destination}")/volumes/${VOLUME}/payload" &&
			[[ "$(node_sha256 "${destination}" "$(pool_mount_for_node "${destination}")/volumes/${VOLUME}/payload")" == "${CHECKSUM}" ]]; then complete=1; break; fi
		sleep 1
	done
	test "${complete}" = 1
	printf '%s\n' "${object}" >"${CASE_DIR}/volume-boundary.json"
	printf '%s\n' "${move}" >"${CASE_DIR}/move-boundary.json"
	# Both actual copies precede original-claim retirement. No synthetic phase or
	# receipt is installed, and old copy/promotion actors retain their images.
	test "$(node_sha256 "${SOURCE}" "/mnt/shiftpv/volumes/${VOLUME}/payload")" = "${CHECKSUM}"
	kubectl -n "${NAMESPACE}" get jobs,pods -o json | jq --arg uid "$(jq -er '.metadata.uid' <<<"${move}")" \
		--arg image "${CONTROLLER_IMAGE}" '
	 [.items[]|select(any(.metadata.ownerReferences[]?; .kind == "ShiftPVMove" and .uid == $uid))] |
	 if length > 0 and all(.[]; if .kind == "Job" then all(.spec.template.spec.containers[]; .image == $image) else .metadata.labels["shiftpv.io/role"] == "placement" or all(.spec.containers[]; .image == $image) end)
	 then map({kind,uid:.metadata.uid,name:.metadata.name}) else error("missing or unpinned old Move actors") end' >"${CASE_DIR}/old-actors.json"
}

test_move_recovery_transition() {
	test "${CHART_VERSION}" = 0.6.13
	RECOVERY_EVIDENCE="${ROOT_DIR}/.tmp/move-recovery"
	mkdir -p "${RECOVERY_EVIDENCE}"
	SOURCE="${CLUSTER_NAME}-worker"
	DESTINATION="${CLUSTER_NAME}-worker2"
	local policy=shiftpv-upgrade-move object move original pvuid original_copy result=0
	local ready_volume ready_before ready_pv index
	ready_pv=$(kubectl get pvc/shiftpv-upgrade-existing -o jsonpath='{.spec.volumeName}')
	ready_volume=$(kubectl get "pv/${ready_pv}" -o jsonpath='{.spec.csi.volumeHandle}')
	ready_before=$(kubectl get "shiftpvvolume/${ready_volume}" -o json)
	printf '%s\n' "${ready_before}" >"${RECOVERY_EVIDENCE}/existing-ready-before.json"
	for resource in validatingadmissionpolicy validatingadmissionpolicybinding; do test -z "$(kubectl get "${resource}/${policy}" --ignore-not-found -o name)"; done
	assert_equal 'old Recreate strategy' Recreate "$(kubectl -n "${NAMESPACE}" get deployment/shiftpv-controller -o jsonpath='{.spec.strategy.type}')"
	kubectl cordon "${DESTINATION}"
	for BOUNDARY in precommit postcommit; do
		for RECLAIM in Delete Retain; do
			local suffix=delete
			if [[ "${RECLAIM}" == Retain ]]; then suffix=retain; fi
			NS="shiftpv-upgrade-${BOUNDARY}-${suffix}"
			test -z "$(kubectl get "namespace/${NS}" --ignore-not-found -o name)"
			CASE_DIR="${RECOVERY_EVIDENCE}/${BOUNDARY}-${RECLAIM}"
			mkdir -p "${CASE_DIR}"
			sed "s/shiftpv-mobility-test/${NS}/g" "${ROOT_DIR}/test/e2e/kind/mobility/manifests/wffc-workload.yaml" | kubectl apply -f -
			kubectl -n "${NS}" rollout status deployment/wffc --timeout=180s
			PVC_UID=$(kubectl -n "${NS}" get pvc/wffc -o jsonpath='{.metadata.uid}')
			PV=$(kubectl -n "${NS}" get pvc/wffc -o jsonpath='{.spec.volumeName}')
			pvuid=$(kubectl get "pv/${PV}" -o jsonpath='{.metadata.uid}')
			VOLUME=$(kubectl get "pv/${PV}" -o jsonpath='{.spec.csi.volumeHandle}')
			object=$(kubectl get "shiftpvvolume/${VOLUME}" -o json)
			VOLUME_UID=$(jq -er '.metadata.uid' <<<"${object}")
			jq -e --arg node "${SOURCE}" '.status.ownerNode == $node and .spec.creationClaim == null' <<<"${object}" >/dev/null
			original_copy=$(jq -c '.status.currentCopy' <<<"${object}")
			# shellcheck disable=SC2016
			kubectl -n "${NS}" exec deployment/wffc -- sh -ec 'printf "%s\n" "$1" > /data/payload; sync' sh "old-retirement-${VOLUME_UID}"
			CHECKSUM=$(pod_sha256 "${NS}" deployment/wffc /data/payload)
			kubectl patch "pv/${PV}" --type=merge -p "{\"spec\":{\"persistentVolumeReclaimPolicy\":\"${RECLAIM}\"}}"
			jq -n --arg namespace "${NS}" --arg boundary "${BOUNDARY}" --arg reclaim "${RECLAIM}" \
				--arg volume "${VOLUME}" --arg volumeUID "${VOLUME_UID}" --arg pv "${PV}" --arg pvUID "${pvuid}" \
				--arg pvcUID "${PVC_UID}" --arg checksum "${CHECKSUM}" --argjson sourceCopy "${original_copy}" \
				'{namespace:$namespace,boundary:$boundary,reclaim:$reclaim,volume:$volume,volumeUID:$volumeUID,pv:$pv,pvUID:$pvUID,pvcUID:$pvcUID,checksum:$checksum,sourceCopy:$sourceCopy}' >"${CASE_DIR}/original.json"
		done
	done
	# Each exact Volume keeps its own pre/post-owner boundary. The last rule holds
	# only native cleanup API receipts, leaving real purge and replay observable.
	jq -s --arg policy "${policy}" --arg source "${SOURCE}" '
	 . as $cases | ($cases|map(.volume)|tojson) as $ids |
	 {apiVersion:"v1",kind:"List",items:[{
	 apiVersion:"admissionregistration.k8s.io/v1",kind:"ValidatingAdmissionPolicy",metadata:{name:$policy},
	 spec:{failurePolicy:"Fail",matchConstraints:{resourceRules:[{apiGroups:["shiftpv.io"],apiVersions:["v1alpha1"],operations:["UPDATE"],resources:["shiftpvvolumes/status","shiftpvmoves/status","shiftpvpools/status"]}]},validations:[
	 {expression:"request.resource.resource != '\''shiftpvpools'\'' || object.metadata.name != '\''worker-a'\'' || !has(request.dryRun) || !request.dryRun",message:"old move boundary activation"},
	 ($cases[]|{expression:("request.resource.resource != '\''shiftpvvolumes'\'' || object.metadata.uid != "+(.volumeUID|tojson)+" || "+
	 (if .boundary == "precommit" then "object.status.ownerNode == oldObject.status.ownerNode" else "object.status.ownerNode == "+($source|tojson)+" || !has(object.status.publishedNodes) || size(object.status.publishedNodes) == 0" end)),message:(.boundary+" owner/publication held")}),
	 {expression:("request.resource.resource != '\''shiftpvmoves'\'' || !(object.spec.volumeID in "+$ids+") || !has(object.status.cleanup) || !has(object.status.cleanup.status.receipt)"),message:"old Move purge receipt held"}]}},
	 {apiVersion:"admissionregistration.k8s.io/v1",kind:"ValidatingAdmissionPolicyBinding",metadata:{name:$policy},spec:{policyName:$policy,validationActions:["Deny"]}}]}' \
	 "${RECOVERY_EVIDENCE}"/*/original.json >"${RECOVERY_EVIDENCE}/barriers.json"
	kubectl apply -f "${RECOVERY_EVIDENCE}/barriers.json"
	for _ in {1..60}; do
		object=$(kubectl get shiftpvpool/worker-a -o json)
		if ! original=$(kubectl replace --subresource=status --dry-run=server -f - <<<"${object}" 2>&1); then
			if grep -Fq 'old move boundary activation' <<<"${original}"; then result=1; break; fi
		fi
		sleep 1
	done
	test "${result}" = 1
	kubectl uncordon "${DESTINATION}"
	kubectl cordon "${SOURCE}"
	for CASE_DIR in "${RECOVERY_EVIDENCE}"/*/; do load_retirement_case "${CASE_DIR%/}"; wait_old_boundary; done
	# Pause the old writer so every first subsequent observation sees the original
	# claims ended. Restart only that old image to reproduce its blocked behavior.
	old_writer_replicas 0
	for CASE_DIR in "${RECOVERY_EVIDENCE}"/*/; do
		load_retirement_case "${CASE_DIR%/}"
		kubectl -n "${NS}" delete deployment/wffc --cascade=foreground --wait=true --timeout=120s
		kubectl -n "${NS}" delete pvc/wffc --wait=true --timeout=120s
	done
	old_writer_replicas 1
	for CASE_DIR in "${RECOVERY_EVIDENCE}"/*/; do
		load_retirement_case "${CASE_DIR%/}"
		MOVE=$(jq -er '.metadata.name' "${CASE_DIR}/move-boundary.json")
		kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.phase}'=Blocked --timeout=240s
		move=$(kubectl get "shiftpvmove/${MOVE}" -o json)
		jq -e '.status.capacityApproved and .spec.recovery == null and (.status.settlementRequested // false | not) and .status.cleanup == null' <<<"${move}" >/dev/null
		printf '%s\n' "${move}" >"${CASE_DIR}/old-blocked.json"
		assert_case_owner
	done
	kubectl uncordon "${SOURCE}"
	stage_candidate_node
	recovery_command old-move-check-before Failed upgrade-check --namespace="${NAMESPACE}" --node-daemonset-name=shiftpv-node
	jq -e '.complete and ([.blockers[]|select(.reason == "ActiveMove")]|length) == 4' "${RECOVERY_EVIDENCE}/old-move-check-before-report.json" >/dev/null
	load_retirement_case "${RECOVERY_EVIDENCE}/precommit-Delete"
	MOVE=$(jq -er '.metadata.name' "${CASE_DIR}/move-boundary.json")
	recovery_command old-writer-excluded Failed settle-retired-move --namespace="${NAMESPACE}" \
		--move-name="${MOVE}" --move-uid="$(jq -er '.metadata.uid' "${CASE_DIR}/old-blocked.json")" \
		--helper-image="${CANDIDATE_IMAGE}" --helper-service-account=shiftpv-controller --timeout=30s
	grep -Fq 'normal controller Deployment must remain scaled to zero' "${RECOVERY_EVIDENCE}/old-writer-excluded.log"
	kubectl get "shiftpvmove/${MOVE}" -o json | jq -e '(.status.settlementRequested // false | not) and .status.capacityApproved and .status.cleanup == null' >/dev/null
	old_writer_replicas 0
	# Execute only explicit per-UID retirement while the normal writer is stopped.
	# Keep the old Deployment at zero until the strict check becomes SAFE.
	# The same lifecycle admission checks must remain reachable for Pool scanEpoch
	# updates. This server has no CSI sidecar or background lifecycle workers.
	kubectl -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: maintenance-admission
  labels:
    shiftpv.io/maintenance-admission: "true"
spec:
  serviceAccountName: shiftpv-controller
  containers:
  - name: admission
    image: ${CANDIDATE_IMAGE}
    imagePullPolicy: IfNotPresent
    command: [/shiftpv-controller]
    args: [--admission-only, --namespace=${NAMESPACE}]
    ports:
    - name: webhook
      containerPort: 9443
    readinessProbe:
      httpGet:
        scheme: HTTPS
        path: /healthz
        port: webhook
EOF
	kubectl -n "${NAMESPACE}" wait pod/maintenance-admission --for=condition=Ready --timeout=120s
	kubectl -n "${NAMESPACE}" get service/shiftpv-webhook -o json >"${RECOVERY_EVIDENCE}/webhook-service-before.json"
	kubectl -n "${NAMESPACE}" patch service/shiftpv-webhook --type=json \
		-p '[{"op":"replace","path":"/spec/selector","value":{"shiftpv.io/maintenance-admission":"true"}}]'
	for CASE_DIR in "${RECOVERY_EVIDENCE}"/*/; do
		load_retirement_case "${CASE_DIR%/}"
		MOVE=$(jq -er '.metadata.name' "${CASE_DIR}/move-boundary.json")
		local command_case
		command_case="${BOUNDARY}-$(tr '[:upper:]' '[:lower:]' <<<"${RECLAIM}")"
		recovery_command "begin-${command_case}" Failed settle-retired-move --namespace="${NAMESPACE}" \
			--move-name="${MOVE}" --move-uid="$(jq -er '.metadata.uid' "${CASE_DIR}/old-blocked.json")" \
			--helper-image="${CANDIDATE_IMAGE}" --helper-service-account=shiftpv-controller --timeout=60s
		kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.cleanup.status.phase}'=Running --timeout=30s
		move=$(kubectl get "shiftpvmove/${MOVE}" -o json)
		local expected_target
		if [[ "${BOUNDARY}" == precommit ]]; then
			expected_target=$(jq -c '.status.destinationCopy' "${CASE_DIR}/move-boundary.json")
		else
			expected_target=$(jq -c '.status.sourceCopy' "${CASE_DIR}/move-boundary.json")
		fi
		jq -e --arg uid "$(jq -er '.metadata.uid' "${CASE_DIR}/old-blocked.json")" --argjson target "${expected_target}" '
		 .metadata.uid == $uid and .status.settlementRequested and .spec.recovery == null and .status.capacityApproved and
		 .status.cleanup.spec.target == $target and .status.cleanup.status.executor.kind == "Node" and .status.cleanup.status.receipt == null' <<<"${move}" >/dev/null
		local target target_node target_mount operation receipt_path receipt purged=0
		target=$(jq -c '.status.cleanup.spec.target' <<<"${move}")
		target_node=$(jq -er '.nodeName' <<<"${target}")
		target_mount=$(pool_mount_for_node "${target_node}")
		operation=$(jq -er '.status.cleanup.spec.operationID' <<<"${move}")
		receipt_path="${target_mount}/.shiftpv/receipt-$(printf '%s' "${operation}" | shasum -a 256 | cut -c1-32).json"
		for _ in {1..120}; do
			if node_path_exists "${target_node}" "${receipt_path}"; then purged=1; break; fi
			sleep 1
		done
		test "${purged}" = 1
		receipt=$(docker exec "${target_node}" cat "${receipt_path}")
		jq -e --arg operation "${operation}" --argjson target "${target}" '.version == 1 and .value.operationID == $operation and .value.target == $target and .value.retired and .value.purged' <<<"${receipt}" >/dev/null
		printf '%s\n' "${receipt}" >"${CASE_DIR}/local-receipt.json"
		assert_node_absent "${target_node}" "${target_mount}/volumes/${VOLUME}"
		assert_node_absent "${target_node}" "${target_mount}/.shiftpv/retired/$(jq -er '.copyID' <<<"${target}")"
		assert_case_owner
		printf '%s\n' "${move}" >"${CASE_DIR}/candidate-receipt-held.json"
		assert_no_cleanup_job "${MOVE}"
		kubectl -n "${NAMESPACE}" get jobs,pods -o json | jq -e --slurpfile actors "${CASE_DIR}/old-actors.json" \
			'all(.items[]; .metadata.uid as $uid|all($actors[0][]; .uid != $uid))' >/dev/null
		kubectl -n "${NAMESPACE}" get jobs,pods -o json | jq -e --arg uid "$(jq -er '.metadata.uid' <<<"${move}")" --arg image "${CANDIDATE_IMAGE}" '
		 all(.items[]; if .metadata.labels["shiftpv.io/move-uid"] == $uid then
		 if .kind == "Job" then all(.spec.template.spec.containers[]; .image == $image) else all(.spec.containers[]; .image == $image) end else true end)' >/dev/null
	done
	# Fresh command Pods continue the same persisted retirement after the held receipt.
	for CASE_DIR in "${RECOVERY_EVIDENCE}"/*/; do
		load_retirement_case "${CASE_DIR%/}"
		assert_case_owner
		MOVE=$(jq -er '.metadata.name' "${CASE_DIR}/move-boundary.json")
		kubectl get "shiftpvmove/${MOVE}" -o json | jq -e '.status.capacityApproved and .status.cleanup.status.receipt == null' >/dev/null
	done
	index=$(jq -er '.items[0].spec.validations|length-1' "${RECOVERY_EVIDENCE}/barriers.json")
	kubectl patch "validatingadmissionpolicy/${policy}" --type=json \
		-p "$(jq -n --arg path "/spec/validations/${index}/expression" '[{op:"replace",path:$path,value:"true"}]')"
	for CASE_DIR in "${RECOVERY_EVIDENCE}"/*/; do
		load_retirement_case "${CASE_DIR%/}"
		MOVE=$(jq -er '.metadata.name' "${CASE_DIR}/move-boundary.json")
		command_case="${BOUNDARY}-$(tr '[:upper:]' '[:lower:]' <<<"${RECLAIM}")"
		recovery_command "finish-${command_case}" Succeeded settle-retired-move --namespace="${NAMESPACE}" \
			--move-name="${MOVE}" --move-uid="$(jq -er '.metadata.uid' "${CASE_DIR}/old-blocked.json")" \
			--helper-image="${CANDIDATE_IMAGE}" --helper-service-account=shiftpv-controller --timeout=180s
		jq -e --arg uid "$(jq -er '.metadata.uid' "${CASE_DIR}/old-blocked.json")" --arg name "${MOVE}" \
			'.complete and .moveUID == $uid and .moveName == $name' "${RECOVERY_EVIDENCE}/finish-${command_case}-report.json" >/dev/null
		kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.recoveryPhase}'=Recovered --timeout=30s
		local settled=0
		for _ in {1..120}; do
			move=$(kubectl get "shiftpvmove/${MOVE}" -o json)
			if jq -e '(.metadata.finalizers // [] | length) == 0' <<<"${move}" >/dev/null; then settled=1; break; fi
			sleep 1
		done
		test "${settled}" = 1
		jq -e '.spec.recovery == null and .status.settlementRequested and .status.capacityApproved == false and .status.preparation == null and (.metadata.finalizers // [] | length) == 0' <<<"${move}" >/dev/null
		printf '%s\n' "${move}" >"${CASE_DIR}/settled-move.json"
		local target reason
		reason=MoveRollback
		if [[ "${BOUNDARY}" == postcommit ]]; then reason=MoveSource; fi
		target=$(jq -er '.status.cleanup.spec.target.copyID' <<<"${move}")
		assert_cleanup_journal "shiftpvmove/${MOVE}" "${reason}" "${VOLUME}" "${target}" ShiftPVMove
		if [[ "${RECLAIM}" == Retain ]]; then
			assert_case_owner
			object=$(kubectl get "shiftpvvolume/${VOLUME}" -o json)
			jq -e '.status.phase == "Ready" and (.status.activeMove // "") == "" and .spec.creationClaim == null' <<<"${object}" >/dev/null
			printf '%s\n' "${object}" >"${CASE_DIR}/retained-volume.json"
			kubectl wait "pv/${PV}" --for=jsonpath='{.status.phase}'=Released --timeout=120s
			kubectl get "pv/${PV}" -o json | jq -e --arg uid "$(jq -er '.pvUID' "${CASE_DIR}/original.json")" --arg claim "${PVC_UID}" \
				'.metadata.uid == $uid and .spec.claimRef.uid == $claim and .spec.persistentVolumeReclaimPolicy == "Retain"' >/dev/null
		fi
		# CSI provisioner is stopped too: even Delete retains original data until
		# normal controller startup after the strict transition gate.
		assert_case_owner
		object=$(kubectl get "shiftpvvolume/${VOLUME}" -o json)
		jq -e '.status.phase == "Ready" and (.status.activeMove // "") == ""' <<<"${object}" >/dev/null
	done
	assert_marker shiftpv-upgrade-existing "${MARKER}"
	object=$(kubectl get "shiftpvvolume/${ready_volume}" -o json)
	jq -e --argjson before "${ready_before}" '.metadata.uid == $before.metadata.uid and .status.currentCopy == $before.status.currentCopy and .spec == $before.spec' <<<"${object}" >/dev/null
	recovery_command old-move-check-settled Succeeded upgrade-check --namespace="${NAMESPACE}" --node-daemonset-name=shiftpv-node
	jq -e '.complete and (.blockers|length) == 0' "${RECOVERY_EVIDENCE}/old-move-check-settled-report.json" >/dev/null
	assert_equal 'old writer still stopped before SAFE cutover' 0 "$(kubectl -n "${NAMESPACE}" get deployment/shiftpv-controller -o jsonpath='{.spec.replicas}')"
	upgrade_candidate
	assert_candidate_images
	kubectl -n "${NAMESPACE}" patch service/shiftpv-webhook --type=json \
		-p "$(jq -c '[{op:"replace",path:"/spec/selector",value:.spec.selector}]' "${RECOVERY_EVIDENCE}/webhook-service-before.json")"
	kubectl -n "${NAMESPACE}" logs pod/maintenance-admission >"${RECOVERY_EVIDENCE}/maintenance-admission.log"
	kubectl -n "${NAMESPACE}" delete pod/maintenance-admission --wait=true --timeout=60s
	assert_equal 'candidate Recreate strategy' Recreate "$(kubectl -n "${NAMESPACE}" get deployment/shiftpv-controller -o jsonpath='{.spec.strategy.type}')"
	for CASE_DIR in "${RECOVERY_EVIDENCE}"/*/; do
		load_retirement_case "${CASE_DIR%/}"
		if [[ "${RECLAIM}" == Retain ]]; then kubectl patch "pv/${PV}" --type=merge -p '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'; fi
		kubectl wait "pv/${PV}" --for=delete --timeout=300s
		kubectl wait "shiftpvvolume/${VOLUME}" --for=delete --timeout=300s
		for node in "${SOURCE}" "${DESTINATION}"; do assert_node_absent "${node}" "$(pool_mount_for_node "${node}")/volumes/${VOLUME}"; done
		kubectl delete "namespace/${NS}" --wait=true --timeout=120s
		jq -n --arg boundary "${BOUNDARY}" --arg policy "${RECLAIM}" --arg uid "${VOLUME_UID}" \
			'{result:"PASS",boundary:$boundary,policy:$policy,volumeUID:$uid,oldWriterBlocked:true,manualRecoveryRequest:false}' >"${CASE_DIR}/result.json"
	done
	kubectl delete "validatingadmissionpolicybinding/${policy}" "validatingadmissionpolicy/${policy}"
	delete_workload shiftpv-upgrade-existing
	create_workload shiftpv-upgrade-after-move "${DEFAULT_CLASS}" 'new creation after native Move recovery'
	delete_workload shiftpv-upgrade-after-move
	jq -s '{result:"PASS",publishedChart:"0.6.13",singleWriterExclusion:true,strictCheckerPreserved:true,cases:.}' "${RECOVERY_EVIDENCE}"/*/result.json >"${RECOVERY_EVIDENCE}/result.json"
	cat "${RECOVERY_EVIDENCE}/result.json"
}
