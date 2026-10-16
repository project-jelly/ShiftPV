#!/usr/bin/env bash
# Shared helpers for the real-node qualification scripts. Source this after
# ROOT_DIR is set and before any helper below is used; every helper reads its
# configuration at call time, so the sourcing script may still set its own
# defaults afterwards.

k() {
	kubectl --context "${KUBECTL_CONTEXT}" --request-timeout=30s "$@"
}

# Remote access is addressed by reviewed role, never by a hostname the cluster
# reports, so a stage can only ever reach the two identities the preflight
# approved.
ssh_node() {
	local role=$1
	shift
	case "${role}" in
	source) ssh -o BatchMode=yes -o ConnectTimeout="${SSH_CONNECT_TIMEOUT}" -o ServerAliveInterval=10 -o ServerAliveCountMax=3 "${SOURCE_SSH_TARGET}" "$@" ;;
	destination) ssh -o BatchMode=yes -o ConnectTimeout="${SSH_CONNECT_TIMEOUT}" -o ServerAliveInterval=10 -o ServerAliveCountMax=3 "${DESTINATION_SSH_TARGET}" "$@" ;;
	*)
		echo "unknown ssh node role: ${role}" >&2
		return 1
		;;
	esac
}

# The fault stages refuse to start unless the reviewed identity of the cluster,
# both nodes, and both candidate images is supplied in full.
require_real_node_env() {
	: "${KUBECTL_CONTEXT:?KUBECTL_CONTEXT is required}"
	: "${SOURCE_NODE:?SOURCE_NODE is required}"
	: "${DESTINATION_NODE:?DESTINATION_NODE is required}"
	: "${FAULT_NODE:?FAULT_NODE is required}"
	: "${FAULT_SSH_TARGET:?FAULT_SSH_TARGET is required}"
	: "${SOURCE_SSH_TARGET:?SOURCE_SSH_TARGET is required}"
	: "${DESTINATION_SSH_TARGET:?DESTINATION_SSH_TARGET is required}"
	: "${EXPECTED_CONTROLLER_IMAGE:?EXPECTED_CONTROLLER_IMAGE is required}"
	: "${EXPECTED_NODE_IMAGE:?EXPECTED_NODE_IMAGE is required}"
	EXPECTED_NON_DAEMONSET_PODS_SHA256=${EXPECTED_NON_DAEMONSET_PODS_SHA256:-}
	export EXPECTED_NON_DAEMONSET_PODS_SHA256
}

require_real_node_commands() {
	local command
	for command in kubectl jq ssh diff tee; do
		command -v "${command}" >/dev/null || {
			echo "required command not found: ${command}" >&2
			exit 1
		}
	done
}

# Defaults shared by every fault stage. ARTIFACT_DIR stays with the caller
# because each stage names its own run directory.
apply_real_node_defaults() {
	SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE:-shiftpv-system}
	CONTROLLER_DEPLOYMENT=${CONTROLLER_DEPLOYMENT:-shiftpv-controller}
	NODE_DAEMONSET=${NODE_DAEMONSET:-shiftpv-node}
	STORAGE_CLASS=${STORAGE_CLASS:-shiftpv}
	SSH_CONNECT_TIMEOUT=${SSH_CONNECT_TIMEOUT:-5}
	WORKLOAD_IMAGE=${WORKLOAD_IMAGE:-busybox:1.37@sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0}
	# The qualified hosts run MicroK8s; these defaults describe that node runtime
	# and may be overridden for another distribution without editing a stage.
	NODE_RUNTIME_STOP_CMD=${NODE_RUNTIME_STOP_CMD:-'sudo systemctl stop snap.microk8s.daemon-containerd.service snap.microk8s.daemon-kubelite.service'}
	NODE_RUNTIME_START_CMD=${NODE_RUNTIME_START_CMD:-'sudo snap start microk8s'}
	NODE_RUNTIME_JOURNAL_UNIT=${NODE_RUNTIME_JOURNAL_UNIT:-snap.microk8s.daemon-kubelite}
	# shellcheck disable=SC2034 # the sourcing stage builds ARTIFACT_DIR from it.
	RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
}

# Both fault stages drive the same synthetic writer: an admission-enabled
# namespace, one RWO claim on the ShiftPV class, and one Deployment that fills a
# payload file once and then idles. They differ only in the names, the claim
# size, and the payload they write.
create_test_workload() {
	local namespace=$1 app_label=$2 storage=$3 payload_mb=$4 payload=$5
	k apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${namespace}
  labels:
    shiftpv.io/admission: enabled
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  namespace: ${namespace}
spec:
  accessModes: [ReadWriteOnce]
  volumeMode: Filesystem
  storageClassName: ${STORAGE_CLASS}
  resources:
    requests:
      storage: ${storage}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: writer
  namespace: ${namespace}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${app_label}
  template:
    metadata:
      labels:
        app: ${app_label}
    spec:
      containers:
        - name: writer
          image: ${WORKLOAD_IMAGE}
          command: [sh, -ec]
          args:
            - |
              if [ ! -f /data/payload ]; then
                dd if=/dev/zero of=/data/payload bs=1M count=${payload_mb}
                printf '%s\\n' '${payload}' >>/data/payload
                sync
              fi
              sleep 86400
          readinessProbe:
            exec:
              command: [test, -f, /data/payload]
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: data
EOF
}

report_error() {
	local result_code=$?
	trap - ERR
	# BASH_SOURCE[1] and BASH_LINENO[0] name the frame that was executing when
	# the error was raised: usually the stage script, but lib.sh itself when a
	# lib.sh function fails internally.
	printf 'FAIL line=%s exit=%d command=%s file=%s\n' \
		"${BASH_LINENO[0]}" "${result_code}" "${BASH_COMMAND}" "${BASH_SOURCE[1]}" |
		tee -a "${ARTIFACT_DIR}/failure.txt" >&2
	exit "${result_code}"
}

# Pass the optional second argument source-kubelite to additionally collect the
# source host control-plane journal; only the fault stages that actually stop
# the source node ask for it.
capture_evidence() {
	local label=$1 extra=${2:-}
	k get nodes -o wide >"${ARTIFACT_DIR}/${label}-nodes.txt" 2>&1 || true
	k get pods -A -o wide >"${ARTIFACT_DIR}/${label}-pods.txt" 2>&1 || true
	k get pvc,pv -A -o wide >"${ARTIFACT_DIR}/${label}-storage.txt" 2>&1 || true
	k get shiftpvpools,shiftpvvolumes,shiftpvmoves -o yaml >"${ARTIFACT_DIR}/${label}-shiftpv.yaml" 2>&1 || true
	k get events -A --sort-by=.metadata.creationTimestamp >"${ARTIFACT_DIR}/${label}-events.txt" 2>&1 || true
	k -n "${SYSTEM_NAMESPACE}" logs deployment/shiftpv-controller --all-containers --tail=-1 >"${ARTIFACT_DIR}/${label}-controller.log" 2>&1 || true
	if [[ "${extra}" == source-kubelite ]]; then
		# service-interruption.sh passes this flag; soak.sh never does.
		ssh_node source sudo journalctl -u "${NODE_RUNTIME_JOURNAL_UNIT}" --since '-1 hour' --no-pager >"${ARTIFACT_DIR}/${label}-${SOURCE_NODE}-kubelite.log" 2>&1 || true
	fi
}

# The stages own their test namespaces differently: pass match=prefix with a
# namespace prefix, or match=exact with a single namespace name. Everything the
# run does not own must be byte-identical before and after.
snapshot_non_shiftpv_specs() {
	local label=$1 match=$2 value=$3 autoscaled_workloads
	case "${match}" in
	prefix | exact) ;;
	*)
		echo "snapshot_non_shiftpv_specs: unknown match mode: ${match}; expected prefix or exact" >&2
		return 1
		;;
	esac
	autoscaled_workloads=$(k get horizontalpodautoscalers.autoscaling -A -o json | jq -c '
		[.items[] | {namespace: .metadata.namespace, kind: .spec.scaleTargetRef.kind, name: .spec.scaleTargetRef.name}]')
	k get deployments.apps,statefulsets.apps -A -o json | jq -S \
		--arg system "${SYSTEM_NAMESPACE}" --arg match "${match}" --arg value "${value}" \
		--argjson autoscaled "${autoscaled_workloads}" '
		def is_test_ns($ns):
			if $match == "prefix" then ($ns | startswith($value))
			elif $match == "exact" then ($ns == $value)
			else error("unknown match mode: \($match)") end;
		[.items[]
		 | select(.metadata.namespace != $system)
		 | select(is_test_ns(.metadata.namespace) | not)
		 | . as $workload
		 | {apiVersion, kind, metadata: {namespace: .metadata.namespace, name: .metadata.name, uid: .metadata.uid},
		    spec: (if any($autoscaled[];
		      .namespace == $workload.metadata.namespace and
		      .kind == $workload.kind and
		      .name == $workload.metadata.name)
		    then ($workload.spec | del(.replicas))
		    else $workload.spec end)}]
		| sort_by(.apiVersion, .kind, .metadata.namespace, .metadata.name)' >"${ARTIFACT_DIR}/${label}-non-shiftpv-workloads.json"
	k get pvc -A -o json | jq -S --arg match "${match}" --arg value "${value}" '
		def is_test_ns($ns):
			if $match == "prefix" then ($ns | startswith($value))
			elif $match == "exact" then ($ns == $value)
			else error("unknown match mode: \($match)") end;
		[.items[]
		 | select(is_test_ns(.metadata.namespace) | not)
		 | {metadata: {namespace: .metadata.namespace, name: .metadata.name, uid: .metadata.uid}, spec}]
		| sort_by(.metadata.namespace, .metadata.name)' >"${ARTIFACT_DIR}/${label}-existing-pvcs.json"
	k get pv -o json | jq -S --arg match "${match}" --arg value "${value}" '
		def is_test_ns($ns):
			if $match == "prefix" then ($ns | startswith($value))
			elif $match == "exact" then ($ns == $value)
			else error("unknown match mode: \($match)") end;
		[.items[]
		 | select(is_test_ns(.spec.claimRef.namespace // "") | not)
		 | {metadata: {name: .metadata.name, uid: .metadata.uid}, spec}]
		| sort_by(.metadata.name)' >"${ARTIFACT_DIR}/${label}-existing-pvs.json"
	k get storageclass -o json | jq -S '
		[.items[] | {metadata: {name: .metadata.name, uid: .metadata.uid}, provisioner, reclaimPolicy, volumeBindingMode, allowVolumeExpansion, mountOptions, parameters}]
		| sort_by(.metadata.name)' >"${ARTIFACT_DIR}/${label}-storageclasses.json"
}

warn_uncordon_needed() {
	local node=$1 reason=$2
	{
		printf '\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n'
		printf 'WARNING: %s\n' "${reason}"
		printf 'Node %s was left cordoned. Run this once resolved:\n' "${node}"
		printf '  kubectl --context %s uncordon %s\n' "${KUBECTL_CONTEXT}" "${node}"
		printf '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n'
	} | tee -a "${ARTIFACT_DIR}/failure.txt" >&2
}

assert_no_unsettled_moves() {
	local unsettled
	unsettled=$(k get shiftpvmoves -o json | jq -r '
		.items[]
		| select(
			((.status.phase == "Succeeded" and .status.cleanup.status.phase == "Completed") or
			 (.status.phase == "Blocked" and .status.recoveryPhase == "Recovered" and
			  (.status.capacityApproved // false) == false and .status.capacityReason == "RecoverySettled")) and
			((.metadata.finalizers // []) | length) == 0
		  | not)
		| .metadata.name')
	if [[ -n "${unsettled}" ]]; then
		echo "unsettled Move journals remain: ${unsettled}" >&2
		return 1
	fi
}

soak_result_marker() {
	local iterations=$1 elapsed=$2 controller_every=$3 cleanup_every=$4 controller_count=$5 cleanup_count=$6
	if ((iterations >= 100 && elapsed >= 43200 && controller_every == 10 && cleanup_every == 20 &&
		controller_count == iterations / 10 && cleanup_count == iterations / 20)); then
		printf 'REAL_NODE_SOAK_OK\n'
	else
		printf 'REAL_NODE_SOAK_SMOKE_OK\n'
	fi
}

install_soak_traps() {
	trap restore_soak_environment EXIT
	trap 'exit 130' INT
	trap 'exit 143' TERM
}

restore_soak_environment() {
	local result_code=$? restore_failed=0
	trap - ERR EXIT INT TERM
	set +e
	if ! release_cleanup_lock; then
		restore_failed=1
		echo 'cleanup lock did not release; inspect its bounded holder' >&2
	fi
	if ! release_soak_cleanup; then
		restore_failed=1
		echo 'cleanup admission policy did not release; inspect retained evidence' >&2
	fi
	if [[ ${SOAK_STARTED:-0} == 1 ]]; then
		local node
		for node in "${SOURCE_NODE}" "${DESTINATION_NODE}"; do
			if ! k uncordon "${node}" >/dev/null 2>&1; then
				restore_failed=1
				warn_uncordon_needed "${node}" "uncordon failed for ${node}"
			fi
		done
	fi
	if ! capture_evidence final; then restore_failed=1; fi
	if [[ ${SOAK_RESULT_READY:-0} == 1 ]]; then
		# Logs are best effort, but final API evidence is required for success.
		if ! k get shiftpvpools,shiftpvvolumes,shiftpvmoves -o json >"${ARTIFACT_DIR}/final-state.json"; then restore_failed=1; fi
	fi
	if ((result_code == 0 && restore_failed)); then result_code=1; fi
	if ((result_code != 0)); then
		echo "soak failed; remaining test resources and evidence were preserved in ${ARTIFACT_DIR}" >&2
	elif [[ ${SOAK_RESULT_READY:-0} == 1 ]]; then
		# shellcheck disable=SC2153,SC2154 # soak.sh sets these results before EXIT.
		printf '%s context=%s iterations=%d elapsedSeconds=%d controllerFaults=%d cleanupFaults=%d artifacts=%s\n' \
			"${result_marker}" "${KUBECTL_CONTEXT}" "${ITERATIONS}" "${elapsed_seconds}" "${controller_faults}" "${cleanup_faults}" "${ARTIFACT_DIR}"
	fi
	exit "${result_code}"
}

soak_controller_pod() {
	local deployment_uid=$1 deployment pod rs_name rs
	deployment=$(k -n "${SYSTEM_NAMESPACE}" get "deployment/${CONTROLLER_DEPLOYMENT}" -o json) || return
	jq -e --arg uid "${deployment_uid}" --arg image "${EXPECTED_CONTROLLER_IMAGE}" '
		.metadata.uid == $uid and .metadata.deletionTimestamp == null and .spec.replicas == 1 and
		any(.spec.template.spec.containers[]?; .name == "shiftpv-controller" and .image == $image)' <<<"${deployment}" >/dev/null || return
	pod=$(k -n "${SYSTEM_NAMESPACE}" get pod -l app.kubernetes.io/component=controller -o json | jq -ec --arg image "${EXPECTED_CONTROLLER_IMAGE}" '
		[.items[] | select(.metadata.deletionTimestamp == null and .status.phase == "Running") |
		 select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) |
		 select(any(.spec.containers[]?; .name == "shiftpv-controller" and .image == $image))] |
		 select(length == 1) | .[0]') || return
	rs_name=$(jq -er '[.metadata.ownerReferences[]? | select(.controller == true)] |
		select(length == 1 and .[0].apiVersion == "apps/v1" and .[0].kind == "ReplicaSet") | .[0].name' <<<"${pod}") || return
	rs=$(k -n "${SYSTEM_NAMESPACE}" get "replicaset/${rs_name}" -o json) || return
	jq -e --arg uid "${deployment_uid}" --arg name "${CONTROLLER_DEPLOYMENT}" --argjson pod "${pod}" '
		.metadata.uid == ([$pod.metadata.ownerReferences[]? | select(.controller == true)][0].uid) and
		any(.metadata.ownerReferences[]?; .controller == true and .apiVersion == "apps/v1" and
		    .kind == "Deployment" and .name == $name and .uid == $uid)' <<<"${rs}" >/dev/null || return
	printf '%s\n' "${pod}"
}

restart_soak_controller() {
	local deployment_uid pod name uid replacement
	deployment_uid=$(k -n "${SYSTEM_NAMESPACE}" get "deployment/${CONTROLLER_DEPLOYMENT}" -o jsonpath='{.metadata.uid}') || return
	test -n "${deployment_uid}" || return 1
	pod=$(soak_controller_pod "${deployment_uid}") || return
	read -r name uid < <(jq -er '[.metadata.name,.metadata.uid] | @tsv' <<<"${pod}") || return
	printf '%s\n' "${pod}" >"${ARTIFACT_DIR}/${uid}-controller-before.json"
	delete_soak_pod "${SYSTEM_NAMESPACE}" "${name}" "${uid}" || return
	k -n "${SYSTEM_NAMESPACE}" rollout status "deployment/${CONTROLLER_DEPLOYMENT}" --timeout=300s || return
	replacement=$(soak_controller_pod "${deployment_uid}") || return
	jq -e --arg uid "${uid}" '.metadata.uid != $uid' <<<"${replacement}" >/dev/null || return
	printf '%s\n' "${replacement}" >"${ARTIFACT_DIR}/${uid}-controller-after.json"
}
