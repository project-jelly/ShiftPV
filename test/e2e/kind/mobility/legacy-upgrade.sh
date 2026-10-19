#!/usr/bin/env bash
set -euo pipefail

# Settle an old transaction with its own writer, inspect with the candidate,
# then replace the controller while preserving the existing data and history.
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
# The older binary predates these flags; preserve and restore the exact candidate
# arguments while changing the image and supported arguments in one update.
LEGACY_ARGS=$(jq -c --arg image "${LEGACY_CONTROLLER}" '
 map(select((startswith("--mobility-placement-priority-class=") or startswith("--mobility-operation-timeout=")) | not) |
 if startswith("--helper-image=") then "--helper-image="+$image
 elif startswith("--mobility-helper-image=") then "--mobility-helper-image="+$image
 else . end)' <<<"${CANDIDATE_ARGS}")
CHECK_ACCOUNT=$(kubectl -n shiftpv-system get deployment/shiftpv-controller -o jsonpath='{.spec.template.spec.serviceAccountName}')
CANDIDATE_HELPER_RULES=$(kubectl get clusterrole/shiftpv-helper -o json | jq -c '.rules')
inspection_dir=$(mktemp -d)

# This Pod starts only the candidate's GET/LIST inspection command. It never
# replaces or starts another controller while the old writer is settling work.
candidate_preflight() {
 local label=$1 name="${POLICY}-$1" deadline phase="" pod exit_code
 test -z "$(kubectl -n shiftpv-system get "pod/${name}" --ignore-not-found -o name)" || return
 kubectl -n shiftpv-system run "${name}" --restart=Never --image="${CANDIDATE_CONTROLLER}" \
  --image-pull-policy=IfNotPresent \
  --overrides="$(jq -nc --arg account "${CHECK_ACCOUNT}" '{spec:{serviceAccountName:$account}}')" \
  --command -- /shiftpv-controller upgrade-check --namespace=shiftpv-system \
  --node-daemonset-name=shiftpv-node --timeout=90s || return
 deadline=$((SECONDS + 150))
 while ((SECONDS < deadline)); do
  pod=$(kubectl -n shiftpv-system get "pod/${name}" --request-timeout=10s -o json) || return
  phase=$(jq -er '.status.phase' <<<"${pod}") || return
  if [[ "${phase}" = Succeeded || "${phase}" = Failed ]]; then break; fi
  sleep 1
 done
 if [[ "${phase}" != Succeeded && "${phase}" != Failed ]]; then
  kubectl -n shiftpv-system describe "pod/${name}" >&2 || true
  echo 'candidate preflight Pod did not terminate' >&2
  return 1
 fi
 kubectl -n shiftpv-system logs "pod/${name}" >"${inspection_dir}/${label}.log" || return
 # stdout JSON and the final unsafe error may share the Pod log stream.
 jq -Rsc 'split("\n") | map(fromjson? | select(type == "object" and has("complete") and has("blockers"))) |
  if length == 1 then .[0] else error("missing unique inspection report") end' \
  "${inspection_dir}/${label}.log" >"${inspection_dir}/${label}.json" || return
 exit_code=$(jq -er '.status.containerStatuses[0].state.terminated.exitCode' <<<"${pod}") || return
 kubectl -n shiftpv-system delete "pod/${name}" --wait=true --timeout=30s || return
 [[ "${phase}" = Succeeded && "${exit_code}" = 0 ]]
}

legacy_actor_uids() {
 kubectl -n shiftpv-system get jobs,pods -o json | jq -c --arg uid "${MOVE_UID}" '
  [.items[] | select(.kind == "Job" and any(.metadata.ownerReferences[]?; .kind == "ShiftPVMove" and .uid == $uid))] as $jobs |
  [$jobs[].metadata.uid] as $jobuids |
  [.items[] | select(any(.metadata.ownerReferences[]?; (.kind == "ShiftPVMove" and .uid == $uid) or
   (.kind == "Job" and (.uid as $id | $jobuids | index($id)) != null)))] | map(.metadata.uid) | sort'
}
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
# Install the preceding creation contract before starting its published writer.
# The current schema intentionally rejects newly created protocol-less Moves.
legacy_contract_file=$(mktemp)
trap 'rm -f "${legacy_contract_file}"; rm -rf -- "${inspection_dir}"' EXIT
kubectl create --dry-run=client --validate=false \
	-f "${ROOT_DIR}/charts/shiftpv/crds/shiftpv.io_shiftpvmoves.yaml" -o json \
	| jq '.spec.versions[].schema.openAPIV3Schema.properties.spec["x-kubernetes-validations"] |=
		map(select(.message != "new Moves require prepared-v1"))' >"${legacy_contract_file}"
kubectl apply -f "${legacy_contract_file}"
# The pinned old cleanup helper writes status. Restore only its preceding grant
# while that writer settles; the candidate helper's grant remains read-only.
kubectl patch clusterrole/shiftpv-helper --type=json -p '[{"op":"add","path":"/rules/-","value":{"apiGroups":["shiftpv.io"],"resources":["shiftpvvolumes/status","shiftpvmoves/status"],"verbs":["update"]}}]'
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
PV_UID=$(kubectl get "pv/${PV}" -o jsonpath='{.metadata.uid}')
VOLUME=$(kubectl get "pv/${PV}" -o jsonpath='{.spec.csi.volumeHandle}')
VOLUME_UID=$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.metadata.uid}')
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
# The candidate must report the active transaction before the old writer stops.
OLD_POD_UID=$(kubectl -n shiftpv-system get pods -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.uid}')
if candidate_preflight active; then
 echo 'active legacy transaction incorrectly passed preflight' >&2
 exit 1
fi
jq -e --arg move "${MOVE}" --arg volume "${VOLUME}" '
 .complete == true and any(.blockers[]; .kind == "ShiftPVMove" and .name == $move) and
 any(.blockers[]; .kind == "ShiftPVVolume" and .name == $volume and .reason == "ActiveMove")
' "${inspection_dir}/active.json" >/dev/null
 test "$(kubectl -n shiftpv-system get pods -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.uid}')" = "${OLD_POD_UID}"
 test "$(kubectl -n shiftpv-system get deployment/shiftpv-controller -o jsonpath='{.spec.template.spec.containers[?(@.name=="shiftpv-controller")].image}')" = "${LEGACY_CONTROLLER}"

# v0.4.22 runs Move cleanup as a Job, using the same pinned old helper image.
# Keep that writer and its helpers until it closes the transaction itself.
kubectl delete validatingadmissionpolicybinding "${POLICY}"
kubectl delete validatingadmissionpolicy "${POLICY}"
kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.phase}'=Succeeded --timeout=600s
kubectl wait "shiftpvmove/${MOVE}" --for=jsonpath='{.status.cleanup.status.phase}'=Completed --timeout=120s
kubectl -n "${NAMESPACE}" rollout status deployment/wffc --timeout=180s
test "$(pod_sha256 "${NAMESPACE}" deployment/wffc /data/payload)" = "${CHECKSUM}"
test "$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.ownerNode}')" = "${DESTINATION}"
test -z "$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.activeMove}')"
kubectl uncordon "${SOURCE}"
kubectl uncordon "${DESTINATION}"
kubectl apply -f "${ROOT_DIR}/charts/shiftpv/crds/shiftpv.io_shiftpvmoves.yaml"

# Exclusion continues through cutover. The old controller is still running
# while the new image proves Ready compatibility, exact history and quiescence.
inspection_ok=0
deadline=$((SECONDS + 180))
attempt=0
while ((SECONDS < deadline)); do
 attempt=$((attempt + 1))
 if candidate_preflight "settled-${attempt}"; then inspection_ok=1; break; fi
 jq -e '.complete == true' "${inspection_dir}/settled-${attempt}.json" >/dev/null
 sleep 1
done
if [[ "${inspection_ok}" != 1 ]]; then
 cat "${inspection_dir}/settled-${attempt}.log" >&2
 echo 'old writer did not leave independently settled transition inputs' >&2
 exit 1
fi
jq -e '.complete == true and .blockers == []' "${inspection_dir}/settled-${attempt}.json" >/dev/null
test "$(kubectl -n shiftpv-system get pods -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.uid}')" = "${OLD_POD_UID}"
test "$(kubectl -n shiftpv-system get deployment/shiftpv-controller -o jsonpath='{.spec.template.spec.containers[?(@.name=="shiftpv-controller")].image}')" = "${LEGACY_CONTROLLER}"
BEFORE_MOVE=$(kubectl get "shiftpvmove/${MOVE}" -o json | jq -cS '{uid:.metadata.uid,spec,status}')
BEFORE_VOLUME=$(kubectl get "shiftpvvolume/${VOLUME}" -o json | jq -cS '{uid:.metadata.uid,owner:.status.ownerNode,copy:.status.currentCopy}')
BEFORE_ACTORS=$(legacy_actor_uids)
BEFORE_MOVES=$(kubectl get shiftpvmoves -o json | jq -c --arg volume "${VOLUME}" '[.items[] | select(.spec.volumeID == $volume) | .metadata.uid] | sort')
kubectl patch clusterrole/shiftpv-helper --type=json -p "$(jq -nc \
 --argjson rules "${CANDIDATE_HELPER_RULES}" '[{op:"replace",path:"/rules",value:$rules}]')"
kubectl -n shiftpv-system patch deployment/shiftpv-controller --type=strategic -p "$(jq -nc \
 --arg image "${CANDIDATE_CONTROLLER}" --argjson args "${CANDIDATE_ARGS}" \
 '{spec:{template:{spec:{containers:[{name:"shiftpv-controller",image:$image,args:$args}]}}}}')"
kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=180s
kubectl -n shiftpv-system get pods -l app.kubernetes.io/component=controller -o json | jq -e --arg old "${OLD_POD_UID}" 'all(.items[]; .metadata.uid != $old)' >/dev/null
candidate_preflight after
jq -e '.complete == true and .blockers == []' "${inspection_dir}/after.json" >/dev/null
test "$(kubectl get "shiftpvmove/${MOVE}" -o json | jq -cS '{uid:.metadata.uid,spec,status}')" = "${BEFORE_MOVE}"
test "$(kubectl get "shiftpvvolume/${VOLUME}" -o json | jq -cS '{uid:.metadata.uid,owner:.status.ownerNode,copy:.status.currentCopy}')" = "${BEFORE_VOLUME}"
legacy_actor_uids | jq -e --argjson before "${BEFORE_ACTORS}" 'all(.[]; . as $uid | $before | index($uid) != null)' >/dev/null
test "$(kubectl get shiftpvmoves -o json | jq -c --arg volume "${VOLUME}" '[.items[] | select(.spec.volumeID == $volume) | .metadata.uid] | sort')" = "${BEFORE_MOVES}"
test "$(kubectl -n "${NAMESPACE}" get pvc/wffc -o jsonpath='{.metadata.uid}')" = "${CLAIM_UID}"
test "$(kubectl get "pv/${PV}" -o jsonpath='{.metadata.uid}')" = "${PV_UID}"
test "$(kubectl get "pv/${PV}" -o jsonpath='{.spec.csi.volumeHandle}')" = "${VOLUME}"
test "$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.metadata.uid}')" = "${VOLUME_UID}"
test "$(pod_sha256 "${NAMESPACE}" deployment/wffc /data/payload)" = "${CHECKSUM}"
kubectl delete namespace "${NAMESPACE}" --wait=true --timeout=180s
kubectl wait "pv/${PV}" --for=delete --timeout=180s
kubectl wait "shiftpvvolume/${VOLUME}" --for=delete --timeout=180s
echo 'legacy transition passed: active preflight rejected; old writer settled; preflight before Recreate; Ready data and history preserved without replay'
