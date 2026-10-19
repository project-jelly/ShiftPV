#!/usr/bin/env bash
set -euo pipefail
: "${CLUSTER_NAME:?isolated Kind cluster name required}"
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"

work_dir=$(mktemp -d)
trap 'rm -rf -- "${work_dir}"' EXIT
umask 077
namespace=shiftpv-system
pod=$(kubectl -n "${namespace}" get pods -l app.kubernetes.io/component=node \
 --field-selector "spec.nodeName=${CLUSTER_NAME}-worker" -o json | jq -ce '.items | select(length == 1) | .[0]')
name=$(jq -er '.metadata.name' <<<"${pod}")
uid=$(jq -er '.metadata.uid' <<<"${pod}")
account=$(jq -er '.spec.serviceAccountName' <<<"${pod}")
peer=$(kubectl -n "${namespace}" get pods -l app.kubernetes.io/component=node \
 --field-selector "spec.nodeName=${CLUSTER_NAME}-worker2" -o json | jq -er '.items | select(length == 1) | .[0].metadata.name')

# A token-only kubeconfig prevents the admin client certificate from taking
# precedence. Never print or archive these short-lived bound credentials.
kubectl config view --minify --flatten --raw -o json | jq \
 '{apiVersion:"v1",kind:"Config",clusters:.clusters,contexts:[{name:"probe",context:{cluster:.contexts[0].context.cluster,user:"probe"}}],"current-context":"probe"}' \
 >"${work_dir}/base.json"
kubectl -n "${namespace}" create token "${account}" --duration=10m \
 --bound-object-kind=Pod --bound-object-name="${name}" --bound-object-uid="${uid}" >"${work_dir}/bound-token"
jq --rawfile token "${work_dir}/bound-token" '.users=[{name:"probe",user:{token:($token|rtrimstr("\n"))}}]' \
 "${work_dir}/base.json" >"${work_dir}/bound.json"

node_request() {
 kubectl --kubeconfig="${work_dir}/bound.json" -n "${namespace}" --request-timeout=10s "$@"
}
node_update() {
 local target=$1 filter=$2
 node_request get "pod/${target}" -o json | jq "${filter}" | node_request replace --dry-run=server -f -
}
denied_update() {
 local target=$1 filter=$2 message=$3 output
 if output=$(node_update "${target}" "${filter}" 2>&1); then
  echo 'Node token unexpectedly changed a protected Pod field' >&2
  return 1
 fi
 grep -Fq "${message}" <<<"${output}"
}

node_update "${name}" '.metadata.annotations["shiftpv.io/node-effects"]="dry-run-probe"' >/dev/null
denied_update "${peer}" '.metadata.annotations["shiftpv.io/node-effects"]="dry-run-probe"' \
 'ShiftPV Node may update only its token-bound Pod'
denied_update "${name}" '.metadata.annotations["example.test/unrelated"]="changed"' \
 'ShiftPV Node may change only capability and RPC annotations'
denied_update "${name}" '.metadata.labels["example.test/unrelated"]="changed"' \
 'ShiftPV Node may change only capability and RPC annotations'
denied_update "${name}" '.metadata.generateName="changed-"' \
 'ShiftPV Node may change only capability and RPC annotations'
denied_update "${name}" '.metadata.finalizers=["example.test/unrelated"]' \
 'ShiftPV Node may change only capability and RPC annotations'

# Container images are otherwise mutable on Pods. Rejection must come from
# this policy, rather than Kubernetes' unrelated immutable-field validation.
denied_update "${name}" '.spec.containers |= map(if .name=="shiftpv-node" then .image="registry.k8s.io/pause:3.10.1" else . end)' \
 'ShiftPV Node may not change Pod execution settings'

kubectl -n "${namespace}" create token "${account}" --duration=10m >"${work_dir}/unbound-token"
jq --rawfile token "${work_dir}/unbound-token" '.users=[{name:"probe",user:{token:($token|rtrimstr("\n"))}}]' \
 "${work_dir}/base.json" >"${work_dir}/unbound.json"
node_request get "pod/${name}" -o json | jq '.metadata.annotations["shiftpv.io/node-effects"]="dry-run-probe"' >"${work_dir}/update.json"
if output=$(kubectl --kubeconfig="${work_dir}/unbound.json" -n "${namespace}" --request-timeout=10s \
 replace --dry-run=server -f "${work_dir}/update.json" 2>&1); then
 echo 'Unbound Node token unexpectedly advertised a Pod capability' >&2
 exit 1
fi
grep -Fq 'ShiftPV Node may update only its token-bound Pod' <<<"${output}"

# Operators/controllers retain their ordinary Pod maintenance permissions.
kubectl -n "${namespace}" patch "pod/${name}" --type=merge --dry-run=server \
 -p '{"metadata":{"labels":{"example.test/operator":"allowed"}}}' >/dev/null
echo 'Node advertisement guard passed: self-bound annotations only; peer/spec/metadata/unbound writes denied'
