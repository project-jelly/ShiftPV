#!/usr/bin/env bash
set -euo pipefail

# Run only inside the disposable Kubernetes primitive cluster.
probe_kubeconfig="${1:?explicit primitive kubeconfig is required}"
probe_dir="${2:?primitive evidence directory is required}"
crd=shiftpvpools.shiftpv.io
rule_message='capacity.limit must be an exact positive int64 byte count'
limit_schema='/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/capacity/properties/limit/x-kubernetes-validations'
kc() { kubectl --kubeconfig "${probe_kubeconfig}" "$@"; }
write_pool() {
  jq -n --arg name "$1" --arg limit "$2" '{apiVersion:"shiftpv.io/v1alpha1",kind:"ShiftPVPool",metadata:{name:$name},spec:{nodeName:"limit-test-node",mountPath:"/mnt/limit-test",capacity:{limit:$limit}}}'
}
expect_limit_rejected() {
  if kc "$@" >"${probe_dir}/limit-output" 2>"${probe_dir}/limit-error"; then
    echo "invalid capacity.limit unexpectedly accepted: $*" >&2
    exit 1
  fi
  if ! grep -Fq "${rule_message}" "${probe_dir}/limit-error"; then
    cat "${probe_dir}/limit-error" >&2
    echo "request failed without exercising the capacity.limit CEL rule" >&2
    exit 1
  fi
}

while IFS=$'\t' read -r limit bytes; do
  write_pool limit-validation "${limit}" >"${probe_dir}/limit-pool.json"
  if [[ "${bytes}" == 0 ]]; then
    expect_limit_rejected create --dry-run=server -f "${probe_dir}/limit-pool.json"
  else
    kc create --dry-run=server -f "${probe_dir}/limit-pool.json" >/dev/null
  fi
done < <(jq -r '.[] | [.limit, .bytes] | @tsv' src/pool/registration/testdata/limits.json)

# Simulate an upgrade from the old schema, which accepted invalid limits.
# Field-level ratcheting must allow recovery without permitting a new bad value.
kc patch crd "${crd}" --type=json -p "$(jq -nc --arg path "${limit_schema}" '[{op:"remove",path:$path}]')" >/dev/null
write_pool legacy-invalid-limit abc >"${probe_dir}/legacy-limit.json"
for attempt in $(seq 1 30); do
  if kc create -f "${probe_dir}/legacy-limit.json" >"${probe_dir}/limit-output" 2>"${probe_dir}/limit-error"; then
    break
  fi
  if ! grep -Fq "${rule_message}" "${probe_dir}/limit-error" || [[ "${attempt}" == 30 ]]; then
    cat "${probe_dir}/limit-error" >&2
    exit 1
  fi
  sleep 1
done
write_pool legacy-delete-limit -1Gi | kc create -f - >/dev/null
kc apply -f charts/shiftpv/crds/shiftpv.io_shiftpvpools.yaml >/dev/null
# CRD schema propagation is asynchronous. Require the actual rejection before
# asserting ratcheting; never infer it from the successful CRD write alone.
write_pool limit-validation abc >"${probe_dir}/limit-pool.json"
for attempt in $(seq 1 30); do
  if ! kc create --dry-run=server -f "${probe_dir}/limit-pool.json" >"${probe_dir}/limit-output" 2>"${probe_dir}/limit-error"; then
    grep -Fq "${rule_message}" "${probe_dir}/limit-error"
    break
  fi
  if [[ "${attempt}" == 30 ]]; then
    echo "new capacity.limit validation did not become effective" >&2
    exit 1
  fi
  sleep 1
done
kc patch shiftpvpool legacy-invalid-limit --subresource=status --type=merge -p '{"status":{"observedGeneration":1}}' >/dev/null
kc patch shiftpvpool legacy-invalid-limit --type=merge -p '{"spec":{"scanEpoch":1},"metadata":{"annotations":{"example.test/recovery":"allowed"}}}' >/dev/null
expect_limit_rejected patch shiftpvpool legacy-invalid-limit --type=merge -p '{"spec":{"capacity":{"limit":"1500m"}}}'
kc patch shiftpvpool legacy-invalid-limit --type=merge -p '{"spec":{"capacity":{"limit":"1Gi"}}}' >/dev/null
[[ "$(kc get shiftpvpool legacy-invalid-limit -o jsonpath='{.spec.capacity.limit}')" == 1Gi ]]
kc delete shiftpvpool legacy-invalid-limit --wait=true >/dev/null

# An unchanged invalid limit must also permit finalization after DELETE.
kc patch shiftpvpool legacy-delete-limit --type=merge -p '{"metadata":{"finalizers":["example.test/limit-protection"]}}' >/dev/null
legacy_uid="$(kc get shiftpvpool legacy-delete-limit -o jsonpath='{.metadata.uid}')"
kc delete shiftpvpool legacy-delete-limit --wait=false >/dev/null
kc get shiftpvpool legacy-delete-limit -o json | jq -e --arg uid "${legacy_uid}" '.metadata.uid == $uid and .metadata.deletionTimestamp != null and .spec.capacity.limit == "-1Gi"' >/dev/null
kc patch shiftpvpool legacy-delete-limit --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null
kc wait --for=delete shiftpvpool/legacy-delete-limit --timeout=30s >/dev/null
echo 'Pool capacity.limit API corpus and legacy recovery ratcheting passed'
