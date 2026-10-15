#!/usr/bin/env bash
# Invoked only by kubernetes-primitives.sh inside its isolated Kind cluster.
set -euo pipefail

probe_kubeconfig=${1:?isolated kubeconfig is required}
probe_dir=${2:?isolated scratch directory is required}
move_api=/apis/shiftpv.io/v1alpha1/shiftpvmoves
move_crd=charts/shiftpv/crds/shiftpv.io_shiftpvmoves.yaml
legacy_name=move-legacy-protocol-probe
prepared_name=move-prepared-protocol-probe

expect_rejected() {
  local diagnostic=$1
  shift
  if "$@" >"${probe_dir}/rejected-output" 2>"${probe_dir}/rejected-error"; then
    echo "API unexpectedly accepted request: $*" >&2
    cat "${probe_dir}/rejected-output" >&2
    exit 1
  fi
  if ! grep -Fq "${diagnostic}" "${probe_dir}/rejected-error"; then
    echo "API rejection did not contain expected diagnostic: ${diagnostic}" >&2
    cat "${probe_dir}/rejected-error" >&2
    exit 1
  fi
}

get_move() {
  kubectl --kubeconfig "${probe_kubeconfig}" get shiftpvmove "$1" -o json
}

# Derive the immediately preceding storage contract without keeping a second
# full CRD. Remove only the new fields and rules that mention those fields.
kubectl --kubeconfig "${probe_kubeconfig}" create --dry-run=client --validate=false \
  -f "${move_crd}" -o json >"${probe_dir}/move-crd.json"
jq '
  .spec.versions[].schema.openAPIV3Schema |= (
    del(.properties.spec.properties.protocolVersion,
        .properties.status.properties.preparation,
        .properties.status.properties.persistentVolumeUID,
        .properties.status.properties.persistentVolumeClaimUID,
        .properties.status.properties.placementPodUID)
    | .properties.status.properties.phase.enum -= ["Preparing", "StartingDisruption", "Canceling", "Cancelled"]
    | walk(if type == "object" and has("x-kubernetes-validations") then
        .["x-kubernetes-validations"] |= map(select(
          .rule | test("protocolVersion|preparation|persistentVolumeUID|persistentVolumeClaimUID|placementPodUID") | not))
      else . end)
  )' "${probe_dir}/move-crd.json" >"${probe_dir}/legacy-move-crd.json"
kubectl --kubeconfig "${probe_kubeconfig}" apply -f "${probe_dir}/legacy-move-crd.json" >/dev/null
kubectl --kubeconfig "${probe_kubeconfig}" wait --for=condition=Established \
  crd/shiftpvmoves.shiftpv.io --timeout=30s >/dev/null

jq -n --arg name "${prepared_name}" '{
  apiVersion: "shiftpv.io/v1alpha1", kind: "ShiftPVMove",
  metadata: {name: $name},
  spec: {volumeID: "shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", sourceNode: "source-node", protocolVersion: "prepared-v1"}
}' >"${probe_dir}/prepared-create.json"
expect_rejected 'unknown field "spec.protocolVersion"' \
  kubectl --kubeconfig "${probe_kubeconfig}" create --raw "${move_api}?fieldValidation=Strict" \
  -f "${probe_dir}/prepared-create.json"
if [[ -n "$(kubectl --kubeconfig "${probe_kubeconfig}" get shiftpvmove "${prepared_name}" --ignore-not-found -o name)" ]]; then
  echo "old CRD persisted a prepared request after Strict Create rejection" >&2
  exit 1
fi

jq --arg name "${legacy_name}" '.metadata.name = $name | del(.spec.protocolVersion)' \
  "${probe_dir}/prepared-create.json" >"${probe_dir}/legacy-create.json"
kubectl --kubeconfig "${probe_kubeconfig}" create --raw "${move_api}?fieldValidation=Strict" \
  -f "${probe_dir}/legacy-create.json" >"${probe_dir}/legacy-before.json"
jq '.status = {phase: "Pending", preparation: {
  pool: {name: "destination-pool", uid: "destination-pool-uid"},
  nodeName: "destination-node", reservedBytes: 1073741824
}}' "${probe_dir}/legacy-before.json" >"${probe_dir}/legacy-status.json"
expect_rejected 'unknown field "status.preparation"' \
  kubectl --kubeconfig "${probe_kubeconfig}" replace --raw "${move_api}/${legacy_name}/status?fieldValidation=Strict" \
  -f "${probe_dir}/legacy-status.json"
get_move "${legacy_name}" >"${probe_dir}/legacy-after.json"
jq -e --slurpfile before "${probe_dir}/legacy-before.json" \
  '.metadata.uid == $before[0].metadata.uid and .metadata.resourceVersion == $before[0].metadata.resourceVersion and .status == $before[0].status' \
  "${probe_dir}/legacy-after.json" >/dev/null
echo "Old Move CRD rejects Strict Create and status Update before pruning"

kubectl --kubeconfig "${probe_kubeconfig}" apply -f "${move_crd}" >/dev/null
kubectl --kubeconfig "${probe_kubeconfig}" create --raw "${move_api}?fieldValidation=Strict" \
  -f "${probe_dir}/prepared-create.json" >"${probe_dir}/prepared-before.json"

# Test absence as well as value immutability; a field-level oldSelf rule alone
# cannot protect an optional protocol from being added to a legacy object.
get_move "${legacy_name}" | jq '.spec.protocolVersion = "prepared-v1"' >"${probe_dir}/protocol-mutation.json"
expect_rejected 'protocolVersion' \
  kubectl --kubeconfig "${probe_kubeconfig}" replace --raw "${move_api}/${legacy_name}?fieldValidation=Strict" \
  -f "${probe_dir}/protocol-mutation.json"
for mutation in 'del(.spec.protocolVersion)' '.spec.protocolVersion = null' '.spec.protocolVersion = "prepared-v2"'; do
  get_move "${prepared_name}" | jq "${mutation}" >"${probe_dir}/protocol-mutation.json"
  expect_rejected 'protocolVersion' \
    kubectl --kubeconfig "${probe_kubeconfig}" replace --raw "${move_api}/${prepared_name}?fieldValidation=Strict" \
    -f "${probe_dir}/protocol-mutation.json"
done

# Existing legacy objects remain writable without acquiring a protocol.
get_move "${legacy_name}" | jq '.status = {phase: "Pending", reason: "LegacyAfterUpgrade"}' >"${probe_dir}/legacy-status.json"
kubectl --kubeconfig "${probe_kubeconfig}" replace --raw "${move_api}/${legacy_name}/status?fieldValidation=Strict" \
  -f "${probe_dir}/legacy-status.json" >/dev/null
get_move "${legacy_name}" | jq -e --slurpfile before "${probe_dir}/legacy-before.json" \
  '.metadata.uid == $before[0].metadata.uid and (.spec | has("protocolVersion") | not) and .status.reason == "LegacyAfterUpgrade"' >/dev/null

jq '.status = {
  phase: "Pending", capacityApproved: false,
  persistentVolumeName: "source-pv", persistentVolumeUID: "source-pv-uid",
  persistentVolumeClaimNamespace: "default", persistentVolumeClaimName: "source-claim", persistentVolumeClaimUID: "source-claim-uid",
  consumerName: "source-consumer", consumerUID: "source-consumer-uid", placementPodUID: "placement-pod-uid",
  sourceCopy: {installationID: "install-1", poolName: "source-pool", poolUID: "source-pool-uid",
    volumeID: .spec.volumeID, volumeUID: "source-volume-uid", copyID: "source-copy", nodeName: "source-node", role: "Serving"},
  preparation: {pool: {name: "destination-pool", uid: "destination-pool-uid"}, nodeName: "destination-node", reservedBytes: 1073741824}
}' "${probe_dir}/prepared-before.json" >"${probe_dir}/prepared-status.json"
kubectl --kubeconfig "${probe_kubeconfig}" replace --raw "${move_api}/${prepared_name}/status?fieldValidation=Strict" \
  -f "${probe_dir}/prepared-status.json" >"${probe_dir}/prepared-written.json"
get_move "${prepared_name}" >"${probe_dir}/prepared-stored.json"
jq -e --slurpfile expected "${probe_dir}/prepared-status.json" \
  '.metadata.uid == $expected[0].metadata.uid and .spec == $expected[0].spec and .status == $expected[0].status' \
  "${probe_dir}/prepared-stored.json" >/dev/null

for mutation in 'del(.status.preparation.pool.uid)' '.status.preparation.pool.uid = ""' \
  '.status.preparation.nodeName = ""' '.status.preparation.reservedBytes = 0' \
  '.status.preparation.reservedBytes = -1' '.status.preparation.reservedBytes = "1024"' \
  '.status.capacityApproved = true' '.status.incomingCopy = .status.sourceCopy' \
  'del(.status.persistentVolumeUID)' 'del(.status.persistentVolumeClaimUID)' 'del(.status.placementPodUID)'; do
  get_move "${prepared_name}" | jq "${mutation}" >"${probe_dir}/invalid-preparation.json"
  expect_rejected 'Invalid' \
    kubectl --kubeconfig "${probe_kubeconfig}" replace --raw "${move_api}/${prepared_name}/status?fieldValidation=Strict" \
    -f "${probe_dir}/invalid-preparation.json"
done
get_move "${legacy_name}" | jq --slurpfile prepared "${probe_dir}/prepared-status.json" \
  '.status = $prepared[0].status' >"${probe_dir}/legacy-preparation.json"
expect_rejected 'Invalid' \
  kubectl --kubeconfig "${probe_kubeconfig}" replace --raw "${move_api}/${legacy_name}/status?fieldValidation=Strict" \
  -f "${probe_dir}/legacy-preparation.json"

# Real status PUT obeys resourceVersion preconditions, unlike the fake client.
jq '.status.reason = "FirstCAS"' "${probe_dir}/prepared-stored.json" >"${probe_dir}/prepared-first-cas.json"
kubectl --kubeconfig "${probe_kubeconfig}" replace --raw "${move_api}/${prepared_name}/status?fieldValidation=Strict" \
  -f "${probe_dir}/prepared-first-cas.json" >/dev/null
jq '.status.reason = "StaleCAS"' "${probe_dir}/prepared-stored.json" >"${probe_dir}/prepared-stale-cas.json"
expect_rejected 'Conflict' \
  kubectl --kubeconfig "${probe_kubeconfig}" replace --raw "${move_api}/${prepared_name}/status?fieldValidation=Strict" \
  -f "${probe_dir}/prepared-stale-cas.json"
get_move "${prepared_name}" | jq -e --slurpfile expected "${probe_dir}/prepared-status.json" \
  '.metadata.uid == $expected[0].metadata.uid and .status.reason == "FirstCAS" and .status.preparation == $expected[0].status.preparation' >/dev/null

# Nullable means explicit null survives storage; release authorization is the
# Go protocol's responsibility, not proof supplied by this schema primitive.
get_move "${prepared_name}" | jq '.status.preparation = null' >"${probe_dir}/prepared-null.json"
kubectl --kubeconfig "${probe_kubeconfig}" replace --raw "${move_api}/${prepared_name}/status?fieldValidation=Strict" \
  -f "${probe_dir}/prepared-null.json" >/dev/null
get_move "${prepared_name}" | jq -e '.status | has("preparation") and .preparation == null' >/dev/null
echo "Prepared Move protocol immutability, schema, roundtrip and status CAS passed"
