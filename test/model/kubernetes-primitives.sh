#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "${ROOT_DIR}"
# shellcheck source=test/e2e/kind/lib/cluster.sh
source "${ROOT_DIR}/test/e2e/kind/lib/cluster.sh"

for command in docker kind kubectl jq go; do
  command -v "${command}" >/dev/null || {
    echo "required command not found: ${command}" >&2
    exit 1
  }
done

cluster_name="${CLUSTER_NAME:-shiftpv-v04-primitives-$$}"
node_image="${NODE_IMAGE:-$(kind_node_image)}"
keep_cluster="${KEEP_CLUSTER:-0}"
cluster_creation_started=0
probe_dir="$(mktemp -d)"
probe_kubeconfig="${probe_dir}/kubeconfig"

cleanup() {
  result_code=$?
  if [[ "${keep_cluster}" != "1" && "${cluster_creation_started}" == "1" ]]; then
    kind delete cluster --name "${cluster_name}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${probe_dir}" && -d "${probe_dir}" ]]; then
    rm -r -- "${probe_dir}"
  fi
  exit "${result_code}"
}
trap cleanup EXIT

if kind get clusters | grep -Fxq "${cluster_name}"; then
  echo "kind cluster ${cluster_name} already exists; choose another CLUSTER_NAME" >&2
  exit 1
fi

cluster_creation_started=1
kind create cluster --name "${cluster_name}" --image "${node_image}" --kubeconfig "${probe_kubeconfig}" --wait 120s >/dev/null

server_version="$(kubectl --kubeconfig "${probe_kubeconfig}" get --raw /version | sed -n 's/.*"gitVersion": "\([^"]*\)".*/\1/p')"
echo "Kubernetes API server: ${server_version}"

# Owner-placement recovery uses Pod Update: add an owner selector and remove
# only our scheduling gate. Keep the user's gate so this probe cannot run.
kubectl --kubeconfig "${probe_kubeconfig}" create -f - >/dev/null <<'YAML'
apiVersion: v1
kind: Pod
metadata:
  name: held-owner-placement
  namespace: default
  annotations:
    shiftpv.io/placement: held
spec:
  schedulingGates:
    - name: example.test/hold
    - name: shiftpv.io/placement-hold
  nodeSelector:
    kubernetes.io/os: linux
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: kubernetes.io/os
                operator: In
                values: [linux]
  containers:
    - name: probe
      image: registry.k8s.io/pause:3.10.1
YAML

held_pod_uid="$(kubectl --kubeconfig "${probe_kubeconfig}" get pod held-owner-placement -o jsonpath='{.metadata.uid}')"
held_pod_affinity="$(kubectl --kubeconfig "${probe_kubeconfig}" get pod held-owner-placement -o jsonpath='{.spec.affinity}')"
owner_node="$(kubectl --kubeconfig "${probe_kubeconfig}" get nodes -o jsonpath='{.items[0].metadata.name}')"
kubectl --kubeconfig "${probe_kubeconfig}" get pod held-owner-placement -o json >"${probe_dir}/held-pod.json"
kubectl --kubeconfig "${probe_kubeconfig}" patch --local -f "${probe_dir}/held-pod.json" --type=json \
  -p "$(printf '[{"op":"test","path":"/spec/schedulingGates/1/name","value":"shiftpv.io/placement-hold"},{"op":"remove","path":"/spec/schedulingGates/1"},{"op":"add","path":"/spec/nodeSelector/kubernetes.io~1hostname","value":"%s"},{"op":"replace","path":"/metadata/annotations/shiftpv.io~1placement","value":"owner"}]' "${owner_node}")" \
  -o json >"${probe_dir}/owner-pod.json"
kubectl --kubeconfig "${probe_kubeconfig}" replace -f "${probe_dir}/owner-pod.json" >/dev/null

owner_pod_uid="$(kubectl --kubeconfig "${probe_kubeconfig}" get pod held-owner-placement -o jsonpath='{.metadata.uid}')"
owner_pod_gates="$(kubectl --kubeconfig "${probe_kubeconfig}" get pod held-owner-placement -o jsonpath='{range .spec.schedulingGates[*]}{.name}{"\n"}{end}')"
owner_pod_selector="$(kubectl --kubeconfig "${probe_kubeconfig}" get pod held-owner-placement -o jsonpath='{.spec.nodeSelector.kubernetes\.io/hostname}')"
owner_pod_os="$(kubectl --kubeconfig "${probe_kubeconfig}" get pod held-owner-placement -o jsonpath='{.spec.nodeSelector.kubernetes\.io/os}')"
owner_pod_affinity="$(kubectl --kubeconfig "${probe_kubeconfig}" get pod held-owner-placement -o jsonpath='{.spec.affinity}')"
owner_pod_annotation="$(kubectl --kubeconfig "${probe_kubeconfig}" get pod held-owner-placement -o jsonpath='{.metadata.annotations.shiftpv\.io/placement}')"
owner_pod_node="$(kubectl --kubeconfig "${probe_kubeconfig}" get pod held-owner-placement -o jsonpath='{.spec.nodeName}')"
if [[ "${owner_pod_uid}" != "${held_pod_uid}" || "${owner_pod_gates}" != "example.test/hold" ||
  "${owner_pod_selector}" != "${owner_node}" || "${owner_pod_os}" != "linux" ||
  "${owner_pod_affinity}" != "${held_pod_affinity}" || "${owner_pod_annotation}" != "owner" || -n "${owner_pod_node}" ]]; then
  echo "gated Pod Update did not preserve identity and user constraints while restoring owner placement" >&2
  kubectl --kubeconfig "${probe_kubeconfig}" get pod held-owner-placement -o yaml >&2
  exit 1
fi
echo "Gated Pod owner-placement Update passed; user gate retained"

bash test/model/move-preparation-primitives.sh "${probe_kubeconfig}" "${probe_dir}"

kubectl --kubeconfig "${probe_kubeconfig}" apply \
  -f charts/shiftpv/crds/shiftpv.io_shiftpvpools.yaml \
  -f charts/shiftpv/crds/shiftpv.io_shiftpvvolumes.yaml \
  -f charts/shiftpv/crds/shiftpv.io_shiftpvmoves.yaml >/dev/null

for crd in shiftpvpools.shiftpv.io shiftpvvolumes.shiftpv.io shiftpvmoves.shiftpv.io; do
  kubectl --kubeconfig "${probe_kubeconfig}" wait \
    --for=condition=Established "customresourcedefinition/${crd}" \
    --timeout=30s >/dev/null
done

bash test/model/pool-limit-primitives.sh "${probe_kubeconfig}" "${probe_dir}"

protected_crds="$(kubectl --kubeconfig "${probe_kubeconfig}" get customresourcedefinitions \
  -l shiftpv.io/uninstall-protected=true \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"
if [[ "${protected_crds}" != $'shiftpvmoves.shiftpv.io\nshiftpvpools.shiftpv.io\nshiftpvvolumes.shiftpv.io' ]]; then
  echo "expected exactly the Pool, Volume, and Move CRDs to be uninstall-protected" >&2
  printf '%s\n' "${protected_crds}" >&2
  exit 1
fi
if kubectl --kubeconfig "${probe_kubeconfig}" get customresourcedefinition/shiftpvcleanups.shiftpv.io >/dev/null 2>&1; then
  echo "standalone cleanup CRD unexpectedly exists" >&2
  exit 1
fi

volume_id="shiftpv-0123456789abcdef0123456789abcdef"
kubectl --kubeconfig "${probe_kubeconfig}" apply -f - >/dev/null <<YAML
apiVersion: shiftpv.io/v1alpha1
kind: ShiftPVPool
metadata:
  name: source-pool
spec:
  nodeName: source-node
  mountPath: /var/lib/shiftpv
  capacity:
    limit: 10Gi
  scanEpoch: 1
---
apiVersion: shiftpv.io/v1alpha1
kind: ShiftPVVolume
metadata:
  name: ${volume_id}
spec:
  volumeID: ${volume_id}
  requestName: pvc-uid-request
  initialNode: source-node
  capacityBytes: 1073741824
---
apiVersion: shiftpv.io/v1alpha1
kind: ShiftPVMove
metadata:
  name: move-sample
spec:
  protocolVersion: prepared-v1
  volumeID: ${volume_id}
  sourceNode: source-node
YAML

pool_uid="$(kubectl --kubeconfig "${probe_kubeconfig}" get shiftpvpool source-pool -o jsonpath='{.metadata.uid}')"
volume_uid="$(kubectl --kubeconfig "${probe_kubeconfig}" get shiftpvvolume "${volume_id}" -o jsonpath='{.metadata.uid}')"
move_uid="$(kubectl --kubeconfig "${probe_kubeconfig}" get shiftpvmove move-sample -o jsonpath='{.metadata.uid}')"
pool_generation="$(kubectl --kubeconfig "${probe_kubeconfig}" get shiftpvpool source-pool -o jsonpath='{.metadata.generation}')"
observed_at="2026-01-01T00:00:00Z"
receipt_digest="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

# A legacy Volume may acquire its initial Pool once. A new Volume retains the
# same fence from Create even though Kubernetes discards Create-time status.
if [[ -n "$(kubectl --kubeconfig "${probe_kubeconfig}" get shiftpvvolume "${volume_id}" -o jsonpath='{.spec.initialPool}')" ]]; then
  echo "legacy ShiftPVVolume unexpectedly requires initialPool" >&2
  exit 1
fi
kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvvolume "${volume_id}" \
  --type=merge -p "$(printf '{"spec":{"initialPool":{"name":"source-pool","uid":"%s"}}}' "${pool_uid}")" >/dev/null

initializing_volume_id="shiftpv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
kubectl --kubeconfig "${probe_kubeconfig}" create -f - >/dev/null <<YAML
apiVersion: shiftpv.io/v1alpha1
kind: ShiftPVVolume
metadata:
  name: ${initializing_volume_id}
spec:
  volumeID: ${initializing_volume_id}
  requestName: pvc-initializing-request
  initialNode: source-node
  initialPool:
    name: source-pool
    uid: ${pool_uid}
  capacityBytes: 1073741824
status:
  phase: Pending
YAML
if [[ -n "$(kubectl --kubeconfig "${probe_kubeconfig}" get shiftpvvolume "${initializing_volume_id}" -o jsonpath='{.status.phase}')" ]]; then
  echo "Create unexpectedly retained status instead of exercising initialization" >&2
  exit 1
fi

# DELETE before the first status write retains the protected spec reservation.
# This is the API state modeled by the capacity initialization/deletion DI test.
terminating_intent_id="shiftpv-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
kubectl --kubeconfig "${probe_kubeconfig}" create -f - >/dev/null <<YAML
apiVersion: shiftpv.io/v1alpha1
kind: ShiftPVVolume
metadata:
  name: ${terminating_intent_id}
  finalizers: [shiftpv.io/volume-protection]
spec:
  volumeID: ${terminating_intent_id}
  requestName: pvc-terminating-intent
  initialNode: source-node
  initialPool:
    name: source-pool
    uid: ${pool_uid}
  capacityBytes: 67108864
YAML
terminating_intent_uid="$(kubectl --kubeconfig "${probe_kubeconfig}" get shiftpvvolume "${terminating_intent_id}" -o jsonpath='{.metadata.uid}')"
kubectl --kubeconfig "${probe_kubeconfig}" delete shiftpvvolume "${terminating_intent_id}" --wait=false >/dev/null
kubectl --kubeconfig "${probe_kubeconfig}" get shiftpvvolume "${terminating_intent_id}" -o json | jq -e \
  --arg uid "${terminating_intent_uid}" --arg pool_uid "${pool_uid}" \
  '.metadata.uid == $uid and .metadata.deletionTimestamp != null and
   .metadata.finalizers == ["shiftpv.io/volume-protection"] and
   .spec.initialPool == {name:"source-pool",uid:$pool_uid} and
   .spec.capacityBytes == 67108864 and (.status // {}) == {}' >/dev/null
kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvvolume "${terminating_intent_id}" --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null
kubectl --kubeconfig "${probe_kubeconfig}" wait --for=delete "shiftpvvolume/${terminating_intent_id}" --timeout=30s >/dev/null
echo 'Unstarted Volume DELETE retained its protected initial reservation until finalization'

# Exercise the real CSI/Registry cancellation against the API server. The
# filesystem dependencies fail the test if this no-effects path invokes them.
SHIFTPV_API_CANCELLATION_TEST=1 KUBECONFIG="${probe_kubeconfig}" \
  go test -count=1 -v ./test/integration/creation-cancellation

for protected_volume in "${volume_id}" "${initializing_volume_id}"; do
  kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvvolume "${protected_volume}" \
    --subresource=status --type=merge -p '{"status":{"phase":"Pending"}}' >/dev/null
  initial_pool="$(kubectl --kubeconfig "${probe_kubeconfig}" get shiftpvvolume "${protected_volume}" -o jsonpath='{.spec.initialPool.name}/{.spec.initialPool.uid}')"
  if [[ "${initial_pool}" != "source-pool/${pool_uid}" ]]; then
    echo "initialPool was pruned or changed by status update: ${initial_pool}" >&2
    exit 1
  fi
  for patch in '{"spec":{"initialPool":{"name":"other-pool"}}}' \
    '{"spec":{"initialPool":{"uid":"other-uid"}}}' '{"spec":{"initialPool":null}}'; do
    if kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvvolume "${protected_volume}" \
      --type=merge -p "${patch}" >/dev/null 2>"${probe_dir}/initial-pool-error"; then
      echo "immutable initialPool mutation unexpectedly succeeded: ${patch}" >&2
      exit 1
    fi
    if ! grep -Fq 'initialPool is immutable once recorded' "${probe_dir}/initial-pool-error"; then
      cat "${probe_dir}/initial-pool-error" >&2
      echo "initialPool mutation failed without exercising its CEL immutability rule" >&2
      exit 1
    fi
  done
done

kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvpool source-pool \
  --subresource=status --type=merge -p "$(printf '{\"status\":{\"observedGeneration\":%s,\"lastProbeTime\":\"%s\",\"conditions\":[{\"type\":\"Ready\",\"status\":\"True\",\"observedGeneration\":%s,\"lastTransitionTime\":\"%s\",\"reason\":\"ScanComplete\",\"message\":\"complete\"}],\"inventory\":{\"observedAt\":\"%s\",\"valid\":true,\"truncated\":false,\"copies\":[]}}}' "${pool_generation}" "${observed_at}" "${pool_generation}" "${observed_at}" "${observed_at}")" >/dev/null

kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvvolume "${volume_id}" \
  --subresource=status --type=merge -p "$(printf '{\"status\":{\"phase\":\"Deleting\",\"ownerNode\":\"source-node\",\"currentCopy\":{\"installationID\":\"install-1\",\"poolName\":\"source-pool\",\"poolUID\":\"%s\",\"volumeID\":\"%s\",\"volumeUID\":\"%s\",\"copyID\":\"copy-source\",\"nodeName\":\"source-node\",\"role\":\"Serving\"},\"cleanup\":{\"spec\":{\"operationID\":\"delete-op\",\"target\":{\"installationID\":\"install-1\",\"poolName\":\"source-pool\",\"poolUID\":\"%s\",\"volumeID\":\"%s\",\"volumeUID\":\"%s\",\"copyID\":\"copy-source\",\"nodeName\":\"source-node\",\"role\":\"Serving\"},\"reason\":\"VolumeDelete\",\"authority\":{\"kind\":\"ShiftPVVolume\",\"name\":\"%s\",\"uid\":\"%s\"}},\"status\":{\"phase\":\"Completed\",\"executor\":{\"jobName\":\"cleanup-job\",\"jobUID\":\"job-uid\",\"podUID\":\"pod-uid\",\"nodeName\":\"source-node\"},\"receipt\":{\"operationID\":\"delete-op\",\"executorUID\":\"pod-uid\",\"observedAt\":\"%s\",\"retired\":true,\"purged\":true,\"localReceiptDigest\":\"%s\"},\"absenceProof\":{\"requestID\":\"scan-request\",\"poolName\":\"source-pool\",\"poolUID\":\"%s\",\"requiredGeneration\":1,\"observedGeneration\":1,\"valid\":true,\"complete\":true,\"absent\":true,\"confirmedAt\":\"%s\"},\"settledAt\":\"%s\"}}}}' "${pool_uid}" "${volume_id}" "${volume_uid}" "${pool_uid}" "${volume_id}" "${volume_uid}" "${volume_id}" "${volume_uid}" "${observed_at}" "${receipt_digest}" "${pool_uid}" "${observed_at}" "${observed_at}")" >/dev/null

kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvmove move-sample \
  --subresource=status --type=merge -p "$(printf '{\"status\":{\"phase\":\"Blocked\",\"reason\":\"AwaitingRecovery\",\"message\":\"sample\",\"sourceCopy\":{\"installationID\":\"install-1\",\"poolName\":\"source-pool\",\"poolUID\":\"%s\",\"volumeID\":\"%s\",\"volumeUID\":\"%s\",\"copyID\":\"copy-source\",\"nodeName\":\"source-node\",\"role\":\"Serving\"},\"cleanup\":{\"spec\":{\"operationID\":\"move-cleanup-op\",\"target\":{\"installationID\":\"install-1\",\"poolName\":\"source-pool\",\"poolUID\":\"%s\",\"volumeID\":\"%s\",\"volumeUID\":\"%s\",\"copyID\":\"copy-source\",\"nodeName\":\"source-node\",\"role\":\"Serving\"},\"reason\":\"MoveSource\",\"authority\":{\"kind\":\"ShiftPVMove\",\"name\":\"move-sample\",\"uid\":\"%s\"}},\"status\":{\"phase\":\"Pending\"}}}}' "${pool_uid}" "${volume_id}" "${volume_uid}" "${pool_uid}" "${volume_id}" "${volume_uid}" "${move_uid}")" >/dev/null

if kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvpool source-pool \
  --type=merge -p '{"spec":{"nodeName":"other-node"}}' >/dev/null 2>&1; then
  echo "ShiftPVPool immutable nodeName update unexpectedly succeeded" >&2
  exit 1
fi
if kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvvolume "${volume_id}" \
  --type=merge -p '{"spec":{"requestName":"other-request"}}' >/dev/null 2>&1; then
  echo "ShiftPVVolume immutable requestName update unexpectedly succeeded" >&2
  exit 1
fi
if kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvmove move-sample \
  --type=merge -p '{"spec":{"sourceNode":"other-node"}}' >/dev/null 2>&1; then
  echo "ShiftPVMove immutable sourceNode update unexpectedly succeeded" >&2
  exit 1
fi

kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvmove move-sample \
  --type=merge -p '{"spec":{"recovery":"ResumeOwner"}}' >/dev/null
if kubectl --kubeconfig "${probe_kubeconfig}" patch shiftpvmove move-sample \
  --type=json -p='[{"op":"remove","path":"/spec/recovery"}]' >/dev/null 2>&1; then
  echo "ShiftPVMove recovery removal unexpectedly succeeded" >&2
  exit 1
fi

kubectl --kubeconfig "${probe_kubeconfig}" apply -f - >/dev/null <<'YAML'
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: protocolprobes.design.shiftpv.io
spec:
  group: design.shiftpv.io
  scope: Cluster
  names:
    plural: protocolprobes
    singular: protocolprobe
    kind: ProtocolProbe
  versions:
    - name: v1alpha1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          required: [spec]
          properties:
            spec:
              type: object
              required: [scanEpoch]
              properties:
                scanEpoch:
                  type: integer
                  format: int64
            status:
              type: object
              properties:
                observedGeneration:
                  type: integer
                  format: int64
                receipt:
                  type: string
      subresources:
        status: {}
YAML

kubectl --kubeconfig "${probe_kubeconfig}" wait \
  --for=condition=Established customresourcedefinition/protocolprobes.design.shiftpv.io \
  --timeout=30s >/dev/null

kubectl --kubeconfig "${probe_kubeconfig}" apply -f - >/dev/null <<'YAML'
apiVersion: design.shiftpv.io/v1alpha1
kind: ProtocolProbe
metadata:
  name: lifecycle
  finalizers:
    - design.shiftpv.io/protect
spec:
  scanEpoch: 1
YAML

initial_generation="$(kubectl --kubeconfig "${probe_kubeconfig}" get protocolprobe lifecycle -o jsonpath='{.metadata.generation}')"
kubectl --kubeconfig "${probe_kubeconfig}" patch protocolprobe lifecycle \
  --subresource=status --type=merge \
  -p "{\"status\":{\"observedGeneration\":${initial_generation},\"receipt\":\"before-delete\"}}" >/dev/null
status_generation="$(kubectl --kubeconfig "${probe_kubeconfig}" get protocolprobe lifecycle -o jsonpath='{.metadata.generation}')"
if [[ "${status_generation}" != "${initial_generation}" ]]; then
  echo "status update unexpectedly changed metadata.generation" >&2
  exit 1
fi

kubectl --kubeconfig "${probe_kubeconfig}" patch protocolprobe lifecycle \
  --type=merge -p '{"spec":{"scanEpoch":2}}' >/dev/null
scan_generation="$(kubectl --kubeconfig "${probe_kubeconfig}" get protocolprobe lifecycle -o jsonpath='{.metadata.generation}')"
if ((scan_generation <= initial_generation)); then
  echo "spec.scanEpoch update did not advance metadata.generation" >&2
  exit 1
fi

probe_uid="$(kubectl --kubeconfig "${probe_kubeconfig}" get protocolprobe lifecycle -o jsonpath='{.metadata.uid}')"
kubectl --kubeconfig "${probe_kubeconfig}" apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: protocol-probe-child
  namespace: default
  ownerReferences:
    - apiVersion: design.shiftpv.io/v1alpha1
      kind: ProtocolProbe
      name: lifecycle
      uid: ${probe_uid}
      controller: true
      blockOwnerDeletion: true
spec:
  suspend: true
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: probe
          image: registry.k8s.io/pause:3.10.1
YAML

kubectl --kubeconfig "${probe_kubeconfig}" delete protocolprobe lifecycle --wait=false >/dev/null
deletion_timestamp="$(kubectl --kubeconfig "${probe_kubeconfig}" get protocolprobe lifecycle -o jsonpath='{.metadata.deletionTimestamp}')"
if [[ -z "${deletion_timestamp}" ]]; then
  echo "finalized object disappeared instead of entering termination" >&2
  exit 1
fi

kubectl --kubeconfig "${probe_kubeconfig}" patch protocolprobe lifecycle \
  --subresource=status --type=merge \
  -p "{\"status\":{\"observedGeneration\":${scan_generation},\"receipt\":\"after-delete\"}}" >/dev/null
receipt="$(kubectl --kubeconfig "${probe_kubeconfig}" get protocolprobe lifecycle -o jsonpath='{.status.receipt}')"
if [[ "${receipt}" != "after-delete" ]]; then
  echo "status journal could not be updated after deletionTimestamp" >&2
  exit 1
fi

kubectl --kubeconfig "${probe_kubeconfig}" get job protocol-probe-child -n default >/dev/null
kubectl --kubeconfig "${probe_kubeconfig}" patch protocolprobe lifecycle \
  --type=json -p='[{"op":"remove","path":"/metadata/finalizers"}]' >/dev/null
kubectl --kubeconfig "${probe_kubeconfig}" wait --for=delete protocolprobe/lifecycle --timeout=30s >/dev/null
kubectl --kubeconfig "${probe_kubeconfig}" wait --for=delete job/protocol-probe-child -n default --timeout=30s >/dev/null

echo "ShiftPV 0.4 Kubernetes protocol primitives passed"
