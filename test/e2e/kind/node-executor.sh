#!/usr/bin/env bash
set -euo pipefail
: "${CLUSTER_NAME:?CLUSTER_NAME is required}"

NAME=shiftpv-node-effects-test
VOLUME=
PV=
WATCH_PID=
CAPABILITY_POD=
WATCH_FILE=$(mktemp)
restore_creation_capability() {
 [[ -n "${CAPABILITY_POD}" ]] || return 0
 local name uid current patch
 name=$(jq -er '.metadata.name' <<<"${CAPABILITY_POD}") || return
 uid=$(jq -er '.metadata.uid' <<<"${CAPABILITY_POD}") || return
 current=$(kubectl -n shiftpv-system get "pod/${name}" -o json) || return
 jq -e --arg uid "${uid}" '.metadata.uid == $uid and .metadata.deletionTimestamp == null' <<<"${current}" >/dev/null || return
 patch=$(jq -c '[{op:"test",path:"/metadata/uid",value:.metadata.uid},
  {op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},
  {op:"add",path:"/metadata/annotations/shiftpv.io~1node-effects",value:"v1"}]' <<<"${current}") || return
 kubectl -n shiftpv-system patch "pod/${name}" --type=json -p "${patch}" >/dev/null || return
 CAPABILITY_POD=
}
cleanup() {
 local result=$?
 restore_creation_capability || result=1
 if [[ -n "${WATCH_PID}" ]]; then
  kill "${WATCH_PID}" >/dev/null 2>&1 || true
  wait "${WATCH_PID}" >/dev/null 2>&1 || true
 fi
 rm -f "${WATCH_FILE}"
 kubectl delete pod "${NAME}" --ignore-not-found --wait=true --timeout=30s >/dev/null 2>&1 || true
 kubectl delete pvc "${NAME}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
 kubectl delete pod "${NAME}-capacity" --ignore-not-found --wait=true --timeout=30s >/dev/null 2>&1 || true
 kubectl delete pvc "${NAME}-capacity" --ignore-not-found --wait=false >/dev/null 2>&1 || true
 return "${result}"
}
trap cleanup EXIT

for node_pod in $(kubectl -n shiftpv-system get pods -l app.kubernetes.io/component=node -o name); do
 kubectl -n shiftpv-system wait --for=jsonpath='{.metadata.annotations.shiftpv\.io/node-rpc-port}'=9760 "${node_pod}" --timeout=1m
 kubectl -n shiftpv-system wait --for=jsonpath='{.metadata.annotations.shiftpv\.io/node-effects}'=v1 "${node_pod}" --timeout=1m
done

apply_effect_workload() {
 local workload=$1
kubectl apply -f - <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${workload}
spec:
  storageClassName: shiftpv
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 8Mi
---
apiVersion: v1
kind: Pod
metadata:
  name: ${workload}
spec:
  nodeSelector:
    kubernetes.io/hostname: ${CLUSTER_NAME}-worker
  terminationGracePeriodSeconds: 1
  containers:
    - name: writer
      image: busybox:1.37
      command: [sh, -ec, "echo resident-node > /data/payload; sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${workload}
YAML
}

# A healthy new admission must exercise authenticated CAPACITY RPC. The later
# capability-retry case intentionally reuses its Pending capacity reservation.
apply_effect_workload "${NAME}-capacity"
kubectl wait --for=condition=Ready "pod/${NAME}-capacity" --timeout=3m
CAPACITY_PV=$(kubectl get "pvc/${NAME}-capacity" -o jsonpath='{.spec.volumeName}')

CAPABILITY_POD=$(kubectl -n shiftpv-system get pods -l app.kubernetes.io/component=node --field-selector "spec.nodeName=${CLUSTER_NAME}-worker" -o json | jq -ce '.items | select(length == 1) | .[0] | select(.metadata.annotations["shiftpv.io/node-effects"] == "v1")')
CAPABILITY_PATCH=$(jq -c '[{op:"test",path:"/metadata/uid",value:.metadata.uid},
 {op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},
 {op:"remove",path:"/metadata/annotations/shiftpv.io~1node-effects"}]' <<<"${CAPABILITY_POD}")
kubectl -n shiftpv-system patch "pod/$(jq -er '.metadata.name' <<<"${CAPABILITY_POD}")" --type=json -p "${CAPABILITY_PATCH}"

apply_effect_workload "${NAME}"
PVC_UID=$(kubectl get "pvc/${NAME}" -o jsonpath='{.metadata.uid}')
PENDING=
for _ in {1..120}; do
 PENDING=$(kubectl get shiftpvvolumes -o json | jq -c --arg request "pvc-${PVC_UID}" '.items[] | select(.spec.requestName == $request)')
 [[ -n "${PENDING}" ]] && break
 sleep 1
done
test -n "${PENDING}"
jq -e '.status.phase == "Pending" and .status.creationExecutor == null and .status.creationReceipt == null and .status.currentCopy != null' <<<"${PENDING}" >/dev/null
PENDING_VOLUME=$(jq -er '.metadata.name' <<<"${PENDING}")
PENDING_UID=$(jq -er '.metadata.uid' <<<"${PENDING}")
PENDING_COPY=$(jq -c '.status.currentCopy' <<<"${PENDING}")
# The persisted Pending request reserves capacity while unsupported Nodes wait.
for _ in {1..3}; do
 kubectl get "shiftpvvolume/${PENDING_VOLUME}" -o json | jq -e --arg uid "${PENDING_UID}" --argjson copy "${PENDING_COPY}" '
  .metadata.uid == $uid and .status.currentCopy == $copy and .status.phase == "Pending" and .spec.capacityBytes == 8388608 and
  .status.creationExecutor == null and .status.creationReceipt == null' >/dev/null
 kubectl -n shiftpv-system get pods -o json | jq -e 'all(.items[]; (.metadata.name | startswith("shiftpv-create-") | not))' >/dev/null
 sleep 1
done
restore_creation_capability
kubectl wait --for=condition=Ready "pod/${NAME}" --timeout=3m
PV=$(kubectl get "pvc/${NAME}" -o jsonpath='{.spec.volumeName}')
VOLUME=$(kubectl get "pv/${PV}" -o jsonpath='{.spec.csi.volumeHandle}')
kubectl get "shiftpvvolume/${VOLUME}" -o json | jq -e '
 .status as $s | $s.phase == "Ready" and
 $s.creationExecutor.podUID == $s.creationReceipt.executorUID and
 $s.creationReceipt.operationID == $s.creationOperationID and
 ($s.creationReceipt.localReceiptDigest | test("^[0-9a-f]{64}$"))
' >/dev/null
test "${VOLUME}" = "${PENDING_VOLUME}"
kubectl get "shiftpvvolume/${VOLUME}" -o json | jq -e --arg uid "${PENDING_UID}" --argjson copy "${PENDING_COPY}" '.metadata.uid == $uid and .status.currentCopy == $copy' >/dev/null
CREATE_UID=$(kubectl get "shiftpvvolume/${VOLUME}" -o jsonpath='{.status.creationExecutor.podUID}')

# New Node Pod identity must not invalidate a completed creation receipt or data.
kubectl -n shiftpv-system rollout restart daemonset/shiftpv-node
kubectl -n shiftpv-system rollout status daemonset/shiftpv-node --timeout=3m
kubectl wait --for=condition=Ready "shiftpvpool/worker-a" --timeout=2m
NODE_POD=$(kubectl -n shiftpv-system get pods -l app.kubernetes.io/component=node --field-selector "spec.nodeName=${CLUSTER_NAME}-worker" -o jsonpath='{.items[0].metadata.name}')
kubectl -n shiftpv-system wait --for=jsonpath='{.metadata.annotations.shiftpv\.io/node-effects}'=v1 "pod/${NODE_POD}" --timeout=1m
test "$(kubectl exec "${NAME}" -- cat /data/payload)" = resident-node
kubectl get "shiftpvvolume/${VOLUME}" -o json --watch --output-watch-events >"${WATCH_FILE}" &
WATCH_PID=$!
# Ensure the watch has an initial snapshot before deleting the parent.
for ((i=0;i<100;i++)); do
 if [[ -s "${WATCH_FILE}" ]]; then break; fi
 sleep 0.1
done
test -s "${WATCH_FILE}"
kubectl delete "pod/${NAME}" --wait=true
kubectl delete "pvc/${NAME}" --wait=true
kubectl wait --for=delete "pv/${PV}" --timeout=3m
kubectl wait --for=delete "shiftpvvolume/${VOLUME}" --timeout=1m
kill "${WATCH_PID}" >/dev/null 2>&1 || true
wait "${WATCH_PID}" >/dev/null 2>&1 || true
WATCH_PID=
jq -e --arg old "${CREATE_UID}" '
 select(.type == "MODIFIED") | .object | .status.cleanup.status as $s |
 select($s.phase == "Completed" and $s.executor.kind == "Node" and
  $s.executor.podUID == $s.receipt.executorUID and $s.executor.podUID != $old and
  $s.receipt.retired and $s.receipt.purged and $s.absenceProof.valid and $s.absenceProof.complete and $s.absenceProof.absent and
  $s.absenceProof.observedGeneration >= $s.absenceProof.requiredGeneration) |
 select((.metadata.finalizers // []) | index("shiftpv.io/volume-protection"))
' "${WATCH_FILE}" >/dev/null
kubectl -n shiftpv-system get pods -o json | jq -e 'all(.items[]; (.metadata.name | startswith("shiftpv-create-") | not))' >/dev/null
kubectl -n shiftpv-system get jobs -o json | jq -e 'all(.items[]; (.metadata.name | startswith("shiftpv-cleanup-") | not))' >/dev/null
# Remove the healthy admission fixture after the tested Node replacement.
kubectl delete "pod/${NAME}-capacity" --wait=true
kubectl delete "pvc/${NAME}-capacity" --wait=true
kubectl wait "pv/${CAPACITY_PV}" --for=delete --timeout=3m
RPC_LOGS=$(kubectl -n shiftpv-system logs deployment/shiftpv-controller -c shiftpv-controller)
for operation in CAPACITY CREATE RECLAIM; do
 if ! grep -Eq "operation=\"${operation}\".*error=null" <<<"${RPC_LOGS}"; then
  echo "missing authenticated ${operation} RPC evidence" >&2
  exit 1
 fi
done
echo "Authenticated Node gRPC capacity, capability retry, create, Pod restart and fenced delete passed"
