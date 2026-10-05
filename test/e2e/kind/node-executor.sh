#!/usr/bin/env bash
set -euo pipefail
: "${CLUSTER_NAME:?CLUSTER_NAME is required}"

NAME=shiftpv-node-effects-test
VOLUME=
PV=
WATCH_PID=
WATCH_FILE=$(mktemp)
cleanup() {
 local result=$?
 if [[ -n "${WATCH_PID}" ]]; then
  kill "${WATCH_PID}" >/dev/null 2>&1 || true
  wait "${WATCH_PID}" >/dev/null 2>&1 || true
 fi
 rm -f "${WATCH_FILE}"
 kubectl delete pod "${NAME}" --ignore-not-found --wait=true --timeout=30s >/dev/null 2>&1 || true
 kubectl delete pvc "${NAME}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
 return "${result}"
}
trap cleanup EXIT

kubectl apply -f - <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${NAME}
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
  name: ${NAME}
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
        claimName: ${NAME}
YAML
kubectl wait --for=condition=Ready "pod/${NAME}" --timeout=3m
PV=$(kubectl get "pvc/${NAME}" -o jsonpath='{.spec.volumeName}')
VOLUME=$(kubectl get "pv/${PV}" -o jsonpath='{.spec.csi.volumeHandle}')
kubectl get "shiftpvvolume/${VOLUME}" -o json | jq -e '
 .status as $s | $s.phase == "Ready" and
 $s.creationExecutor.podUID == $s.creationReceipt.executorUID and
 $s.creationReceipt.operationID == $s.creationOperationID and
 ($s.creationReceipt.localReceiptDigest | test("^[0-9a-f]{64}$"))
' >/dev/null
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
echo "Resident Node create, Pod restart, receipt and fenced delete passed"
