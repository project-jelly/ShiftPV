#!/usr/bin/env bash
set -euo pipefail
: "${CLUSTER_NAME:?isolated Kind cluster name required}"
: "${KUBECONFIG:?isolated kubeconfig required}"
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"

source_node="${CLUSTER_NAME}-worker"
peer=shiftpv-retirement-peer
namespace=shiftpv-pool-retirement-test
class=shiftpv-retirement-test
work_dir=$(mktemp -d)
trap 'rm -rf -- "${work_dir}"' EXIT
pool=$(kubectl get shiftpvpools -o json | jq -ce --arg node "${source_node}" \
 '[.items[] | select(.spec.nodeName==$node)] | select(length==1) | .[0]')
name=$(jq -er '.metadata.name' <<<"${pool}")
uid=$(jq -er '.metadata.uid' <<<"${pool}")
root=$(jq -er '.spec.mountPath | select(.=="/mnt/shiftpv")' <<<"${pool}")
peer_root=/var/lib/shiftpv-retirement-test
jq '{apiVersion,kind,metadata:{name:.metadata.name},spec}' <<<"${pool}" >"${work_dir}/original.json"
test -z "$(kubectl get namespace "${namespace}" --ignore-not-found -o name)"
test -z "$(kubectl get storageclass "${class}" --ignore-not-found -o name)"
test -z "$(kubectl get shiftpvpool "${peer}" --ignore-not-found -o name)"
docker exec "${source_node}" sh -ec 'test ! -e "$1/.retirement-test"; printf "%s\n" "$2" >"$1/.retirement-test"; mkdir -p "$3"' sh "${root}" "${uid}" "${peer_root}"
marker=$(docker exec "${source_node}" sha256sum "${root}/.shiftpv/pool.json")

kubectl apply -f - <<YAML
apiVersion: shiftpv.io/v1alpha1
kind: ShiftPVPool
metadata:
  name: ${peer}
spec:
  nodeName: ${source_node}
  mountPath: ${peer_root}
  poolGroup: retirement-test
  capacity:
    limit: 32Mi
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ${class}
provisioner: csi.shiftpv.io
parameters:
  shiftpv.io/pool-group: retirement-test
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
---
apiVersion: v1
kind: Namespace
metadata:
  name: ${namespace}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  namespace: ${namespace}
spec:
  storageClassName: ${class}
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Mi
---
apiVersion: v1
kind: Pod
metadata:
  name: writer
  namespace: ${namespace}
spec:
  nodeSelector:
    kubernetes.io/hostname: ${source_node}
  terminationGracePeriodSeconds: 1
  containers:
    - name: writer
      image: busybox:1.37
      command: [sh, -ec, "echo retirement-data > /data/payload; sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: data
YAML
kubectl wait "shiftpvpool/${peer}" --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=False --timeout=90s
test -z "$(kubectl -n "${namespace}" get pvc/data -o jsonpath='{.spec.volumeName}')"
if kubectl annotate "shiftpvpool/${name}" "shiftpv.io/registration-retired=${uid}" --dry-run=server >"${work_dir}/forgery.log" 2>&1; then
 echo 'operator forged retirement approval' >&2
 exit 1
fi
grep -Fq 'trusted controller' "${work_dir}/forgery.log"
kubectl annotate "shiftpvpool/${name}" "shiftpv.io/retire-registration=${uid}"
kubectl wait "shiftpvpool/${name}" --for=jsonpath='{.metadata.annotations.shiftpv\.io/registration-retired}'="${uid}" --timeout=180s
kubectl get "shiftpvpool/${name}" -o json | jq -e --arg uid "${uid}" '
 .metadata.uid==$uid and (.metadata.finalizers | index("shiftpv.io/pool-protection") != null) and
 all(.status.conditions[]?; .type!="IdentityReleased" or .status!="True")' >/dev/null
test "$(docker exec "${source_node}" cat "${root}/.retirement-test")" = "${uid}"
test "$(docker exec "${source_node}" sha256sum "${root}/.shiftpv/pool.json")" = "${marker}"
kubectl wait "shiftpvpool/${peer}" --for=condition=Ready --timeout=180s
kubectl -n "${namespace}" wait pod/writer --for=condition=Ready --timeout=180s
test "$(kubectl -n "${namespace}" exec writer -- cat /data/payload)" = retirement-data
pv=$(kubectl -n "${namespace}" get pvc/data -o jsonpath='{.spec.volumeName}')
volume=$(kubectl get "pv/${pv}" -o jsonpath='{.spec.csi.volumeHandle}')
test "$(kubectl get "shiftpvvolume/${volume}" -o jsonpath='{.status.currentCopy.poolName}')" = "${peer}"

# Retirement preserved the root. An explicit normal deletion must still use
# live inventory and exact node identity release before the name can be reused.
docker exec "${source_node}" rm "${root}/.retirement-test"
kubectl delete "shiftpvpool/${name}" --wait=true --timeout=180s
test "$(kubectl -n "${namespace}" exec writer -- cat /data/payload)" = retirement-data
kubectl delete namespace "${namespace}" --wait=true --timeout=180s
kubectl wait "pv/${pv}" --for=delete --timeout=180s
kubectl wait "shiftpvvolume/${volume}" --for=delete --timeout=180s
kubectl delete "shiftpvpool/${peer}" --wait=true --timeout=180s
kubectl delete "storageclass/${class}"
kubectl create -f "${work_dir}/original.json"
kubectl wait "shiftpvpool/${name}" --for=condition=Ready --timeout=180s
test "$(kubectl get "shiftpvpool/${name}" -o jsonpath='{.metadata.uid}')" != "${uid}"
echo 'Pool retirement passed: UID approval, marker/data preservation, peer allocation, normal deletion and exact re-registration'
