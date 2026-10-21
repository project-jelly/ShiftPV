#!/usr/bin/env bash
# Sourced in a subshell by run.sh after the two base Pools become Ready.
set -euo pipefail
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"
node="${CLUSTER_NAME}-worker"
test "$(docker inspect -f '{{index .Config.Labels "io.x-k8s.kind.cluster"}}' "${node}")" = "${CLUSTER_NAME}"
pool=worker-a
candidate=shiftpv-preserved-overlap
namespace=shiftpv-preserved-uninstall
root=/mnt/shiftpv
backup=/var/lib/shiftpv-preserved-original
evidence="${ROOT_DIR}/.tmp/preserved-uninstall/${CLUSTER_NAME}"
mkdir -p "${evidence}"
registration=$(kubectl get "shiftpvpool/${pool}" -o json)
uid=$(jq -er '.metadata.uid' <<<"${registration}")
jq -e '.status.registrationApproved == true' <<<"${registration}" >/dev/null
# scanEpoch is a mutable inventory request counter, not registration identity.
jq '{apiVersion,kind,metadata:{name:.metadata.name},spec:(.spec | del(.scanEpoch))}' <<<"${registration}" >"${evidence}/original.json"
revision=$(helm history shiftpv -n shiftpv-system -o json | jq -er '[.[] | select(.status=="deployed")][-1].revision')
masked=0
backed=0
restore_mounts() {
	if [[ "${masked}" == 1 ]]; then docker exec "${node}" umount "${root}"; fi
	if [[ "${backed}" == 1 ]]; then docker exec "${node}" sh -ec 'umount "$1"; rmdir "$1"' sh "${backup}"; fi
}
trap restore_mounts EXIT
helm_apply() {
	helm upgrade --install shiftpv "${ROOT_DIR}/charts/shiftpv" -n shiftpv-system \
		-f "${ROOT_DIR}/test/e2e/kind/values.yaml" --wait --timeout=5m "$@"
	kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=5m
	kubectl -n shiftpv-system rollout status daemonset/shiftpv-node --timeout=5m
}
assert_preserved() {
	kubectl get "shiftpvpool/${pool}" -o json | tee "${evidence}/$1.json" | jq -e --arg uid "${uid}" --argjson spec "$(jq -c '.spec' "${evidence}/original.json")" '
	 .metadata.uid==$uid and (.spec | del(.scanEpoch))==$spec and .metadata.deletionTimestamp!=null and
	 (.metadata.finalizers | index("shiftpv.io/pool-protection")!=null) and
	 .metadata.annotations["shiftpv.io/registration-retired"]==$uid and
	 all(.status.conditions[]?; .type!="IdentityReleased" or .status!="True")' >/dev/null
	test "$(docker exec "${node}" sha256sum "${backup}/.shiftpv/pool.json")" = "${marker}"
}

# Initialize the real Pool identity through CSI, then settle its exact Volume.
kubectl create namespace "${namespace}"
kubectl apply -f - <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: warmup, namespace: ${namespace}}
spec:
  storageClassName: shiftpv
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 1Mi}}
---
apiVersion: v1
kind: Pod
metadata: {name: warmup, namespace: ${namespace}}
spec:
  nodeSelector: {kubernetes.io/hostname: ${node}}
  terminationGracePeriodSeconds: 1
  containers:
    - name: writer
      image: busybox:1.37
      command: [sh, -ec, "echo warmup > /data/payload; sleep 3600"]
      volumeMounts: [{name: data, mountPath: /data}]
  volumes: [{name: data, persistentVolumeClaim: {claimName: warmup}}]
YAML
kubectl -n "${namespace}" wait pod/warmup --for=condition=Ready --timeout=3m
pv=$(kubectl -n "${namespace}" get pvc/warmup -o jsonpath='{.spec.volumeName}')
volume=$(kubectl get "pv/${pv}" -o jsonpath='{.spec.csi.volumeHandle}')
test "$(kubectl get "shiftpvvolume/${volume}" -o jsonpath='{.status.currentCopy.poolUID}')" = "${uid}"
kubectl delete namespace "${namespace}" --wait=true --timeout=3m
kubectl wait "pv/${pv}" "shiftpvvolume/${volume}" --for=delete --timeout=3m

# Only this isolated Kind node is mounted. Keep the original marker readable
# through a second bind; an empty read-only overmount makes backing unavailable.
docker exec "${node}" sh -ec 'test ! -e "$2"; mkdir "$2"; mount --bind "$1" "$2"' sh "${root}" "${backup}"
backed=1
marker=$(docker exec "${node}" sha256sum "${backup}/.shiftpv/pool.json")
docker exec "${node}" mount -t tmpfs -o ro,size=1m shiftpv-preserved-uninstall "${root}"
masked=1
docker exec "${node}" test ! -e "${root}/.shiftpv/pool.json"
if docker exec "${node}" touch "${root}/.write-probe"; then echo 'fault Pool remained writable' >&2; exit 1; fi
kubectl wait "shiftpvpool/${pool}" --for=jsonpath='{.status.conditions[?(@.type=="Writable")].reason}'=ReadOnly --timeout=2m
kubectl wait "shiftpvpool/${pool}" --for=condition=Ready=false --timeout=2m
kubectl annotate "shiftpvpool/${pool}" "shiftpv.io/retire-registration=${uid}"
kubectl wait "shiftpvpool/${pool}" --for=jsonpath='{.metadata.annotations.shiftpv\.io/registration-retired}'="${uid}" --timeout=3m
kubectl delete "shiftpvpool/${pool}" --wait=false
kubectl wait "shiftpvpool/${pool}" --for=jsonpath='{.status.conditions[?(@.type=="IdentityReleased")].reason}'=PoolStorageRestorationRequired --timeout=2m
kubectl get "shiftpvpool/${pool}" -o json >"${evidence}/readonly-release-blocked.json"
assert_preserved before-uninstall
if helm uninstall shiftpv -n shiftpv-system --wait --timeout=2m; then echo 'default uninstall accepted lost backing' >&2; exit 1; fi
kubectl -n shiftpv-system logs job/shiftpv-uninstall-guard | tee "${evidence}/default-denied.log" | grep -F 'ShiftPV uninstall denied'
kubectl -n shiftpv-system wait configmap/shiftpv-uninstall-permit --for=delete --timeout=30s
assert_preserved after-denial

# Recover the failed-uninstall Helm state with the same deployed chart revision.
helm rollback shiftpv "${revision}" -n shiftpv-system --wait --timeout=5m
test "$(helm status shiftpv -n shiftpv-system -o json | jq -r '.info.status')" = deployed
helm_apply --set-json "lifecycle.preserveRetiredPoolUIDs=[\"${uid}\"]"
helm uninstall shiftpv -n shiftpv-system --wait --timeout=2m
assert_shiftpv_release_removed
assert_preserved driver-removed
helm_apply
wait_for_lifecycle_webhook
assert_preserved reinstalled

# A new UID at the retired path cannot become approved or adopt its marker.
jq --arg name "${candidate}" '.metadata.name=$name' "${evidence}/original.json" | kubectl create -f -
kubectl wait "shiftpvpool/${candidate}" --for=jsonpath='{.status.conditions[?(@.type=="CapacityIndependent")].reason}'=CapacityPathsOverlap --timeout=2m
kubectl get "shiftpvpool/${candidate}" -o json | tee "${evidence}/overlap-rejected.json" | jq -e --arg uid "${uid}" '
 .metadata.uid!=$uid and .status.registrationApproved!=true and
 any(.status.conditions[]; .type=="Ready" and .status=="False")' >/dev/null
assert_preserved overlap-rejected
kubectl wait "shiftpvpool/${candidate}" --for=jsonpath='{.metadata.finalizers[0]}'=shiftpv.io/pool-protection --timeout=2m
kubectl delete "shiftpvpool/${candidate}" --wait=false

# Restoring the original backing permits the existing exact identity-release
# protocol to remove both empty registrations. No finalizer/status is forged.
docker exec "${node}" umount "${root}"
masked=0
kubectl wait "shiftpvpool/${pool}" "shiftpvpool/${candidate}" --for=delete --timeout=3m
docker exec "${node}" test ! -e "${root}/.shiftpv/pool.json"
kubectl create -f "${evidence}/original.json"
kubectl wait "shiftpvpool/${pool}" --for=condition=Ready --timeout=2m
test "$(kubectl get "shiftpvpool/${pool}" -o jsonpath='{.metadata.uid}')" != "${uid}"
kubectl get "shiftpvpool/${pool}" -o json >"${evidence}/reregistered.json"
echo 'Preserved retired Pool passed: default denial, rollback, guarded uninstall, exact UID/finalizer/marker preservation, path fencing and normal restored release'
