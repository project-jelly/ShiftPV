#!/usr/bin/env bash
set -euo pipefail

: "${CLUSTER_NAME:?CLUSTER_NAME is required}"
: "${WORK_DIR:?WORK_DIR is required}"
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
NODE_A="${CLUSTER_NAME}-worker"
NODE_B="${CLUSTER_NAME}-worker2"
CLASS=shiftpv-capacity-tracking
GROUP_CLASS=shiftpv-capacity-group
GROUP_POOL=capacity-group-b
WORKLOADS=(capacity-fill capacity-soft capacity-hard capacity-group)

configure_tracking() {
	helm upgrade shiftpv "${ROOT_DIR}/charts/shiftpv" -n shiftpv-system \
		--reuse-values --set "capacityTracking.enabled=$1" \
		--set capacityTracking.pollInterval=2s --wait --timeout=5m
}

wait_capacity() {
	local class=$1 node=$2 expected=$3 attempt
	for ((attempt = 0; attempt < 90; attempt++)); do
		if kubectl -n shiftpv-system get csistoragecapacities -o json | jq -e \
			--arg class "${class}" --arg node "${node}" --arg expected "${expected}" '
			[.items[] | select(.storageClassName == $class and
			 .nodeTopology.matchLabels["topology.csi.shiftpv.io/node"] == $node)] |
			length == 1 and (.[0].capacity == $expected and .[0].maximumVolumeSize == $expected)' >/dev/null; then
			return
		fi
		sleep 1
	done
	kubectl -n shiftpv-system get csistoragecapacities -o yaml >&2
	echo "capacity not published: class=${class} node=${node} expected=${expected}" >&2
	return 1
}

create_class() {
	local name=$1 group=$2
	kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ${name}
provisioner: csi.shiftpv.io
parameters:
  shiftpv.io/pool-group: ${group}
  csi.storage.k8s.io/fstype: ext4
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
EOF
}

create_workload() {
	local name=$1 class=$2 size=$3 placement=$4 node=$5 scheduling
	if [[ "${placement}" == hard ]]; then
		scheduling="  nodeSelector:
    kubernetes.io/hostname: ${node}"
	else
		scheduling="  affinity:
    nodeAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          preference:
            matchExpressions:
              - key: kubernetes.io/hostname
                operator: In
                values: [${node}]"
	fi
	kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${name}
spec:
  storageClassName: ${class}
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: ${size}
---
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
spec:
${scheduling}
  terminationGracePeriodSeconds: 1
  containers:
    - name: consumer
      image: busybox:1.37
      command: [sh, -c, 'echo capacity-tracking > /data/marker; sleep 3600']
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${name}
EOF
}

assert_bound() {
	local name=$1 node=$2
	kubectl wait --for=jsonpath='{.status.phase}'=Bound "pvc/${name}" --timeout=2m
	kubectl wait --for=condition=Ready "pod/${name}" --timeout=3m
	test "$(kubectl get pod "${name}" -o jsonpath='{.spec.nodeName}')" = "${node}"
	test "$(kubectl exec "${name}" -- cat /data/marker)" = capacity-tracking
}

delete_workload() {
	local name=$1 pv
	pv=$(kubectl get pvc "${name}" --ignore-not-found -o jsonpath='{.spec.volumeName}')
	kubectl delete pod "${name}" --ignore-not-found --wait=true --timeout=60s
	kubectl delete pvc "${name}" --ignore-not-found --wait=true --timeout=60s
	if [[ -n "${pv}" ]]; then
		kubectl wait --for=delete "pv/${pv}" --timeout=2m
	fi
}

cleanup() {
	local name
	for name in "${WORKLOADS[@]}"; do
		delete_workload "${name}" >/dev/null 2>&1 || true
	done
	kubectl delete storageclass "${CLASS}" "${GROUP_CLASS}" --ignore-not-found >/dev/null 2>&1 || true
	kubectl delete shiftpvpool "${GROUP_POOL}" --ignore-not-found --wait=true --timeout=2m >/dev/null 2>&1 || true
	kubectl apply -f "${WORK_DIR}/pools.yaml" >/dev/null 2>&1 || true
	configure_tracking false >/dev/null 2>&1 || true
}
trap cleanup EXIT

kubectl patch shiftpvpool worker-a --type=merge -p '{"spec":{"capacity":{"limit":"128Mi"}}}'
kubectl patch shiftpvpool worker-b --type=merge -p '{"spec":{"capacity":{"limit":"128Mi"}}}'
kubectl wait --for=condition=Ready shiftpvpool/worker-a shiftpvpool/worker-b --timeout=2m
create_class "${CLASS}" default
configure_tracking true
test "$(kubectl get csidriver csi.shiftpv.io -o jsonpath='{.spec.storageCapacity}')" = true
wait_capacity "${CLASS}" "${NODE_A}" 128Mi
wait_capacity "${CLASS}" "${NODE_B}" 128Mi

# Deployment ownership survives ReplicaSet replacement and allows uninstall GC.
DEPLOYMENT_UID=$(kubectl -n shiftpv-system get deployment shiftpv-controller -o jsonpath='{.metadata.uid}')
kubectl -n shiftpv-system get csistoragecapacities -l csi.storage.k8s.io/drivername=csi.shiftpv.io -o json |
	jq -e --arg uid "${DEPLOYMENT_UID}" '.items | length > 0 and all(.[];
		any(.metadata.ownerReferences[]; .kind == "Deployment" and .uid == $uid))' >/dev/null

# Real scheduler: a full preferred node must lose to an eligible free node.
create_workload capacity-fill "${CLASS}" 128Mi hard "${NODE_A}"
assert_bound capacity-fill "${NODE_A}"
wait_capacity "${CLASS}" "${NODE_A}" 0
create_workload capacity-soft "${CLASS}" 16Mi soft "${NODE_A}"
assert_bound capacity-soft "${NODE_B}"
wait_capacity "${CLASS}" "${NODE_B}" 112Mi

create_workload capacity-hard "${CLASS}" 16Mi hard "${NODE_A}"
kubectl wait --for=jsonpath='{.status.conditions[?(@.type=="PodScheduled")].reason}'=Unschedulable \
	pod/capacity-hard --timeout=60s
kubectl get pod capacity-hard -o json | jq -e '
	.status.conditions[] | select(.type == "PodScheduled") |
	.message | contains("did not have enough free storage")' >/dev/null
test "$(kubectl get pvc capacity-hard -o jsonpath='{.status.phase}')" = Pending
test -z "$(kubectl get pvc capacity-hard -o jsonpath='{.metadata.annotations.volume\.kubernetes\.io/selected-node}')"
delete_workload capacity-fill
assert_bound capacity-hard "${NODE_A}"
wait_capacity "${CLASS}" "${NODE_A}" 112Mi

# Polling survives controller restart without duplicate node/class objects.
kubectl -n shiftpv-system rollout restart deployment/shiftpv-controller
kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=3m
wait_capacity "${CLASS}" "${NODE_A}" 112Mi
wait_capacity "${CLASS}" "${NODE_B}" 112Mi
delete_workload capacity-soft
delete_workload capacity-hard

# Register a group on B only; poolGroup is immutable, so use a new registration.
kubectl delete shiftpvpool worker-b --wait=true --timeout=2m
kubectl apply -f - <<EOF
apiVersion: shiftpv.io/v1alpha1
kind: ShiftPVPool
metadata:
  name: ${GROUP_POOL}
spec:
  nodeName: ${NODE_B}
  mountPath: /srv/shiftpv-b
  poolGroup: capacity-group
  capacity:
    limit: 128Mi
EOF
kubectl wait --for=condition=Ready "shiftpvpool/${GROUP_POOL}" --timeout=2m
create_class "${GROUP_CLASS}" capacity-group
wait_capacity "${GROUP_CLASS}" "${NODE_A}" 0
wait_capacity "${GROUP_CLASS}" "${NODE_B}" 128Mi
create_workload capacity-group "${GROUP_CLASS}" 16Mi soft "${NODE_A}"
assert_bound capacity-group "${NODE_B}"
delete_workload capacity-group

# Removing the last registration for a group must withdraw its positive value.
kubectl delete shiftpvpool "${GROUP_POOL}" --wait=true --timeout=2m
wait_capacity "${GROUP_CLASS}" "${NODE_B}" 0
kubectl delete storageclass "${CLASS}" "${GROUP_CLASS}" --wait=true
kubectl apply -f "${WORK_DIR}/pools.yaml"
kubectl wait --for=condition=Ready shiftpvpool/worker-a shiftpvpool/worker-b --timeout=2m
configure_tracking false
kubectl get shiftpvvolumes -o json | jq -e '.items | length == 0' >/dev/null
trap - EXIT
test "$(kubectl get csidriver csi.shiftpv.io -o jsonpath='{.spec.storageCapacity}')" = false
echo "ShiftPV CSIStorageCapacity scheduling, recovery and rollback passed"
