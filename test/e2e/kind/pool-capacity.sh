#!/usr/bin/env bash
set -euo pipefail

: "${CLUSTER_NAME:?CLUSTER_NAME is required}"

CAPACITY_NODE="${CLUSTER_NAME}-worker"
CAPACITY_POOL=worker-a
CAPACITY_PATH=/mnt/shiftpv
PHYSICAL_NAME=shiftpv-capacity-physical
LOGICAL_NAME=shiftpv-capacity-logical
FIXED_NAME=shiftpv-capacity-fixed
PHYSICAL_PV=
LOGICAL_PV=
FIXED_PV=

volume_hold_count() {
	kubectl get shiftpvvolumes -o name | wc -l | tr -d ' '
}

wait_for_volume_hold_count() {
	local expected=$1
	local attempt
	for ((attempt = 0; attempt < 120; attempt++)); do
		if [[ "$(volume_hold_count)" == "${expected}" ]]; then
			return
		fi
		sleep 1
	done
	echo "ShiftPVVolume capacity hold count did not become ${expected}" >&2
	return 1
}

wait_for_resource_exhausted() {
	local name=$1
	local attempt messages
	for ((attempt = 0; attempt < 120; attempt++)); do
		messages=$(kubectl get events \
			--field-selector "involvedObject.kind=PersistentVolumeClaim,involvedObject.name=${name}" \
			-o jsonpath='{range .items[*]}{.message}{"\n"}{end}' 2>/dev/null || true)
		if grep -Fq 'code = ResourceExhausted' <<<"${messages}"; then
			return
		fi
		sleep 1
	done
	echo "PersistentVolumeClaim/${name} did not report ResourceExhausted" >&2
	return 1
}

wait_for_unavailable() {
	local name=$1
	local attempt messages
	for ((attempt = 0; attempt < 120; attempt++)); do
		messages=$(kubectl get events \
			--field-selector "involvedObject.kind=PersistentVolumeClaim,involvedObject.name=${name}" \
			-o jsonpath='{range .items[*]}{.message}{"\n"}{end}' 2>/dev/null || true)
		if grep -Fq 'code = Unavailable' <<<"${messages}"; then
			return
		fi
		sleep 1
	done
	echo "PersistentVolumeClaim/${name} did not report retryable Unavailable" >&2
	return 1
}

create_workload() {
	local name=$1
	local size=$2
	kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${name}
spec:
  storageClassName: shiftpv-capacity-test
  accessModes: [ReadWriteOnce]
  volumeMode: Filesystem
  resources:
    requests:
      storage: ${size}
---
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
spec:
  nodeSelector:
    kubernetes.io/hostname: ${CAPACITY_NODE}
  terminationGracePeriodSeconds: 1
  containers:
    - name: workload
      image: busybox:1.37
      command: ["sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${name}
EOF
}

# Model a CDI scratch claim: the importer Pod already has a node and owns the
# PVC, while CDI copies the selected-node annotation when it creates the PVC.
create_fixed_consumer() {
	kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${FIXED_NAME}
spec:
  nodeName: ${CAPACITY_NODE}
  terminationGracePeriodSeconds: 1
  containers:
    - name: importer
      image: busybox:1.37
      command: ["sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: scratch
          mountPath: /scratch
  volumes:
    - name: scratch
      persistentVolumeClaim:
        claimName: ${FIXED_NAME}
EOF
	local pod_uid
	pod_uid=$(kubectl get pod "${FIXED_NAME}" -o jsonpath='{.metadata.uid}')
	kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${FIXED_NAME}
  annotations:
    volume.kubernetes.io/selected-node: ${CAPACITY_NODE}
  ownerReferences:
    - apiVersion: v1
      kind: Pod
      name: ${FIXED_NAME}
      uid: ${pod_uid}
spec:
  storageClassName: shiftpv-capacity-test
  accessModes: [ReadWriteOnce]
  volumeMode: Filesystem
  resources:
    requests:
      storage: 64Mi
EOF
}

cleanup_capacity_test() {
	kubectl delete pod "${PHYSICAL_NAME}" "${LOGICAL_NAME}" "${FIXED_NAME}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
	kubectl delete pvc "${PHYSICAL_NAME}" "${LOGICAL_NAME}" "${FIXED_NAME}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
	if [[ -n "${PHYSICAL_PV}" ]]; then
		kubectl wait --for=delete "pv/${PHYSICAL_PV}" --timeout=2m >/dev/null 2>&1 || true
	fi
	if [[ -n "${LOGICAL_PV}" ]]; then
		kubectl wait --for=delete "pv/${LOGICAL_PV}" --timeout=2m >/dev/null 2>&1 || true
	fi
	if [[ -n "${FIXED_PV}" ]]; then
		kubectl wait --for=delete "pv/${FIXED_PV}" --timeout=2m >/dev/null 2>&1 || true
	fi
	kubectl delete storageclass shiftpv-capacity-test --ignore-not-found --wait=true >/dev/null 2>&1 || true
	docker exec "${CAPACITY_NODE}" sh -c "rm -f '${CAPACITY_PATH}/external-fill'; mountpoint -q '${CAPACITY_PATH}' && umount '${CAPACITY_PATH}' || true" >/dev/null 2>&1 || true
	kubectl patch shiftpvpool "${CAPACITY_POOL}" --type=merge -p '{"spec":{"capacity":{"limit":"10Gi"}}}' >/dev/null 2>&1 || true
}
trap cleanup_capacity_test EXIT

kubectl patch shiftpvpool "${CAPACITY_POOL}" --type=merge -p '{"spec":{"capacity":{"limit":"128Mi"}}}' >/dev/null
docker exec "${CAPACITY_NODE}" mount -t tmpfs -o size=128m shiftpv-capacity "${CAPACITY_PATH}"
docker exec "${CAPACITY_NODE}" sh -c "dd if=/dev/zero of='${CAPACITY_PATH}/external-fill' bs=1M count=80 >/dev/null 2>&1"

kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: shiftpv-capacity-test
provisioner: csi.shiftpv.io
reclaimPolicy: Delete
allowVolumeExpansion: false
volumeBindingMode: WaitForFirstConsumer
EOF

# statfs must see bytes consumed outside ShiftPV and reject before creating a
# Volume-owned capacity hold.
create_workload "${PHYSICAL_NAME}" 64Mi
wait_for_resource_exhausted "${PHYSICAL_NAME}"
wait_for_volume_hold_count 0

# Removing the external file makes the same pending claim converge.
docker exec "${CAPACITY_NODE}" rm -f "${CAPACITY_PATH}/external-fill"
kubectl wait --for=condition=Ready "pod/${PHYSICAL_NAME}" --timeout=5m
kubectl wait --for=jsonpath='{.status.phase}'=Bound "pvc/${PHYSICAL_NAME}" --timeout=2m
PHYSICAL_PV=$(kubectl get pvc "${PHYSICAL_NAME}" -o jsonpath='{.spec.volumeName}')
PHYSICAL_VOLUME=$(kubectl get "pv/${PHYSICAL_PV}" -o jsonpath='{.spec.csi.volumeHandle}')
PHYSICAL_PVC_UID=$(kubectl get "pvc/${PHYSICAL_NAME}" -o jsonpath='{.metadata.uid}')
wait_for_volume_hold_count 1
test "$(kubectl get "shiftpvvolume/${PHYSICAL_VOLUME}" -o jsonpath='{.spec.requestName}')" = "pvc-${PHYSICAL_PVC_UID}"
test "$(kubectl get "shiftpvvolume/${PHYSICAL_VOLUME}" -o jsonpath='{.spec.capacityBytes}')" = 67108864
test "$(kubectl get "shiftpvvolume/${PHYSICAL_VOLUME}" -o jsonpath='{.spec.initialNode}')" = "${CAPACITY_NODE}"

# An empty 64Mi PVC uses almost no bytes, but its Volume-owned hold must still
# leave only 64Mi of the Pool limit and reject a new 80Mi request.
create_workload "${LOGICAL_NAME}" 80Mi
wait_for_resource_exhausted "${LOGICAL_NAME}"
wait_for_volume_hold_count 1

# Releasing the first Volume hold allows the pending second claim to converge.
kubectl delete pod "${PHYSICAL_NAME}" --wait=true
kubectl delete pvc "${PHYSICAL_NAME}" --wait=true
kubectl wait --for=delete "pv/${PHYSICAL_PV}" --timeout=2m
PHYSICAL_PV=
wait_for_volume_hold_count 0
kubectl wait --for=condition=Ready "pod/${LOGICAL_NAME}" --timeout=5m
kubectl wait --for=jsonpath='{.status.phase}'=Bound "pvc/${LOGICAL_NAME}" --timeout=2m
LOGICAL_PV=$(kubectl get pvc "${LOGICAL_NAME}" -o jsonpath='{.spec.volumeName}')
LOGICAL_VOLUME=$(kubectl get "pv/${LOGICAL_PV}" -o jsonpath='{.spec.csi.volumeHandle}')
LOGICAL_PVC_UID=$(kubectl get "pvc/${LOGICAL_NAME}" -o jsonpath='{.metadata.uid}')
wait_for_volume_hold_count 1
test "$(kubectl get "shiftpvvolume/${LOGICAL_VOLUME}" -o jsonpath='{.spec.requestName}')" = "pvc-${LOGICAL_PVC_UID}"
test "$(kubectl get "shiftpvvolume/${LOGICAL_VOLUME}" -o jsonpath='{.spec.capacityBytes}')" = 83886080
test "$(kubectl get "shiftpvvolume/${LOGICAL_VOLUME}" -o jsonpath='{.spec.initialNode}')" = "${CAPACITY_NODE}"

# The Pod is already assigned to a node. ResourceExhausted would erase this
# PVC's selected-node annotation and leave it pending after capacity returns.
create_fixed_consumer
wait_for_unavailable "${FIXED_NAME}"
test "$(kubectl get pvc "${FIXED_NAME}" -o jsonpath='{.metadata.annotations.volume\.kubernetes\.io/selected-node}')" = "${CAPACITY_NODE}"
wait_for_volume_hold_count 1

kubectl delete pod "${LOGICAL_NAME}" --wait=true
kubectl delete pvc "${LOGICAL_NAME}" --wait=true
kubectl wait --for=delete "pv/${LOGICAL_PV}" --timeout=2m
LOGICAL_PV=
kubectl wait --for=condition=Ready "pod/${FIXED_NAME}" --timeout=5m
kubectl wait --for=jsonpath='{.status.phase}'=Bound "pvc/${FIXED_NAME}" --timeout=2m
FIXED_PV=$(kubectl get pvc "${FIXED_NAME}" -o jsonpath='{.spec.volumeName}')
FIXED_VOLUME=$(kubectl get "pv/${FIXED_PV}" -o jsonpath='{.spec.csi.volumeHandle}')
test "$(kubectl get "shiftpvvolume/${FIXED_VOLUME}" -o jsonpath='{.spec.initialNode}')" = "${CAPACITY_NODE}"
wait_for_volume_hold_count 1

kubectl delete pod "${FIXED_NAME}" --wait=true
kubectl delete pvc "${FIXED_NAME}" --ignore-not-found --wait=true
kubectl wait --for=delete "pv/${FIXED_PV}" --timeout=2m
FIXED_PV=
wait_for_volume_hold_count 0

trap - EXIT
cleanup_capacity_test
echo "ShiftPV Pool filesystem capacity admission passed"
