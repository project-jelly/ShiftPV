#!/usr/bin/env bash
set -euo pipefail
: "${CLUSTER_NAME:?explicit disposable cluster name required}"
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# shellcheck source=test/e2e/kind/lib/cluster.sh
source "${ROOT_DIR}/test/e2e/kind/lib/cluster.sh"
cd "${ROOT_DIR}"
EVIDENCE="${ROOT_DIR}/.tmp/boundary-regressions/native"
mkdir -p "${EVIDENCE}"
WORK_DIR=$(mktemp -d)
export KUBECONFIG="${WORK_DIR}/kubeconfig"
created=0
finish() {
 result=$?
 trap - EXIT
 if [[ "${created}" == 1 ]]; then
  kubectl --request-timeout=8s get pods -A -o json >"${EVIDENCE}/pods.json" 2>/dev/null || true
  kubectl --request-timeout=8s get events -A -o json >"${EVIDENCE}/events.json" 2>/dev/null || true
  kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
 fi
 rm -r -- "${WORK_DIR}"
 exit "${result}"
}
trap finish EXIT
if kind get clusters | grep -Fxq "${CLUSTER_NAME}"; then
 echo 'disposable cluster name already exists' >&2
 exit 1
fi
cat >"${WORK_DIR}/kind.yaml" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
YAML
created=1
kind create cluster --name "${CLUSTER_NAME}" --image "$(kind_node_image)" \
 --config "${WORK_DIR}/kind.yaml" --kubeconfig "${KUBECONFIG}" --wait 120s
test "$(kubectl config current-context)" = "kind-${CLUSTER_NAME}"

# Exercise stale cancellation against actual API UID/resourceVersion semantics.
SHIFT_PV_REPRO_NATIVE=1 go test -count=1 -v \
 -run '^TestReportNativeStaleCancelPreservesNewerAttempt$' ./src/lifecycle/uninstall \
 >"${EVIDENCE}/native-cancel.log" 2>&1
echo '{"case":"old-cancel-new-attempt","passed":true}' >"${EVIDENCE}/cancel-result.json"

node="${CLUSTER_NAME}-worker"
cpu=$(kubectl get "node/${node}" -o jsonpath='{.status.allocatable.cpu}')
request=$(python3 - "${cpu}" <<'PY'
import sys
value=sys.argv[1]
milli=int(value[:-1]) if value.endswith('m') else int(float(value)*1000)
assert milli>=1000, 'fixture requires at least one allocatable CPU'
print(str(milli*3//5)+'m')
PY
)
docker pull busybox:1.37
docker image inspect busybox:1.37 --format '{{json .RepoDigests}}' >"${EVIDENCE}/busybox-digests.json"
kind load docker-image busybox:1.37 --name "${CLUSTER_NAME}"

# Export the exact PodSpec produced by ensurePlacement after approval. The Go
# fixture removes only nonexistent owner metadata for this scheduler regression.
SHIFT_PV_REPRO_NODE="${node}" SHIFT_PV_REPRO_CPU="${request}" \
 SHIFT_PV_REPRO_PLACEMENT_OUTPUT="${EVIDENCE}/placement.json" \
 go test -count=1 -v -run '^TestReportRecreatedPlacementRemainsNonPreempting$' ./src/mobility/controller \
 >"${EVIDENCE}/placement-export.log" 2>&1
test -s "${EVIDENCE}/placement.json"
kubectl create namespace system
kubectl apply -f - <<YAML
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: victim-low
value: 100
preemptionPolicy: Never
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: workload-high
value: 1000
preemptionPolicy: PreemptLowerPriority
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: dedicated-placement
value: 0
preemptionPolicy: Never
---
apiVersion: v1
kind: Pod
metadata:
  name: victim
  namespace: system
spec:
  priorityClassName: victim-low
  terminationGracePeriodSeconds: 1
  nodeSelector:
    kubernetes.io/hostname: ${node}
  containers:
    - name: victim
      image: busybox:1.37
      command: [sh, -ec, "sleep 3600"]
      resources:
        requests:
          cpu: ${request}
YAML
kubectl -n system wait pod/victim --for=condition=Ready --timeout=120s
kubectl -n system get pod/victim -o json >"${EVIDENCE}/victim-before.json"
kubectl create -f "${EVIDENCE}/placement.json"
placement=$(jq -er '.metadata.name' "${EVIDENCE}/placement.json")
# A full node must keep the reservation Pending without evicting the original
# workload. Once the fixture frees space, the same reservation must run.
kubectl -n system wait "pod/${placement}" --for=jsonpath='{.status.conditions[?(@.type=="PodScheduled")].reason}'=Unschedulable --timeout=60s
kubectl -n system get "pod/${placement}" -o json >"${EVIDENCE}/placement-pending.json"
jq -e '.spec.priorityClassName == "dedicated-placement" and .spec.priority == 0 and .spec.preemptionPolicy == "Never" and (.spec.nodeName // "") == ""' "${EVIDENCE}/placement-pending.json"
kubectl -n system get pod/victim -o json >"${EVIDENCE}/victim-after.json"
jq -e --slurpfile before "${EVIDENCE}/victim-before.json" '.metadata.uid == $before[0].metadata.uid and .metadata.deletionTimestamp == null and any(.status.conditions[]; .type == "Ready" and .status == "True")' "${EVIDENCE}/victim-after.json"
kubectl -n system get events -o json >"${EVIDENCE}/events-before-space-release.json"
jq -e --slurpfile before "${EVIDENCE}/victim-before.json" '[.items[] | select(.involvedObject.uid == $before[0].metadata.uid and .reason == "Preempted")] | length == 0' "${EVIDENCE}/events-before-space-release.json"
kubectl -n system delete pod/victim --wait=true
kubectl -n system wait "pod/${placement}" --for=condition=Ready --timeout=120s
kubectl -n system get "pod/${placement}" -o json >"${EVIDENCE}/placement-observed.json"
jq -e --slurpfile before "${EVIDENCE}/placement-pending.json" '.metadata.uid == $before[0].metadata.uid and .spec.priorityClassName == "dedicated-placement" and .spec.preemptionPolicy == "Never"' "${EVIDENCE}/placement-observed.json"
echo '{"case":"approved-placement-recreation","passed":true,"victimPreserved":true,"scheduledAfterSpaceReleased":true}' >"${EVIDENCE}/placement-result.json"
cat "${EVIDENCE}/cancel-result.json" "${EVIDENCE}/placement-result.json"
