#!/usr/bin/env bash

# Quiesce and resume the controller so a fault can be injected at an exact
# transaction boundary. controller_down only returns once no controller Pod
# remains, so the caller never races an overlapping reconcile.
controller_down() {
	kubectl -n shiftpv-system scale deployment/shiftpv-controller --replicas=0
	local deadline=$((SECONDS + 120))
	while ((SECONDS < deadline)); do
		if [[ -z "$(kubectl -n shiftpv-system get pod -l app.kubernetes.io/component=controller -o name 2>/dev/null)" ]]; then
			return
		fi
		sleep 1
	done
	echo 'controller Pod did not stop' >&2
	return 1
}

controller_up() {
	kubectl -n shiftpv-system scale deployment/shiftpv-controller --replicas=1
	kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=180s
}

# Keep the source authoritative until a recreated held consumer is observed.
# Scope the barrier to one Volume in the disposable test cluster.
hold_owner_commit() {
	local volume=$1
	kubectl apply -f - <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: shiftpv-replacement-commit-barrier
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
    - apiGroups: [shiftpv.io]
      apiVersions: [v1alpha1]
      operations: [UPDATE]
      resources: [shiftpvvolumes/status]
  validations:
  - expression: "object.metadata.name != '${volume}' || object.status.ownerNode == oldObject.status.ownerNode"
    message: replacement recreation owner commit barrier
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: shiftpv-replacement-commit-barrier
spec:
  policyName: shiftpv-replacement-commit-barrier
  validationActions: [Deny]
EOF
}

release_owner_commit() {
	kubectl delete validatingadmissionpolicybinding/shiftpv-replacement-commit-barrier --ignore-not-found
	kubectl delete validatingadmissionpolicy/shiftpv-replacement-commit-barrier --ignore-not-found
}
