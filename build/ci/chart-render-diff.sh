#!/usr/bin/env bash
# Prove that a chart refactor is render-neutral: template charts/shiftpv at a
# base ref and at the working tree for every values combination the test suite
# and the known downstream installs use, then fail on any byte difference.
# A chart release normalizes only its version label and default runtime tags.
#
# Usage: build/ci/chart-render-diff.sh [base-ref]            (default: origin/main)
# Extra values files (for example a downstream GitOps values file kept outside
# this repository) can be appended as combinations:
#   CHART_RENDER_DIFF_EXTRA_VALUES=/path/a.yaml:/path/b.yaml build/ci/chart-render-diff.sh
set -euo pipefail

BASE_REF=${1:-origin/main}
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
CHART_PATH=charts/shiftpv

WORK_DIR=$(mktemp -d)
trap 'rm -rf "${WORK_DIR}"' EXIT

mkdir -p "${WORK_DIR}/base"
git -C "${ROOT_DIR}" archive "${BASE_REF}" "${CHART_PATH}" | tar -x -C "${WORK_DIR}/base"

BASE_CHART="${WORK_DIR}/base/${CHART_PATH}"
HEAD_CHART="${ROOT_DIR}/${CHART_PATH}"

# --version selects remote chart packages; it does not replace local metadata.
# Render a temporary copy with the base metadata for an intentional release.
BASE_VERSION=$(awk '/^version:/ {print $2}' "${BASE_CHART}/Chart.yaml")
HEAD_VERSION=$(awk '/^version:/ {print $2}' "${HEAD_CHART}/Chart.yaml")
RELEASE_OVERRIDES=()
if [[ ${BASE_VERSION} != "${HEAD_VERSION}" ]]; then
	mkdir -p "${WORK_DIR}/head"
	cp -R "${HEAD_CHART}" "${WORK_DIR}/head/shiftpv"
	HEAD_CHART="${WORK_DIR}/head/shiftpv"
	sed -i.bak "s/^version:.*/version: ${BASE_VERSION}/" "${HEAD_CHART}/Chart.yaml"
	rm "${HEAD_CHART}/Chart.yaml.bak"
	for component in controller node; do
		tag=$(awk -v section="${component}:" '$0 == section { found = 1 } found && /^    tag:/ { print $2; exit }' "${BASE_CHART}/values.yaml")
		[[ -n ${tag} ]] || { echo "missing base ${component} image tag" >&2; exit 1; }
		RELEASE_OVERRIDES+=(--set-string "${component}.image.tag=${tag}")
	done
fi

# Every values combination exercised by test/helm/*.go, test/e2e/kind/**/*.sh,
# and the default install. One line per combination: a label, then helm args.
COMBINATIONS=(
	"default"
	"kind e2e values|--values|${ROOT_DIR}/test/e2e/kind/values.yaml"
	"kind e2e default class off|--values|${ROOT_DIR}/test/e2e/kind/values.yaml|--set|storageClass.defaultClass=false"
	"kind e2e mobility overrides|--values|${ROOT_DIR}/test/e2e/kind/values.yaml|--set|helperPod.image=shiftpv-mobility-fault:dev,mobility.helperImage=shiftpv-mobility-fault:dev,mobility.interval=10s"
	"kind e2e mobility disabled|--values|${ROOT_DIR}/test/e2e/kind/values.yaml|--set|mobility.enabled=false"
	"published release wiring|--set|storageClass.defaultClass=true|--set-string|controller.image.repository=ghcr.io/project-jelly/shiftpv-controller|--set-string|controller.image.tag=0.4.1|--set-string|controller.image.pullPolicy=IfNotPresent|--set-string|node.image.repository=ghcr.io/project-jelly/shiftpv-node|--set-string|node.image.tag=0.4.1|--set-string|node.image.pullPolicy=IfNotPresent|--set-string|mobility.helperImage=ghcr.io/project-jelly/shiftpv-controller:0.4.1"
	"upgrade readiness args|--set|poolReadiness.interval=2s|--set|poolReadiness.staleAfter=10s|--set|storageClass.defaultClass=true"
	"replica fence rejects two|--set|controller.replicas=2"
	"overridden images|--set|controller.image.repository=controller,controller.image.tag=test,node.image.repository=node,node.image.tag=test,helperPod.image=controller:test,mobility.helperImage=helper:test"
	"separate measurement and move helper images|--set|controller.image.repository=controller,controller.image.tag=test,helperPod.image=create-helper:test,mobility.helperImage=move-helper:test"
	"external helper service account|--set|serviceAccount.helper.create=false,serviceAccount.helper.name=existing-helper"
	"external controller and node service accounts|--set|serviceAccount.controller.create=false,serviceAccount.controller.name=ext-controller,serviceAccount.node.create=false,serviceAccount.node.name=ext-node"
	"argocd uninstall mode|--set|lifecycle.uninstallMode=argocd"
	"invalid uninstall mode|--set|lifecycle.uninstallMode=flux"
	"mobility disabled|--set|mobility.enabled=false"
	"microk8s kubelet root|--set|node.kubeletRootDir=/var/snap/microk8s/common/var/lib/kubelet"
	"journal retention hours|--set|mobility.journalRetention=1h"
	"journal retention zero|--set|mobility.journalRetention=0h"
	"journal retention minutes|--set|mobility.journalRetention=30m"
	"metrics enabled|--set|metrics.enabled=true"
	"service monitor|--set|metrics.enabled=true,metrics.serviceMonitor.enabled=true,metrics.serviceMonitor.additionalLabels.release=kube-prometheus-stack|--api-versions|monitoring.coreos.com/v1/ServiceMonitor"
	"prometheus rule|--set|metrics.enabled=true,metrics.prometheusRule.enabled=true,metrics.prometheusRule.additionalLabels.release=kube-prometheus-stack|--api-versions|monitoring.coreos.com/v1/PrometheusRule"
	"service monitor without metrics|--set|metrics.serviceMonitor.enabled=true"
	"prometheus rule without metrics|--set|metrics.prometheusRule.enabled=true|--api-versions|monitoring.coreos.com/v1/PrometheusRule"
	"service monitor without CRD|--set|metrics.enabled=true,metrics.serviceMonitor.enabled=true"
	"prometheus rule without CRD|--set|metrics.enabled=true,metrics.prometheusRule.enabled=true"
	"webhook port collision|--set|metrics.enabled=true,metrics.port=9443"
	"health port collision|--set|metrics.port=9808"
	"invalid snapshot interval|--set|metrics.snapshotInterval=0s"
	"unknown metrics setting|--set|metrics.enabeld=true"
	"scrape timeout exceeds interval|--set|metrics.enabled=true,metrics.serviceMonitor.enabled=true,metrics.serviceMonitor.scrapeTimeout=40s|--api-versions|monitoring.coreos.com/v1/ServiceMonitor"
	"dashboard|--set|metrics.dashboard.enabled=true,metrics.dashboard.labels.team=storage"
	"storage class reclaim swap|--set|storageClass.reclaimPolicy=Retain"
	"retain class reclaim swap|--set|retainStorageClass.reclaimPolicy=Delete"
	"retain class default|--set|retainStorageClass.defaultClass=true"
	"duplicate storage class names|--set|retainStorageClass.name=shiftpv"
	"storage classes disabled|--set|storageClass.create=false,retainStorageClass.create=false"
	"image pull secrets|--set|imagePullSecrets[0].name=regcred"
	"name and fullname overrides|--set|nameOverride=spv,fullnameOverride=custom-shiftpv"
	"workload placement and resources|--set-string|node.nodeSelector.shiftpv\\.io/storage-node=true|--set|node.tolerations[0].key=storage,node.tolerations[0].operator=Exists,controller.resources.requests.cpu=50m,controller.resources.limits.memory=256Mi,node.resources.requests.cpu=25m,sidecars.provisioner.resources.requests.cpu=25m,sidecars.registrar.resources.requests.cpu=10m,sidecars.livenessProbe.resources.requests.cpu=5m"
	"log levels|--set|controller.logLevel=5,node.logLevel=5"
	"helper pod tuning|--set|helperPod.timeout=5m,helperPod.resources.requests.cpu=20m,helperPod.resources.limits.memory=128Mi"
)

if [[ -n ${CHART_RENDER_DIFF_EXTRA_VALUES:-} ]]; then
	IFS=: read -r -a extra_values <<<"${CHART_RENDER_DIFF_EXTRA_VALUES}"
	for values_file in "${extra_values[@]}"; do
		[[ -n ${values_file} ]] || continue
		COMBINATIONS+=("extra values $(basename "${values_file}")|--values|${values_file}")
	done
fi

# helm template, capturing stdout and stderr together so a deliberate template
# failure (fail/schema rejection) is compared as text like any other render.
render() {
	local chart=$1
	shift
	helm template test "${chart}" \
		--namespace storage \
		--kube-version 1.35.8 \
		"${RELEASE_OVERRIDES[@]}" \
		"$@" 2>&1 || true
}

failures=0
count=0
for combination in "${COMBINATIONS[@]}"; do
	IFS='|' read -r -a fields <<<"${combination}"
	label=${fields[0]}
	args=("${fields[@]:1}")
	count=$((count + 1))
	base_out="${WORK_DIR}/base.${count}.yaml"
	head_out="${WORK_DIR}/head.${count}.yaml"
	render "${BASE_CHART}" "${args[@]}" >"${base_out}"
	render "${HEAD_CHART}" "${args[@]}" >"${head_out}"
	# Paths appear in helm's own error text; normalise them out.
	sed -i.bak "s#${BASE_CHART}#CHART#g" "${base_out}" && rm -f "${base_out}.bak"
	sed -i.bak "s#${HEAD_CHART}#CHART#g" "${head_out}" && rm -f "${head_out}.bak"
	if cmp -s "${base_out}" "${head_out}"; then
		printf 'ok   %s\n' "${label}"
	else
		failures=$((failures + 1))
		printf 'DIFF %s\n' "${label}"
		diff -u "${base_out}" "${head_out}" || true
	fi
done

printf '\n%d combinations, %d diffs (base %s)\n' "${count}" "${failures}" "${BASE_REF}"
if [[ ${BASE_VERSION} != "${HEAD_VERSION}" ]]; then
	printf 'chart version changed: %s -> %s\n' "${BASE_VERSION}" "${HEAD_VERSION}"
fi
[[ ${failures} -eq 0 ]]
