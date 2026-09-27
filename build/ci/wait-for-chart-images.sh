#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
registry=${IMAGE_REGISTRY:-ghcr.io}
owner=${IMAGE_OWNER:-${GITHUB_REPOSITORY_OWNER:-project-jelly}}
attempts=${IMAGE_WAIT_ATTEMPTS:-240}
delay=${IMAGE_WAIT_DELAY_SECONDS:-10}

for command in docker helm jq; do
	command -v "${command}" >/dev/null || {
		echo "required command not found: ${command}" >&2
		exit 1
	}
done

if ! [[ "${attempts}" =~ ^[1-9][0-9]*$ && "${delay}" =~ ^[0-9]+$ ]]; then
	echo "IMAGE_WAIT_ATTEMPTS must be positive and IMAGE_WAIT_DELAY_SECONDS must be non-negative" >&2
	exit 1
fi

expected_platforms=$'linux/amd64\nlinux/arm64'
chart_component_version() {
	local component=$1
	awk -v component="${component}" '
		$0 ~ "^" component ":" { in_component = 1; next }
		in_component && $0 ~ /^[^[:space:]]/ { in_component = 0 }
		in_component && $1 == "tag:" { gsub(/"/, "", $2); print $2; exit }
	' "${repo_root}/charts/shiftpv/values.yaml"
}

controller_version=${SHIFTPV_CONTROLLER_VERSION:-$(chart_component_version controller)}
node_version=${SHIFTPV_NODE_VERSION:-$(chart_component_version node)}
controller_image="${registry}/${owner,,}/shiftpv-controller:${controller_version}"
node_image="${registry}/${owner,,}/shiftpv-node:${node_version}"
rendered=$(helm template shiftpv "${repo_root}/charts/shiftpv" --namespace shiftpv-system --kube-version 1.35.8)

if ! grep -Fq "image: \"${controller_image}\"" <<<"${rendered}" ||
	! grep -Fq "image: \"${node_image}\"" <<<"${rendered}" ||
	! grep -Fq -- "--helper-image=${controller_image}" <<<"${rendered}" ||
	! grep -Fq -- "--mobility-helper-image=${controller_image}" <<<"${rendered}"; then
	echo "::error::chart defaults do not consistently reference their pinned component images" >&2
	exit 1
fi

for component in controller node; do
	version=$(<"${repo_root}/versions/${component}")
	image="${registry}/${owner,,}/shiftpv-${component}:${version}"
	available=false

	for ((attempt = 1; attempt <= attempts; attempt++)); do
		if manifest=$(docker buildx imagetools inspect --raw "${image}" 2>/dev/null); then
			if platforms=$(jq -r '.manifests[]? | select(.platform.os != null and .platform.architecture != null and .platform.os != "unknown") | "\(.platform.os)/\(.platform.architecture)"' <<<"${manifest}" 2>/dev/null | sort -u) &&
				[[ "${platforms}" == "${expected_platforms}" ]]; then
				echo "chart image ready: ${image} (${platforms//$'\n'/, })"
				available=true
				break
			fi
		fi

		if ((attempt < attempts)); then
			echo "waiting for chart image ${image} (${attempt}/${attempts})"
			sleep "${delay}"
		fi
	done

	if [[ "${available}" != true ]]; then
		echo "::error::chart image ${image} is unavailable or lacks linux/amd64 and linux/arm64" >&2
		exit 1
	fi
done
