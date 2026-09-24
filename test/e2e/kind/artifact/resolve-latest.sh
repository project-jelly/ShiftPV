#!/usr/bin/env bash
set -euo pipefail

test_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=test/e2e/kind/lib/artifact.sh
source "${test_dir}/../lib/artifact.sh"
require_commands awk curl docker helm jq

work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT
# Resolving a release must not update the caller's Helm repositories.
export HELM_CONFIG_HOME="${work_dir}/helm-config"
export HELM_CACHE_HOME="${work_dir}/helm-cache"
export HELM_DATA_HOME="${work_dir}/helm-data"

"${test_dir}/validate-lock.sh" "${test_dir}/versions.env" >&2
repository=$(awk -F= '$1 == "CHART_REPOSITORY" { print $2 }' "${test_dir}/versions.env")
kind_image=$(awk -F= '$1 == "KIND_NODE_IMAGE" { print $2 }' "${test_dir}/versions.env")
helm repo add shiftpv-public "${repository}" >&2
helm repo update shiftpv-public >&2
# Helm selects the latest stable chart by default, excluding prereleases.
version=$(helm show chart shiftpv-public/shiftpv | awk '$1 == "version:" { print $2; exit }')
[[ "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
  echo "latest published chart has an invalid version: ${version}" >&2
  exit 1
}
helm pull shiftpv-public/shiftpv --version "${version}" --destination "${work_dir}" >&2
package="${work_dir}/shiftpv-${version}.tgz"
# Compare the repository package with the checksum attached to its release.
curl --fail --silent --show-error --location --retry 3 \
  "https://github.com/project-jelly/ShiftPV/releases/download/chart%2Fv${version}/shiftpv-${version}.tgz.sha256" \
  >"${work_dir}/release.sha256"
read -r expected_sha expected_file <"${work_dir}/release.sha256"
[[ "${expected_sha}" =~ ^[0-9a-f]{64}$ && "${expected_file}" == "shiftpv-${version}.tgz" ]] || {
  echo "invalid published chart checksum" >&2
  exit 1
}
assert_equal "published chart checksum" "${expected_sha}" "$(sha256_file "${package}")"
assert_equal "published chart version" "${version}" \
  "$(helm show chart "${package}" | awk '$1 == "version:" { print $2; exit }')"
helm show values "${package}" >"${work_dir}/values.yaml"

resolve_image() {
  local component=$1 image digest manifest platforms image_pattern
  image=$(awk -v component="${component}" '
    $0 == component ":" { inside = 1; next }
    inside && /^[^[:space:]]/ { inside = 0 }
    inside && $1 == "repository:" { repository = $2; gsub(/"/, "", repository) }
    inside && $1 == "tag:" { tag = $2; gsub(/"/, "", tag) }
    END { print repository ":" tag }
  ' "${work_dir}/values.yaml")
  image_pattern="^ghcr\\.io/project-jelly/shiftpv-${component}:[0-9]+\\.[0-9]+\\.[0-9]+$"
  [[ "${image}" =~ ${image_pattern} ]] || {
    echo "unexpected ${component} image in published chart: ${image}" >&2
    return 1
  }
  digest=$(docker buildx imagetools inspect "${image}" --format '{{json .Manifest}}' | jq -er '.digest')
  [[ "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]] || return 1
  # Inspect the resolved digest, so a tag change cannot mix two observations.
  manifest=$(docker buildx imagetools inspect --raw "${image}@${digest}")
  platforms=$(jq -r '.manifests[]? | select(.platform.os == "linux") | .platform.architecture' <<<"${manifest}" | sort -u)
  [[ "${platforms}" == $'amd64\narm64' ]] || {
    echo "published ${component} image lacks the supported platforms" >&2
    return 1
  }
  printf '%s@%s\n' "${image}" "${digest}"
}

controller_image=$(resolve_image controller)
node_image=$(resolve_image node)
cat >"${work_dir}/resolved.env" <<EOF
CHART_REPOSITORY=${repository}
CHART_VERSION=${version}
CHART_SHA256=${expected_sha}
CONTROLLER_IMAGE=${controller_image}
NODE_IMAGE=${node_image}
KIND_NODE_IMAGE=${kind_image}
EOF
"${test_dir}/validate-lock.sh" "${work_dir}/resolved.env" >&2
# Emit a lock only after every artifact has been resolved and checked. Never
# fall back to an older release when the latest publication is incomplete.
cat "${work_dir}/resolved.env"
