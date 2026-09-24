#!/usr/bin/env bash
# Shared helpers for the suites that install a published ShiftPV release from
# the artifact lock. This file is sourced.

# Every suite needs the same digest helper: macOS ships shasum, Linux runners
# ship sha256sum, and the lock is verified against whichever exists.
sha256_file() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$1" | awk '{print $1}'
	else
		echo "neither sha256sum nor shasum is available" >&2
		return 1
	fi
}

require_commands() {
	local command
	for command in "$@"; do
		command -v "${command}" >/dev/null || {
			echo "required command not found: ${command}" >&2
			return 1
		}
	done
}

# The pinned node image in the lock needs a Kind that understands it.
require_kind_version() {
	local version major minor
	version=$(kind version | awk '{print $2}' | sed 's/^v//')
	IFS=. read -r major minor _ <<<"${version}"
	if ((10#${major} == 0 && 10#${minor} < 33)); then
		echo "kind >= 0.33.0 is required for the pinned node image; found ${version}" >&2
		return 1
	fi
}

# Keep the run off the developer's Docker and Helm state: a desktop credential
# helper must not be inherited by an isolated engine such as Colima, and the
# published chart repository must not be written into the user's Helm home.
# DOCKER_HOST preserves the engine selected before DOCKER_CONFIG is replaced.
isolate_tool_environment() {
	local work_dir=$1 active_context active_host
	active_context=$(docker context show)
	active_host=$(docker context inspect "${active_context}" --format '{{.Endpoints.docker.Host}}')
	mkdir -p "${work_dir}/docker-config" "${work_dir}/helm-config" \
		"${work_dir}/helm-cache" "${work_dir}/helm-data"
	export DOCKER_HOST=${DOCKER_HOST:-${active_host}}
	export DOCKER_CONFIG="${work_dir}/docker-config"
	export HELM_CONFIG_HOME="${work_dir}/helm-config"
	export HELM_CACHE_HOME="${work_dir}/helm-cache"
	export HELM_DATA_HOME="${work_dir}/helm-data"
	unset DOCKER_CONTEXT
}

# Validate the lock, then define its keys in the caller's shell. The validator
# restricts values to non-executable URL/version/digest forms before the file is
# sourced.
load_artifact_lock() {
	local lock_file=$1 lib_dir
	lib_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
	"${lib_dir}/../artifact/validate-lock.sh" "${lock_file}"
	# shellcheck disable=SC1090
	source "${lock_file}"
}

# Pull the locked chart version and prove it is the published artifact before
# anything is installed from it. Prints the verified package path.
# Requires CHART_REPOSITORY, CHART_VERSION and CHART_SHA256 from the lock.
pull_published_chart() {
	local dest_dir=$1 package actual_sha
	helm repo add shiftpv-public "${CHART_REPOSITORY}" >&2
	helm repo update shiftpv-public >&2
	helm pull shiftpv-public/shiftpv --version "${CHART_VERSION}" --destination "${dest_dir}" >&2
	package="${dest_dir}/shiftpv-${CHART_VERSION}.tgz"
	actual_sha=$(sha256_file "${package}")
	if [[ "${actual_sha}" != "${CHART_SHA256}" ]]; then
		echo "public chart digest mismatch: expected ${CHART_SHA256}, got ${actual_sha}" >&2
		return 1
	fi
	if [[ "$(helm show chart "${package}" | awk '$1 == "version:" { print $2; exit }')" != "${CHART_VERSION}" ]]; then
		echo "public chart reports a version other than ${CHART_VERSION}" >&2
		return 1
	fi
	printf '%s\n' "${package}"
}

# A failed comparison has to name the value it saw, otherwise a red suite only
# reports the line number of a bare `test`.
assert_equal() {
	local label=$1 expected=$2 actual=$3
	if [[ "${actual}" != "${expected}" ]]; then
		echo "${label}: expected '${expected}', got '${actual}'" >&2
		return 1
	fi
}

# Install the locked release from its verified package. Both the artifact smoke
# and the upgrade suite use the same image wiring with separate release locks;
# extra helm arguments, including default StorageClass opt-in, belong to callers.
# Requires CONTROLLER_IMAGE and NODE_IMAGE from the lock.
install_published_release() {
	local chart_package=$1 namespace=$2 release=$3
	shift 3
	helm upgrade --install "${release}" "${chart_package}" \
		--namespace "${namespace}" --create-namespace \
		--set-string controller.image.repository="${CONTROLLER_IMAGE%%:*}" \
		--set-string controller.image.tag="${CONTROLLER_IMAGE#*:}" \
		--set-string node.image.repository="${NODE_IMAGE%%:*}" \
		--set-string node.image.tag="${NODE_IMAGE#*:}" \
		--set-string mobility.helperImage="${CONTROLLER_IMAGE}" \
		"$@" \
		--wait --timeout 8m
	kubectl -n "${namespace}" wait --for=condition=Ready pod \
		-l app.kubernetes.io/instance="${release}" --timeout=5m
}
