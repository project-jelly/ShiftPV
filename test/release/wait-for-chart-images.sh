#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "${fixture}"' EXIT
mkdir -p "${fixture}/bin"

fake_docker="${fixture}/bin/docker"
cp "${repo_root}/test/release/fixtures/fake-docker.sh" "${fake_docker}"
chmod +x "${fake_docker}"

run_wait() {
	FAKE_DOCKER_MODE=$1 \
		FAKE_DOCKER_STATE="${fixture}/state" \
		IMAGE_WAIT_ATTEMPTS=$2 \
		IMAGE_WAIT_DELAY_SECONDS=0 \
		PATH="${fixture}/bin:${PATH}" \
		"${repo_root}/build/ci/wait-for-chart-images.sh"
}

rm -f "${fixture}/state"
run_wait success 1
run_wait attestations 1

rm -f "${fixture}/state"
run_wait retry 2
[[ "$(<"${fixture}/state")" -eq 3 ]]

for mode in unavailable missing-platform; do
	rm -f "${fixture}/state"
	if run_wait "${mode}" 1 >/dev/null 2>&1; then
		echo "expected ${mode} image manifests to block chart publication" >&2
		exit 1
	fi
done

echo "chart image wait tests passed"
