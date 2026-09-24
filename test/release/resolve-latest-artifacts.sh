#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "${fixture}"' EXIT
export REAL_HELM
REAL_HELM=$(command -v helm)
export ARTIFACT_FIXTURE="${fixture}"
mkdir -p "${fixture}/bin"
helm package "${repo_root}/charts/shiftpv" --destination "${fixture}" >/dev/null
export FIXTURE_CHART_VERSION
FIXTURE_CHART_VERSION=$(awk '$1 == "version:" { print $2 }' "${repo_root}/charts/shiftpv/Chart.yaml")
for command in helm curl docker; do
  cp "${repo_root}/test/release/fixtures/fake-artifact-command.sh" "${fixture}/bin/${command}"
  chmod +x "${fixture}/bin/${command}"
done

resolve() {
  FAKE_ARTIFACT_MODE=$1 PATH="${fixture}/bin:${PATH}" \
    "${repo_root}/test/e2e/kind/artifact/resolve-latest.sh" >"${fixture}/resolved.env" 2>"${fixture}/diagnostics"
}

resolve success
"${repo_root}/test/e2e/kind/artifact/validate-lock.sh" "${fixture}/resolved.env" >/dev/null
grep -Fxq "CHART_VERSION=${FIXTURE_CHART_VERSION}" "${fixture}/resolved.env"
# Both images must be pinned to the resolved digest, not a previous lock's digest.
test "$(grep -c '@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$' "${fixture}/resolved.env")" = 2

for mode in checksum-mismatch missing-checksum unavailable-image missing-platform invalid-version; do
  if resolve "${mode}"; then
    echo "expected ${mode} to prevent publishing a resolved lock" >&2
    exit 1
  fi
  test ! -s "${fixture}/resolved.env"
done
echo "latest artifact resolution tests passed"
