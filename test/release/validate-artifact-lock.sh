#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
validator="${repo_root}/test/e2e/kind/artifact/validate-lock.sh"
source_lock="${repo_root}/test/e2e/kind/artifact/versions.env"
fixture=$(mktemp -d)
trap 'rm -rf "${fixture}"' EXIT

expect_rejected() {
  local file=$1
  if "${validator}" "${file}" >"${fixture}/rejected.log" 2>&1; then
    echo "expected invalid artifact lock to be rejected: ${file}" >&2
    exit 1
  fi
}

"${validator}" "${source_lock}" >/dev/null
"${validator}" "${repo_root}/test/e2e/kind/upgrade/versions.env" >/dev/null

grep -v '^CHART_SHA256=' "${source_lock}" >"${fixture}/missing.env"
expect_rejected "${fixture}/missing.env"

sed 's/^CHART_VERSION=.*/CHART_VERSION=0.1.0-rc.1/' "${source_lock}" >"${fixture}/invalid-version.env"
expect_rejected "${fixture}/invalid-version.env"

sed 's/@sha256:[0-9a-f]*/:latest/' "${source_lock}" >"${fixture}/mutable-image.env"
expect_rejected "${fixture}/mutable-image.env"

cp "${source_lock}" "${fixture}/duplicate.env"
grep '^NODE_IMAGE=' "${source_lock}" >>"${fixture}/duplicate.env"
expect_rejected "${fixture}/duplicate.env"

marker="${fixture}/executed"
sed "s|^CONTROLLER_IMAGE=.*|CONTROLLER_IMAGE=\$(touch ${marker})|" \
  "${source_lock}" >"${fixture}/injection.env"
expect_rejected "${fixture}/injection.env"
test ! -e "${marker}"

cp "${source_lock}" "${fixture}/unknown-key.env"
printf '%s\n' "EXTRA=\$(touch ${marker})" >>"${fixture}/unknown-key.env"
expect_rejected "${fixture}/unknown-key.env"
test ! -e "${marker}"

echo "artifact lock validation tests passed"
