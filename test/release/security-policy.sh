#!/usr/bin/env bash
set -euo pipefail
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
cd "$scratch"
git init -q
git config user.name test
git config user.email test@example.invalid
mkdir versions
printf '0.1.0\n' > versions/controller
git add .
git commit -qm initial
first=$(git rev-parse HEAD)
git tag controller/v0.1.0
export RELEASE_BEFORE=$first RELEASE_SHA=$first GITHUB_REPOSITORY=project-jelly/ShiftPV
export GITHUB_EVENT_PATH="$scratch/event.json" GITHUB_ENV="$scratch/env"
resolver="$repo_root/build/ci/resolve-release-version.sh"
guard="$repo_root/build/ci/release-context.sh"
scan="$repo_root/build/ci/check-trivy-report.sh"
reject() {
  if "$@" >/dev/null 2>&1; then
    echo "unexpected policy success: $*" >&2
    exit 1
  fi
}
[[ $("$resolver" controller 0.1.0 versions/controller) == false ]]
printf '0.2.0\n' > versions/controller
git commit -qam bump
bump=$(git rev-parse HEAD)
export RELEASE_SHA=$bump
[[ $("$resolver" controller 0.2.0 versions/controller) == true ]]
# A cancelled CI on the bump must not lose the release on the next successful push.
printf 'docs\n' > README.md
git add .
git commit -qm docs
export RELEASE_BEFORE=$bump RELEASE_SHA
tip=$(git rev-parse HEAD)
RELEASE_SHA=$tip
[[ $("$resolver" controller 0.2.0 versions/controller) == true ]]
git tag controller/v0.2.0 "$first"
# A collision during the original changed push is rejected.
export RELEASE_BEFORE=$first
reject "$resolver" controller 0.2.0 versions/controller
git tag -d controller/v0.2.0 >/dev/null
reject "$resolver" controller 0.0.9 versions/controller
reject "$resolver" controller v0.2.0 versions/controller
jq -n --arg sha "$tip" '{workflow_run:{conclusion:"success",event:"push",head_branch:"main",head_repository:{full_name:"project-jelly/ShiftPV"},head_sha:$sha,path:".github/workflows/ci.yaml"}}' > "$GITHUB_EVENT_PATH"
jq -n --arg before "$first" --arg sha "$tip" '{repository:"project-jelly/ShiftPV",before:$before,sha:$sha}' > push.json
"$guard" push.json
for result in failure cancelled skipped neutral timed_out; do
  jq --arg result "$result" '.workflow_run.conclusion=$result' "$GITHUB_EVENT_PATH" > bad.json
  GITHUB_EVENT_PATH="$scratch/bad.json" reject "$guard" push.json
done
for expression in '.workflow_run.event="pull_request"' '.workflow_run.head_branch="feature"' '.workflow_run.head_repository.full_name="attacker/ShiftPV"' '.workflow_run.head_sha="bad"' '.workflow_run.path="other.yaml"'; do
  jq "$expression" "$GITHUB_EVENT_PATH" > bad.json
  GITHUB_EVENT_PATH="$scratch/bad.json" reject "$guard" push.json
done
for expression in '.sha="bad"' '.repository="attacker/ShiftPV"' '.before="0000000000000000000000000000000000000000"' '.before="bad"'; do
  jq "$expression" push.json > bad-context.json
  reject "$guard" bad-context.json
done
printf '{"SchemaVersion":2,"ArtifactName":"image","Results":[]}' > scan.json
"$scan" scan.json
for finding in Vulnerabilities Secrets; do
  jq --arg field "$finding" '.Results=[{($field):[{Severity:"HIGH"}]}]' scan.json > finding.json
  reject "$scan" finding.json
  jq --arg field "$finding" '.Results=[{($field):[{Severity:"CRITICAL"}]}]' scan.json > finding.json
  reject "$scan" finding.json
done
printf '{}\n' > invalid.json
reject "$scan" invalid.json
printf 'not json\n' > invalid.json
reject "$scan" invalid.json
reject "$scan" missing.json
printf 'release security policy tests passed\n'
