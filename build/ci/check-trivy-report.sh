#!/usr/bin/env bash
set -euo pipefail
# Missing, malformed, or incomplete reports fail closed; scanner errors also fail the job.
jq -e '
  .SchemaVersion == 2 and (.ArtifactName | type == "string") and
  (.Results | type == "array") and
  ([.Results[] | (.Vulnerabilities // [])[], (.Secrets // [])[] |
    select(.Severity == "HIGH" or .Severity == "CRITICAL")] | length == 0)
' "$1" >/dev/null || {
  echo "::error::Scan report is invalid or contains HIGH/CRITICAL vulnerabilities or secrets" >&2
  exit 1
}
