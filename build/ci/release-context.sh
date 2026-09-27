#!/usr/bin/env bash
set -euo pipefail
# The artifact is downloaded only from the triggering CI run, never a PR run.
jq -e --arg repository "$GITHUB_REPOSITORY" --arg sha "$RELEASE_SHA" '
  .workflow_run | .conclusion == "success" and .event == "push" and
  .head_branch == "main" and .head_repository.full_name == $repository and
  .head_sha == $sha and .path == ".github/workflows/ci.yaml"
' "$GITHUB_EVENT_PATH" >/dev/null
jq -e --arg repository "$GITHUB_REPOSITORY" --arg sha "$RELEASE_SHA" '
  .repository == $repository and .sha == $sha and
  (.before | test("^[0-9a-f]{40}$")) and (.before != "0000000000000000000000000000000000000000")
' "$1" >/dev/null
before=$(jq -r .before "$1")
git merge-base --is-ancestor "$before" "$RELEASE_SHA"
printf 'RELEASE_BEFORE=%s\n' "$before" >> "$GITHUB_ENV"
