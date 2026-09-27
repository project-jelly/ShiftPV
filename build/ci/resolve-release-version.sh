#!/usr/bin/env bash
set -euo pipefail
# Compare against published tags so a cancelled main CI does not lose a version bump.
# Output true/false to stdout; any rollback or conflicting changed tag is an error.
prefix=$1
version=$2
version_file=$3
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || exit 1
tag="${prefix}/v${version}"
previous_version=$(git tag --list "${prefix}/v*" | sed "s|^${prefix}/v||" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1 || true)
if [[ -n "$previous_version" && "$previous_version" != "$version" ]]; then
  "$(dirname "$0")/assert-version-increase.sh" "$version" "$previous_version"
fi
if tagged_commit=$(git rev-parse --verify "refs/tags/${tag}^{commit}" 2>/dev/null); then
  if [[ "$tagged_commit" != "$RELEASE_SHA" ]] &&
    ! git diff --quiet "$RELEASE_BEFORE" "$RELEASE_SHA" -- "$version_file"; then
    echo "::error::tag ${tag} already points to a different commit" >&2
    exit 1
  fi
  echo false
  exit 0
fi
echo true
