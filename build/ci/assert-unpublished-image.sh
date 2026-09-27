#!/usr/bin/env bash
set -euo pipefail
error_file=$(mktemp)
trap 'rm -f "$error_file"' EXIT
if docker buildx imagetools inspect "$1" >/dev/null 2>"$error_file"; then
  echo "::error::Immutable release image already exists: $1; do not rebuild or overwrite it" >&2
  exit 1
fi
if ! grep -Fxq "ERROR: $1: not found" "$error_file" &&
  ! grep -Fxq "$1: not found" "$error_file"; then
  cat "$error_file" >&2
  echo "::error::Cannot establish that the release tag is absent" >&2
  exit 1
fi
