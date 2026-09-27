#!/usr/bin/env bash
set -euo pipefail

count=0
if [[ -f "${FAKE_DOCKER_STATE}" ]]; then
	count=$(<"${FAKE_DOCKER_STATE}")
fi
count=$((count + 1))
printf '%s\n' "${count}" >"${FAKE_DOCKER_STATE}"

if [[ "${FAKE_DOCKER_MODE}" == retry && "${count}" -eq 1 ]]; then
	exit 1
fi
if [[ "${FAKE_DOCKER_MODE}" == unavailable ]]; then
	exit 1
fi

if [[ "${FAKE_DOCKER_MODE}" == missing-platform ]]; then
	printf '%s\n' '{"manifests":[{"platform":{"os":"linux","architecture":"amd64"}}]}'
elif [[ "${FAKE_DOCKER_MODE}" == attestations ]]; then
	printf '%s\n' '{"manifests":[{"platform":{"os":"linux","architecture":"amd64"}},{"platform":{"os":"linux","architecture":"arm64"}},{"platform":{"os":"unknown","architecture":"unknown"},"annotations":{"vnd.docker.reference.type":"attestation-manifest"}}]}'
else
	printf '%s\n' '{"manifests":[{"platform":{"os":"linux","architecture":"amd64"}},{"platform":{"os":"linux","architecture":"arm64"}}]}'
fi
