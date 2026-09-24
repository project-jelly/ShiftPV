#!/usr/bin/env bash
set -euo pipefail

case "$(basename "$0")" in
  helm)
    case "$1 $2" in
      'repo add' | 'repo update') exit 0 ;;
      'show chart')
        if [[ "$3" == shiftpv-public/shiftpv ]]; then
          if [[ "${FAKE_ARTIFACT_MODE}" == invalid-version ]]; then
            echo 'version: invalid'
          else
            echo "version: ${FIXTURE_CHART_VERSION}"
          fi
          exit 0
        fi
        ;;
      'pull shiftpv-public/shiftpv')
        cp "${ARTIFACT_FIXTURE}/shiftpv-${FIXTURE_CHART_VERSION}.tgz" "$6/"
        exit 0
        ;;
    esac
    exec "${REAL_HELM}" "$@"
    ;;
  curl)
    [[ "${FAKE_ARTIFACT_MODE}" != missing-checksum ]] || exit 22
    if [[ "${FAKE_ARTIFACT_MODE}" == checksum-mismatch ]]; then
      digest=0000000000000000000000000000000000000000000000000000000000000000
    else
      digest=$(shasum -a 256 "${ARTIFACT_FIXTURE}/shiftpv-${FIXTURE_CHART_VERSION}.tgz" | awk '{print $1}')
    fi
    echo "${digest}  shiftpv-${FIXTURE_CHART_VERSION}.tgz"
    ;;
  docker)
    [[ "${FAKE_ARTIFACT_MODE}" != unavailable-image ]] || exit 1
    if [[ "$4" == --raw ]]; then
      if [[ "${FAKE_ARTIFACT_MODE}" == missing-platform ]]; then
        echo '{"manifests":[{"platform":{"os":"linux","architecture":"amd64"}}]}'
      else
        echo '{"manifests":[{"platform":{"os":"linux","architecture":"amd64"}},{"platform":{"os":"linux","architecture":"arm64"}}]}'
      fi
    else
      echo '{"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'
    fi
    ;;
  *) exit 1 ;;
esac
