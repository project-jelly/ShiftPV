#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
CONTAINER_NAME="shiftpv-dashboard-query-${RANDOM}-$$"
IMAGE=prom/prometheus:v3.14.0@sha256:5ce7540c3c00ef4ab0c9d2c995c6a5b9c421f44b4a115d97a2c7af3b1c21cbb0
cleanup() {
  docker rm -fv "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm -d --name "${CONTAINER_NAME}" -p 127.0.0.1::9090 \
  -v "${ROOT_DIR}/charts/shiftpv/alerts:/etc/shiftpv-alerts:ro" \
  -v "${ROOT_DIR}/test/helm/dashboard:/etc/prometheus:ro" "${IMAGE}" \
  --config.file=/etc/prometheus/prometheus.yaml --storage.tsdb.path=/prometheus >/dev/null
docker exec "${CONTAINER_NAME}" promtool check rules /etc/prometheus/preview-rules.json
docker exec "${CONTAINER_NAME}" promtool check rules /etc/shiftpv-alerts/shiftpv-rules.yaml
docker exec "${CONTAINER_NAME}" promtool test rules /etc/prometheus/alerts-test.yml
docker exec "${CONTAINER_NAME}" promtool test rules /etc/prometheus/lifecycle-alerts-test.yml
endpoint="http://$(docker port "${CONTAINER_NAME}" 9090/tcp)"
ready=0
for _ in {1..60}; do
  if curl -fsS --max-time 2 --get --data-urlencode 'query=shiftpv_pool_ready{cluster="preview-healthy"}' \
    "${endpoint}/api/v1/query" | jq -e '.status == "success" and (.data.result | length) == 2' >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "${ready}" != 1 ]]; then
  docker logs --tail 30 "${CONTAINER_NAME}"
  exit 1
fi
cd "${ROOT_DIR}"
SHIFTPV_DASHBOARD_PROMETHEUS_URL="${endpoint}" go test ./test/helm -run TestDashboardQueries -count=1 -v
