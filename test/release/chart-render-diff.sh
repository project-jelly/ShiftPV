#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "${fixture}"' EXIT
mkdir -p "${fixture}/build/ci" "${fixture}/charts" "${fixture}/test/e2e/kind"
cp "${root}/build/ci/chart-render-diff.sh" "${fixture}/build/ci/"
cp -R "${root}/charts/shiftpv" "${fixture}/charts/"
cp "${root}/test/e2e/kind/values.yaml" "${fixture}/test/e2e/kind/"
git -C "${fixture}" init -q
git -C "${fixture}" add .
git -C "${fixture}" -c user.name=fixture -c user.email=fixture@example.invalid commit -qm baseline
base=$(git -C "${fixture}" rev-parse HEAD)

compare() {
	"${fixture}/build/ci/chart-render-diff.sh" "${base}" >"${fixture}/result.log" 2>&1
}
compare

python3 - "${fixture}/charts/shiftpv" <<'PY'
from pathlib import Path
import re
import sys

chart = Path(sys.argv[1])
for name, pattern in [("Chart.yaml", r"(?m)^(version: \d+\.\d+\.)(\d+)$"),
                      ("values.yaml", r"(?m)^(    tag: \d+\.\d+\.)(\d+)$")]:
    path = chart / name
    value, count = re.subn(pattern, lambda m: m[1] + str(int(m[2]) + 1), path.read_text())
    assert count == (1 if name == "Chart.yaml" else 2), (name, count)
    path.write_text(value)
PY
compare

cat >>"${fixture}/charts/shiftpv/templates/controller/deployment.yaml" <<'YAML'
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: unexpected-release-resource
YAML
if compare; then
	echo 'release version normalization concealed a resource change' >&2
	exit 1
fi
grep -q 'DIFF default' "${fixture}/result.log"
grep -q 'unexpected-release-resource' "${fixture}/result.log"
echo 'chart render diff preserves release resource checks'
