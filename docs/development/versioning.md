# Versioning and Releases

ShiftPV Controller, Node와 Helm Chart는 독립적인 SemVer release unit이다.

| Artifact | Version source | Tag |
|---|---|---|
| Controller image | `versions/controller` | `controller/v<version>` |
| Node image | `versions/node` | `node/v<version>` |
| Helm Chart | `charts/shiftpv/Chart.yaml` `version` | `chart/v<version>` |

각 version은 numeric `major.minor.patch` 형식을 사용하고 이전 값보다 반드시 커야 한다. Release tag와
published artifact는 immutable하다.

Controller와 Node는 서로 같은 version일 필요가 없다. Chart의 `values.yaml`이 기본 설치에서 검증한 두
image version을 정확히 pin하며, Chart release gate는 그 image가 amd64와 arm64로 실제 publication됐는지
확인한다. `Chart.yaml`의 단일 `appVersion`으로 두 runtime artifact를 표현하지 않는다.

## Increment rules

- Component patch: 기존 외부 계약을 유지하는 bug 또는 security fix
- Component minor: 새 기능이나 CRD, CSI 또는 Controller/Node 상호작용 계약 변경
- Chart patch: 기존 install/upgrade 의미를 유지하는 template 또는 packaging fix
- Chart minor: default, rendered resource 또는 values contract 변경
- 1.0 이후 major: 호환되지 않는 외부 계약 변경

0.x에서도 기본 data lifecycle처럼 운영 의미가 바뀌면 minor를 증가시킨다.

## Release flow

1. 변경된 Controller 또는 Node version만 증가시켜 image를 publish한다.
2. Chart가 채택할 검증된 image tag를 `values.yaml`에 pin한다.
3. Chart version을 증가시키고 정확한 조합으로 repository와 E2E gate를 통과시킨다.
4. Chart를 publish한 뒤 GitOps가 승인한 Chart version과 image digest를 pin한다.

Chart-only 변경은 Controller와 Node version을 올리지 않는다. Component image release도 Chart가 그
version을 채택하기 전까지 운영 배포를 의미하지 않는다.

## Existing installation upgrades

`helm upgrade`는 Chart의 `crds/` schema를 갱신하지 않는다. 새 Controller가 사용하는 status field가
기존 CRD에 없으면 API가 그 값을 제거하므로, target Chart의 CRD를 먼저 적용한 뒤 runtime을 올린다.
특히 rollback generation fence를 도입한 버전은 `status.rollbackRequiredGeneration` schema가 필요하다.
기록이 유지되지 않으면 Controller는 capacity hold를 보존한 채 새 스캔을 재요청한다.

아래 `TARGET_CHART_VERSION`은 설치할 정확한 Chart version으로 지정한다. 기존 release의 values도
함께 전달한다.

```sh
TARGET_CHART_VERSION=0.5.9
CHART_WORKDIR=$(mktemp -d)
helm pull shiftpv/shiftpv --version "${TARGET_CHART_VERSION}" --untar --untardir "${CHART_WORKDIR}"
kubectl apply --server-side --field-manager=shiftpv-crd-upgrade -f "${CHART_WORKDIR}/shiftpv/crds/"
kubectl wait --for=condition=Established crd/shiftpvmoves.shiftpv.io --timeout=60s
helm upgrade shiftpv "${CHART_WORKDIR}/shiftpv" --namespace shiftpv-system --values /path/to/values.yaml --wait
```

GitOps에서도 target CRD schema 적용이 Controller rollout보다 먼저 완료되도록 순서를 보장한다.
