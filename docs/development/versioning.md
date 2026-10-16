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

Production release는 이 repository의 main push `CI`가 성공한 정확한 SHA에서 시작한다.
취소된 CI의 version 변경도 다음 성공한 main CI에서 published tag와 비교해 이어서 처리한다.
Image는 digest로 먼저 빌드하고 두 architecture의 보안 검사와 provenance 검증이 통과한 뒤
같은 digest를 version/latest tag로 발행한다. 자세한 gate와 예외 정책은
[image-security.md](image-security.md)를 참조한다.

1. 변경된 Controller 또는 Node version만 증가시켜 image를 publish한다.
2. Chart가 채택할 검증된 image tag를 `values.yaml`에 pin한다.
3. Chart version을 증가시키고 정확한 조합으로 repository와 E2E gate를 통과시킨다.
4. Chart를 publish한 뒤 GitOps가 승인한 Chart version과 image digest를 pin한다.

Chart-only 변경은 Controller와 Node version을 올리지 않는다. Component image release도 Chart가 그
version을 채택하기 전까지 운영 배포를 의미하지 않는다.

## Existing installation upgrades

Chart 0.6.0은 `storageClass.defaultClass` 기본값을 `true`에서 `false`로 변경한다.
기존에 ShiftPV를 기본 StorageClass로 사용했다면 upgrade values에 다음 값을 명시한다.
생략하면 upgrade 시 기본 class annotation이 해제될 수 있으며, 이후 class를 생략한 PVC의 배치가
달라질 수 있다. 이미 바인딩된 PVC/PV의 class와 데이터는 이 annotation 변경으로 이동하지 않는다.

```yaml
storageClass:
  defaultClass: true
```

`helm upgrade`는 Chart의 `crds/` schema를 갱신하지 않는다. 새 Controller가 사용하는 spec/status field가
기존 CRD에 없으면 API가 그 값을 제거하므로, target Chart의 CRD를 먼저 적용한 뒤 runtime을 올린다.
특히 rollback generation fence를 도입한 버전은 `status.rollbackRequiredGeneration` schema가 필요하다.
기록이 유지되지 않으면 Controller는 capacity hold를 보존한 채 새 스캔을 재요청한다.
생성 초기화 복구에는 `ShiftPVVolume.spec.initialPool` schema도 필요하다. 이 값이 보존되지 않으면
새 생성은 중단한다. 기존 Ready Volume은 필드 추가 없이 계속 사용할 수 있다.

아래 `TARGET_CHART_VERSION`은 설치할 정확한 Chart version으로 지정한다. 기존 release의 values도
함께 전달한다.

```sh
TARGET_CHART_VERSION='<approved-chart-version>'
CHART_WORKDIR=$(mktemp -d)
helm pull shiftpv/shiftpv --version "${TARGET_CHART_VERSION}" --untar --untardir "${CHART_WORKDIR}"
kubectl apply --field-manager=shiftpv-crd-upgrade -f "${CHART_WORKDIR}/shiftpv/crds/"
kubectl wait --for=condition=Established crd/shiftpvmoves.shiftpv.io --timeout=60s
helm upgrade shiftpv "${CHART_WORKDIR}/shiftpv" --namespace shiftpv-system --values /path/to/values.yaml --wait
```

GitOps에서도 target CRD schema 적용이 Controller rollout보다 먼저 완료되도록 순서를 보장한다.

Node 0.4.13이 Pool의 `status.registrationApproved: true`를 기록하면 이 값은 제거할 수 없다.
Node 0.4.12 이하로 되돌리면 전체 status 쓰기가 이 필드를 보존하지 않아 새 CRD에서 거부된다.

## Planned mobility upgrades

새 준비 protocol과 resident Node cleanup을 도입하는 업그레이드는 다음 순서를 따른다.
실행 계약은 [volume mobility](../spec/volume-mobility.md), 정산 조건은
[source cleanup](../spec/source-cleanup.md)이 소유한다.

1. 추가 cordon과 PVC 생성·삭제 등 새 작업 유입을 멈추고, 기존 controller가 진행 중 작업을
   정산하도록 둔다. 정산 전에 mobility를 끄면 해당 작업의 reconciliation도 멈춘다.
2. Volume Ready·빈 `activeMove`, 정산된 hold/cleanup, 진행 중 creation/copy/promotion/cleanup/
   usage helper 부재를 확인한다. 해결되지 않은 journal·finalizer를 삭제하지 않는다.
3. target CRD와 Node의 Move 조회·watch·status 쓰기 RBAC를 먼저 적용한다. 운영값으로
   렌더링한 target Node를 먼저 갱신하고 informer sync·resident capability·현재 Pool 관측을 확인한다.
4. target placement PriorityClass와 호환되는 chart/controller/helper 조합을 적용한다.
   새 chart는 구 controller가 모르는 flag를 전달하므로 전체 chart에 구 controller image만
   덮어쓰는 방식은 사용하지 않는다. controller는 단일 writer와 `Recreate`를 유지한다.
5. 배포 후 controller·Pool·capability 상태를 확인한 다음 새 유지보수 작업을 시작한다.

### 전환 사전 검사

승인된 **새 Controller 이미지**를 별도 one-shot Pod로 실행하면 기존 Controller를 종료하기 전에
`upgrade-check`를 수행할 수 있다. 기존 바이너리에는 이 명령이 없을 수 있으므로 기존 Pod에서
실행한다고 가정하지 않는다. 아래 이름은 기본 설치 기준이며 실제 namespace·ServiceAccount·
Node DaemonSet 이름과 새 이미지 digest를 지정한다.

```sh
TARGET_CONTROLLER_IMAGE='<approved-controller-image@sha256:...>'
kubectl -n shiftpv-system run shiftpv-upgrade-check --restart=Never \
  --image="${TARGET_CONTROLLER_IMAGE}" \
  --overrides='{"spec":{"serviceAccountName":"shiftpv-controller"}}' \
  --command -- /shiftpv-controller upgrade-check \
  --namespace=shiftpv-system --node-daemonset-name=shiftpv-node --timeout=90s
kubectl -n shiftpv-system wait --for=jsonpath='{.status.phase}'=Succeeded \
  pod/shiftpv-upgrade-check --timeout=120s
kubectl -n shiftpv-system logs shiftpv-upgrade-check
```

검사는 GET/LIST만 사용한다. 정상 Ready Volume과 기존 PVC/PV·게시된 데이터, 정산된 완료 이력은
유지하며, 미정산 이동·생성·삭제·cleanup·실행자와 필요한 Node 기능 부재를 보고한다.
실패하면 기존 Controller를 계속 실행하고 원인을 정산한다. journal·finalizer·예약을 강제로
삭제하거나 Job 실행자를 Node로 바꿔 적지 않는다.

성공은 그 시점의 관측이며 영속적인 전환 허가가 아니다. 새 작업 유입을 멈춘 상태를 검사부터
업그레이드 완료까지 유지한다. 검사는 새 작업을 차단하거나 기록을 변환하지 않는다.
구 rollback이 inventory로만 완료되어 독립적인 journal 증거가 없는 이력은 보수적으로 보고할 수
있다. 기존 runtime의 정산·보존·정상 GC 절차로 처리하며 검사를 통과시키려고 증거를 만들지 않는다.

helper image 변경은 작업 정산 후가 기본이다. 중간 controller 갱신이 필요하면 CLI 호환성을
확인한 기존 immutable image를 `mobility.helperImage`와 `helperPod.image`에 모두 pin하고
service-account/resource/timeout profile도 유지한다. 빈 값은 controller image를 상속한다.
기존 source Pod와 Job은 image·command·실행 조건이 달라지면 채택을 거부한다. 읽기 전용
usage Pod의 명시적 폐기·재측정 경로를 다른 helper에도 적용된다고 가정하지 않는다.

신규 생성·삭제·이동 cleanup은 resident Node만 실행한다. 필요한 capability가 없으면 같은
intent·예약을 유지하며 재시도하고 Job을 만들지 않는다. 이전 생성 Pod나 미정산 Job-bound
cleanup이 발견되면 기존 writer로 정산한 뒤 전환한다. 새 writer는 이전 Job을 실행하거나
Node로 재바인딩하지 않으며, 완료 journal·absence 증거와 실행 종료를 확인한 이력만 읽는다.
정상 Ready Volume의 생성 재호출은 현재 owner·Pool group·요청 호환성·생성 이력을 검증하고
효과 없이 응답한다. 최초 Pool이나 생성 Node가 계속 존재할 필요는 없다.

`prepared-v1` 기록이 남아 있으면 보존 중 terminal 기록까지 이해하는 controller가 필요하다.
이를 모르는 구 controller와 공존하거나 downgrade하지 않는다. rollback 시 CRD를 축소하거나
기록·finalizer를 삭제해 우회하지 않는다. `capacityTracking.enabled`를 사용 중이라면
GetCapacity를 지원하지 않는 runtime으로 되돌리기 전에 별도로 해제한다.
