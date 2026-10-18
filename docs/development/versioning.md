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
함께 전달한다. 실행 protocol을 전환할 때는 아래 [mobility 전환 순서](#planned-mobility-upgrades)의
사전 검사를 마친 뒤 `helm upgrade`를 실행한다.

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

새 runtime은 `prepared-v1` Move만 실행한다. 신규 생성·삭제·Move cleanup은 resident Node만
실행하고, copy·owner verification·promotion Job과 읽기 전용 계측 helper는 유지한다.
기존 status를 읽을 수 있다는 것은 이전 실행기를 재개할 수 있다는 의미가 아니다.

이 실행 계약으로 전환하는 업그레이드는 다음 순서를 따른다.
실행 계약은 [volume mobility](../spec/volume-mobility.md), 정산 조건은
[source cleanup](../spec/source-cleanup.md)이 소유한다.

1. 추가 cordon과 PVC 생성·삭제 등 새 작업 유입을 멈춘다. 호환되는 기존 Controller와
   immutable helper image를 유지한 채 진행 중 작업을 정산한다. 정산 전에 mobility를 끄거나
   candidate로 교체하지 않는다.
2. Volume Ready·빈 `activeMove`, 정산된 hold/cleanup 및 helper 실행 종료를 확인한다.
   해결되지 않은 journal·finalizer·예약을 삭제하거나 Job binding을 Node로 바꾸지 않는다.
3. target CRD와 Node의 Move 조회·watch·status 쓰기 RBAC를 먼저 적용한다. 운영값으로
   렌더링한 target Node를 먼저 갱신하고 informer sync·resident capability·현재 Pool 관측을 확인한다.
4. candidate 이미지의 읽기 전용 `upgrade-check`를 별도 Pod에서 실행한다. 종료 코드 0과
   `complete: true`, 빈 `blockers`인 SAFE 결과를 확인한 뒤에만 Controller를 교체한다.
5. target placement PriorityClass와 호환되는 chart/controller/helper 조합을 적용한다.
   새 chart는 구 controller가 모르는 flag를 전달하므로 전체 chart에 구 controller image만
   덮어쓰는 방식은 사용하지 않는다. controller는 단일 writer와 `Recreate`를 유지한다.
6. 배포 후 controller·Pool·capability 상태와 기존 데이터·UID를 확인한 뒤 새 작업 유입을 재개한다.

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

검사는 GET/LIST만 사용한다. 정상 Ready Volume과 기존 PVC/PV·데이터 및 정산된 완료 이력을
보존하고, 미정산 작업·실행자와 필요한 Node 기능 부재를 보고한다. API 오류·decode 실패·불완전한
관측은 승인이 아니다. 실패하면 기존 writer와 helper image를 유지한 채 원인을 정산한다.

성공은 그 시점의 관측이며 영속적인 전환 허가가 아니다. 새 작업 유입을 멈춘 상태를 검사부터
업그레이드 완료까지 유지한다. 검사는 새 작업을 차단하거나 기록을 변환하지 않는다.
구 rollback이 inventory로만 완료되어 독립적인 journal 증거가 없는 이력은 보수적으로 보고할 수
있다. 기존 runtime의 정산·보존·정상 GC 절차로 처리하며 검사를 통과시키려고 증거를 만들지 않는다.

legacy 작업 정산에는 그 writer와 호환되는 immutable image를 `mobility.helperImage`와
`helperPod.image`에 모두 pin하고 기존 실행 설정을 유지한다. candidate로 구 Job을 이어서 실행하는
중간 업그레이드는 지원하지 않는다. helper image는 정산 뒤 새 조합으로 바꾼다.

신규 생성·삭제·이동 cleanup은 resident Node만 실행한다. 필요한 capability가 없으면 같은
intent·예약을 유지하며 재시도하고 Job을 만들지 않는다. 이전 생성 Pod나 미정산 Job-bound
cleanup이 발견되면 기존 writer로 정산한 뒤 전환한다. 새 writer는 이전 Job을 실행하거나
Node로 재바인딩하지 않으며, 완료 journal·absence 증거와 실행 종료를 확인한 이력만 읽는다.

### 미완료 native 생성의 명시적 취소

0.6.13의 native 생성이 완료되지 않았고 원래 요청을 취소해야 한다면 새 이미지의
`cancel-creation`을 별도 Pod에서 실행할 수 있다. 새 작업 유입을 멈추고 대상 Volume의
ID·UID·초기 사본과 취소 의도를 확인한다. target CRD·RBAC·Node를 먼저 적용하고 기존
Controller와 webhook은 정산 중 유지한다. 이전 helper 책임은 이 명령으로 이전하지 않는다.

```sh
kubectl -n shiftpv-system run shiftpv-cancel-creation --restart=Never \
  --image="${TARGET_CONTROLLER_IMAGE}" \
  --overrides='{"spec":{"serviceAccountName":"shiftpv-controller"}}' \
  --command -- /shiftpv-controller cancel-creation --namespace=shiftpv-system \
  --volume-id='<exact-volume-id>' --volume-uid='<exact-volume-uid>' --timeout=120s
kubectl -n shiftpv-system logs -f pod/shiftpv-cancel-creation
```

승인은 기존 생성 완료 CAS와 경쟁한다. Ready가 먼저 되면 취소를 거부하고 보존한다.
취소가 먼저 되면 동일한 초기 사본만 정리하고, native 영수증과 최신 부재 증명 뒤에
보호와 예약을 해제한다. `complete: true`는 해당 Volume이 사라진 것을 확인한 결과다.
API 오류·이전 helper·외부 finalizer는 완료로 보고하지 않는다.

명령이 중단되면 같은 ID·UID로 재실행할 수 있다. 저장된 취소 의도는 이를 지원하는 새
Controller도 이어서 정산한다. 이때 출처 정보를 추측해 채우지 않는다. 정산 후 별도의
읽기 전용 `upgrade-check`가 SAFE인 것을 확인한 뒤 Controller를 교체한다.
정상 Ready Volume의 생성 재호출은 현재 owner·Pool group·요청 호환성·생성 이력을 검증하고
효과 없이 응답한다. 최초 Pool이나 생성 Node가 계속 존재할 필요는 없다.

legacy Move는 독립적인 완료 증거·정확한 parent/UID·실행 종료·Volume 참조 해제를 검증한 뒤
retention과 finalizer 조건에 따라 metadata GC만 수행한다. 미정산·불완전한 이력은 실행하거나
보정하지 않는다. 승인된 전체 copy/operation plan이 없으면 recovery는 `CleanupNeedsReview`로
멈추고 hold를 보존하며, inventory에서 plan이나 승인을 추론하지 않는다.
Cleanup intent가 없던 prepared source 복구는 exact CAS 정산·causal rollback fence와 helper 종료를
확인한다. 기존 cleanup journal의 receipt·absence 요구와 legacy 이력의 보수적 판정은 유지한다.

[`legacy-upgrade.sh`](../../test/e2e/kind/mobility/legacy-upgrade.sh)는 active legacy 작업을
candidate 사전 검사가 거부하는지, 기존 writer·helper가 직접 정산하는지, 교체 전에 SAFE인지
확인한다. `Recreate` 전환 뒤에도 PVC/PV/Volume/Move UID·checksum·이력을 보존하고 새 실행자를
만들지 않는지 검사한다.

`prepared-v1` 기록이 남아 있으면 보존 중 terminal 기록까지 이해하는 controller가 필요하다.
이를 모르는 구 controller와 공존하거나 downgrade하지 않는다. rollback 시 CRD를 축소하거나
기록·finalizer를 삭제해 우회하지 않는다. `capacityTracking.enabled`를 사용 중이라면
GetCapacity를 지원하지 않는 runtime으로 되돌리기 전에 별도로 해제한다.
