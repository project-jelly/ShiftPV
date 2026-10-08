# StorageClass Contract

현재 구현의 계약이다. 운영 검증 범위는 [Testing](../development/testing.md)을 따른다.

ShiftPV StorageClass는 등록된 node-local Pool의 directory를 RWO Filesystem PV로 동적 provisioning한다.
Chart는 일반 lifecycle용 `shiftpv`와 명시적 보존용 `shiftpv-retain`을 함께 제공한다.

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: shiftpv
provisioner: csi.shiftpv.io
reclaimPolicy: Delete
allowVolumeExpansion: false
volumeBindingMode: WaitForFirstConsumer
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: shiftpv-retain
provisioner: csi.shiftpv.io
reclaimPolicy: Retain
allowVolumeExpansion: false
volumeBindingMode: WaitForFirstConsumer
```

| Class | Default | Reclaim | 용도 |
|---|---:|---|---|
| `shiftpv` | no | `Delete` | PVC 삭제와 함께 data를 폐기하는 workload |
| `shiftpv-retain` | no | `Retain` | PVC 삭제 뒤에도 PV와 원본 data를 보존해야 하는 workload |

두 class의 provisioner는 `csi.shiftpv.io`, access/mode는 `ReadWriteOnce`/`Filesystem`, binding은
`WaitForFirstConsumer`다. Expansion은 범위 밖이다.

## Placement and admission

StorageClass의 `shiftpv.io/pool-group` parameter는 `ShiftPVPool.spec.poolGroup`을 선택한다.
두 필드를 생략하면 `default`다. 그룹은 배치와 자동 이동의 범위이며 각 Pool의 용량은
Pool UID별로 계산한다. 예를 들어 `fast` 그룹을 선택하는 class는 다음과 같다.

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: shiftpv-fast
provisioner: csi.shiftpv.io
reclaimPolicy: Delete
allowVolumeExpansion: false
volumeBindingMode: WaitForFirstConsumer
parameters:
  shiftpv.io/pool-group: fast
```

한 node에서 함께 사용하는 Pool은 모두 `capacityPolicy: FixedBlock`으로 독립 용량을 검증해야 한다.
지원하는 ext4/xfs의 fixed block backing 구간과 경로가 겹치면 배치할 수 없다.
같은 그룹에서는 각 Pool의 한도·예약·filesystem 여유를 따로 검사해 수용 가능한 Pool
하나를 선택한다. PVC를 여러 Pool로 나누거나 그룹 용량을 합산하지 않는다.
노드는 kube-scheduler가 선택한다. 선택된 node·그룹의 Ready Pool을 이름 사전순으로 검사해
용량 조건을 처음 충족한 Pool에 할당한다. 이동의 destination Pool도 같은 순서로 선택하며,
기록된 생성·이동 승인의 재시도는 기존 Pool UID를 유지한다.
지원 구성과 등록 절차는 [Chart guide](../../charts/shiftpv/README.md#multiple-pools-on-one-node)를 따른다.
기존 class에 parameter를 추가하려면 Kubernetes의 immutable parameter 제약을 고려해
새 class를 생성한다.

mobility를 허용한 PVC의 topology는 그룹에 등록된 node 범위다. 일시적인 Ready 실패나
용량 한도 오류로 이 범위를 줄이지 않으며, 실제 배치는 현재 Ready와 용량 조건을 검사한다.
기존 PV의 topology는 소급 변경되지 않는다. 점검·보정 조건은
[Existing PV topology](../../charts/shiftpv/README.md#existing-pv-topology)를 따른다.

첫 consumer가 정한 topology 안에서 Ready Pool을 선택한다. 할당은 다음을 모두 확인해야 한다.

- Pool installation/name/UID/node identity가 현재 등록과 일치한다.
- `spec.scanEpoch`로 요청한 `metadata.generation`에 대해 `status.observedGeneration`이 정확히 일치한다.
- inventory가 valid하고 complete하며 exact copy·mount identity에 모순이 없다.
- 논리 한도에서 모든 durable capacity hold를 뺀 양과 현재 filesystem 여유가 requested bytes를 충족한다.

stale, invalid 또는 incomplete observation은 allocation을 승인할 수 없다. `statfs`는 같은 filesystem의
현재 여유를 확인하는 admission 신호일 뿐 공간을 예약하거나 hard quota를 제공하지 않는다.

### Measurement verification

측정 helper는 Pool을 read-only로 마운트하고 측정 전후에 다음을 확인한다. 실패하면 결과를 폐기한다.

- Pool name/UID/node, generation과 등록된 mount/capacity 증명의 digest가 요청과 일치한다.
- Pool은 Ready이며 inventory는 fresh·complete다. 설정된 capacity/mount 정책에 따라
  실제 mount가 등록된 filesystem과 일치하는지 검증한다.
- usage의 원본은 게시되지 않은 현재 serving copy이며 installation과 디스크 marker가 일치한다.

신규 Node는 Pool의 `shiftpv.io/capacity-probe-request` 변경을 watch하고 상주 프로세스에서
`fstatfs`를 실행한다. Controller는 매 호출의 고유 request ID와 Pool evidence digest가 일치하는
새 응답만 사용한다. Node는 측정 전후 API 권한과 mount identity를 검증하고, 응답은 일반 readiness
status 갱신과 독립적으로 보존된다. 요청 annotation은 generation을 바꾸지 않아 absence fence에
영향을 주지 않는다. 구버전 Node/CRD나 2초 응답 대기는 기존 live helper로 fallback한다.
usage는 기존 helper의 `du -sbx`로 측정한다.
Helper는 host device 권한을 갖지 않는다. `FixedBlock` Pool은 노드가 publish·identity release 직전과
inventory 전후에 실제 backing 구간까지 재검증한다. 외부 변경을 원자적으로 차단하지는 않으므로
[Pool 변경 절차](../../charts/shiftpv/README.md#multiple-pools-on-one-node)를 따른다.

### Capacity denial

용량 부족의 원인과 PVC 재배치 가능 여부를 별도로 판정한다.

- 일반 미배치 consumer는 `ResourceExhausted`로 다른 노드 선택을 허용한다.
- 배치된 consumer와 CDI prime·scratch는 `Unavailable`로 노드 지정을 유지한다.
  prime은 원본 PVC의 UID·selected-node·populator 종류를 확인하므로 Pod 생성 전에도 보호한다.
- 조회 실패·불명확한 배치 계약은 재시도한다. PVC UID·노드 변경은 `FailedPrecondition`이다.
- 고정된 요청이 개별 Pool 한도나 filesystem 전체 크기를 초과해도 노드를 지우지 않는다.
  이 경우 공간 반환만으로 해결되지 않으며 Pool 설정 또는 요청 크기 변경이 필요하다.

이미 selected-node가 지워진 CDI PVC는 이 수정으로 자동 보정되지 않는다. 원본 PVC·prime·
consumer의 identity를 확인하고 CDI 운영 절차로 작업 Pod를 재생성해야 한다. ShiftPV는
노드 annotation을 임의 복원하거나 importer Pod를 삭제하지 않는다.

### Capacity retry notification

Node readiness reports `status.filesystemTotalBytes` with the existing generation
and probe timestamp. A fresh Ready observation may classify a logical shortage
as temporary without another helper. Unknown, stale or apparently undersized
observations require a live probe; approval always checks live free space.

Fixed or uncertain PVCs are indexed by PVC UID, node and Pool group after
a capacity denial. Volume, Move and Pool events re-read the durable
ledger and notify eligible Pending PVCs through `shiftpv.io/capacity-retry`.
PVC updates retain UID and resourceVersion, and never change selected-node.
The index is bounded and ephemeral; normal provisioner retries recover missed
events and controller restarts. Notifications grant no capacity or fairness guarantee.

## Capacity ownership

Capacity는 Volume과 Move journal의 hold로 계산한다.

| 상태 | capacity owner |
|---|---|
| 정상 Volume | `ShiftPVVolume`이 current owner Pool의 requested bytes 보유 |
| Move pre-commit | Volume의 source hold + `ShiftPVMove`의 destination hold |
| Move post-commit | Volume의 destination hold + Move의 retained-source hold |
| Move abort cleanup | Volume의 source hold + Move의 destination hold 유지 |
| Volume deletion | Volume hold를 cleanup closure까지 유지 |

Move commit 시 destination hold는 Move에서 Volume으로, source hold는 Volume에서 Move로 넘어간다.
두 copy가 남아 있는 동안 두 Pool에 모두 계수하며 hold의 공백을 허용하지 않는다.

Move의 destination/source hold와 삭제 중 Volume hold는 exact purge API receipt와 그 이후 generation의 fresh,
valid, complete absence proof가 모두 있을 때만 해제한다. stale scan, node heartbeat 소실, deadline 경과,
Job 종료 또는 object absence 하나만으로는 capacity를 반환하지 않는다. Move identity 모순은 `Blocked`,
cleanup identity 모순은 cleanup subjournal `NeedsReview`로 수렴하며 관련 hold를 보존한다.

## Delete lifecycle

Controller는 삭제 receipt 후 최대 5초 동안 같은 causal absence proof를 기다린다.
그 안에 증명이 도착하면 동일한 `DeleteVolume` 호출에서 완료한다. 시간 초과나 취소는 hold를
유지하며 다음 호출에서 재개한다. `--cleanup-absence-wait=0s`는 즉시 재시도 응답을 반환한다.

```text
PVC 삭제
  -> PV와 Volume deletion 시작
  -> NodeUnpublish mount recheck + empty publication fence
  -> exact purge API receipt + fresh absence proof
  -> Volume, PV와 capacity hold 종결
```

`shiftpv`의 `Delete`는 workload의 PVC lifecycle과 data 폐기를 결합한다. 삭제가 시작된 뒤에도 mount,
node outage 또는 cleanup evidence가 불충분하면 finalizer와 capacity hold를 보존하고 eventually retry한다.

## Retain lifecycle

```text
PVC 삭제
  -> PV Released
  -> Volume, owner data와 current capacity hold 유지
  -> 운영자가 해당 PV를 명시적으로 폐기
  -> NodeUnpublish mount recheck + empty publication fence
  -> exact purge API receipt + fresh absence proof
  -> Volume 종결과 capacity release
```

`shiftpv-retain`의 `Retain`은 workload 삭제와 data 폐기를 분리한다. PV나 API object를 직접 제거해 filesystem data와 capacity
ownership을 분리하면 안 된다. 폐기 중에도 fresh observation이나 cleanup 증거가 불충분하면 data와 hold를
보존한다.

두 class 모두 기본 StorageClass가 아니다. PVC에 사용할 class를 명시한다.
기존 PVC의 class와 PV reclaim policy는 Chart upgrade만으로 소급 변경되지 않는다. node/disk의 영구
손실 처리, HA/replication, RWX, snapshot, expansion과 hard quota는 이 계약 범위 밖이다.

StorageClass `reclaimPolicy`는 immutable이다. 설치된 class의 정책을 바꾸려면 ShiftPV PV/PVC 및 진행 중인
provisioning이 없는 안전한 구간에서 해당 StorageClass를 삭제하고 Chart sync로 즉시 재생성해야 한다.
