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
생성 admission·예약 기록·Move 예약·등록 해제는 같은 Pool UID 잠금을 공유한다.
owner 전환 CAS도 목적지 Pool UID를 잠근다. 용량 승인은 `Volumes → Moves` 순서로 읽으며,
그 사이 목적지로 owner가 들어와 예약이 빠지는 것을 막는다. 원본에서 나가는 이동은
오래된 Volume 관측이 예약을 유지하므로 잠그지 않아도 보수적으로 계상된다. 독립 Pool의 admission은
병렬로 진행하며, 같은 Pool은 예약 기록이 끝날 때까지 직렬화한다. 이 보장은 단일 controller의
공유 Locker에 의존하며 GetCapacity·재시도 알림·metrics 조회는 계속 참고값이다.
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

가용 bytes 또는 inode가 0이면 작은 쓰기 probe가 성공해도 신규 할당용 `Ready`는
`False / NoSpace`다. `Writable`은 해당 probe의 결과이며 다음 metadata 쓰기의 성공을 보장하지 않는다.

read-only backing 수집과 기존 Pool 관측은 각각 병렬로 진행하며 작업 대기는 기본 5초로 제한한다.
신규 등록은 fresh peer 증거를 재검증하며 직렬 승인하되, 한 패스의 공통 5초 예산을 쓴다.
host 호출이 멈추면 해당 UID의 작업 하나를 유지한다. Pool별 대기를 순서대로 합산하지 않으며,
만료 후 결과는 승인에 쓰지 않고 복구 뒤 새로 관측한다.
승인된 peer의 backing이 불명확하면 신규 할당은 계속 차단하며 기존 copy의 inventory는 갱신한다.
기존 copy의 게시·승인된 정리는 자기 backing과 실행 권한을 별도로 확인한다. 정리에 필요한 durable
intent를 저장하지 못하면 삭제를 시작하지 않으며, 공간을 반환한 뒤 같은 기록으로 재시도한다.

### Existing copy operations

`CapacityBackingVerified`는 현재 filesystem·block 영역이 해당 Pool의 anchor와 일치하는지,
`CapacityIndependent`는 신규 할당을 위해 다른 승인 Pool과 용량이 분리됐는지 나타낸다.
승인된 B의 증거가 사라지면 정상 A도 신규 할당을 보류하지만, A의 자기 검증과 inventory는 계속한다.
A의 기존 copy 게시·승인된 실행·정리는 exact identity와 자기 backing을 검증해 진행한다.
NodePublish는 새 공간을 할당하지 않으며, 별도 scan 요청도 기존 copy의 게시 권한을 취소하지 않는다.

native 실행은 매 경계에서 live mount·block 영역을 재검증하므로 기존의 신선한 observed generation을
사용할 수 있다. serve-source·copy·promote·verify-owner helper는 현재 generation의 own condition과
live bind mount를 확인하며 새 시간 예산을 추가하지 않는다. 사용량 측정·cleanup Job 시작과
destination publication·absence 증거는 현재 generation과 기존 freshness 조건을 요구한다.
새 condition이 없는 구버전 관측만 동일 generation의
`CapacityIndependent=True`로 호환하며, 새 False·Unknown·구세대 condition을 이전 성공으로 덮지 않는다.

### Measurement verification

신규 할당용 statfs는 열린 directory의 filesystem 쓰기 모드를 함께 확인한다.
readiness 갱신 전이라도 read-only mount에는 initialPool 예약을 기록하지 않는다.
대체 statfs helper는 원본 mount의 접근 모드를 관찰하며 파일을 쓰지 않는다.
기존 copy의 usage helper는 read-only bind를 유지한다. 측정 전후에 다음을 확인하고 실패하면 결과를 폐기한다.

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
이 측정은 시점 관찰이며 mount 변경을 잠그지 않는다. 예약 뒤 read-only로 바뀌면
실행 시 재검증과 실제 filesystem 쓰기가 실패하고 기존 intent·예약을 보존한다.
Helper는 host device 권한을 갖지 않는다. `FixedBlock` Pool은 노드가 publish·identity release 직전과
inventory 전후에 실제 backing 구간까지 재검증한다. 외부 변경을 원자적으로 차단하지는 않으므로
[Pool 변경 절차](../../charts/shiftpv/README.md#multiple-pools-on-one-node)를 따른다.

### Placement and capacity denial

용량 부족의 원인과 PVC 재배치 가능 여부를 별도로 판정한다.

CSI는 topology 내 생성 불가에 `ResourceExhausted`를 지정한다. ShiftPV의 고정 consumer
`Unavailable` 응답은 external-provisioner가 selected-node를 지우는 것을 막기 위한 Kubernetes/CDI
호환성 정책이다. 다른 CSI orchestrator에 동일한 재시도 동작을 보장하지 않는다.

- 일반 미배치 consumer는 `ResourceExhausted`로 다른 노드 선택을 허용한다.
- 선택한 노드에 요청 그룹의 Pool이 없을 때도 같은 consumer 검사를 거쳐 재선택을 허용한다.
  등록된 Pool이 있지만 Ready가 아니면 `Unavailable`로 관찰 회복을 기다린다.
- 배치된 consumer와 CDI prime·scratch는 `Unavailable`로 노드 지정을 유지한다.
  prime은 원본 PVC의 UID·selected-node·populator 종류를 확인하므로 Pod 생성 전에도 보호한다.
- 실제 미배치 consumer가 관찰되지 않으면 재배치를 허가하지 않는다. PVC가 Pod보다 먼저
  만들어진 경우에는 노드를 유지하고 다음 요청에서 consumer를 다시 확인한다.
- 조회 실패·불명확한 배치 계약은 재시도한다. PVC UID·노드 변경은 `FailedPrecondition`이다.
- 고정된 요청이 개별 Pool 한도나 filesystem 전체 크기를 초과해도 노드를 지우지 않는다.
  이 경우 공간 반환만으로 해결되지 않으며 Pool 설정 또는 요청 크기 변경이 필요하다.

기록된 생성 의도는 위 재선택보다 우선하며 원래 노드·Pool UID를 유지한다.
용량 게시가 꺼져 있으면 scheduler가 같은 노드를 다시 고를 수 있다. 일부 노드에만 존재하는
그룹의 StorageClass에는 `allowedTopologies`를 지정하거나 아래 용량 게시를 활성화한다.

이미 selected-node가 지워진 CDI PVC는 이 수정으로 자동 보정되지 않는다. 원본 PVC·prime·
consumer의 identity를 확인하고 CDI 운영 절차로 작업 Pod를 재생성해야 한다. ShiftPV는
노드 annotation을 임의 복원하거나 importer Pod를 삭제하지 않는다.

### Scheduler capacity tracking

`capacityTracking.enabled: true`로 기존 external-provisioner가 노드·StorageClass별
`CSIStorageCapacity`를 게시하고 scheduler가 새 WFFC PVC 배치에 사용한다. 기본값은 false다.
CRD·controller·node를 이 기능 지원 버전으로 갱신한 뒤 켠다. 최초 게시 전에는 새 PVC가 잠시
Pending일 수 있다. 기존 Bound PV의 위치나 데이터는 바뀌지 않는다.

CSI Controller `GetCapacity`는 기존 Pool readiness의 filesystem 가용 bytes·inodes 관측과
Volume·Move 예약 장부를 읽는다. 추가 helper, 실시간 probe, 예약 또는 승인 잠금을 만들지 않는다.

```text
Pool 여유 = max(0, min(예약 한도 - 예약 합계, 관측된 filesystem 여유))
노드·그룹 총 여유 = 각 Ready Pool 여유의 합
PVC 하나의 최대 크기 = 각 Ready Pool 여유 중 최댓값
```

같은 그룹의 독립 Pool이 6Gi씩 남으면 총 여유는 12Gi지만 PVC 하나는 6Gi까지다.
다른 StorageClass가 같은 그룹을 가리키면 같은 용량을 공유한다. Pool이 없거나 Ready가 아니거나
가용 관측이 누락됐거나 inode가 없으면 그 Pool은 0으로 반영한다. 구버전 node의 누락 필드도
0으로 처리한다. 초기 생성 예약, Retain, 삭제·이동 중 hold는 기존 장부대로 유지한다.

기본 게시 주기는 1분이다. 관측·게시 사이의 지연과 동시 생성 때문에 게시값은 예약 보장이 아니다.
API 조회나 장부 검증 실패 시 GetCapacity는 오류를 반환하며 provisioner의 이전 게시값이 남을 수 있다.
최종 생성은 기존 Pool UID 잠금 안에서 장부와 실시간 여유를 다시 검증한다.
설정·롤백은 [차트 사용법](../../charts/shiftpv/README.md#scheduler-capacity-tracking)을 따른다.

계약 근거: [CSI GetCapacity](https://github.com/container-storage-interface/spec/blob/v1.12.0/spec.md#getcapacity),
[external-provisioner capacity support](https://github.com/kubernetes-csi/external-provisioner/blob/v6.3.0/README.md#capacity-support).

### Create request compatibility

미지원 snapshot/clone source와 mutable parameters, 음수 capacity bound는 생성 전에 거절한다.
새 볼륨은 양수 required bound를 사용하고, upper bound만 있으면 그 크기를 사용한다.
기존 볼륨이 요청 범위에 들어가고 원래 노드가 허용 topology에 포함되면 원래 크기·Pool·copy로
재시도한다. 범위 또는 preference 변경은 resize나 이동을 요청하지 않는다. Pool group 변경이나
원래 노드를 제외한 요청은 `AlreadyExists`다. Requisite가 없는 단일 Preferred 노드는 기존
WaitForFirstConsumer 배치 계약으로 취급한다.
PVC metadata가 있고 선호 노드가 바뀌면 저장된 노드의 UID·selected-node도 확인한다.
배치 identity 변경은 `FailedPrecondition`, 조회 실패나 불명확한 배치는 `Unavailable`이다.

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
