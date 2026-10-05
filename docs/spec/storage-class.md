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

한 node의 여러 Pool은 모두 `capacityPolicy: FixedBlock`으로 독립 용량을 검증해야 한다.
지원하는 ext4/xfs의 fixed block backing 구간과 경로가 겹치면 배치할 수 없다.
같은 그룹에서는 각 Pool의 한도·예약·filesystem 여유를 따로 검사해 수용 가능한 Pool
하나를 선택한다. PVC를 여러 Pool로 나누거나 그룹 용량을 합산하지 않는다.
지원 구성과 등록 절차는 [Chart guide](../../charts/shiftpv/README.md#multiple-pools-on-one-node)를 따른다.
기존 class에 parameter를 추가하려면 Kubernetes의 immutable parameter 제약을 고려해
새 class를 생성한다.

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

`statfs`는 syscall로, usage는 `du -sbx`로 측정하며 명령 실패를 그대로 반환한다.
Helper는 host device 권한을 갖지 않는다. `FixedBlock` Pool은 노드가 publish·identity release 직전과
inventory 전후에 실제 backing 구간까지 재검증한다. 외부 변경을 원자적으로 차단하지는 않으므로
[Pool 변경 절차](../../charts/shiftpv/README.md#multiple-pools-on-one-node)를 따른다.

### Capacity denial

용량 부족 시 scheduler가 아직 배치할 수 있는 consumer에는 `ResourceExhausted`를 반환해 다른 노드
선택을 허용한다. CDI scratch처럼 PVC가 Pod 소유이고 그 Pod가 이미 선택 노드에 배치된 경우에는,
요청량이 Pool 한도와 filesystem 전체 크기 안에 들어갈 수 있다면 `Unavailable`을 반환한다.
external-provisioner가 `selected-node`를 유지한 채 재시도해야 용량 반환 후 해당 PVC가 수렴한다.
PVC 또는 Pod 조회가 일시적으로 실패하면 재배치 결정을 추측하지 않고 재시도한다.

### Capacity retry notification

Node readiness reports `status.filesystemTotalBytes` with the existing generation
and probe timestamp. A fresh Ready observation may classify a logical shortage
as temporary without another helper. Unknown, stale or apparently undersized
observations require a live probe; approval always checks live free space.

Pod-owned PVCs fixed to a node are indexed by PVC UID, node and Pool group after
a temporary capacity denial. Volume, Move and Pool events re-read the durable
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
