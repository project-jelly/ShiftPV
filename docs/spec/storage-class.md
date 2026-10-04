# StorageClass Contract

> **Status:** ShiftPV 0.4 구현·검증 목표다. 모든 acceptance gate가 끝나야 runtime 보증이 된다.

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
| `shiftpv` | yes | `Delete` | PVC와 함께 data lifecycle을 끝내는 일반 workload와 DR 시험 |
| `shiftpv-retain` | no | `Retain` | PVC 삭제 뒤에도 PV와 원본 data를 보존해야 하는 workload |

두 class의 provisioner는 `csi.shiftpv.io`, access/mode는 `ReadWriteOnce`/`Filesystem`, binding은
`WaitForFirstConsumer`다. Expansion은 범위 밖이다.

## Placement and admission

첫 consumer가 정한 topology 안에서 Ready Pool을 선택한다. 할당은 다음을 모두 확인해야 한다.

- Pool installation/name/UID/node identity가 현재 등록과 일치한다.
- `spec.scanEpoch`로 요청한 `metadata.generation`에 대해 `status.observedGeneration`이 정확히 일치한다.
- inventory가 valid하고 complete하며 exact copy·mount identity에 모순이 없다.
- 논리 한도에서 모든 durable capacity hold를 뺀 양과 현재 filesystem 여유가 requested bytes를 충족한다.

stale, invalid 또는 incomplete observation은 allocation을 승인할 수 없다. `statfs`는 같은 filesystem의
현재 여유를 확인하는 admission 신호일 뿐 공간을 예약하거나 hard quota를 제공하지 않는다.

용량 부족 시 scheduler가 아직 배치할 수 있는 consumer에는 `ResourceExhausted`를 반환해 다른 노드
선택을 허용한다. CDI scratch처럼 PVC가 Pod 소유이고 그 Pod가 이미 선택 노드에 배치된 경우에는,
요청량이 Pool 한도와 filesystem 전체 크기 안에 들어갈 수 있다면 `Unavailable`을 반환한다.
external-provisioner가 `selected-node`를 유지한 채 재시도해야 용량 반환 후 해당 PVC가 수렴한다.
PVC 또는 Pod 조회가 일시적으로 실패하면 재배치 결정을 추측하지 않고 재시도한다.

## Capacity ownership

Capacity는 directory 존재 추정이나 wall-clock TTL이 아니라 세 durable API의 명시적 hold로 계산한다.

| 상태 | capacity owner |
|---|---|
| 정상 Volume | `ShiftPVVolume`이 current owner Pool의 requested bytes 보유 |
| Move pre-commit | Volume의 source hold + `ShiftPVMove`의 destination hold |
| Move post-commit | Volume의 destination hold + Move의 retained-source hold |
| Move abort cleanup | Volume의 source hold + Move의 destination hold 유지 |
| Volume deletion | Volume hold를 cleanup closure까지 유지 |

한 physical copy라도 남을 수 있는 동안 해당 hold를 보수적으로 유지한다. Move commit 시 destination hold의
소유권은 Move에서 Volume로 넘어가고, source hold는 Volume에서 같은 Move의 retained-source hold로 이어진다.
소유권 이전에는 release gap이 없어야 하며, source와 destination 두 physical copy가 존재하는 구간은 두
Pool에 동시에 계수하는 conservative double accounting을 적용한다.

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

`shiftpv`만 cluster default다. 보존이 필요한 PVC는 default에 의존하지 않고 `shiftpv-retain`을 명시한다.
기존 PVC의 class와 PV reclaim policy는 Chart upgrade만으로 소급 변경되지 않는다. node/disk의 영구
손실 처리, HA/replication, RWX, snapshot, expansion과 hard quota는 이 계약 범위 밖이다.

StorageClass `reclaimPolicy`는 immutable이다. 설치된 class의 정책을 바꾸려면 ShiftPV PV/PVC 및 진행 중인
provisioning이 없는 안전한 구간에서 해당 StorageClass를 삭제하고 Chart sync로 즉시 재생성해야 한다.
