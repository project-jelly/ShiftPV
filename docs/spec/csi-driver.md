# CSI Driver Contract

현재 구현의 계약이다. 운영 검증 범위는 [Testing](../development/testing.md)을 따른다.

`csi.shiftpv.io`는 node-local `hostPath` directory를 Kubernetes의 persistent RWO Filesystem volume으로
제공한다. 이동은 source node와 disk가 정상인 상태에서 수행하는 planned cold mobility다. I/O를 멈추고
실제 mount 해제를 증명한 뒤 copy와 owner 전환을 진행한다.

## Product boundary

| 속성 | 계약 |
|---|---|
| Storage | 등록된 node-local Pool의 기존 filesystem directory |
| Access | `ReadWriteOnce`, `Filesystem` |
| Attach | 사용하지 않음 |
| Binding | `WaitForFirstConsumer`와 node topology |
| Mobility | source를 quiesce할 수 있는 planned cold copy |
| Normal I/O | owner node의 directory를 kubelet target에 bind mount |

Identity, Controller, Node 서비스는 provisioning, deletion, publish와 unpublish에 필요한 최소 CSI RPC만
광고한다. Attach, stage/unstage, expansion, snapshot, raw block, RWX와 live migration은 제품 범위 밖이다.

ShiftPV가 정의하는 durable API는 정확히 세 개다.

| API | 소유하는 사실 |
|---|---|
| `ShiftPVPool` | installation에 등록된 node·directory·Pool identity, capacity와 causal scan 요청·결과 |
| `ShiftPVVolume` | logical volume identity, requested bytes, 현재 owner/copy, 현재 owner capacity hold와 delete journal |
| `ShiftPVMove` | 한 번의 cold move identity, pre-commit destination hold, post-commit retained-source hold, copy·publish·cleanup journal |

Helper Job, Pod와 local operation file은 effect를 실행하거나 복구하기 위한 수단이지 네 번째 source of truth가
아니다. 재시작 후 동작은 세 API의 journal과 exact node-local evidence에서 복원되어야 한다.

## Exact identity

모든 allocation, mount, promotion과 deletion은 최소한 다음 identity를 함께 확인한다.

```text
installation + Pool name/UID + node + Volume name/UID
+ copy ID + role + operation identity
```

이름만 같거나 path만 같은 것은 권한이 아니다. 재생성된 object, 다른 Pool incarnation, 다른 copy role 또는
marker/inode 불일치는 자동 진행을 닫고 data와 capacity hold를 유지한다.

## Causal Pool observation

시간 비교는 storage effect의 권한이 아니다. Controller가 cleanup 후 인과적 absence 증거를 요구할 때
`ShiftPVPool.spec.scanEpoch`를 변경하고, Kubernetes가 증가시킨 `metadata.generation`을 필요한 generation으로
기록한다. Node는 Pool marker와 실제 mount reference를 함께 조사한 뒤 다음을 한 status update로 게시한다.

- `status.observedGeneration`이 요청된 `metadata.generation`과 정확히 일치한다.
- inventory가 bounded scan 전체를 포함해 `valid`하고 `complete`하다.
- 각 항목이 exact Pool, Volume, copy, role identity와 present/mounted 증거를 가진다.

generation이 뒤처지거나 identity가 모순되거나 scan이 invalid/incomplete이면 그 관찰은 allocation,
destination actual-publish 판정, cleanup absence 또는 capacity release를 승인할 수 없다. `observedAt`
같은 wall-clock 시각은 readiness·진단·경보에 사용하지만 cleanup의 causal fence나 TTL로 사용하지 않는다.

운영 acceptance는 source/destination filesystem 조합이 rsync 계약의 metadata를 보존할 수 있는지 별도로
검증한다. Node readiness는 Pool 접근성, 쓰기 가능성, capacity 관찰 가능성을 확인하며 모든 metadata primitive를
runtime probe하지 않는다. Device node 재생성과 nested filesystem traversal은 copy option에서 명시적으로
제외되므로 Volume data contract에 포함되지 않는다.

`NodePublishVolume`과 `NodeUnpublishVolume`은 같은 volume에 대해 동일한 node-local filesystem lock을
사용한다. Inventory scan은 Pool marker와 kubelet mount reference를 관찰하지만 같은 lock의 critical
section은 아니다. 따라서 Controller는 `scanEpoch` generation fence, valid/complete inventory, exact copy
identity와 `published` 관찰을 함께 요구한다. Controller가 기록한 latch, boolean이나 node 목록은 실제 mount
evidence를 대신하지 않는다.

## Provision and publish

Provisioning은 선택된 Pool의 exact identity와 fresh, valid, complete inventory를 확인하고 durable
`ShiftPVVolume` intent와 current-owner capacity hold를 먼저 기록한다. 그 뒤 helper가 exact directory와 marker를
멱등 생성하고 API receipt를 남긴다. 불명확한 capacity, observation 또는 identity에서는 directory를 만들지
않는다.

Publish는 다음 조건을 모두 만족할 때만 같은 per-volume lock 안에서 bind mount한다.

- Volume이 publish 가능한 phase이고 요청 node가 current owner다.
- current copy identity가 API와 local marker에서 일치한다.
- Pool identity와 local marker/device/inode가 모순되지 않는다.
- 다른 owner 또는 concurrent mount effect가 실제 mount evidence에 없다.

Unpublish도 같은 lock 안에서 kubelet target과 actual mount를 확인해 해제한다. API 오류, node-local 오류 또는
모순이 있으면 성공을 추측하지 않고 재시도한다. Move 관련 모순은 `Blocked`, cleanup 모순은 cleanup
subjournal `NeedsReview`로 수렴한다.

## Planned cold move

```text
Volume(source hold)
  -> Move(destination hold)
  -> source unpublish and observed empty publication fence
  -> exact copy + API receipt
  -> atomic owner/currentCopy commit
  -> destination publish evidence from PublishedNodes and fresh Pool inventory
  -> Move(retained-source hold)
  -> source purge receipt + fresh absence proof
  -> Move completion and source hold release
```

Pre-commit에는 Volume이 source capacity를, Move가 destination capacity를 소유한다. Commit에서는 Volume의
current owner와 `currentCopy`를 destination으로 전환하고 Move가 retained source의 hold를 이어받는다.
Destination publish가 실제 mount로 확인되기 전에는 source cleanup을 시작하지 않는다. Abort도 destination
effect의 API receipt와 fresh absence proof 전에는 destination hold를 해제하지 않는다.

어느 단계에서도 source와 destination의 동시 writable publication을 허용하지 않는다. stale, invalid,
incomplete scan이나 identity contradiction은 promotion과 cleanup을 멈추고 authoritative copy와 모든 관련
hold를 보존한다.

## Delete and cleanup closure

Delete는 Volume을 먼저 fenced 상태로 만들고 신규 publication을 차단한다. Node가 actual unpublish를 다시
확인하고 `publishedNodes`가 비어 있으면 exact target과 executor를 durable delete journal에 결합하고
node-local purge를 실행한다.

Cleanup 완료에는 두 증거가 모두 필요하다.

1. exact operation, executor와 target identity에 결합된 local effect의 API receipt
2. purge 뒤 새 `scanEpoch`가 만든 generation에서 target이 없고 모든 per-copy evidence가 internally
   consistent하다는 fresh, valid, complete inventory

Effect가 끝나고 receipt 게시 전에 process가 실패하면 같은 exact operation을 멱등 재개한다. Receipt 없는
우연한 absence, receipt만 있고 fresh absence proof가 없는 상태, wall-clock timeout은 cleanup 완료나 capacity
release의 근거가 아니다. time-based GC나 capacity release는 금지한다. 두 증거가 모여야 Volume 또는 Move
journal을 종결하고 해당 hold를 해제한다.

## Failure boundary

일시적인 API, process 또는 network 실패는 durable journal과 holds를 유지한 채 재시도한다. 모순되거나
복구 권한을 자동으로 증명할 수 없는 Move는 `Blocked`, cleanup은 `NeedsReview`이며 삭제, owner 전환과
capacity release가 모두 닫힌다.

영구 node/disk 손실의 자동 복구, HA/replication, RWX, snapshot과 hard quota는 0.4 계약 범위 밖이다. 특히
영구 손실을 시간 경과만으로 간주해 copy나 capacity hold를 제거하지 않는다.
