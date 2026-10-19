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

Identity, Controller, Node 서비스는 provisioning, deletion, capacity 조회, publish와 unpublish에 필요한 CSI RPC만
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

Scan은 물리 경로를 먼저 수집한 뒤 최신 placement 기록과 대조한다. 미등록 경로는 symlink를 따라가지
않고 다시 확인한다. 사라진 생성 stage는 대응하는 serving copy의 identity와 inode를 검증한 경우에만
완료로 처리하며, 그 외 경로 변경은 scan을 invalid로 만들고 다음 관찰로 넘긴다. 원자적 snapshot은 아니다.

## Provision and publish

Provisioning은 선택된 Pool의 exact identity와 fresh, valid, complete inventory를 확인하고 durable
`ShiftPVVolume` intent와 current-owner capacity hold를 먼저 기록한다. Controller는 상주 Node Pod UID를
`creationExecutor`에 고정하고 `NodeCreating`으로 전환한다. Node가 exact directory와 marker를 멱등 생성하고
placement digest를 포함한 `creationReceipt`를 기록하면 Controller가 `Ready`로 전환한다.

Volume 생성과 함께 `spec.initialPool`에 Pool name/UID를 저장한다. 첫 status 쓰기가 실패해도 이 예약을
계산하며, 재시도는 같은 Pool에서 API가 부여한 Volume UID로 copy intent를 완성한다. 구버전의 spec만
남은 객체는 실행 증거가 전혀 없는 경우에만 원래 node·요청 크기로 용량 승인을 다시 받는다.
이때 용량이 부족해도 `Unavailable`로 같은 node에서 기다리며 재스케줄을 요청하지 않는다.
차트의 PVC 생성 요청은 실행 전에 원본의 namespace·name·UID를 `spec.creationClaim`으로 고정한다.
원본 요청이 종료된 미완료 생성은 별도 reconciler가 [생성 취소 계약](source-cleanup.md#initial-creation)에
따라 정산한다. 출처가 불명확하면 추측해 취소하지 않는다. `Ready`는 PV 등록이 지연될 수 있으므로
PVC가 사라지고 PV가 아직 없어도 자동 생성 취소 대상이 아니며 기존 사본과 예약을 보존한다.

승인된 생성의 실행·복구는 동일한 Volume·copy·operation·executor와 보호된 Pool의 최신 backing을
재검증한다. 생성 중간 경로 때문에 inventory가 invalid여도 이 작업은 복구할 수 있으며, 로컬 marker와
inode 검증은 유지한다. 전체 inventory 검사는 새 배치 승인에 계속 적용한다.

Controller는 승인 기록 후 해당 Pod에 gRPC `CreateCopy`를 호출하고, 응답 뒤 API receipt를 검증한다.
Node 내부 RPC `GetCapacity`는 exact Pool UID·measurement evidence·nonce를 확인해 실시간 filesystem 용량을 반환한다.
요청에는 filesystem 경로를 받지 않는다. Node는 API에서 승인된 경로를 읽고 mount identity를 재검증한다.
할당용 측정은 열린 directory의 read-only mount를 거부한다. 대체 statfs helper도 같은 검사를
수행하므로 원본 mount의 접근 모드를 사용한다. 사용량 측정은 read-only bind를 유지한다.

삭제도 Controller가 cleanup journal에 `executor.kind: Node`와 Pod UID를 기록한 뒤 Node가 실행한다.
Node의 purge receipt 이후 Controller가 fresh absence proof를 확인해야 capacity hold를 반환한다.
Node는 capacity 승인, `Ready` 전환, metadata 삭제를 수행하지 않는다.

gRPC `ReclaimCopy`도 같은 journal을 사용한다. RPC와 Watch 복구는 동일한 per-volume 실행 잠금과
filesystem primitive를 공유한다. 응답 유실은 같은 operation ID의 저장된 receipt로 해결한다.
용량 RPC는 최대 2초, effect RPC는 최대 2분이며 Controller의 더 짧은 timeout이 우선한다.

통신은 TLS 1.3과 Controller 전용 projected ServiceAccount 토큰으로 인증한다. Controller는 Kubernetes에서
확인한 정확한 Pod UID의 공개 인증서를 신뢰하고, Node는 TokenReview로 audience·namespace·account를
확인한다. 인증서는 Node 프로세스마다 새로 생성하며 비밀키는 메모리에만 둔다. 연결은 노드별로 재사용하고
Pod UID·주소·인증서 변경 시 교체한다. 실행과 복구의 권한은 API intent와 local proof에 남는다.

Node 재시작은 같은 operation과 local marker/receipt로 재개한다. 실행 중인 Pod를 다른 Pod로 바꾸지 않으며,
종료되거나 사라진 Pod만 receipt 기록 전에 재지정한다. Node에 고정된 작업은 시간 초과 후 helper로 전환하지
않는다. 새 생성·삭제·Move cleanup은 capability를 갖춘 resident Node로만 실행한다. capability가 없으면
intent와 hold를 유지하고 재시도한다. Move copy·verify·promote Job과 측정 helper는 유지한다.
기존 Job 책임은 candidate가 실행·재결합·Node 전환하지 않는다. 호환되는 이전 writer와 helper image에서
먼저 정산해야 한다. 자세한 경계는 [source cleanup](source-cleanup.md)을 따른다.
불명확한 capacity, observation 또는 identity에서는 directory를 만들지 않는다.

Publish는 다음 조건을 모두 만족할 때만 같은 per-volume lock 안에서 bind mount한다.

- Volume이 publish 가능한 phase이고 요청 node가 current owner다.
- current copy identity가 API와 local marker에서 일치한다.
- Pool identity와 local marker/device/inode가 모순되지 않는다.
- 다른 owner 또는 concurrent mount effect가 실제 mount evidence에 없다.

Unpublish는 검증된 kubelet target을 먼저 해제하고, 같은 per-volume lock에서 남은 mount와 API 게시 상태를
정산한다. Pool이 사라지거나 교체되어 identity를 확인할 수 없으면 target 해제는 성공하되 이전 사본의
게시 기록을 보존한다. 게시 정산의 관측 실패는 `Unavailable`, 확인된 권한 모순과 읽기 전용 Publish는
`FailedPrecondition`이다. Move 관련 모순은 `Blocked`, cleanup 모순은 subjournal `NeedsReview`로 수렴한다.

## Planned cold move

Candidate는 `prepared-v1` Move만 아래 순서로 실행한다. Protocol이 없는 기존 기록은 완결된 journal,
Volume 무참조와 executor 종료 증거를 읽어 확인한 뒤 metadata만 정리한다. API reader의 과거 기록 호환은
실행 호환을 뜻하지 않는다. 전환 전 기존 writer에서 정산하고, 새 작업을 차단한 채 candidate의 읽기 전용
preflight를 통과한 뒤 `Recreate`로 교체한다. Preflight는 원자적 snapshot이 아니다.

```text
Volume Ready + activeMove (source hold and publication retained)
  -> Move logical preparation hold + destination placement
  -> durable StartingDisruption -> source unpublish and observed empty publication fence
  -> quiesced source usage + fresh destination space -> execution approval
  -> exact copy + API receipt
  -> atomic owner/currentCopy commit
  -> destination publish evidence from PublishedNodes and fresh Pool inventory
  -> Move(retained-source hold)
  -> source purge receipt + fresh absence proof
  -> Move completion and source hold release
```

Pre-commit에는 Volume이 source capacity를, Move가 destination capacity를 소유한다. Commit에서는 Volume의
current owner와 `currentCopy`를 destination으로 전환하고 Move가 retained source의 hold를 이어받는다.
서비스를 계속할 때는 destination publish가 실제 mount로 확인되기 전에는 source cleanup을 시작하지 않는다.
원본 PVC UID가 끝난 경우는 별도의 [workload retirement](volume-mobility.md#workload-retirement) 증거를 요구한다. Abort도 destination
effect의 API receipt와 fresh absence proof 전에는 destination hold를 해제하지 않는다.

어느 단계에서도 source와 destination의 동시 writable publication을 허용하지 않는다. stale, invalid,
incomplete scan이나 identity contradiction은 promotion과 cleanup을 멈추고 authoritative copy와 모든 관련
hold를 보존한다. 승인 기록에 완전한 목적지 계획이 없으면 review 상태로 보존하며, copy identity나
cleanup 권한을 추론하거나 추론한 계획으로 scan하지 않는다.

## Delete and cleanup closure

`NodeUnpublishVolume`은 target 해제 후 실제 backing identity와 남은 mount reference를 잠금 안에서
확인하여 publication 상태를 기록한다. 다른 볼륨의 삭제가 `scanEpoch`를 올려 용량 관측이 잠시
오래되어도 이 해제는 진행한다. 새 publish의 최신 용량 관측과 삭제 완료의 fresh absence proof는
계속 필요하다. 디스크·Pool·copy identity나 mount 관측이 불명확하면 publication 상태를 해제하지 않는다.
이 정산은 기존 volume lock을 읽기 모드로 열어 동일한 flock에 참여하므로 backing이 read-only여도
진행한다. 잠금 파일이 없는 writable Pool에서는 기존처럼 초기화하며, 잠금 경합이나 관측 오류를
성공으로 처리하지 않는다. 이 경로는 새 publish나 디스크 cleanup에 쓰기 권한을 부여하지 않는다.

정확한 Ready copy가 아직 게시 중이면 `DeleteVolume`은 `Unavailable`로 unpublish를 기다린다. 이때 삭제 fence·cleanup·예약 반환을 시작하지 않는다. copy/UID/owner 불일치, active Move, 잘못된 phase는 계속 `FailedPrecondition`으로 거부한다.

첫 status 쓰기 전의 보호된 spec-only 생성 의도는 아직 copy·operation·executor를 승인하지 않았다.
`DeleteVolume`은 이 상태를 UID·resourceVersion이 일치하는 DELETE로 취소한다. 초기화가 먼저 이기면
DELETE가 충돌하고, 삭제 timestamp가 먼저 저장되면 이후 초기화는 거부된다. 디스크 receipt를 만들지
않으며 다른 finalizer가 있으면 보호와 예약을 유지한다. 객체가 삭제된 뒤에만 예약이 사라진다.
`Pending`·`NodeCreating`의 부분 실행 정리는 별도 경계이며 이 예외로 삭제하지 않는다.

승인된 초기 사본의 부분 실행은 생성 전용 CAS로 취소한다. 생성 완료가 먼저 이기면 Ready를
보존하며, 취소가 먼저 이기면 `CreationCancelled` journal의 영수증과 최신 부재 증명을 확인한 뒤
예약을 해제한다. CSI 삭제·자동 정산·명시적 복구 명령은 같은 생성 취소 기능을 사용한다.

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

## Node API permissions

Node의 Pod update는 capability·RPC 공개 인증서 광고에 사용한다. Chart는 Pod-bound token의 Pod name·UID가
수정 대상과 같은지, 실행 사양과 그 외 metadata가 그대로인지 Kubernetes ValidatingAdmissionPolicy로 검사한다.
Node ServiceAccount를 Controller·helper와 공유하면 chart가 배포를 거부한다. 정책은 RPC 비활성화 때도 적용한다.

Node의 cluster-wide Volume/Move status 권한은 생성 영수증, native cleanup 영수증과 publication 기록에
필요하다. 이 권한이나 privileged host 접근이 불필요해졌다는 의미는 아니다. Node는 신뢰된 저장소 실행자이며,
이번 Pod 정책은 status 전체를 필드별로 격리하거나 침해된 host를 격리하는 경계가 아니다.

Pod token의 name·UID는 [Kubernetes authentication metadata](https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/#additional-metadata-in-pod-bound-tokens)를 사용한다.

## Failure boundary

일시적인 API, process 또는 network 실패는 durable journal과 holds를 유지한 채 재시도한다. 모순되거나
복구 권한을 자동으로 증명할 수 없는 Move는 `Blocked`, cleanup은 `NeedsReview`이며 삭제, owner 전환과
capacity release가 모두 닫힌다.

영구 node/disk 손실의 자동 복구, HA/replication, RWX, snapshot과 hard quota는 0.4 계약 범위 밖이다. 특히
영구 손실을 시간 경과만으로 간주해 copy나 capacity hold를 제거하지 않는다.
