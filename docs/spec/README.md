# ShiftPV Contracts

현재 구현의 동작과 운영 경계를 설명한다. 검증 수준과 합격 조건은
[Testing](../development/testing.md)에 있다.

| 문서 | 단일 책임 |
|---|---|
| [csi-driver.md](csi-driver.md) | CSI RPC, topology, Pool 관측과 publish 권한 |
| [storage-class.md](storage-class.md) | StorageClass, placement와 보수적 capacity admission |
| [volume-mobility.md](volume-mobility.md) | 계획된 cold move, owner commit과 장애 수렴 |
| [source-cleanup.md](source-cleanup.md) | parent journal, exact-copy 삭제와 GC 경계 |
| [metrics.md](metrics.md) | 운영 관측 신호와 freshness; 권한 증거와의 경계 |

`MUST`, `MUST NOT`, `SHOULD`는 구현과 테스트가 따라야 할 요구사항을 뜻한다.

## Product boundary

| 영역 | 0.4 계약 |
|---|---|
| Volume | Linux filesystem의 node-local RWO PVC |
| Pool | 운영자가 준비한 absolute non-root host directory; 다중 Pool/node는 FixedBlock 검증 필수 |
| Placement | `WaitForFirstConsumer`; 현재 owner node에만 publish |
| Mobility | source와 destination이 다시 사용 가능해지는 계획된 cold move |
| Consistency | 일시 장애와 재시작 뒤 동일 transaction이 eventually converge |
| Authority | 정확히 한 Volume owner; owner compare-and-swap만 commit |
| Durable APIs | `ShiftPVPool`, `ShiftPVVolume`, `ShiftPVMove` |
| Cleanup | Volume/Move에 내장된 journal과 finalizer가 의무를 보존 |
| GC | 알려진 transaction garbage만 자동 정리; unknown orphan은 report-only |
| Capacity | 물리 copy와 미완료 reservation을 보수적으로 모두 계산 |

다음 기능은 제품 경계 밖이다.

- 영구적으로 유실된 authoritative node 또는 disk에서의 데이터 복구
- replication, HA failover, remote shared storage, RWX
- snapshot, backup, raw block, online expansion
- hard per-volume filesystem quota와 application I/O 성능 보증

## Non-negotiable invariants

1. authoritative copy가 확인되지 않은 상태에서 owner를 바꾸거나 해당 copy를 삭제하지 않는다.
2. NodePublish/NodeUnpublish와 cleanup effect는 exact volume의 node-local lock과 API publication fence를 지킨다. Pool scanner는 그 critical section 밖의 관찰자다.
3. stale, invalid, incomplete inventory는 allocation과 destination publication 판정을 승인하지 않는다. Cleanup release는 wall clock이 아니라 post-receipt generation fence와 valid·complete absence를 필요로 한다.
4. partial copy는 검증 receipt 전까지 promotion 또는 owner commit에 사용할 수 없다.
5. commit 전 실패는 source로 abort하고, commit 후 실패는 destination으로만 수렴한다.
6. destination의 실제 publish 증거 전에는 source cleanup을 시작하지 않는다.
7. cleanup은 durable intent, exact executor, local receipt, API receipt, fresh absence proof 순서를 지킨다.
8. API receipt와 fresh absence proof가 모두 있어야 cleanup 의무와 capacity hold를 해제한다.
9. Parent-owned identity contradiction은 Move `Blocked` 또는 cleanup `NeedsReview`로 격리한다. Intent가 없는 path와 unknown orphan은 report-only이며 자동 삭제하지 않는다.
10. timestamp와 metric은 진단 정보일 뿐 destructive authority가 아니다.

## Acceptance

계약의 검증 계층과 실행 명령은 [Testing](../development/testing.md)이 소유한다. 구조적 이유는
[ADR](../adr/README.md), 설치와 운영 절차는 [Helm chart guide](../../charts/shiftpv/README.md)가 소유한다.
