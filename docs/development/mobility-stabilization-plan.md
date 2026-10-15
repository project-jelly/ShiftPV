# Mobility stabilization plan

상태: 2026-10-15 1단계 [PR #199](https://github.com/project-jelly/ShiftPV/pull/199),
2A 저장 계약 [PR #200](https://github.com/project-jelly/ShiftPV/pull/200) 머지.
2B-1 용량/owner 경계 [PR #201](https://github.com/project-jelly/ShiftPV/pull/201) 머지.
2B-2 준비 실행·취소 연결과 통합 검증 [PR #202](https://github.com/project-jelly/ShiftPV/pull/202) 머지.
3단계 자기 backing 검증과 신규 할당 조건 분리 [PR #203](https://github.com/project-jelly/ShiftPV/pull/203) 머지.
4단계 Move cleanup의 resident Node 실행 [PR #204](https://github.com/project-jelly/ShiftPV/pull/204) 머지.
5A 단계 Volume별 queue와 Pool 잠금 경합 시 재시도 [PR #205](https://github.com/project-jelly/ShiftPV/pull/205) 머지.
5B-1 단계 cleanup의 짧은 관측과 Job 정산 재진입 보강 진행 중.
현재 계약은 [volume mobility](../spec/volume-mobility.md)가 소유한다.

근거: NoteGate `daily:/research/2026/10/2026-10-15-shiftpv-two-node/02-basic-results-2026-10-15.md`.
0.6.12 외부 재시험에서는 capacity tracking을 켠 022·023 개선이 확인됐다.
031–037의 남은 경계는 아래 순서로 다룬다.

## 최종 목표

지원하는 planned cold mobility에서 조건 변경·응답 유실·재시작 후에도 데이터 권한,
Pod 배치, 용량 예약과 cleanup이 같은 결론으로 수렴하게 한다.

- 목적지의 논리 용량 예약을 얻기 전에는 기존 consumer를 중단하지 않는다.
- gate·예약·실행 기록마다 유지, 재시도, 취소, 완료를 판정하는 책임이 있다.
- 한 Pool 또는 Volume의 대기가 독립 작업의 진행을 막지 않는다.
- 모순된 증거는 보존하고, 일시적인 실패는 현재 증거를 재관측해 복구한다.

owner CAS, exact copy identity, checksum/fsync, API purge receipt와 그 이후 fresh absence
조건을 유지한다. 영구 디스크 손실 복구와 모든 이동 도중 drain의 성공은 범위 밖이다.

## 구현 순서와 완료 조건

각 단계는 회귀 테스트·독립 리뷰를 갖춘 별도 변경 단위로 진행한다.

진행 규칙: 구현 → 테스트 → PR → 독립 리뷰·CI → 발견 이슈 수정 → 머지 → 최신 main에서 다음 단계.
차단 이슈나 필수 검사 실패가 남으면 다음 단계로 넘어가지 않는다. 마지막 통합 검증과 릴리즈
준비 보고까지가 이번 목표이며, 실제 이미지·Helm 릴리즈와 클러스터 배포는 포함하지 않는다.
릴리즈를 자동 실행하는 component/Chart 버전 증가는 마지막 승인 전까지 하지 않는다.

| 순서 | 문제와 책임 | 완료 조건 |
|---|---|---|
| 1 | **037: Move 생성 전 남은 Pod gate**. 기존 controller가 owner 배치를 재관측한다. | cordon → admission hold → Move 생성 전 uncordon 후 같은 Pod가 자동 해제된다. 정확한 binding/copy/UID, 정상 owner와 작업 부재를 요구하며 사용자 gate·제약을 보존한다. |
| 2 | **034: 목적지 준비와 원본 중단 분리**. Move가 durable 준비 예약과 취소를 소유한다. | 남은 5 GiB에 2 GiB 이동 4개를 요청하면 승인받지 못한 작업의 원본 consumer는 유지된다. 응답 유실·취소 후 예약은 한 번만 계상하고 정산한다. 035의 binding/consumer 복구 절차도 문서화한다. |
| 3 | **031: 자기 backing 검증과 새 할당 가능성 분리**. Pool 관측을 작업별 허용 조건으로 해석한다. | B의 mount 유실이 독립된 정상 A의 기존 copy 사용·정리를 막지 않는다. 독립성을 증명하지 못한 새 할당은 계속 거부한다. |
| 4 | **036: 신규 Move cleanup의 resident Node 실행**. 기존 journal·RPC·local receipt를 확장한다. | executor 교체와 응답 유실에도 같은 operation으로 재개하고 receipt와 absence 전에는 hold를 해제하지 않는다. 기존 Job-bound journal은 기존 경로로 정산한다. |
| 5 | **033·032: 진행 격리와 측정 기반 성능 개선**. 짧은 reconcile, Volume별 queue와 제한된 worker를 도입한다. | A의 대기 중 B가 진행하고 동일 Volume은 직렬화된다. copy·검증·startup·queue 시간을 분리 측정하며 데이터 검증을 유지한다. |

1단계는 기존 admission의 owner pin/hold 동작을 유지하고 복구 경로를 추가한다.
실제 공통 판정이 생기는 범위에서만 helper를 공유하며, 새 범용 policy/transaction framework를 만들지 않는다.

## 책임 경계

```text
Admission                 Pod 생성 시 owner pin 또는 hold
    |
Events + periodic relist
    |
Volume별 처리              동일 Volume 직렬화, 다른 Volume 진행 (5단계)
    +-- Owner placement    Move 없는 hold의 현재 owner 복구 (1단계)
    +-- Move               준비 → 중단 → 실행 → owner commit → 정산 (2단계)
          +-- Pool 판정    기존 copy 작업 / 새 할당 조건 구분 (3단계)
          +-- Node 실행    승인된 효과, local receipt, 재시작 복구 (4단계)
          +-- Copy Jobs    복사·검증·promotion
```

Queue는 admission이나 외부 API 쓰기를 직렬화하지 않으므로, 현재 증거 재조회·CAS·
NodePublish 권한 검사가 필요하다.

5단계는 네 PR 단위로 나눈다. 5A는 Volume별 queue·제한된 worker와 Pool 잠금 경합의
즉시 재시도로 진행을 격리한다. 5B-1은 cleanup, 5B-2는 source 사용량 측정을 관측 가능한
짧은 단계로 바꿔 worker를 반환한다. 5C는 동일 이미지·workload의 copy 측정과
중단·재시도·metadata 검증을 통과한 성능 변경만 채택한다.

## 준비 예약의 설계 조건

2단계는 단순 사전 용량 조회로 끝내지 않는다.

다음 변경 단위로 순서대로 검증한다.

1. **2A 저장 계약**: immutable `prepared-v1` protocol, 정확한 UID/resourceVersion CAS,
   준비 hold의 공통 용량 계상과 Pool 삭제 보호, 실제 API 서버에서 CRD 검증.
   이 단계는 새 protocol을 생성하거나 실행하지 않으며, 발견한 기록도 기존 실행·복구·GC 경로로 넘기지 않는다.
2. **2B-1 용량 조회와 owner 전환**: `ListVolumes`가 commit 전 원본 owner를 읽고
   `ListMoves`가 cleanup 후 완료를 읽으면 목적지 예약을 놓칠 수 있다. 실제 Registry·공유
   Pool 잠금의 경합 테스트를 먼저 만들고, owner CAS가 destination Pool의 용량 승인과
   직렬화되게 한다. `Volumes → Moves` 조회 순서를 유지하면 source 유출은 보수적으로
   계상되므로 추가 잠금이 필요 없다. 이 수정은 별도 PR로 먼저 검증한다.
3. **2B-2 실행 연결**: 준비 → 예약 → 중단 → 실행 승인 전환과 구간별 취소를 연결한다.
   034의 consumer 유지 회귀·장애 주입·통합 시험과 035 복구 절차까지 완료해야 2단계가 끝난다.

- 준비 중 Volume은 `Ready + activeMove`로 원본 서비스를 유지한다.
- Pool UID 잠금 안에서 최신 ledger를 읽고 durable logical hold를 저장한다.
- scheduler 배치와 hold를 확인한 뒤 중단 시작 단계를 먼저 기록하고 `Moving`/eviction을 수행한다.
- quiesce 후 source 사용량과 목적지 공간을 재검증한다. 준비 hold와 실행 승인은 원자적으로 전환하며 이중 계상하지 않는다.
- 취소는 중단 전, 중단 후·디스크 승인 전, 승인 후·commit 전, commit 후의 네 구간으로 구분한다.
- 기존 `Ready + activeMove`에는 commit 후 상태도 있으므로 단계별 의미를 명시한다.
- 새 상태 전이는 예상 단계·Move/Volume/Pool identity를 비교한다. 새 CRD 필드와 legacy Move 호환을 함께 처리한다.

logical hold는 외부 writer의 공간 소비까지 막는 hard quota가 아니다.
원본 사용량 증가·목적지 장애·외부 writer의 공간 소비는 중단 후 재검증에서도 실패할 수 있다.
적용 순서는 CRD → 새 protocol을 이해하는 controller → 새 protocol 활성화다.
protocol이 없는 기존 Move는 기존 경로로 정산한다. 새 protocol 기록이 남아 있는 동안에는
이를 이해하지 못하는 controller와의 동시 실행이나 구버전 downgrade를 허용하지 않는다.

## 검증과 완료 판정

| 관점 | 필수 증거 |
|---|---|
| 안전성 | 잘못된 UID·binding·owner·copy 또는 모순된 증거로 권한이 확대되지 않는다. |
| 진행성 | 조건이 회복되면 같은 Pod/Move가 재관측으로 진행한다. 수동 Pod 재생성이 필요하지 않다. |
| 동시성 | stale read·CAS 충돌·응답 유실에서도 예약 중복과 이전 executor의 권한 재사용을 막는다. |
| 기록 수명 | 실제 책임이 끝나기 전 gate·hold·journal을 버리지 않고, 완료 후에는 남기지 않는다. |

기존 API client·Repository·capacity probe·clock 주입 지점을 사용한다. 단위 판정과 함께 실제
admission/Registry/lock을 연결한 경계 테스트를 남긴다. 상세 gate는 [testing](testing.md)을 따른다.

코드·로컬 테스트 완료, CI, merge, 이미지/Chart release, 클러스터 배포, 외부 재시험을 구분한다.
1단계 완료를 031–037 전체 해결로 보고하지 않는다. 성능 향상률은 변경 전후 같은 workload를
측정한 뒤 정하며, 단계마다 새 추상화의 수보다 남은 실패 경계와 운영 결과로 채택 여부를 판단한다.
