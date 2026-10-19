# 0015. 소실된 Pool은 등록 중단과 저장소 제거를 구분한다

- 상태: Accepted
- 날짜: 2026-10-19
- 관련 계약: [Pool 제거](0011-fail-closed-uninstall-guard.md), [독립 용량](0014-independent-capacity-pools-per-node.md)

## Context

승인 Pool의 마운트가 소실되면 현재 inventory로 물리적 빈 상태를 증명할 수 없다.
기존 볼륨의 publish/delete는 격리했지만, 미확인 peer가 있는 노드의 신규 배치와 Pool 제거는 보류한다.
API 참조가 없다는 사실은 디스크가 비었다는 증거가 아니다.

## Decision

기존 `ShiftPVPool`에 명시적인 등록 중단 상태를 기록한다. 별도 CRD, 서비스, 정리 Job은 추가하지 않는다.

```text
등록됨 → 운영자 등록 중단 요청(정확한 Pool UID) → 신규 배치 중단
           ├─ API 참조·실행자 존재 또는 조회 실패 → 요청 상태 유지
           └─ 참조·실행자 정산 확인 → Controller의 등록 중단 승인
                                      ├─ 다른 정상 Pool의 신규 배치 허용
                                      └─ 원래 Pool CR·마커·데이터 보존
```

- 요청과 승인은 Pool UID를 값으로 가지는 별도 annotation으로 표현한다. 승인 annotation은 기존
  lifecycle approval처럼 Controller만 변경하며, 승인 후 요청 제거로 다시 활성화하지 않는다.
- 기존 Pool UID 잠금 안에서 Volume의 초기 예약/current copy, PV, Move의 preparation·hold·cleanup과
  실행 중인 helper를 확인한다. 참조가 남거나 전체 관측에 실패하면 승인하지 않는다.
- 승인된 Pool은 allocation과 active peer 용량 비교에서 제외한다. 이는 디스크의 독립성이나 빈 상태를
  새로 증명한 것이 아니라 해당 등록이 더 이상 용량을 배정하지 않는다는 뜻이다.
- 중단 의도를 CR과 metrics에서 식별한다. 의도된 중단을 Pool NotReady 장애 경보와 구분하고,
  현재 디스크의 빈 상태나 cleanup 완료로 표현하지 않는다.
- 기존 Pool CR을 기록으로 남긴다. 원래 마커·데이터·마운트에는 쓰지 않고 `IdentityReleased=True`도
  만들지 않는다. 경로 예약도 유지한다. 디스크 복귀만으로 재활성화·재등록·정리하지 않는다.
- 정상 Pool 삭제와 uninstall은 기존 inventory·identity release 조건을 유지한다. 원래 저장소를
  복원하기 전까지 중단된 Pool의 최종 제거 보호는 남는다. 명시적인 삭제 요청이 있다면 원래
  저장소의 빈 상태와 node release를 새로 증명한 뒤 정상 제거한다.

### 검증 기준

| 경우 | 필요한 결과 |
|---|---|
| 잘못된 UID, 이름만 같은 교체 Pool, 승인 위조 | 거부 |
| Volume/PV, 초기 예약, Move preparation/hold/cleanup 존재 | 보류 |
| 실행 중인 source/copy/promote/verify helper 존재 | 보류 |
| 참조 목록 실패, 불명확한 identity | 보류 |
| Pool에 묶이지 않은 legacy 생성 요청·미승인 이동 후보 | 중단을 막지 않음; 새 binding은 요청·UID 잠금으로 차단 |
| 생성·이동 admission과 요청 동시 실행 | 같은 UID 잠금으로 정산; 뒤늦은 새 예약 금지 |
| 승인 직후 응답 유실·Controller 재시작·요청 제거 | 중단 승인 유지; 중복 저장소 작업 없음 |
| 중단 승인 뒤 capacity 조회·경보 관측 | 해당 Pool의 가용 배정량 0; 정상 peer 배치 재개; 중단 의도 식별 |
| 중단 뒤 원래 디스크 복귀 또는 다른 디스크가 같은 경로 사용 | 자동 쓰기·identity 해제·활성화 없음 |
| 삭제 대기 Pool의 등록 중단 승인 | 정상 peer 배치 허용; finalizer와 데이터 유지 |
| 중단된 Pool 경로와 겹치는 신규 등록 | 경로 예약을 유지하여 거부 |

## Alternatives considered

| 대안 | 절충점 |
|---|---|
| 마운트 복원만 지원 | 현재 계약이지만 영구 소실 peer의 배치 보류가 계속됨 |
| inventory 없이 Pool 완전 삭제·identity 해제 | API 참조 부재를 물리적 빈 상태로 오인할 수 있음 |
| 별도 tombstone CRD/서비스 | 기록을 보존하지만 durable API와 제거 순서가 늘어남 |

## Consequences

정상 peer의 배치 재개와 원본 보존을 기존 Pool 객체 하나로 표현한다. 중단된 CR은 남으며, 이 단계는
영구 소실 Pool의 완전 삭제나 사용 중인 손실 볼륨의 복구를 제공하지 않는다. 기능의 검증 기준은 위
동시성·재시작·identity 검사이며, 실제 LVM·디스크 장애와 장시간 qualification은 별도의 실행 증거다.
