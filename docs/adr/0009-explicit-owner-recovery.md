# 0009. 현재 owner를 명시적으로 복구한다

- 상태: Accepted
- 날짜: 2026-09-03
- 적용 범위: 0.4 target contract
- 상세 계약: [volume mobility](../spec/volume-mobility.md#blocked-recovery)

## Context

Blocked 이동에는 이전 copy, promotion 또는 cleanup 작업이 남을 수 있다. Commit 뒤 destination write가
가능하므로 실패 시점을 기준으로 source를 선택하면 stale data를 owner로 만들 수 있다. 모순된 identity,
authority, publication, cleanup 증거는 복구보다 먼저 destructive action을 멈춰야 한다.

## Decision

Owner CAS 이후에는 destination을 current owner로 보고 forward recovery만 허용한다. CAS 전에는 source
owner를 보존하고 abort recovery로 수렴한다. 운영자가 복구를 요청하더라도 reconciler는 exact current
copy와 publication authority를 검증한 뒤 같은 owner의 mount만 다시 연다.

Move authority contradiction은 `Blocked`, parent-owned cleanup contradiction은 cleanup subjournal
`NeedsReview`로 기록하고 destructive action을 금지한다. Unknown orphan은 recovery가 삭제하지 않고
bounded inventory에서 report-only로 보존한다. Exact Move intent가 소유한 non-owner copy만
rollback/source cleanup 계약에 따라 정리할 수 있다.

Source rollback은 `Retiring`을 저장한 뒤 destination Pool의 `spec.scanEpoch`를 증가시키고,
API가 반환한 generation을 Move의 `status.rollbackRequiredGeneration`에 기록한다. 다음 reconcile은
동일 Pool UID의 현재 generation이 이 값 이상이고 `status.observedGeneration`과 일치하는
valid·complete inventory만 사용한다. `ObservedAt`은 freshness 판단에만 쓰며, 노드와 controller의
시각 비교로 rollback 이후 관찰임을 증명하지 않는다.

스캔 요청이나 Move 기록의 결과가 불명확하면 durable fence를 다시 읽고, 기록이 없으면 새 스캔을
요청한다. 이미 사본이 없는 경우에도 이 증거가 있어야 capacity hold를 해제하며, 실제 purge 없이
cleanup receipt를 만들어내지 않는다. 이전 버전에서 시작한 recovery도 fence가 없으면 새로 요청한다.

## Alternatives considered

| 대안 | 절충점 |
|---|---|
| 자동 source rollback | destination write 뒤 stale source가 owner가 될 수 있다. |
| 수동 status patch | 실행 중 helper와 filesystem artifact를 함께 조정하지 못한다. |
| recovery에서 non-owner 자동 삭제 | 불완전한 journal이 stale copy에 삭제 권한을 부여할 수 있다. |
| 별도 recovery controller | 한 transaction의 상태 소유자가 둘이 된다. |
| contradiction을 best effort로 해소 | 잘못된 증거에서 promote 또는 purge를 진행할 수 있다. |

## Consequences

명확한 current owner와 접근 가능한 filesystem이 복구의 전제다. 불명확한 authority와 검증되지 않은
artifact는 운영자가 확인할 수 있도록 보존된다. Safe stop은 가용성보다 data authority를 우선한다.
