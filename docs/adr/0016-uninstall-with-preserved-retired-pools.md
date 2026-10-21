# 0016. 중단된 Pool 기록을 보존하고 Driver를 제거한다

- 상태: Accepted
- 날짜: 2026-10-21
- 관련 계약: [제거 보호](0011-fail-closed-uninstall-guard.md), [등록 중단](0015-retire-unavailable-pool-registration.md)

## Context

영구 소실된 저장소는 fresh inventory를 만들 수 없다. 등록 중단 승인 뒤 API 의무가 모두 정산돼도
Driver uninstall이 막힌다. API 참조 부재를 물리적 빈 상태나 Pool 완전 삭제의 근거로 삼을 수는 없다.

## Decision

기본 제거 조건은 유지한다. 운영자가 `--preserve-retired-pool=<exact UID>`를 반복 지정한 경우에만
해당 Pool의 물리 관측을 보존된 신원 기록으로 대체한다. Helm은 기본값이 빈 배열인
`lifecycle.preserveRetiredPoolUIDs`를 guard에 전달한다.

- 현재 UID가 일치하고, 등록 및 중단 승인이 있으며, protection finalizer와 유효한 경로가 남아야 한다.
  존재하지 않는 UID, 교체된 Pool, 승인되지 않은 Pool, 불명확한 조회는 거부한다.
- quiesce 뒤 전체 PVC/PV/Volume/Move/hold/cleanup 의무를 검사한다. 중단 승인을 재사용해 현재 검사를
  생략하지 않는다. 기존 retirement 검사를 재사용하여 helper Pod와 Pod가 없는 nonterminal Job도 확인한다.
- finalizer 해제 직전 같은 분류와 quiesce 관측 경계로 다시 검사한다. 보존 대상은 해제하지 않는다.
  이미 Terminating인 Pool도 같은 UID와 finalizer를 남긴다. 정상 Pool은 기존 inventory 조건을 따른다.
- 기존 Pool CR이 durable 기록이다. permit ConfigMap의 TTL이나 CSIDriver owner에 기록을 맡기지 않는다.
  CRD·finalizer·경로 예약·마커·데이터를 보존하며 `IdentityReleased`나 물리적 부재를 만들지 않는다.
- 재설치도 기존 중단 승인과 경로 예약을 유지한다. 저장소의 실제 복원 및 기존 exact identity release
  절차 없이는 완전 삭제나 같은 경로의 새 Pool 등록을 허용하지 않는다.

이 결정은 ADR 0015의 기본 uninstall 차단에 명시적인 기록 보존 경로를 추가한다. Pool 완전 삭제,
사용 중인 손실 볼륨 복구, 강제 정리는 제공하지 않는다.

## Alternatives considered

| 대안 | 절충점 |
|---|---|
| inventory 없는 Pool 삭제 | 신원과 경로 예약을 잃음 |
| 별도 tombstone CRD 또는 영구 permit | durable protocol과 수명주기가 늘어남 |
| 원본 복원 전까지 항상 uninstall 차단 | 안전하지만 Driver의 종료가 불가능할 수 있음 |

## Consequences

운영자는 보존할 UID를 명시해야 한다. 제거 뒤에도 CR과 finalizer가 남는 것은 정상이다. 허용 검사와
finalizer 해제가 같은 분류를 사용하며, 현재 의무·실행자·조회 오류가 있으면 계속 fail closed한다.
