# 0014. 한 node의 독립 용량 단위를 Pool로 등록한다

- 상태: Proposed
- 날짜: 2026-10-04
- 선행 결정: [0005](0005-pool-filesystem-capacity-admission.md), [0006](0006-node-reported-pool-readiness.md), [0013](0013-opt-in-pool-mount-identity.md)

## Context

현재 `ShiftPVPool`은 node당 하나만 배치 대상으로 사용할 수 있다. 용량 예약과
`statfs` helper, NodePublish, Move의 source/destination 및 자동 이동 후보가 node
하나에 Pool 하나가 있다는 가정을 공유한다. 이미 마운트된 thick LV나 별도 filesystem을
같은 node에 추가하려면 이 가정을 동시에 바꿔야 한다. 경로가 다르다는 사실만으로
용량 독립성을 보장할 수는 없다.

## Decision

`ShiftPVPool` 한 개는 독립 할당된 용량 단위 한 개다. 서로 다른 `mountPath`를 가진
Pool 두 개를 같은 node에 등록할 수 있지만, 두 Pool이 같은 filesystem device 또는
알려진 공유 backing을 쓰면 배치에 사용하지 않는다. 겹치거나 중첩된 경로도 거부한다.
새로 추가하는 Pool에는 [0013](0013-opt-in-pool-mount-identity.md)의 mount 검증을
요구한다. 기존 directory Pool은 node의 기존 filesystem 용량 단위로 남을 수 있으나,
새 Pool과 실제 용량을 공유하면 함께 배치할 수 없다. 복잡한 LVM thin pool, Btrfs
subvolume, 외부 스토리지처럼 backing 독립성을 경로에서 증명할 수 없는 경우에는
지원하는 증거 또는 별도 capacity-domain 회계가 생기기 전까지 독립 Pool로 승인하지
않는다.

Pool의 `spec.poolGroup`은 StorageClass 선택 범위다. 생략하면 `default`이며 기존
`shiftpv`와 `shiftpv-retain`은 그 그룹을 사용한다. StorageClass의 `shiftpv.io/pool-group`
parameter로 다른 그룹을 지정할 수 있다. 같은 그룹의 여러 Pool이 같은 node에
있으면 요청량, 논리 예약 여유, 실제 filesystem 여유를 모두 충족하는 Pool 하나를
선택하고 그 Pool name/UID를 Volume copy identity에 고정한다. 서로 다른 그룹이면
StorageClass가 명시적으로 선택한다. 그룹은 용량을 합산하는 회계 단위가 아니다.

예약, 직렬화 lock, `statfs` 및 helper hostPath는 node가 아닌 정확한 Pool UID를
사용한다. 기존 Volume의 publish/unpublish/delete와 Move의 source/target은 저장된
copy identity로 Pool을 찾는다. 자동 계획 이동은 같은 `poolGroup`의 다른 Ready
Pool만 후보로 삼고, destination Pool UID를 Move에 기록해 재시도 시 바꾸지 않는다.
같은 그룹에 다른 node의 후보가 없으면 이동 가능한 곳이 없는 것으로 취급한다.
서로 다른 그룹 사이의 수동 이동은 별도 결정으로 다룬다.

등록, 조회, readiness, metrics, 삭제 finalizer는 Pool별로 수렴한다. 특정 Pool이
NotReady여도 같은 node의 다른 Pool에 대한 관찰과 기존 복사본 처리를 막지 않는다.
Pool 선택 근거나 용량 독립성이 불명확하면 새 배치와 위험한 이동을 보류한다.

## Alternatives considered

| 대안 | 판단 |
|---|---|
| 경로만 다르면 독립 Pool로 허용 | 같은 filesystem이나 thin backing의 용량을 중복 계산한다. |
| node 전체 예약량을 각 Pool의 한도에서 차감 | 독립 Pool의 가용량을 서로 침범하고 정확한 admission을 할 수 없다. |
| StorageClass마다 Pool 객체 하나만 지정 | 여러 node와 같은 node의 여러 독립 단위를 한 class에서 선택할 수 없다. |
| Pool group을 별도 용량 객체로 만듦 | 현재 요구에는 선택 범위만 필요하며 추가 회계 객체를 만들 근거가 없다. |

## Consequences

`ShiftPVPool`과 `poolGroup`의 책임을 분리한다. Pool은 용량과 복사본 identity를
소유하고 그룹은 선택 범위만 표현한다. 기존 하나의 Pool/node 설치와 기본
StorageClass의 동작은 유지한다. 다중 Pool을 실제로 열기 전에는 Pool별 예약,
헬퍼·publication 경로, Move recovery, topology, 용량 독립성 거부 검사를 함께
검증해야 한다. 이 결정은 replication, HA, RWX 또는 그룹 간 자동 이동을 추가하지
않는다.

### Implementation progress

Pool group 선택, 정확한 copy identity 기반 생성·게시 경로, Pool UID 기준 용량
예약을 단계적으로 연결한다. Move의 source usage, destination admission, 복사·승격
helper, scanner publication proof와 recovery도 정확한 Pool incarnation을 따른다.
생성과 이동은 같은 Pool UID lock으로 예약과 durable intent를 함께 기록한다.
승인된 Move는 재시도에서 다른 Pool로 바뀌지 않는다.

현재 다중 Pool 등록 제한은 유지한다. 독립 용량 검증과 Pool별 metrics를 연결하고
실제 다중 Pool E2E를 통과한 뒤 등록 제한을 해제한다. 서로 다른 그룹 또는 같은
node 안의 수동 이동은 이 단계의 구현 범위에 포함되지 않는다.
