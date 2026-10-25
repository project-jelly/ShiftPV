# 0014. 한 node의 독립 용량 단위를 Pool로 등록한다

- 상태: Proposed
- 날짜: 2026-10-04
- 선행 결정: [0005](0005-pool-filesystem-capacity-admission.md), [0006](0006-node-reported-pool-readiness.md), [0013](0013-opt-in-pool-mount-identity.md)

## Context

기존 배치와 용량 계산은 node당 Pool 하나를 가정했다. 같은 node에 이미 마운트된
thick LV나 별도 filesystem을 추가하려면 독립 용량을 Pool별로 구분해야 한다.
서로 다른 경로만으로는 용량 독립성을 보장할 수 없다.

## Decision

- `ShiftPVPool` 하나가 독립 용량 단위 하나를 소유한다. 같은 node에서 함께 사용하는 Pool에
  `capacityPolicy: FixedBlock`을 요구하며 backing 구간과 경로의 중복을 거부한다.
- ext4/xfs의 고정 device, partition, thick LVM linear allocation을 검증한다.
  독립성을 증명할 수 없는 thin/shared backing은 승인하지 않는다. 별도 mount에는
  [RequireMountPoint](0013-opt-in-pool-mount-identity.md)를 권장한다.
- 설정과 실제 backing 검증은 공통 등록 정책을 사용한다. Node는 성공한 등록의
  `status.registrationApproved`를 보존한다. 미승인 후보의 실패는 기존 승인 Pool과
  분리하며, 승인된 peer의 backing 증거가 사라지면 신규 배치를 차단한다.
- `spec.poolGroup`은 StorageClass 선택 범위이며 생략하면 `default`다. class는
  `shiftpv.io/pool-group`으로 그룹을 선택한다. PVC 하나는 수용 가능한 Pool 하나에 고정된다.
- 예약과 생성·이동·삭제 lock은 Pool UID를 사용한다. 게시·삭제·이동 경로는 저장된
  copy identity로 찾으며, 승인된 destination Pool은 재시도에서도 바뀌지 않는다.
- 자동 이동은 같은 그룹의 다른 Ready node로 제한한다. 같은 node의 Pool 간 이동과
  그룹 간 수동 이동은 후속 기능이다.

측정 전후 검증은 [StorageClass contract](../spec/storage-class.md#measurement-verification),
등록과 backing 변경 절차는 [Chart guide](../../charts/shiftpv/README.md#multiple-pools-on-one-node)를 따른다.

## Alternatives considered

| 대안 | 판단 |
|---|---|
| 경로만 다르면 독립 Pool로 허용 | 같은 filesystem이나 thin backing의 용량을 중복 계산한다. |
| node 전체 예약량을 각 Pool의 한도에서 차감 | 독립 Pool의 가용량을 서로 침범하고 정확한 admission을 할 수 없다. |
| StorageClass마다 Pool 객체 하나만 지정 | 여러 node와 같은 node의 여러 독립 단위를 한 class에서 선택할 수 없다. |
| Pool group을 별도 용량 객체로 만듦 | 현재 요구에는 선택 범위만 필요하며 추가 회계 객체를 만들 근거가 없다. |

## Consequences

Pool은 용량과 copy identity를, 그룹은 선택 범위를 표현한다. 용량을 그룹 단위로
합산하거나 PVC를 여러 Pool에 분할하지 않는다. 기존 단일 directory Pool도 사용할 수 있다.

구현과 unit/Kind 검증은 완료했다. 실제 thick LVM·독립 disk의 다중 Pool 검증이 남아
있으므로 상태는 Proposed로 유지한다. 합격 조건은 [Testing](../development/testing.md#multi-pool-qualification)에 있다.
