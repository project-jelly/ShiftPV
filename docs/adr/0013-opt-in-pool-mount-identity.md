# 0013. 기존 directory Pool을 유지하고 전용 mount 검증을 선택한다

- 상태: Accepted
- 날짜: 2026-10-04
- 선행 결정: [0002](0002-mounted-filesystem-boundary.md), [0006](0006-node-reported-pool-readiness.md)

## Context

기존 Pool은 root filesystem 아래의 일반 directory도 허용한다. 별도 filesystem이 이미
마운트된 경로를 Pool로 등록할 때는 마운트가 빠져도 그 아래 directory가 접근 가능해서
기존 directory/write/statfs probe만으로는 잘못된 filesystem에 쓰는 일을 막을 수 없다.

## Decision

`spec.mountPolicy: RequireMountPoint`를 명시한 Pool은 Node가 `/host`에 전파된 mount
table에서 경로 자체가 mount point인지 확인한다. filesystem root가 아닌 bind mount와
부모 filesystem과 같은 device의 mount는 거부한다. 첫 성공 관찰의 device 번호,
mount root, source, filesystem type을 `status.mountIdentity`에 고정한다. 이후 값이
달라지거나 mount가 없어지면 `Mounted=False`, `Ready=False`, invalid inventory로
기록하고 새 배치 및 cleanup proof에 사용하지 않는다. Node는 mount 검증이 실패한
경로에 probe 파일을 쓰거나 inventory를 스캔하지 않는다.
Node publish와 생성·이동·정리 helper도 작업 직전에 기록된 mount identity를
실제 경로 또는 helper의 bind mount에서 재확인한다. helper의 반복 authority 검사에도
이 검사를 포함한다.

정책을 생략한 Pool은 기존 일반 directory 계약을 유지한다. `mountPolicy`는 Pool
생성 후 변경할 수 없다. Disk 준비, filesystem 생성, mount 관리는 운영자 책임이다.

## Alternatives considered

| 대안 | 판단 |
|---|---|
| 모든 Pool에 mount point 강제 | 기존 directory 기반 설치를 중단시킨다. |
| 경로 존재와 statfs만 검사 | mount 유실 뒤 부모 filesystem으로의 fallback을 구분하지 못한다. |
| mount ID만 영구 identity로 사용 | 정상 remount와 재부팅에서도 ID가 달라질 수 있다. |

## Consequences

이 검사는 한 경로의 mount 유지와 관찰된 논리적 filesystem identity를 확인한다.
서로 다른 mount가 물리 용량까지 독립됐다는 증거는 아니다. 두 bind alias, 같은 thin
pool을 쓰는 LV, 같은 quota를 공유하는 외부 mount는 별도 capacity Pool로 취급하면
안 된다. 다중 Pool 등록 단계에서는 같은 device의 중복 등록을 거부하고, 공유
backing은 별도의 capacity-domain 회계가 생기기 전까지 지원하지 않는다.
독립 할당된 thick LV의 filesystem이나 전용 block filesystem은 각각 별도 capacity
단위가 될 수 있다. 외부 mount는 backend가 별도 quota/용량을 보장한다는 운영 증거가
있을 때만 별도 Pool 후보로 삼는다.

device 번호나 source가 바뀐 remount는 자동으로 재승인하지 않는다. 운영자는 원본
데이터와 mount를 확인한 뒤 복구해야 한다. 경로와 mountinfo 검사는 호출 시점의
관찰이다. mount 변경과 실제 filesystem 작업 사이의 경합을 완전히 제거하려면
열린 디렉터리 fd에 대한 device 확인이 필요하다. Pool별 operation 경로 검증은
다중 Pool 수명주기 작업에서 별도로 닫아야 한다.
