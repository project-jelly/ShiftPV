# Architecture Decision Records

ADR은 구조적 결정과 그 결과만 기록한다. 동작 계약은 [`spec/`](../spec/README.md), 현재 검증 기준은
[`development/testing.md`](../development/testing.md)가 소유한다.

0005-0011은 0.4 target contract다. 구현과 acceptance evidence가 함께 닫힌 결정만 runtime 보증이 된다.

```mermaid
flowchart LR
    SCOPE[서비스 경계] --> FS[Pool filesystem 경계]
    FS --> CSI[CSI lifecycle]
    CSI --> POOL[Capacity + readiness]
    POOL --> MOVE[계획 이동]
    MOVE --> SAFE[복구 + 관측]
    SAFE --> OPS[제거 + 인증서]
```

| 계층 | ADR | 결정 | 상태 |
|---|---|---|---|
| 제품 | [0001](0001-service-boundary.md) | 기존 filesystem의 로컬 directory를 관리 | Accepted |
| 제품 | [0002](0002-mounted-filesystem-boundary.md) | Pool directory와 host filesystem 책임 분리 | Accepted |
| CSI | [0003](0003-csi-product-foundation.md) | Kubernetes CSI를 제품 인터페이스로 사용 | Accepted |
| CSI | [0004](0004-minimal-csi-bootstrap.md) | 최소 CSI lifecycle 제공 | Accepted |
| Pool | [0005](0005-pool-filesystem-capacity-admission.md) | owner와 Move hold가 모든 물리 copy를 보수적으로 회계 | 0.4 target |
| Pool | [0006](0006-node-reported-pool-readiness.md) | generation-fenced complete Pool observation 사용 | 0.4 target |
| 이동 | [0007](0007-automatic-cordon-volume-mobility.md) | owner CAS를 유일한 commit으로 삼는 cold planned mobility | 0.4 target |
| 이동 | [0008](0008-nondisruptive-mobility-preflight.md) | preflight, quiesce, local publish lock으로 consumer 보존 | 0.4 target |
| 이동 | [0009](0009-explicit-owner-recovery.md) | Move는 Blocked, cleanup은 NeedsReview로 모순을 보존 | 0.4 target |
| 이동 | [0010](0010-operator-visible-mobility-diagnostics.md) | parent journal과 finalizer를 truth로 사용 | 0.4 target |
| 운영 | [0011](0011-fail-closed-uninstall-guard.md) | unresolved Volume, Move, hold가 있으면 uninstall 실패 | 0.4 target |
| 운영 | [0012](0012-controller-managed-webhook-certificates.md) | Controller가 admission 인증서 관리 | Accepted |
| Pool | [0013](0013-opt-in-pool-mount-identity.md) | 기존 directory를 유지하며 전용 mount identity 검증 선택 | Accepted |

모든 ADR은 `Context → Decision → Alternatives considered → Consequences` 순서를 사용한다.
파일명은 `NNNN-kebab-title.md` 형식이며 서비스 경계에서 세부 운영 결정 순으로 정렬한다.
