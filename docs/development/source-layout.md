# Source Layout

현재 소스의 책임과 의존 방향을 설명한다.

## Boundaries

```text
cmd wiring
   │
   ├── CSI adapters ───────────────┐
   ├── Pool / Volume / Move loops ─┼── Kubernetes repositories
   └── metrics / admission ────────┘
                    │
               pure protocol
          identity · state · capacity
                    │
               node-local effects
          lock · inventory · copy · purge
```

## Current tree

```text
src/
├── cmd/                    process entrypoints and the node-bound helper CLI
├── csi/                    Controller, Identity, Node and gRPC adapters
├── kubernetes/             durable API repositories, helper authority, and helper Pod execution
│   ├── cleanupapi/         parent-owned cleanup journal
│   ├── helperauth/         node-bound helper authority predicates
│   ├── helperpod/          exact child executor lifecycle
│   └── volumeapi/          Pool, Volume and Move persistence
├── lifecycle/              admission, Pool lifecycle and uninstall guards
├── mobility/
│   ├── admission/          workload mutation boundary
│   ├── controller/         observe, decide and execute one Move action
│   └── fsm/                pure Move state transition rules
├── node/                   mount, inventory and identity-bound filesystem effects
├── pool/                   readiness and conservative capacity accounting
├── metrics/                cached observation only
├── volume/                 pure IDs, copy identity and path rules
└── webhook/                serving certificate lifecycle

test/
├── model/                  exhaustive state/invariant checks
├── integration/            Linux mount boundary
├── e2e/kind/               isolated Kubernetes fault injection
└── e2e/real-node/          service, reboot and soak qualification
```

| 책임 | 규칙 |
|---|---|
| `cmd/controller`, `cmd/node`, `cmd/uninstall-guard` | flag, dependency wiring, process lifecycle만 소유 |
| `cmd/volume-helper` | CLI, 측정 전후 검증과 node-local effect 호출; Move/cleanup 승인은 `kubernetes/helperauth` 사용 |
| CSI | RPC validation과 protocol command 변환; filesystem effect 직접 실행 금지 |
| Kubernetes repositories | Pool/Volume/Move read, status patch, resourceVersion CAS, child executor 생성 |
| Pure protocol | 외부 I/O 없는 state decision, identity comparison, capacity holds |
| Pool loop | readiness와 요청 generation에 대한 valid·complete inventory 게시 |
| Volume loop | provision/delete lifecycle, current owner, deletion journal와 finalizer |
| Move loop | cold move, 단일 owner CAS, rollback/source cleanup journal와 finalizer |
| Node-local effects | per-volume lock 아래 publish/unpublish와 exact copy/promote/purge/receipt effect 실행 |
| Metrics | cached observation; protocol decision에 입력하지 않음 |
| Admission | 사용자가 소유한 workload를 사전 점검하고 unsafe mutation/delete를 fail closed |

## Dependency rules

1. Pure protocol package는 Kubernetes client, filesystem, clock, goroutine을 import하지 않는다.
2. Reconciler는 먼저 immutable observation snapshot을 만들고, 한 번 결정한 뒤, exact action 하나만 실행한다.
3. Filesystem path는 node-local effect 계층에서 validated identity로만 계산한다. API가 arbitrary path를 받지 않는다.
4. Child Job 이름이나 성공 상태는 receipt가 아니다. Parent journal의 intent와 exact executor UID가 일치해야 한다.
5. NodePublish/NodeUnpublish와 cleanup filesystem effect는 per-volume local lock 규약을 사용한다. Pool
   scanner는 lock 밖에서 marker와 mount reference를 관찰하며, generation-fenced consumer만 그 결과를
   causal proof로 사용할 수 있다.
6. Capacity 계산은 Volume owner hold와 Move temporary hold의 합으로만 도출한다. 별도 mutable reservation source를 두지 않는다.
7. Wall clock은 retry, timeout, metric에만 사용하고 owner 변경·삭제·hold 해제 권한에 사용하지 않는다.

## Durable ownership

| 사실 | 유일한 owner |
|---|---|
| Pool identity와 inventory generation | `ShiftPVPool` |
| current copy, owner node, requested bytes | `ShiftPVVolume` |
| 이동 phase, commit input, temporary holds, cleanup receipts | `ShiftPVMove` |
| 삭제 cleanup receipts | deleting `ShiftPVVolume` |
| 물리 effect 실행 | parent가 소유한 node-bound Job; durable truth 없음 |

관련 동작은 [Volume mobility](../spec/volume-mobility.md)와
[Cleanup and GC](../spec/source-cleanup.md), 검증 기준은 [Testing](testing.md)이 소유한다.
