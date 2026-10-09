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
│   ├── executor/           resident execution; RPC and Watch share one Volume gate
│   ├── metadata/           settled-record retention and live API reference checks
│   └── ownership/          exact local identity, stable locks, effects and receipts
├── provisioning/           capacity retry and Controller-side execution requests
│   └── nodeexecutor/       executor selection, binding, transport and receipt waiting
├── pool/                   readiness and conservative capacity accounting
├── metrics/                cached observation only
├── volume/                 pure IDs, copy identity and path rules
└── webhook/                serving certificate lifecycle

test/
├── model/                  exhaustive state/invariant checks
├── integration/            Linux mount boundary
├── measurement/            isolated direct CSI latency profiles
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
| Controller execution client | 실행자 선택·바인딩·요청·receipt 대기; Node filesystem 실행 구현에 의존하지 않음 |
| Resident Node executor | 승인 재검증·물리 실행·receipt 기록; RPC와 Watch는 같은 gate 사용 |
| Node metadata collector | 완료 기록의 보존 기간과 live API 참조 확인; 파일 삭제는 `node/ownership` 사용, 데이터·owner·hold 변경 금지 |
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
7. Wall clock은 retry, timeout, metric과 완료된 metadata의 최소 보존 기간에 사용한다. 시간 경과는 데이터 삭제·owner 변경·hold 해제 권한이 아니다.
8. `provisioning/nodeexecutor`와 `node/executor`는 production에서 서로 import하지 않는다. 통합 테스트는 두 역할의 실제 구현을 연결한다.

## Durable ownership

| 사실 | 유일한 owner |
|---|---|
| Pool identity와 inventory generation | `ShiftPVPool` |
| current copy, owner node, requested bytes | `ShiftPVVolume` |
| 이동 phase, commit input, temporary holds, cleanup receipts | `ShiftPVMove` |
| 삭제 cleanup receipts | deleting `ShiftPVVolume` |
| 물리 effect 실행 | parent가 승인한 Node Pod 또는 helper Job; 완료 증거는 parent 소유 |

관련 동작은 [Volume mobility](../spec/volume-mobility.md)와
[Cleanup and GC](../spec/source-cleanup.md), 검증 기준은 [Testing](testing.md)이 소유한다.
