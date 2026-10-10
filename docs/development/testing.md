# Testing

테스트의 목적은 “한 번 성공했다”가 아니라 0.4 계약이 경쟁, 응답 유실, process/node 재시작 뒤에도
안전하게 수렴한다는 증거를 만드는 것이다. 모든 명령은 repository root에서 실행한다.

## Evidence ladder

| Gate | 환경 | 증명하는 범위 | 운영 승인 조건 |
|---|---|---|---|
| Design model | pure Go state graph | invariant와 stable recovery path의 내부 일관성 | 현재 사용 가능 |
| Primitive probe | ephemeral Kind/container | Kubernetes와 rsync 전제의 사용 가능성 | 현재 사용 가능 |
| Unit/race | fake API + fault injection | 실제 CRD field, CAS, journal, 멱등 action | 0.4 구현과 함께 필수 |
| Linux integration | real mount namespace/filesystem | local lock, bind mount, rename/fsync/purge | 0.4 구현과 함께 필수 |
| Isolated Kind | API server, scheduler, kubelet, CSI | end-to-end lifecycle와 node/process failure | 0.4 구현과 함께 필수 |
| Real node fault | 대상 filesystem과 process/unclean OS reboot | reboot 뒤 receipt와 directory durability | 운영 전 필수 |
| Soak | 운영 동형 다중 node | 반복 이동·삭제, leak, capacity drift, alert | 운영 전 필수 |

낮은 gate의 성공은 높은 gate를 대체하지 않는다. 특히 model, Kind, Docker overlay 결과를 실제 disk의
reboot durability로 해석하지 않는다. 물리 전원 차단은 별도 환경 qualification이며 기본 release gate가 아니다.

## Design preflight

```bash
make v04-model
make v04-kubernetes-primitives
make v04-filesystem-primitives
```

`v04-model`은 source publish race, partial copy, owner commit 전후 실패, unlink 직후 receipt 기록 전
재시작, stale/invalid/incomplete observation, identity contradiction, capacity hold를 상태 그래프로 탐색한다.
두 node가 다시 사용 가능하고 증거가 모순되지 않으면 terminal state로 수렴해야 한다. 모순은 자동
삭제 대신 review 상태로 격리한다.

Kubernetes probe는 다음 전제를 실제 API server에서 확인한다.

- status write는 `metadata.generation`을 올리지 않는다.
- `spec.scanEpoch` 변경은 generation을 올린다.
- deletionTimestamp 뒤에도 finalizer가 parent와 journal을 보존한다.
- terminating parent의 status journal을 갱신할 수 있다.
- owner-referenced suspended Job은 parent finalizer가 있는 동안 유지되고 parent 삭제 뒤 정리된다.

Filesystem probe는 target helper image의 rsync daemon에서
`-aHAXS --numeric-ids --one-file-system --no-devices --delete --fsync`와 checksum dry-run이 hardlink,
symlink, FIFO, UID/GID, mode, sparse file, ACL, xattr를 보존하는지 확인한다.
Device node 재생성과 nested filesystem traversal은 제외 범위다. 검사 도구 설치에 network를 사용하며 성능과
power-loss를 증명하지 않는다.

## Fast repository gate

```bash
make verify
go test -race -count=1 ./...
git diff --check
```

`make verify`는 model과 repository-local gate를 함께 실행한다. Docker/Kind probe는 로컬 환경에
의존하므로 별도 gate로 유지하고, copy option·base image·Kubernetes version·CRD protocol 전제가
바뀐 때 다시 실행한다.

## Required protocol tests

| Contract | 최소 fault boundary |
|---|---|
| Node creation | copy intent·stage mkdir·placement marker·rename·API receipt 경계의 중단 상태를 실제 Pool reconcile·승인 검사에 연결; inventory 수동 보정 없이 동일 작업으로 복구 |
| Owner commit | CAS 수락 전/후, 응답 유실, stale currentCopy/owner 관찰, 동시 delete/move |
| Publication | NodePublish lock 획득 전/후, publishedNodes와 actual mount 관찰 차이, kubelet target 잔존, 재시작 |
| Copy | partial write, ENOSPC, read-only, checksum mismatch, Job 교체, copy receipt 응답 유실 |
| Promotion | rename 전/후, file/tree fsync 실패, marker mismatch, process kill |
| Source cleanup | intent 전/후, unlink 전/후, local/API receipt 전/후, absence scan 전/후 |
| Volume delete | mounted target, node down, already-absent-with-intent, unexpected absence, finalizer race |
| Capacity | destination reserve, commit 직후 두 copy, 각 receipt 단독, fresh absence, abort cleanup |
| Measurement | 측정 전후 Pool/mount/copy 변경, API·du 실패, read-only bind, mount 유실 |
| Inventory | generation stale, invalid signature/identity, incomplete/truncated scan, copy reappearance, 물리 수집 전후 생성 완료, stage 이동·경로 타입 변경·parent symlink, 경로 변경 중 부재 승인 거부 |
| Removal | unresolved Volume/Move/hold, unavailable node, API read error, generation-fenced empty inventory |

각 fault case는 반복 reconcile 뒤 다음을 함께 확인한다.

1. current owner identity와 authoritative bytes
2. source/destination 물리 경로와 mount 상태
3. Volume/Move phase, journal, finalizer와 child Job UID
4. Pool observed generation, validity, completeness와 inventory
5. source/destination capacity hold 합계
6. restart count, warning event, bounded-cardinality metric
7. terminal state 이후 남은 resource와 test fixture cleanup

## Four review criteria

테스트 파일은 컴포넌트별로 유지한다. 각 변경은 다음 네 기준에서 검토하며, DI는 관찰·시간·요청
예산의 실패를 주입한다. 실제 판정·Registry·lock을 연결하는 회귀 테스트도 함께 유지한다.

| 대상 | 안정성 | 진행성 | 동시성 | 기록 수명 |
|---|---|---|---|---|
| 삭제 | identity/Move 모순이면 거부 | 정상 publication은 대기 후 재개 | CAS 충돌 뒤 새 상태로 재판정 | receipt와 fresh absence 전에는 finalizer/hold 유지 |
| Pool 배치 | capacity 초과 승인 금지 | 독립 Pool의 요청은 진행 | 같은 Pool UID만 직렬화 | durable intent retry는 같은 copy/hold 유지 |
| Metadata GC | API 참조·불명확한 물리 증거면 보존 | fresh evidence 복구 후 수집 재개 | API 대기 중 Pool marker lock과 foreground 예산 사용 금지 | 완료·보존 기간·무참조·부재가 모두 충족된 reclaim 기록만 제거 |

| 회귀 테스트 | 실제 연결 경계 |
|---|---|
| `volume/deletion`: `TestDeletionAdmissionSafety`, `TestDeletionAdmissionProgress` | 외부 I/O 없는 판정과 상태 전이 |
| `kubernetes/volumeapi`: `TestBeginDeleteRechecksAdmissionAfterCASConflict` | 실제 Registry CAS + 충돌/동시 변경 주입 |
| `csi/controller`: `TestDeleteWaitsForPublicationWithoutStartingCleanup` | CSI 응답 + 실제 Registry + cleanup effect 호출 여부 |
| `csi/controller`: `TestConcurrentAdmissionKeepsIndependentPoolHoldsOnSameNode`, `TestConcurrentAdmissionToSamePoolCannotOversubscribe` | 실제 Pool Locker + capacity probe 지연 + durable hold |
| `cmd/node`: `TestNodeBackgroundBudgetDoesNotBlockForegroundRequests` | 실제 Collector/Registry/REST clients + 차단된 GC limiter + HTTP 서버 |
| `node/metadata`: `TestPoolCollectionEvidenceDecisions`, `TestCollectorProgressAfterFreshEvidence`, `TestCollectorRequiresLiveUnreferencedReadyPool` | 순수 eligibility + 새 관찰/참조/오류 주입 + effect 진입 여부 |
| `node/ownership`: `TestMetadataGCRequiresClosedExpiredExactRecord`, `TestMetadataGCRechecksPhysicalAbsenceAfterAuthority`, `TestMetadataGCDoesNotHoldPoolMarkerLockDuringAPIRead`, `TestMetadataGCAfterPoolReregistration`, `TestPoolReleasePreservesLockedNamespaceAndThenRemovesLocks` | 실제 파일·receipt·flock + 보존 시간/재등장/등록 교체/중단 상태 |

GC HTTP 테스트는 공유 limiter로 인한 정체를 검증한다. API 서버 장애와 실제 publish/클러스터
지연은 별도 gate다. 중단 상태 fixture는 process kill이나 전원 차단의 내구성을 증명하지 않는다.

## Isolated Kind

Target suite는 서로 다른 host directory를 mount한 최소 두 worker를 사용한다. Suite마다 고유한
`CLUSTER_NAME`과 kubeconfig를 사용하고 성공·실패 모두 자신이 만든 cluster와 directory만 정리한다.

```text
control-plane
├── worker-a ── Pool A
└── worker-b ── Pool B
```

필수 시나리오는 provision/publish, cold move 양방향, 각 phase의 Controller 재시작, source/destination
node stop/start, Volume 삭제, Pool 삭제, uninstall 차단과 재시도다. 테스트가 status를 직접 성공으로
patch하거나 host-side copy로 제품 effect를 대신하면 안 된다.

전체 suite와 독립적으로 재현 가능한 lifecycle gate는 다음과 같이 실행한다.

```bash
NODE_EFFECTS_ONLY=1 CLUSTER_NAME=shiftpv-node-focused ./test/e2e/kind/run.sh
VOLUME_DELETE_CLEANUP_ONLY=1 CLUSTER_NAME=shiftpv-delete-focused ./test/e2e/kind/run.sh
CLEANUP_JOB_RETRY_ONLY=1 CLUSTER_NAME=shiftpv-cleanup-retry-focused ./test/e2e/kind/run.sh
MOBILITY_NODE_RESTARTS_ONLY=1 CLUSTER_NAME=shiftpv-mobility-restart-focused ./test/e2e/kind/run.sh
```

Node gate는 상주 Node 생성 receipt, Pod 교체 뒤 데이터 보존, 새 Pod UID의 purge receipt와 fenced cleanup 완료를 확인한다.
Volume delete gate는 cleanup 완료 전 Volume finalizer와 capacity hold가 유지되고, generation-fenced absence 뒤에만
삭제와 용량 재사용이 일어나는지 검증한다. Job retry gate는 cleanup Job의 첫 Pod가 receipt 기록 전에 사라져도
같은 Job identity 아래 새 Pod로 재결합해 수렴하는지 검증한다. Mobility restart gate는 owner commit 전후 및
`CleaningSource`에서 source/destination node가 중단됐다가 돌아온 뒤 현재 transaction이 계속 수렴하는지
검증한다.

## Multi-Pool qualification

실제 ext4/xfs의 thick LVM 또는 독립 disk에서 다음을 확인한다. 합성 backing, tmpfs와
단일 Pool Kind 결과는 이 검증을 대신하지 않는다.

1. 같은 node의 독립 Pool 두 개가 Ready가 되고 그룹별 PVC가 각 경로에 생성된다.
2. 같은 그룹에서 한 Pool의 여유가 부족하면 다른 Pool을 선택한다.
3. 공유 filesystem·겹치는 backing·thin 구성은 쓰기 probe와 inventory 전에 거부한다.
4. mount 유실·교체와 Pool 재생성 시 새 배치·게시가 거부된다.
5. 삭제·이동 hold가 해당 Pool UID에 남고 기존 단일 directory Pool도 동작한다.

## Real-node qualification

운영과 같은 filesystem 및 mount option에서 다음을 수행한다.

- copy와 promotion 각 crash point에서 helper `SIGKILL` 및 node reboot
- source/destination 중 한 node를 장시간 차단한 뒤 같은 transaction 재개
- destination publish 직후 reboot 뒤 source 보존 여부 확인
- cleanup unlink 직후 reboot 뒤 receipt/absence 재구성 확인
- disk full, inode full, read-only remount, 느린 I/O와 API outage 조합

합격은 checksum만으로 판정하지 않는다. Metadata, identity marker, fsync/rename 결과, Kubernetes journal,
capacity accounting이 같은 결론을 가리켜야 한다.

## Soak exit criteria

운영 동형 환경에서 정한 횟수의 provision/publish/move/delete loop를 실행하고 다음이 모두 0으로
돌아와야 한다.

- non-terminal Move와 deleting Volume
- parent 없는 executor Job
- 설명되지 않는 physical copy와 capacity hold drift
- dual mount 또는 non-owner publish
- restart 증가와 persistent `Blocked`/cleanup `NeedsReview`

Soak 시간과 횟수는 cluster 규모와 disk 속도에 맞춰 release checklist에서 고정한다. 실패한 run은
resource snapshot, logs, metrics, directory inventory를 보존하고 자동 재실행으로 덮지 않는다.

## CDI provisioning latency

Inject claim/Pod reads and placement inspection failures. Cover CDI import/upload
prime before Pod creation, reverse ownership, ordinary scheduler consumers,
fixed oversized requests, UID/node changes, and capacity-return convergence.
The focused Pool capacity Kind test models scratch and prime with the real
external-provisioner; full CDI DataVolume import remains a separate qualification.

CSI request regression tests reject unsupported sources, mutable parameters,
negative bounds and invalid topology before admission. Compatible retries cover
Pending/NodeCreating/Ready and retain actual capacity and copy identity; a real
Registry test resumes a saved intent through fresh controller instances.

Create observation tests separate new admission from existing-intent retries,
inject Volume/Move list failures, and block a real Pool UID lock and capacity
probe independently. Lock-wait timing must finish before protected work; failed
API stages must report duration without proceeding to intent or filesystem work.

For the two-VM/250Gi Pool scenario, compare reservation return → next
`CreateVolume` start, rejection helper count, and successful creation duration.
Use Controller level-2 step logs and, when enabled,
`shiftpv_provisioning_step_duration_seconds`. Nested durations must not be summed.
Use the [CreateVolume step names](../spec/metrics.md#emitted-signals) to separate
lock wait, API lists, intent recording, node effects and topology response.
Compare repeated calls for the same volume ID; `create_resume` marks an existing
intent retry, while `create_intent_record` marks a new admission attempt.
The [direct CSI profile](../../test/measurement/provisioning/README.md) compares
new calls, completed-intent retries and concurrent callers in an isolated cluster.
Its DI regression checks CSI client budget isolation and independent Pool holds.
Confirm `shiftpv.io/capacity-retry` preserves selected-node, and repeat with a
Controller restart and a different Pool group. Check that Deleting Volumes and
unsettled Move holds remain charged. Fake API timing is not VM latency evidence.

For Node gRPC, record `live_capacity_probe` and `ShiftPV Node RPC` logs for
CAPACITY/CREATE/RECLAIM. Verify Pod replacement reconnects, concurrent RPC and
Watch recovery write one immutable receipt, and lost replies retain holds.
Node effect observation tests block authority API reads independently of the
shared gate, reject failed authority before filesystem work, and preserve the
creation receipt on retry. An injected clock verifies local durations exclude
nested authority checks, including failures and subsequent stages.
`node/executor` retains four fresh creation-authority checks, with one Pool GET
per check. Inject Pool/Pod identity, protection, path and backing changes at
each boundary; no failed check may record a creation receipt.
Mobility tests keep rejected Pool identities visible, isolate unapproved
candidates, retain approved peer conflicts, and continue persisted Moves after
discovery errors. API/decode failures must never grant placement.
Inject invalid inventory after the serving directory exists: the approved
creation must record the same operation's receipt while new placement stays
blocked. Reconcile interrupted serving stages without manually restoring inventory.
Reject wrong caller/audience, executor UID, operation ID and Pool evidence.
Repeat capacity reads with mount loss; Pool generation/scanEpoch must not change.
With RPC disabled or older Nodes, verify the API probe nonce and answer still
match and readiness updates preserve concurrently recorded answers.
Delete completion within `--cleanup-absence-wait` must release the hold in one
call; timeout/cancellation and stale inventory must keep it charged.
